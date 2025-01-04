# This is the common file for our CLI. Please keep it clean (as possible)
#
# - Put here any common utility function you consider.
# - If any function is only called within a specific command, consider moving
#   the function to the proper command file.
# - Please, **do not** define commands here.

import asyncio
import json
import os
import re
import socket
import sys
import uuid
from contextlib import closing
from copy import deepcopy
from enum import Enum
from functools import wraps
from os import chmod, environ, getcwd, getenv
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse

import aiofiles
import click
import click.formatting
import humanfriendly
import humanfriendly.tables
import pyperclip
from click import Context
from click._termui_impl import ProgressBar
from humanfriendly.tables import format_pretty_table
from packaging.version import Version

from tinybird.client import (
    AuthException,
    AuthNoTokenException,
    ConnectorNothingToLoad,
    DoesNotExistException,
    JobException,
    OperationCanNotBePerformed,
    TinyB,
)
from tinybird.config import (
    DEFAULT_API_HOST,
    DEFAULT_UI_HOST,
    DEPRECATED_PROJECT_PATHS,
    PROJECT_PATHS,
    SUPPORTED_CONNECTORS,
    VERSION,
    FeatureFlags,
    get_config,
    get_display_host,
    write_config,
)

if TYPE_CHECKING:
    from tinybird.connectors import Connector

from tinybird.feedback_manager import FeedbackManager, warning_message
from tinybird.git_settings import DEFAULT_TINYENV_FILE
from tinybird.syncasync import async_to_sync
from tinybird.tb_cli_modules.cicd import APPEND_FIXTURES_SH, DEFAULT_REQUIREMENTS_FILE, EXEC_TEST_SH
from tinybird.tb_cli_modules.config import CLIConfig
from tinybird.tb_cli_modules.exceptions import (
    CLIAuthException,
    CLIConnectionException,
    CLIException,
    CLIReleaseException,
    CLIWorkspaceException,
)
from tinybird.tb_cli_modules.regions import Region
from tinybird.tb_cli_modules.telemetry import (
    add_telemetry_event,
    add_telemetry_sysinfo_event,
    flush_telemetry,
    init_telemetry,
    is_ci_environment,
)

SUPPORTED_FORMATS = ["csv", "ndjson", "json", "parquet"]
OLDEST_ROLLBACK = "oldest_rollback"
MAIN_BRANCH = "main"


def obfuscate_token(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return f"{value[:4]}...{value[-8:]}"


def create_connector(connector: str, options: Dict[str, Any]):
    # Imported here to improve startup time when the connectors aren't used
    from tinybird.connectors import UNINSTALLED_CONNECTORS
    from tinybird.connectors import create_connector as _create_connector

    if connector in UNINSTALLED_CONNECTORS:
        raise CLIException(FeedbackManager.error_connector_not_installed(connector=connector))
    return _create_connector(connector, options)


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


async def gather_with_concurrency(n, *tasks):
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))


def echo_safe_humanfriendly_tables_format_smart_table(data: Iterable[Any], column_names: List[str]) -> None:
    """
    There is a bug in the humanfriendly library: it breaks to render the small table for small terminals
    (`format_robust_table`) if we call format_smart_table with an empty dataset. This catches the error and prints
    what we would call an empty "robust_table".
    """
    try:
        click.echo(humanfriendly.tables.format_smart_table(data, column_names=column_names))
    except ValueError as exc:
        if str(exc) == "max() arg is an empty sequence":
            click.echo("------------")
            click.echo("Empty")
            click.echo("------------")
        else:
            raise exc


def normalize_datasource_name(s: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z_]", "_", s)
    if s[0] in "0123456789":
        return "c_" + s
    return s


def generate_datafile(
    datafile: str, filename: str, data: Optional[bytes], force: Optional[bool] = False, _format: Optional[str] = "csv"
):
    p = Path(filename)
    base = Path("datasources")
    datasource_name = normalize_datasource_name(p.stem)
    if not base.exists():
        base = Path()
    f = base / (datasource_name + ".datasource")
    if not f.exists() or force:
        with open(f"{f}", "w") as ds_file:
            ds_file.write(datafile)
        click.echo(FeedbackManager.success_generated_file(file=f, stem=datasource_name, filename=filename))

        if data and (base / "fixtures").exists():
            # Generating a fixture for Parquet files is not so trivial, since Parquet format
            # is column-based. We would need to add PyArrow as a dependency (which is huge)
            # just to analyze the whole Parquet file to extract one single row.
            if _format == "parquet":
                click.echo(FeedbackManager.warning_parquet_fixtures_not_supported())
            else:
                f = base / "fixtures" / (p.stem + f".{_format}")
                newline = b"\n"  # TODO: guess
                with open(f, "wb") as fixture_file:
                    fixture_file.write(data[: data.rfind(newline)])
                click.echo(FeedbackManager.success_generated_fixture(fixture=f))
    else:
        click.echo(FeedbackManager.error_file_already_exists(file=f))


async def get_current_workspace(config: CLIConfig) -> Optional[Dict[str, Any]]:
    client = config.get_client()
    workspaces: List[Dict[str, Any]] = (await client.user_workspaces_and_branches()).get("workspaces", [])
    return _get_current_workspace_common(workspaces, config["id"])


def get_workspace_member_email(workspace, member_id) -> str:
    return next((member["email"] for member in workspace["members"] if member["id"] == member_id), "-")


def _get_current_workspace_common(
    workspaces: List[Dict[str, Any]], current_workspace_id: str
) -> Optional[Dict[str, Any]]:
    return next((workspace for workspace in workspaces if workspace["id"] == current_workspace_id), None)


async def get_current_environment(client, config):
    workspaces: List[Dict[str, Any]] = (await client.user_workspaces_and_branches()).get("workspaces", [])
    return next((workspace for workspace in workspaces if workspace["id"] == config["id"]), None)


async def get_current_workspace_branches(config: CLIConfig) -> List[Dict[str, Any]]:
    current_main_workspace: Optional[Dict[str, Any]] = await get_current_main_workspace(config)
    if not current_main_workspace:
        raise CLIException(FeedbackManager.error_unable_to_identify_main_workspace())

    client = config.get_client()
    user_branches: List[Dict[str, Any]] = (await client.user_workspace_branches()).get("workspaces", [])
    all_branches: List[Dict[str, Any]] = (await client.branches()).get("environments", [])
    branches = user_branches + [branch for branch in all_branches if branch not in user_branches]

    return [branch for branch in branches if branch.get("main") == current_main_workspace["id"]]


class AliasedGroup(click.Group):
    def get_command(self, ctx, cmd_name):
        # Step one: built-in commands as normal
        cm = click.Group.get_command(self, ctx, cmd_name)
        if cm is not None:
            return cm

    def resolve_command(self, ctx, args):
        # always return the command's name, not the alias
        _, cmd, args = super().resolve_command(ctx, args)
        return cmd.name, cmd, args  # type: ignore[union-attr]


class CatchAuthExceptions(AliasedGroup):
    """utility class to get all the auth exceptions"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        init_telemetry()
        add_telemetry_sysinfo_event()
        super().__init__(*args, **kwargs)

    def format_epilog(self, ctx: Context, formatter: click.formatting.HelpFormatter) -> None:
        super().format_epilog(ctx, formatter)

        formatter.write_paragraph()
        formatter.write_heading("Telemetry")
        formatter.write_text(
            """
  Tinybird collects anonymous usage data and errors to improve the command
line experience. To opt-out, set TB_CLI_TELEMETRY_OPTOUT environment
variable to '1' or 'true'."""
        )
        formatter.write_paragraph()

    def __call__(self, *args, **kwargs) -> None:
        error_msg: Optional[str] = None
        error_event: str = "error"

        exit_code: int = 0

        try:
            self.main(*args, **kwargs)
        except AuthNoTokenException:
            error_msg = FeedbackManager.error_notoken()
            error_event = "auth_error"
            exit_code = 1
        except AuthException as ex:
            error_msg = FeedbackManager.error_exception(error=str(ex))
            error_event = "auth_error"
            exit_code = 1
        except SystemExit as ex:
            exit_code = int(ex.code) if ex.code else 0
        except Exception as ex:
            error_msg = str(ex)
            exit_code = 1

        if error_msg:
            click.echo(error_msg)
            add_telemetry_event(error_event, error=error_msg)
        flush_telemetry(wait=True)

        sys.exit(exit_code)


def load_connector_config(ctx: Context, connector_name: str, debug: bool, check_uninstalled: bool = False):
    config_file = Path(getcwd()) / f".tinyb_{connector_name}"
    try:
        if connector_name not in ctx.ensure_object(dict):
            with open(config_file) as file:
                config = json.loads(file.read())
            from tinybird.connectors import UNINSTALLED_CONNECTORS

            if check_uninstalled and connector_name in UNINSTALLED_CONNECTORS:
                click.echo(FeedbackManager.warning_connector_not_installed(connector=connector_name))
                return
            ctx.ensure_object(dict)[connector_name] = create_connector(connector_name, config)
    except IOError:
        if debug:
            click.echo(f"** {connector_name} connector not configured")
        pass


def getenv_bool(key: str, default: bool) -> bool:
    v: Optional[str] = getenv(key)
    if v is None:
        return default
    return v.lower() == "true" or v == "1"


def _get_tb_client(token: str, host: str, semver: Optional[str] = None) -> TinyB:
    disable_ssl: bool = getenv_bool("TB_DISABLE_SSL_CHECKS", False)
    return TinyB(token, host, version=VERSION, disable_ssl_checks=disable_ssl, send_telemetry=True, semver=semver)


def create_tb_client(ctx: Context) -> TinyB:
    token = ctx.ensure_object(dict)["config"].get("token", "")
    host = ctx.ensure_object(dict)["config"].get("host", DEFAULT_API_HOST)
    semver = ctx.ensure_object(dict)["config"].get("semver", "")
    return _get_tb_client(token, host, semver=semver)


async def _analyze(filename: str, client: TinyB, format: str, connector: Optional["Connector"] = None):
    data: Optional[bytes] = None
    if not connector:
        parsed = urlparse(filename)
        if parsed.scheme in ("http", "https"):
            meta = await client.datasource_analyze(filename)
        else:
            async with aiofiles.open(filename, "rb") as file:
                # We need to read the whole file in binary for Parquet, while for the
                # others we just read 1KiB
                if format == "parquet":
                    data = await file.read()
                else:
                    data = await file.read(1024 * 1024)

            meta = await client.datasource_analyze_file(data)
    else:
        meta = connector.datasource_analyze(filename)
    return meta, data


async def _generate_datafile(
    filename: str, client: TinyB, format: str, connector: Optional["Connector"] = None, force: Optional[bool] = False
):
    meta, data = await _analyze(filename, client, format, connector=connector)
    schema = meta["analysis"]["schema"]
    schema = schema.replace(", ", ",\n    ")
    datafile = f"""DESCRIPTION >\n    Generated from {filename}\n\nSCHEMA >\n    {schema}"""
    return generate_datafile(datafile, filename, data, force, _format=format)


async def folder_init(
    client: TinyB,
    folder: str,
    generate_datasources: Optional[bool] = False,
    force: Optional[bool] = False,
    generate_releases: Optional[bool] = False,
):
    for x in filter(lambda x: x not in DEPRECATED_PROJECT_PATHS, PROJECT_PATHS):
        try:
            f = Path(folder) / x
            f.mkdir()
            click.echo(FeedbackManager.info_path_created(path=x))
        except FileExistsError:
            if not force:
                click.echo(FeedbackManager.info_path_already_exists(path=x))
            pass

    if generate_datasources:
        for format in SUPPORTED_FORMATS:
            for path in Path(folder).glob(f"*.{format}"):
                await _generate_datafile(str(path), client, format=format, force=force)

    if generate_releases:
        base = Path(".")
        f = base / (".tinyenv")
        if not f.exists() or force:
            async with aiofiles.open(".tinyenv", "w") as file:
                await file.write(DEFAULT_TINYENV_FILE)
                click.echo(FeedbackManager.info_file_created(file=".tinyenv"))
        else:
            click.echo(FeedbackManager.info_dottinyenv_already_exists())

        base = Path(".")
        f = base / ("requirements.txt")
        if not f.exists() or force:
            async with aiofiles.open("requirements.txt", "w") as file:
                await file.write(DEFAULT_REQUIREMENTS_FILE)
                click.echo(FeedbackManager.info_file_created(file="requirements.txt"))

        base = Path("scripts")
        if not base.exists():
            base = Path()
        f = base / ("exec_test.sh")
        if not f.exists() or force:
            async with aiofiles.open(f"{f}", "w") as t_file:
                await t_file.write(EXEC_TEST_SH)
            click.echo(FeedbackManager.info_file_created(file="scripts/exec_test.sh"))
        chmod(f, 0o755)

        f = base / ("append_fixtures.sh")
        if not f.exists() or force:
            async with aiofiles.open(f"{f}", "w") as t_file:
                await t_file.write(APPEND_FIXTURES_SH)
            click.echo(FeedbackManager.info_file_created(file="scripts/append_fixtures.sh"))
        chmod(f, 0o755)

        base = Path("tests")
        if not base.exists():
            base = Path()
        f = base / ("example.yml")
        if not base.exists() or force:
            async with aiofiles.open(f"{f}", "w") as t_file:
                await t_file.write(
                    """
##############################################################################################################################
###   Visit https://www.tinybird.co/docs/production/implementing-test-strategies.html#data-quality-tests              ###
###   for more details on Data Quality tests                                                                               ###
##############################################################################################################################

- example_no_negative_numbers:
    max_bytes_read: null
    max_time: null
    sql: |
        SELECT
            number
        FROM numbers(10)
        WHERE
            number < 0

#    - example_top_products_params_no_empty_top_10_on_2023:
#       max_bytes_read: null
#       max_time: null
#       sql: |
#          SELECT *
#          FROM top_products_params
#          WHERE empty(top_10)
#       pipe:
#          name: top_products_params
#          params:
#             start: '2023-01-01'
#             end: '2023-12-31'

"""
                )

        f = base / ("regression.yaml")
        if not base.exists() or force:
            async with aiofiles.open(f"{f}", "w") as t_file:
                await t_file.write(
                    """
############################################################################################################################
###   Visit https://www.tinybird.co/docs/production/implementing-test-strategies.html#regression-tests              ###
###   for more details on Regression tests                                                                               ###
############################################################################################################################

###
### New pipes are covered by this rule, rules below this one supersede this setting
###
- pipe: '.*'
  tests:
    - coverage:



###
### These are rules to customize regression testing by pipe using regular expressions
### For instance skip regression tests for the pipes matching `endpoint_name.*`
###
- pipe: 'endpoint_name.*'
  tests:
    - coverage:
        config:
          skip: True

"""
                )


async def configure_connector(connector):
    if connector not in SUPPORTED_CONNECTORS:
        raise CLIException(FeedbackManager.error_invalid_connector(connectors=", ".join(SUPPORTED_CONNECTORS)))

    file_name = f".tinyb_{connector}"
    config_file = Path(getcwd()) / file_name
    if connector == "bigquery":
        project = click.prompt("BigQuery project ID")
        service_account = click.prompt(
            "Path to a JSON service account file with permissions to export from BigQuery, write in Storage and sign URLs (leave empty to use GOOGLE_APPLICATION_CREDENTIALS environment variable)",
            default=environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        )
        bucket_name = click.prompt("Name of a Google Cloud Storage bucket to store temporary exported files")

        try:
            config = {"project_id": project, "service_account": service_account, "bucket_name": bucket_name}
            await write_config(config, file_name)
        except Exception:
            raise CLIException(FeedbackManager.error_file_config(config_file=config_file))
    elif connector == "snowflake":
        sf_account = click.prompt("Snowflake Account (e.g. your-domain.west-europe.azure)")
        sf_warehouse = click.prompt("Snowflake warehouse name")
        sf_database = click.prompt("Snowflake database name")
        sf_schema = click.prompt("Snowflake schema name")
        sf_role = click.prompt("Snowflake role name")
        sf_user = click.prompt("Snowflake user name")
        sf_password = click.prompt("Snowflake password")
        sf_storage_integration = click.prompt(
            "Snowflake GCS storage integration name (leave empty to auto-generate one)", default=""
        )
        sf_stage = click.prompt("Snowflake GCS stage name (leave empty to auto-generate one)", default="")
        project = click.prompt("Google Cloud project ID to store temporary files")
        service_account = click.prompt(
            "Path to a JSON service account file with permissions to write in Storagem, sign URLs and IAM (leave empty to use GOOGLE_APPLICATION_CREDENTIALS environment variable)",
            default=environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        )
        bucket_name = click.prompt("Name of a Google Cloud Storage bucket to store temporary exported files")

        if not service_account:
            service_account = getenv("GOOGLE_APPLICATION_CREDENTIALS")

        try:
            config = {
                "account": sf_account,
                "warehouse": sf_warehouse,
                "database": sf_database,
                "schema": sf_schema,
                "role": sf_role,
                "user": sf_user,
                "password": sf_password,
                "storage_integration": sf_storage_integration,
                "stage": sf_stage,
                "service_account": service_account,
                "bucket_name": bucket_name,
                "project_id": project,
            }
            await write_config(config, file_name)
        except Exception:
            raise CLIException(FeedbackManager.error_file_config(config_file=config_file))

        click.echo(FeedbackManager.success_connector_config(connector=connector, file_name=file_name))


def _compare_region_host(region_name_or_host: str, region: Dict[str, Any]) -> bool:
    if region["name"].lower() == region_name_or_host:
        return True
    if region["host"] == region_name_or_host:
        return True
    if region["api_host"] == region_name_or_host:
        return True
    return False


def ask_for_region_interactively(regions):
    region_index = -1

    while region_index == -1:
        click.echo(FeedbackManager.info_available_regions())
        for index, region in enumerate(regions):
            provider = f" ({region.get('provider')})" if region.get("provider") else ""
            click.echo(f"   [{index + 1}] {region['name'].lower()}{provider} ({region['host']}) ")
        click.echo("   [0] Cancel")

        region_index = click.prompt("\nUse region", default=1)

        if region_index == 0:
            click.echo(FeedbackManager.info_auth_cancelled_by_user())
            return None

        try:
            return regions[int(region_index) - 1]
        except Exception:
            available_options = ", ".join(map(str, range(1, len(regions) + 1)))
            click.echo(FeedbackManager.error_region_index(host_index=region_index, available_options=available_options))
            region_index = -1


def get_region_info(ctx, region=None):
    name = region["name"] if region else "default"
    api_host = format_host(
        region["api_host"] if region else ctx.obj["config"].get("host", DEFAULT_API_HOST), subdomain="api"
    )
    ui_host = format_host(region["host"] if region else ctx.obj["config"].get("host", DEFAULT_UI_HOST), subdomain="ui")
    return name, api_host, ui_host


def format_host(host: str, subdomain: Optional[str] = None) -> str:
    """
    >>> format_host('api.tinybird.co')
    'https://api.tinybird.co'
    >>> format_host('https://api.tinybird.co')
    'https://api.tinybird.co'
    >>> format_host('http://localhost:8001')
    'http://localhost:8001'
    >>> format_host('localhost:8001')
    'http://localhost:8001'
    >>> format_host('localhost:8001', subdomain='ui')
    'http://localhost:8001'
    >>> format_host('localhost:8001', subdomain='api')
    'http://localhost:8001'
    >>> format_host('https://api.tinybird.co', subdomain='ui')
    'https://ui.tinybird.co'
    >>> format_host('https://api.us-east.tinybird.co', subdomain='ui')
    'https://ui.us-east.tinybird.co'
    >>> format_host('https://api.us-east.tinybird.co', subdomain='api')
    'https://api.us-east.tinybird.co'
    >>> format_host('https://ui.us-east.tinybird.co', subdomain='api')
    'https://api.us-east.tinybird.co'
    >>> format_host('https://inditex-rt-pro.tinybird.co', subdomain='ui')
    'https://inditex-rt-pro.tinybird.co'
    >>> format_host('https://cluiente-tricky.tinybird.co', subdomain='api')
    'https://cluiente-tricky.tinybird.co'
    """
    is_localhost = FeatureFlags.is_localhost()
    if subdomain and not is_localhost:
        url_info = urlparse(host)
        current_subdomain = url_info.netloc.split(".")[0]
        if current_subdomain == "api" or current_subdomain == "ui":
            host = host.replace(current_subdomain, subdomain)
    if "localhost" in host or is_localhost:
        host = f"http://{host}" if "http" not in host else host
    elif not host.startswith("http"):
        host = f"https://{host}"
    return host


def region_from_host(region_name_or_host, regions):
    """Returns the region that matches region_name_or_host"""

    return next((r for r in regions if _compare_region_host(region_name_or_host, r)), None)


def ask_for_user_token(action: str, ui_host: str) -> str:
    return click.prompt(
        f'\nUse the token called "user token" in order to {action}. Copy it from {ui_host}/tokens and paste it here',
        hide_input=True,
        show_default=False,
        default=None,
    )


async def check_user_token(ctx: Context, token: str):
    client: TinyB = ctx.ensure_object(dict)["client"]
    try:
        user_client: TinyB = deepcopy(client)
        user_client.token = token

        is_authenticated = await user_client.check_auth_login()
    except Exception as e:
        raise CLIWorkspaceException(FeedbackManager.error_exception(error=str(e)))

    if not is_authenticated.get("is_valid", False):
        raise CLIWorkspaceException(
            FeedbackManager.error_exception(
                error='Invalid token. Please, be sure you are using the "user token" instead of the "admin your@email" token.'
            )
        )
    if is_authenticated.get("is_valid") and not is_authenticated.get("is_user", False):
        raise CLIWorkspaceException(
            FeedbackManager.error_exception(
                error='Invalid user authentication. Please, be sure you are using the "user token" instead of the "admin your@email" token.'
            )
        )


async def get_available_starterkits(ctx: Context) -> List[Dict[str, Any]]:
    ctx_dict = ctx.ensure_object(dict)
    available_starterkits = ctx_dict.get("available_starterkits", None)
    if available_starterkits is not None:
        return available_starterkits

    try:
        client: TinyB = ctx_dict["client"]

        available_starterkits = await client.starterkits()
        ctx_dict["available_starterkits"] = available_starterkits
        return available_starterkits
    except Exception as ex:
        raise CLIException(FeedbackManager.error_exception(error=ex))


async def get_starterkit(ctx: Context, name: str) -> Optional[Dict[str, Any]]:
    available_starterkits = await get_available_starterkits(ctx)
    if not available_starterkits:
        return None
    return next((sk for sk in available_starterkits if sk.get("friendly_name", None) == name), None)


async def is_valid_starterkit(ctx: Context, name: str) -> bool:
    return name == "blank" or (await get_starterkit(ctx, name) is not None)


async def ask_for_starterkit_interactively(ctx: Context) -> Optional[str]:
    starterkit = [{"friendly_name": "blank", "description": "Empty workspace"}]
    starterkit.extend(await get_available_starterkits(ctx))
    rows = [(index + 1, sk["friendly_name"], sk["description"]) for index, sk in enumerate(starterkit)]

    echo_safe_humanfriendly_tables_format_smart_table(rows, column_names=["Idx", "Id", "Description"])
    click.echo("")
    click.echo("   [0] to cancel")

    sk_index = -1
    while sk_index == -1:
        sk_index = click.prompt("\nUse starter kit", default=1)
        if sk_index < 0 or sk_index > len(starterkit):
            click.echo(FeedbackManager.error_starterkit_index(starterkit_index=sk_index))
            sk_index = -1

    if sk_index == 0:
        click.echo(FeedbackManager.info_cancelled_by_user())
        return None

    return starterkit[sk_index - 1]["friendly_name"]


async def fork_workspace(client: TinyB, user_client: TinyB, created_workspace):
    config = CLIConfig.get_project_config()

    datasources = await client.datasources()
    for datasource in datasources:
        await user_client.datasource_share(datasource["id"], config["id"], created_workspace["id"])


async def create_workspace_non_interactive(
    ctx: Context, workspace_name: str, starterkit: str, user_token: str, fork: bool
):
    """Creates a workspace using the provided name and starterkit"""
    client: TinyB = ctx.ensure_object(dict)["client"]

    try:
        user_client: TinyB = deepcopy(client)
        user_client.token = user_token

        created_workspace = await user_client.create_workspace(workspace_name, starterkit)
        click.echo(FeedbackManager.success_workspace_created(workspace_name=workspace_name))

        if fork:
            await fork_workspace(client, user_client, created_workspace)

    except Exception as e:
        raise CLIWorkspaceException(FeedbackManager.error_exception(error=str(e)))


async def create_workspace_interactive(
    ctx: Context, workspace_name: Optional[str], starterkit: Optional[str], user_token: str, fork: bool
):
    if not starterkit and not is_ci_environment():
        click.echo("\n")
        starterkit = await ask_for_starterkit_interactively(ctx)
        if not starterkit:  # Cancelled by user
            return

        if starterkit == "blank":  # 'blank' == empty workspace
            starterkit = None

    if not workspace_name:
        """Creates a workspace guiding the user"""
        click.echo("\n")
        click.echo(FeedbackManager.info_workspace_create_greeting())
        default_name = f"new_workspace_{uuid.uuid4().hex[0:4]}"
        workspace_name = click.prompt("\nWorkspace name", default=default_name, err=True, type=str)

    await create_workspace_non_interactive(ctx, workspace_name, starterkit, user_token, fork)  # type: ignore


async def create_workspace_branch(
    branch_name: Optional[str],
    last_partition: bool,
    all: bool,
    ignore_datasources: Optional[List[str]],
    wait: Optional[bool],
) -> None:
    """
    Creates a workspace branch
    """
    config = CLIConfig.get_project_config()
    _ = await try_update_config_with_remote(config)

    try:
        workspace = await get_current_workspace(config)
        if not workspace:
            raise CLIWorkspaceException(FeedbackManager.error_workspace())

        if not branch_name:
            click.echo(FeedbackManager.info_workspace_branch_create_greeting())
            default_name = f"{workspace['name']}_{uuid.uuid4().hex[0:4]}"
            branch_name = click.prompt("\Branch name", default=default_name, err=True, type=str)
        assert isinstance(branch_name, str)

        response = await config.get_client().create_workspace_branch(
            branch_name,
            last_partition,
            all,
            ignore_datasources,
        )
        assert isinstance(response, dict)

        is_job: bool = "job" in response
        is_summary: bool = "partitions" in response

        if not is_job and not is_summary:
            raise CLIException(str(response))

        if all and not is_job:
            raise CLIException(str(response))

        click.echo(
            FeedbackManager.success_workspace_branch_created(workspace_name=workspace["name"], branch_name=branch_name)
        )

        job_id: Optional[str] = None

        if is_job:
            job_id = response["job"]["job_id"]
            job_url = response["job"]["job_url"]
            click.echo(FeedbackManager.info_data_branch_job_url(url=job_url))

        if wait and is_job:
            assert isinstance(job_id, str)

            # Await the job to finish and get the result dict
            job_response = await wait_job(config.get_client(), job_id, job_url, "Branch creation")
            if job_response is None:
                raise CLIException(f"Empty job API response (job_id: {job_id}, job_url: {job_url})")
            else:
                response = job_response.get("result", {})
                is_summary = "partitions" in response

        await switch_workspace(config, branch_name, only_environments=True)
        if is_summary and (bool(last_partition) or bool(all)):
            await print_data_branch_summary(config.get_client(), None, response)

    except Exception as e:
        raise CLIException(FeedbackManager.error_exception(error=str(e)))


async def print_data_branch_summary(client, job_id, response=None):
    response = await client.job(job_id) if job_id else response or {"partitions": []}
    columns = ["Data Source", "Partition", "Status", "Error"]
    table = []
    for partition in response["partitions"]:
        for p in partition["partitions"]:
            table.append([partition["datasource"]["name"], p["partition"], p["status"], p.get("error", "")])
    echo_safe_humanfriendly_tables_format_smart_table(table, column_names=columns)


async def print_branch_regression_tests_summary(client, job_id, host, response=None):
    def format_metric(metric: Union[str, float], is_percentage: bool = False) -> str:
        if isinstance(metric, float):
            if is_percentage:
                return f"{round(metric, 3):+} %"
            else:
                return f"{round(metric, 3)} seconds"
        else:
            return metric

    failed = False
    response = await client.job(job_id) if job_id else response or {"progress": []}
    output = "\n"
    for step in response["progress"]:
        run = step["run"]
        if run["output"]:
            # If the output contains an alert emoji, it means that it should be ou
            output += (
                warning_message(run["output"])()
                if isinstance(run["output"], str) and "🚨" in run["output"]
                else "".join(run["output"])
            )
        if not run["was_successfull"]:
            failed = True
    click.echo(output)

    if failed:
        click.echo("")
        click.echo("")
        click.echo("==== Failures Detail ====")
        click.echo("")
        for step in response["progress"]:
            if not step["run"]["was_successfull"]:
                for failure in step["run"]["failed"]:
                    try:
                        click.echo(f"❌ {failure['name']}")
                        click.echo(FeedbackManager.error_branch_check_pipe(error=failure["error"]))
                        click.echo("")
                    except Exception:
                        pass

    click.echo("")
    click.echo("")
    click.echo("==== Performance metrics ====")
    click.echo("")
    for step in response["progress"]:
        run = step["run"]
        if run.get("metrics_summary") and run.get("metrics_timing"):
            column_names = [f"{run['pipe_name']}({run['test_type']})", "Origin", "Branch", "Delta"]

            click.echo(
                format_pretty_table(
                    [
                        [
                            metric,
                            format_metric(run["metrics_timing"][metric][0]),
                            format_metric(run["metrics_timing"][metric][1]),
                            format_metric(run["metrics_timing"][metric][2], is_percentage=True),
                        ]
                        for metric in [
                            "min response time",
                            "max response time",
                            "mean response time",
                            "median response time",
                            "p90 response time",
                            "min read bytes",
                            "max read bytes",
                            "mean read bytes",
                            "median read bytes",
                            "p90 read bytes",
                        ]
                    ],
                    column_names=column_names,
                )
            )

    click.echo("")
    click.echo("")
    click.echo("==== Results Summary ====")
    click.echo("")
    click.echo(
        format_pretty_table(
            [
                [
                    step["run"]["pipe_name"],
                    step["run"]["test_type"],
                    step["run"]["metrics_summary"].get("run", 0),
                    step["run"]["metrics_summary"].get("passed", 0),
                    step["run"]["metrics_summary"].get("failed", 0),
                    format_metric(
                        (
                            step["run"]["metrics_timing"]["mean response time"][2]
                            if "mean response time" in step["run"]["metrics_timing"]
                            else 0.0
                        ),
                        is_percentage=True,
                    ),
                    format_metric(
                        (
                            step["run"]["metrics_timing"]["mean read bytes"][2]
                            if "mean read bytes" in step["run"]["metrics_timing"]
                            else 0.0
                        ),
                        is_percentage=True,
                    ),
                ]
                for step in response["progress"]
            ],
            column_names=["Endpoint", "Test", "Run", "Passed", "Failed", "Mean response time", "Mean read bytes"],
        )
    )
    click.echo("")
    if failed:
        for step in response["progress"]:
            if not step["run"]["was_successfull"]:
                for failure in step["run"]["failed"]:
                    click.echo(f"❌ FAILED {failure['name']}\n")
    if failed:
        raise CLIException(
            "Check Failures Detail above for more information. If the results are expected, skip asserts or increase thresholds, see 💡 Hints above (note skip asserts flags are applied to all regression tests, so use them when it makes sense).\n\nIf you are using the CI template for GitHub or GitLab you can add skip asserts flags as labels to the MR and they are automatically applied. Find available flags to skip asserts and thresholds here => https://www.tinybird.co/docs/production/implementing-test-strategies.html#fixture-tests"
        )


class PlanName(Enum):
    DEV = "Build"
    PRO = "Pro"
    ENTERPRISE = "Enterprise"


def _get_workspace_plan_name(plan):
    """
    >>> _get_workspace_plan_name("dev")
    'Build'
    >>> _get_workspace_plan_name("pro")
    'Pro'
    >>> _get_workspace_plan_name("enterprise")
    'Enterprise'
    >>> _get_workspace_plan_name("branch_enterprise")
    'Enterprise'
    >>> _get_workspace_plan_name("other_plan")
    'Custom'
    """
    if plan == "dev":
        return PlanName.DEV.value
    if plan == "pro":
        return PlanName.PRO.value
    if plan in ("enterprise", "branch_enterprise"):
        return PlanName.ENTERPRISE.value
    return "Custom"


def get_format_from_filename_or_url(filename_or_url: str) -> str:
    """
    >>> get_format_from_filename_or_url('wadus_parquet.csv')
    'csv'
    >>> get_format_from_filename_or_url('wadus_csv.parquet')
    'parquet'
    >>> get_format_from_filename_or_url('wadus_csv.ndjson')
    'ndjson'
    >>> get_format_from_filename_or_url('wadus_csv.json')
    'ndjson'
    >>> get_format_from_filename_or_url('wadus_parquet.csv?auth=pepe')
    'csv'
    >>> get_format_from_filename_or_url('wadus_csv.parquet?auth=pepe')
    'parquet'
    >>> get_format_from_filename_or_url('wadus_parquet.ndjson?auth=pepe')
    'ndjson'
    >>> get_format_from_filename_or_url('wadus.json?auth=pepe')
    'ndjson'
    >>> get_format_from_filename_or_url('wadus_csv_')
    'csv'
    >>> get_format_from_filename_or_url('wadus_json_csv_')
    'csv'
    >>> get_format_from_filename_or_url('wadus_json_')
    'ndjson'
    >>> get_format_from_filename_or_url('wadus_ndjson_')
    'ndjson'
    >>> get_format_from_filename_or_url('wadus_parquet_')
    'parquet'
    >>> get_format_from_filename_or_url('wadus')
    'csv'
    >>> get_format_from_filename_or_url('https://storage.googleapis.com/tinybird-waduscom/stores_stock__v2_1646741850424_final.csv?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=44444444444-compute@developer.gserviceaccount.com/1234/auto/storage/goog4_request&X-Goog-Date=20220308T121750Z&X-Goog-Expires=86400&X-Goog-SignedHeaders=host&X-Goog-Signature=8888888888888888888888888888888888888888888888888888888')
    'csv'
    """
    filename_or_url = filename_or_url.lower()
    if filename_or_url.endswith("json") or filename_or_url.endswith("ndjson"):
        return "ndjson"
    if filename_or_url.endswith("parquet"):
        return "parquet"
    if filename_or_url.endswith("csv"):
        return "csv"
    try:
        parsed = urlparse(filename_or_url)
        if parsed.path.endswith("json") or parsed.path.endswith("ndjson"):
            return "ndjson"
        if parsed.path.endswith("parquet"):
            return "parquet"
        if parsed.path.endswith("csv"):
            return "csv"
    except Exception:
        pass
    if "csv" in filename_or_url:
        return "csv"
    if "json" in filename_or_url:
        return "ndjson"
    if "parquet" in filename_or_url:
        return "parquet"
    return "csv"


async def push_data(
    ctx: Context,
    datasource_name: str,
    url,
    connector: Optional[str],
    sql: Optional[str],
    mode: str = "append",
    sql_condition: Optional[str] = None,
    replace_options=None,
    ignore_empty: bool = False,
    concurrency: int = 1,
):
    if url and type(url) is tuple:
        url = url[0]
    client: TinyB = ctx.obj["client"]

    if connector and sql:
        load_connector_config(ctx, connector, False, check_uninstalled=False)
        if connector not in ctx.obj:
            raise CLIException(FeedbackManager.error_connector_not_configured(connector=connector))
        else:
            _connector: "Connector" = ctx.obj[connector]
            click.echo(FeedbackManager.info_starting_export_process(connector=connector))
            try:
                url = _connector.export_to_gcs(sql, datasource_name, mode)
            except ConnectorNothingToLoad as e:
                if ignore_empty:
                    click.echo(str(e))
                    return
                else:
                    raise e

    def cb(res):
        if cb.First:  # type: ignore[attr-defined]
            blocks_to_process = len([x for x in res["block_log"] if x["status"] == "idle"])
            if blocks_to_process:
                cb.bar = click.progressbar(label=FeedbackManager.info_progress_blocks(), length=blocks_to_process)  # type: ignore[attr-defined]
                cb.bar.update(0)  # type: ignore[attr-defined]
                cb.First = False  # type: ignore[attr-defined]
                cb.blocks_to_process = blocks_to_process  # type: ignore[attr-defined]
        else:
            done = len([x for x in res["block_log"] if x["status"] == "done"])
            if done * 2 > cb.blocks_to_process:  # type: ignore[attr-defined]
                cb.bar.label = FeedbackManager.info_progress_current_blocks()  # type: ignore[attr-defined]
            cb.bar.update(done - cb.prev_done)  # type: ignore[attr-defined]
            cb.prev_done = done  # type: ignore[attr-defined]

    cb.First = True  # type: ignore[attr-defined]
    cb.prev_done = 0  # type: ignore[attr-defined]

    click.echo(FeedbackManager.info_starting_import_process())

    if isinstance(url, list):
        urls = url
    else:
        urls = [url]

    async def process_url(
        datasource_name: str, url: str, mode: str, sql_condition: Optional[str], replace_options: Optional[Set[str]]
    ):
        parsed = urlparse(url)
        # poor man's format detection
        _format = get_format_from_filename_or_url(url)
        if parsed.scheme in ("http", "https"):
            res = await client.datasource_create_from_url(
                datasource_name,
                url,
                mode=mode,
                status_callback=cb,
                sql_condition=sql_condition,
                format=_format,
                replace_options=replace_options,
            )
        else:
            res = await client.datasource_append_data(
                datasource_name,
                file=url,
                mode=mode,
                sql_condition=sql_condition,
                format=_format,
                replace_options=replace_options,
            )

        datasource_name = res["datasource"]["name"]
        try:
            datasource = await client.get_datasource(datasource_name)
        except DoesNotExistException:
            raise CLIException(FeedbackManager.error_datasource_does_not_exist(datasource=datasource_name))
        except Exception as e:
            raise CLIException(FeedbackManager.error_exception(error=str(e)))

        total_rows = (datasource.get("statistics", {}) or {}).get("row_count", 0)
        appended_rows = 0
        parser = None

        if res.get("error"):
            raise CLIException(FeedbackManager.error_exception(error=res["error"]))
        if res.get("errors"):
            raise CLIException(FeedbackManager.error_exception(error=res["errors"]))
        if res.get("blocks"):
            for block in res["blocks"]:
                if "process_return" in block and block["process_return"] is not None:
                    process_return = block["process_return"][0]
                    parser = process_return["parser"] if process_return.get("parser") else parser
                    if parser and parser != "clickhouse":
                        parser = process_return["parser"]
                        appended_rows += process_return["lines"]

        return parser, total_rows, appended_rows

    try:
        tasks = [process_url(datasource_name, url, mode, sql_condition, replace_options) for url in urls]
        output = await gather_with_concurrency(concurrency, *tasks)
        parser, total_rows, appended_rows = list(output)[-1]
    except AuthNoTokenException:
        raise
    except OperationCanNotBePerformed as e:
        raise CLIException(FeedbackManager.error_operation_can_not_be_performed(error=e))
    except Exception as e:
        raise CLIException(FeedbackManager.error_exception(error=e))
    else:
        click.echo(FeedbackManager.success_progress_blocks())
        if mode == "append" and parser and parser != "clickhouse":
            click.echo(FeedbackManager.success_appended_rows(appended_rows=appended_rows))

        click.echo(FeedbackManager.success_total_rows(datasource=datasource_name, total_rows=total_rows))

        if mode == "replace":
            click.echo(FeedbackManager.success_replaced_datasource(datasource=datasource_name))
        else:
            click.echo(FeedbackManager.success_appended_datasource(datasource=datasource_name))
        click.echo(FeedbackManager.info_data_pushed(datasource=datasource_name))
    finally:
        try:
            for url in urls:
                _connector.clean(urlparse(url).path.split("/")[-1])
        except Exception:
            pass


async def sync_data(ctx, datasource_name: str, yes: bool):
    client: TinyB = ctx.obj["client"]
    datasource = await client.get_datasource(datasource_name)

    VALID_DATASOURCES = ["bigquery", "snowflake", "s3", "gcs"]
    if datasource["type"] not in VALID_DATASOURCES:
        raise CLIException(FeedbackManager.error_sync_not_supported(valid_datasources=VALID_DATASOURCES))

    warning_message = (
        FeedbackManager.warning_datasource_sync_bucket(datasource=datasource_name)
        if datasource["type"] in ["s3", "gcs"]
        else FeedbackManager.warning_datasource_sync(
            datasource=datasource_name,
        )
    )
    if yes or click.confirm(warning_message):
        await client.datasource_sync(datasource["id"])
        click.echo(FeedbackManager.success_sync_datasource(datasource=datasource_name))


# eval "$(_TB_COMPLETE=source_bash tb)"
def autocomplete_topics(ctx: Context, args, incomplete):
    try:
        config = async_to_sync(get_config)(None, None)
        ctx.ensure_object(dict)["config"] = config
        client = create_tb_client(ctx)
        topics = async_to_sync(client.kafka_list_topics)(args[2])
        return [t for t in topics if incomplete in t]
    except Exception:
        return []


def validate_datasource_name(name):
    if not isinstance(name, str) or name == "":
        raise CLIException(FeedbackManager.error_datasource_name())


def validate_connection_id(connection_id):
    if not isinstance(connection_id, str) or connection_id == "":
        raise CLIException(FeedbackManager.error_datasource_connection_id())


def validate_kafka_topic(topic):
    if not isinstance(topic, str):
        raise CLIException(FeedbackManager.error_kafka_topic())


def validate_kafka_group(group):
    if not isinstance(group, str):
        raise CLIException(FeedbackManager.error_kafka_group())


def validate_kafka_auto_offset_reset(auto_offset_reset):
    valid_values = {"latest", "earliest", "none"}
    if auto_offset_reset not in valid_values:
        raise CLIException(FeedbackManager.error_kafka_auto_offset_reset())


def validate_kafka_schema_registry_url(schema_registry_url):
    if not is_url_valid(schema_registry_url):
        raise CLIException(FeedbackManager.error_kafka_registry())


def is_url_valid(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def validate_kafka_bootstrap_servers(host_and_port):
    if not isinstance(host_and_port, str):
        raise CLIException(FeedbackManager.error_kafka_bootstrap_server())
    parts = host_and_port.split(":")
    if len(parts) > 2:
        raise CLIException(FeedbackManager.error_kafka_bootstrap_server())
    host = parts[0]
    port_str = parts[1] if len(parts) == 2 else "9092"
    try:
        port = int(port_str)
    except Exception:
        raise CLIException(FeedbackManager.error_kafka_bootstrap_server())
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        try:
            sock.settimeout(3)
            sock.connect((host, port))
        except socket.timeout:
            raise CLIException(FeedbackManager.error_kafka_bootstrap_server_conn_timeout())
        except Exception:
            raise CLIException(FeedbackManager.error_kafka_bootstrap_server_conn())


def validate_kafka_key(s):
    if not isinstance(s, str):
        raise CLIException("Key format is not correct, it should be a string")


def validate_kafka_secret(s):
    if not isinstance(s, str):
        raise CLIException("Password format is not correct, it should be a string")


def validate_string_connector_param(param, s):
    if not isinstance(s, str):
        raise CLIConnectionException(param + " format is not correct, it should be a string")


async def validate_connection_name(client, connection_name, service):
    if await client.get_connector(connection_name, service) is not None:
        raise CLIConnectionException(FeedbackManager.error_connection_already_exists(name=connection_name))


def _get_setting_value(connection, setting, sensitive_settings):
    if setting in sensitive_settings:
        return "*****"
    return connection.get(setting, "")


async def switch_workspace(config: CLIConfig, workspace_name_or_id: str, only_environments: bool = False) -> None:
    try:
        if only_environments:
            workspaces = await get_current_workspace_branches(config)
        else:
            response = await config.get_client().user_workspaces()
            workspaces = response["workspaces"]

        workspace = next(
            (
                workspace
                for workspace in workspaces
                if workspace["name"] == workspace_name_or_id or workspace["id"] == workspace_name_or_id
            ),
            None,
        )

        if not workspace:
            if only_environments:
                raise CLIException(FeedbackManager.error_branch(branch=workspace_name_or_id))
            else:
                raise CLIException(FeedbackManager.error_workspace(workspace=workspace_name_or_id))

        config.set_token(workspace["token"])
        config.set_token_for_host(workspace["token"], config.get_host())
        _ = await try_update_config_with_remote(config)

        # Set the id and name afterwards.
        # When working with branches the call to try_update_config_with_remote above
        # sets the data with the main branch ones
        config["id"] = workspace["id"]
        config["name"] = workspace["name"]

        config.persist_to_file()

        click.echo(FeedbackManager.success_now_using_config(name=config["name"], id=config["id"]))
    except AuthNoTokenException:
        raise
    except CLIException:
        raise
    except Exception as e:
        raise CLIException(FeedbackManager.error_exception(error=str(e)))


async def switch_to_workspace_by_user_workspace_data(config: CLIConfig, user_workspace_data: Dict[str, Any]):
    try:
        config["id"] = user_workspace_data["id"]
        config["name"] = user_workspace_data["name"]
        config.set_token(user_workspace_data["token"])
        config.set_token_for_host(user_workspace_data["token"], config.get_host())
        config.persist_to_file()

        click.echo(FeedbackManager.success_now_using_config(name=config["name"], id=config["id"]))
    except Exception as e:
        raise CLIException(FeedbackManager.error_exception(error=str(e)))


async def print_current_workspace(config: CLIConfig) -> None:
    _ = await try_update_config_with_remote(config, only_if_needed=True)

    current_main_workspace = await get_current_main_workspace(config)
    assert isinstance(current_main_workspace, dict)

    columns = ["name", "id", "role", "plan", "current"]

    table = [
        (
            current_main_workspace["name"],
            current_main_workspace["id"],
            current_main_workspace["role"],
            _get_workspace_plan_name(current_main_workspace["plan"]),
            True,
        )
    ]

    click.echo(FeedbackManager.info_current_workspace())
    echo_safe_humanfriendly_tables_format_smart_table(table, column_names=columns)


async def print_current_branch(config: CLIConfig) -> None:
    _ = await try_update_config_with_remote(config, only_if_needed=True)

    response = await config.get_client().user_workspaces_and_branches()

    columns = ["name", "id", "workspace"]
    table = []

    for workspace in response["workspaces"]:
        if config["id"] == workspace["id"]:
            click.echo(FeedbackManager.info_current_branch())
            if workspace.get("is_branch"):
                name = workspace["name"]
                main_workspace = await get_current_main_workspace(config)
                assert isinstance(main_workspace, dict)
                main_name = main_workspace["name"]
            else:
                name = MAIN_BRANCH
                main_name = workspace["name"]
            table.append([name, workspace["id"], main_name])
            break

    echo_safe_humanfriendly_tables_format_smart_table(table, column_names=columns)


class ConnectionReplacements:
    _PARAMS_REPLACEMENTS: Dict[str, Dict[str, str]] = {
        "s3": {
            "service": "service",
            "connection_name": "name",
            "key": "s3_access_key_id",
            "secret": "s3_secret_access_key",
            "region": "s3_region",
        },
        "s3_iamrole": {
            "service": "service",
            "connection_name": "name",
            "role_arn": "s3_iamrole_arn",
            "region": "s3_iamrole_region",
        },
        "gcs_hmac": {
            "service": "service",
            "connection_name": "name",
            "key": "gcs_hmac_access_id",
            "secret": "gcs_hmac_secret",
            "region": "gcs_region",
        },
        "gcs": {
            "project_id": "gcs_project_id",
            "client_id": "gcs_client_id",
            "client_email": "gcs_client_email",
            "client_x509_cert_url": "gcs_client_x509_cert_url",
            "private_key": "gcs_private_key",
            "private_key_id": "gcs_private_key_id",
            "connection_name": "name",
        },
        "dynamodb": {
            "service": "service",
            "connection_name": "name",
            "role_arn": "dynamodb_iamrole_arn",
            "region": "dynamodb_iamrole_region",
        },
    }

    @staticmethod
    def map_api_params_from_prompt_params(service: str, **params: Any) -> Dict[str, Any]:
        """Maps prompt parameters to API parameters."""

        api_params = {}
        for key in params.keys():
            try:
                api_params[ConnectionReplacements._PARAMS_REPLACEMENTS[service][key]] = params[key]
            except KeyError:
                api_params[key] = params[key]

        api_params["service"] = service
        return api_params


# ======
# Temporal new functions while we fully merge the new CLIConfig
# ======


async def get_host_from_region(
    config: CLIConfig, region_name_or_host_or_id: str, host: Optional[str] = None
) -> Tuple[List[Region], str]:
    regions: List[Region]
    region: Optional[Region]

    host = host or config.get_host(use_defaults_if_needed=True)

    try:
        regions = await get_regions(config)
        assert isinstance(regions, list)
    except Exception:
        regions = []

    if not regions:
        assert isinstance(host, str)
        click.echo(f"No regions available, using host: {host}")
        return [], host

    try:
        index = int(region_name_or_host_or_id)
        try:
            host = regions[index - 1]["api_host"]
        except Exception:
            raise CLIException(FeedbackManager.error_getting_region_by_index())
    except ValueError:
        region_name = region_name_or_host_or_id.lower()
        try:
            region = get_region_from_host(region_name, regions)
            host = region["api_host"] if region else None
        except Exception:
            raise CLIException(FeedbackManager.error_getting_region_by_name_or_url())

    if not host:
        raise CLIException(FeedbackManager.error_getting_region_by_name_or_url())

    return regions, host


async def get_regions(config: CLIConfig) -> List[Region]:
    regions: List[Region] = []

    try:
        response = await config.get_client().regions()
        regions = response.get("regions", [])
    except Exception:
        pass

    try:
        if "tokens" not in config:
            return regions

        for key in config["tokens"]:
            region = next((region for region in regions if key == region["api_host"] or key == region["host"]), None)
            if region:
                region["default_password"] = config["tokens"][key]
                region["provider"] = region["provider"] or ""
            else:
                regions.append(
                    {
                        "api_host": format_host(key, subdomain="api"),
                        "host": get_display_host(key),
                        "name": key,
                        "default_password": config["tokens"][key],
                        "provider": "",
                    }
                )

    except Exception:
        pass

    return regions


def get_region_from_host(region_name_or_host: str, regions: List[Region]) -> Optional[Region]:
    """Returns the region that matches region_name_or_host by name, API host or ui host"""
    for region in regions:
        if region_name_or_host in (region["name"].lower(), region["host"], region["api_host"]):
            return region
    return None


async def try_update_config_with_remote(
    config: CLIConfig, raise_on_errors: bool = True, only_if_needed: bool = False, auto_persist: bool = True
) -> bool:
    response: Dict[str, Any]

    if not config.get_token():
        if not raise_on_errors:
            return False
        raise AuthNoTokenException()

    if "id" in config and only_if_needed:
        return True

    try:
        response = await config.get_client().workspace_info()
    except AuthException:
        if raise_on_errors:
            raise CLIAuthException(FeedbackManager.error_invalid_token_for_host(host=config.get_host()))
        return False
    except Exception as ex:
        if raise_on_errors:
            ex_message = str(ex)
            if "cannot parse" in ex_message.lower():
                raise CLIAuthException(FeedbackManager.error_invalid_host(host=config.get_host()))

            raise CLIAuthException(FeedbackManager.error_exception(error=ex_message))
        return False

    for k in ("id", "name", "user_email", "user_id", "scope"):
        if k in response:
            config[k] = response[k]

    config.set_token_for_host(config.get_token(), config.get_host())

    if auto_persist:
        config.persist_to_file()

    return True


def ask_for_admin_token_interactively(ui_host: str, default_token: Optional[str]) -> str:
    return (
        click.prompt(
            f"\nCopy the \"admin your@email\" token from {ui_host}/tokens and paste it here { 'OR press enter to use the token from .tinyb file' if default_token else ''}",
            hide_input=True,
            show_default=False,
            default=default_token,
            type=str,
        )
        or ""
    )


async def try_authenticate(
    config: CLIConfig,
    regions: Optional[List[Region]] = None,
    interactive: bool = False,
    try_all_regions: bool = False,
) -> bool:
    host: Optional[str] = config.get_host()

    if not regions and interactive:
        regions = await get_regions(config)

    selected_region: Optional[Region] = None
    default_password: Optional[str] = None

    if regions:
        if interactive:
            selected_region = ask_for_region_interactively(regions)
            if selected_region is None:
                return False

            host = selected_region.get("api_host")
            default_password = selected_region.get("default_password")
        else:
            assert isinstance(host, str)
            selected_region = get_region_from_host(host, regions)

    name: str
    api_host: str
    ui_host: str
    token: Optional[str]
    if host and not selected_region:
        name, api_host, ui_host = (host, format_host(host, subdomain="api"), format_host(host, subdomain="ui"))
        token = config.get_token()
    else:
        name, api_host, ui_host = get_region_info(config, selected_region)
        token = config.get_token_for_host(api_host)
    config.set_host(api_host)

    if not token:
        token = ask_for_admin_token_interactively(get_display_host(ui_host), default_token=default_password)
    config.set_token(token)

    add_telemetry_event("auth_token", token=token)
    authenticated: bool = await try_update_config_with_remote(config, raise_on_errors=not try_all_regions)

    # No luck? Let's try auth in all other regions
    if not authenticated and try_all_regions and not interactive:
        if not regions:
            regions = await get_regions(config)

        # Check other regions, ignoring the previously tested region
        for region in [r for r in regions if r is not selected_region]:
            name, host, ui_host = get_region_info(config, region)
            config.set_host(host)
            authenticated = await try_update_config_with_remote(config, raise_on_errors=False)
            if authenticated:
                click.echo(FeedbackManager.success_using_host(name=name, host=get_display_host(ui_host)))
                break

    if not authenticated:
        raise CLIAuthException(FeedbackManager.error_invalid_token())

    config.persist_to_file()

    click.echo(FeedbackManager.success_auth())
    click.echo(FeedbackManager.success_remember_api_host(api_host=host))

    if not config.get("scope"):
        click.echo(FeedbackManager.warning_token_scope())

    add_telemetry_event("auth_success")

    return True


async def wait_job(
    tb_client: TinyB,
    job_id: str,
    job_url: str,
    label: str,
    wait_observer: Optional[Callable[[Dict[str, Any], ProgressBar], None]] = None,
) -> Dict[str, Any]:
    progress_bar: ProgressBar
    with click.progressbar(
        label=f"{label} ",
        length=100,
        show_eta=False,
        show_percent=wait_observer is None,
        fill_char=click.style("█", fg="green"),
    ) as progress_bar:

        def progressbar_cb(res: Dict[str, Any]):
            if wait_observer:
                wait_observer(res, progress_bar)
                return

            if "progress_percentage" in res:
                progress_bar.update(int(round(res["progress_percentage"])) - progress_bar.pos)
            elif res["status"] != "working":
                progress_bar.update(progress_bar.length if progress_bar.length else 0)

        try:
            # TODO: Simplify this as it's not needed to use two functions for
            result = await wait_job_no_ui(tb_client, job_id, progressbar_cb)
            if result["status"] != "done":
                raise CLIException(FeedbackManager.error_while_running_job(error=result["error"]))
            return result
        except asyncio.TimeoutError:
            raise CLIException(FeedbackManager.error_while_running_job(error="Reach timeout, job cancelled"))
        except JobException as e:
            raise CLIException(FeedbackManager.error_while_running_job(error=str(e)))
        except Exception as e:
            raise CLIException(FeedbackManager.error_getting_job_info(error=str(e), url=job_url))


async def wait_job_no_ui(
    tb_client: TinyB,
    job_id: str,
    status_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    try:
        result = await asyncio.wait_for(tb_client.wait_for_job(job_id, status_callback=status_callback), None)
        if result["status"] != "done":
            raise JobException(result.get("error"))
        return result
    except asyncio.TimeoutError:
        await tb_client.job_cancel(job_id)
        raise


async def get_current_main_workspace(config: CLIConfig) -> Optional[Dict[str, Any]]:
    current_workspace = await config.get_client().user_workspaces_and_branches()
    return _get_current_main_workspace_common(current_workspace, config.get("id", current_workspace["id"]))


def _get_current_main_workspace_common(
    user_workspace_and_branches: Dict[str, Any], current_workspace_id: str
) -> Optional[Dict[str, Any]]:
    def get_workspace_by_id(workspaces: List[Dict[str, Any]], id: str) -> Optional[Dict[str, Any]]:
        return next((ws for ws in workspaces if ws["id"] == id), None)

    workspaces: Optional[List[Dict[str, Any]]] = user_workspace_and_branches.get("workspaces")
    if not workspaces:
        return None

    current: Optional[Dict[str, Any]] = get_workspace_by_id(workspaces, current_workspace_id)
    if current and current.get("is_branch"):
        current = get_workspace_by_id(workspaces, current["main"])

    return current


def is_post_semver(new_version: Version, current_version: Version) -> bool:
    """
    Check if only the post part of the semantic version has changed.

    Args:
    new_version (Version): The new version to check.
    current_version (Version): The current version to compare with.

    Returns:
    bool: True if only the post part of the version has changed, False otherwise.

    Examples:
    >>> is_post_semver(Version("0.0.0-2"), Version("0.0.0-1"))
    True
    >>> is_post_semver(Version("0.0.0-1"), Version("0.0.0-1"))
    False
    >>> is_post_semver(Version("0.0.1-1"), Version("0.0.0-1"))
    False
    >>> is_post_semver(Version("0.1.0-1"), Version("0.0.0-1"))
    False
    >>> is_post_semver(Version("1.0.0-1"), Version("0.0.0-1"))
    False
    >>> is_post_semver(Version("0.0.1-1"), Version("0.0.0"))
    False
    """
    if (
        new_version.major == current_version.major
        and new_version.minor == current_version.minor
        and new_version.micro == current_version.micro
    ):
        return new_version.post is not None and new_version.post != current_version.post

    return False


def is_major_semver(new_version: Version, current_version: Version) -> bool:
    """
    Check if only the major part of the semantic version has changed.

    Args:
    new_version (Version): The new version to check.
    current_version (Version): The current version to compare with.

    Returns:
    bool: True if only the major part of the version has changed, False otherwise.

    Examples:
    >>> is_major_semver(Version("1.0.0"), Version("0.0.0"))
    True
    >>> is_major_semver(Version("0.0.0"), Version("0.0.0"))
    False
    >>> is_major_semver(Version("1.0.1"), Version("1.0.0"))
    False
    >>> is_major_semver(Version("2.0.0-1"), Version("1.0.1-2"))
    True
    """

    return new_version.major != current_version.major


async def print_release_summary(config: CLIConfig, semver: Optional[str], info: bool = False, dry_run=False):
    if not semver:
        click.echo(FeedbackManager.info_release_no_rollback())
        return
    try:
        client = config.get_client()
        response = await client.release_rm(config["id"], semver, confirmation=config["name"], dry_run=True)
    except Exception as e:
        raise CLIReleaseException(FeedbackManager.error_exception(error=str(e)))
    else:
        columns = ["name", "id", "notes"]
        if not response:
            click.echo(FeedbackManager.info_release_no_rollback())
            return

        if len(response["datasources"]) or len(response["pipes"]):
            semver = response.get("semver", semver)
            if info:
                if dry_run:
                    click.echo(FeedbackManager.info_release_rm_resources_dry_run(semver=semver))
                else:
                    click.echo(FeedbackManager.info_release_rm_resources())
            else:
                click.echo(FeedbackManager.info_release_rollback(semver=semver))

        if len(response["datasources"]):
            click.echo("\nDatasources:")
            rows = [
                [ds, response["datasources"][ds], response["notes"].get(response["datasources"][ds], "")]
                for ds in response["datasources"]
            ]
            echo_safe_humanfriendly_tables_format_smart_table(rows, column_names=columns)

        if len(response["pipes"]):
            click.echo("\nPipes:")
            rows = [
                [pipe, response["pipes"][pipe], response["notes"].get(response["pipes"][pipe], "")]
                for pipe in response["pipes"]
            ]
            echo_safe_humanfriendly_tables_format_smart_table(rows, column_names=columns)


async def get_oldest_rollback(config: CLIConfig, client: TinyB) -> Optional[str]:
    oldest_rollback_response = await client.release_oldest_rollback(config["id"])
    return oldest_rollback_response.get("semver") if oldest_rollback_response else None


async def remove_release(
    dry_run: bool, config: CLIConfig, semver: Optional[str], client: TinyB, force: bool, show_print=True
):
    if semver == OLDEST_ROLLBACK:
        semver = await get_oldest_rollback(config, client)
    if show_print:
        await print_release_summary(config, semver, info=True, dry_run=True)
    if not dry_run:
        if semver:
            response = await client.release_rm(
                config["id"], semver, confirmation=config["name"], dry_run=dry_run, force=force
            )
            click.echo(FeedbackManager.success_release_delete(semver=response.get("semver")))
        else:
            click.echo(FeedbackManager.info_no_release_deleted())


async def validate_aws_iamrole_integration(
    client: TinyB,
    service: str,
    role_arn: Optional[str],
    region: Optional[str],
    policy: str = "write",
    no_validate: Optional[bool] = False,
):
    if no_validate is False:
        access_policy, trust_policy, external_id = await get_aws_iamrole_policies(
            client, service=service, policy=policy
        )

        if not role_arn:
            if not click.confirm(
                FeedbackManager.prompt_s3_iamrole_connection_login_aws(),
                show_default=False,
                prompt_suffix="Press y to continue:",
            ):
                sys.exit(1)

            access_policy_copied = True
            try:
                pyperclip.copy(access_policy)
            except Exception:
                access_policy_copied = False

            replacements_dict = {
                "<bucket>": "<bucket> with your bucket name",
                "<table_name>": "<table_name> with your DynamoDB table name",
            }

            replacements = [
                replacements_dict.get(replacement, "")
                for replacement in replacements_dict.keys()
                if replacement in access_policy
            ]

            if not click.confirm(
                (
                    FeedbackManager.prompt_s3_iamrole_connection_policy(
                        access_policy=access_policy, replacements=", ".join(replacements)
                    )
                    if access_policy_copied
                    else FeedbackManager.prompt_s3_iamrole_connection_policy_not_copied(access_policy=access_policy)
                ),
                show_default=False,
                prompt_suffix="Press y to continue:",
            ):
                sys.exit(1)

            trust_policy_copied = True
            try:
                pyperclip.copy(trust_policy)
            except Exception:
                trust_policy_copied = False

            if not click.confirm(
                (
                    FeedbackManager.prompt_s3_iamrole_connection_role(trust_policy=trust_policy)
                    if trust_policy_copied
                    else FeedbackManager.prompt_s3_iamrole_connection_role_not_copied(trust_policy=trust_policy)
                ),
                show_default=False,
                prompt_suffix="Press y to continue:",
            ):
                sys.exit(1)
    else:
        try:
            trust_policy = await client.get_trust_policy(service)
            external_id = trust_policy["Statement"][0]["Condition"]["StringEquals"]["sts:ExternalId"]
        except Exception:
            external_id = ""

    if not role_arn:
        role_arn = click.prompt("Enter the ARN of the role you just created")
        validate_string_connector_param("Role ARN", role_arn)

    if not region:
        region_resource = "table" if service == DataConnectorType.AMAZON_DYNAMODB else "bucket"
        region = click.prompt(f"Enter the region where the {region_resource} is located")
        validate_string_connector_param("Region", region)

    return role_arn, region, external_id


async def get_aws_iamrole_policies(client: TinyB, service: str, policy: str = "write"):
    access_policy: Dict[str, Any] = {}
    if service == DataConnectorType.AMAZON_S3_IAMROLE:
        service = DataConnectorType.AMAZON_S3
    try:
        if policy == "write":
            access_policy = await client.get_access_write_policy(service)
        elif policy == "read":
            access_policy = await client.get_access_read_policy(service)
        else:
            raise Exception(f"Access policy {policy} not supported. Choose from 'read' or 'write'")
        if not len(access_policy) > 0:
            raise Exception(f"{service.upper()} Integration not supported in this region")
    except Exception as e:
        raise CLIConnectionException(FeedbackManager.error_connection_integration_not_available(error=str(e)))

    trust_policy: Dict[str, Any] = {}
    try:
        trust_policy = await client.get_trust_policy(service)
        if not len(trust_policy) > 0:
            raise Exception(f"{service.upper()} Integration not supported in this region")
    except Exception as e:
        raise CLIConnectionException(FeedbackManager.error_connection_integration_not_available(error=str(e)))
    try:
        external_id = trust_policy["Statement"][0]["Condition"]["StringEquals"]["sts:ExternalId"]
    except Exception:
        external_id = ""
    return json.dumps(access_policy, indent=4), json.dumps(trust_policy, indent=4), external_id


async def validate_aws_iamrole_connection_name(
    client: TinyB, connection_name: Optional[str], no_validate: Optional[bool] = False
) -> str:
    if connection_name and no_validate is False:
        if await client.get_connector(connection_name, skip_bigquery=True) is not None:
            raise CLIConnectionException(FeedbackManager.info_connection_already_exists(name=connection_name))
    else:
        while not connection_name:
            connection_name = click.prompt("Enter the name for this connection", default=None, show_default=False)
            assert isinstance(connection_name, str)

            if no_validate is False and await client.get_connector(connection_name) is not None:
                click.echo(FeedbackManager.info_connection_already_exists(name=connection_name))
                connection_name = None
    assert isinstance(connection_name, str)
    return connection_name


class DataConnectorType(str, Enum):
    KAFKA = "kafka"
    GCLOUD_SCHEDULER = "gcscheduler"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    GCLOUD_STORAGE = "gcs"
    GCLOUD_STORAGE_HMAC = "gcs_hmac"
    GCLOUD_STORAGE_SA = "gcs_service_account"
    AMAZON_S3 = "s3"
    AMAZON_S3_IAMROLE = "s3_iamrole"
    AMAZON_DYNAMODB = "dynamodb"

    def __str__(self) -> str:
        return self.value


async def create_aws_iamrole_connection(client: TinyB, service: str, connection_name, role_arn, region) -> None:
    conn_file_name = f"{connection_name}.connection"
    conn_file_path = Path(getcwd(), conn_file_name)

    if os.path.isfile(conn_file_path):
        raise CLIConnectionException(FeedbackManager.error_connection_file_already_exists(name=conn_file_name))

    if service == DataConnectorType.AMAZON_S3_IAMROLE:
        click.echo(FeedbackManager.info_creating_s3_iamrole_connection(connection_name=connection_name))
    if service == DataConnectorType.AMAZON_DYNAMODB:
        click.echo(FeedbackManager.info_creating_dynamodb_connection(connection_name=connection_name))

    params = ConnectionReplacements.map_api_params_from_prompt_params(
        service, connection_name=connection_name, role_arn=role_arn, region=region
    )

    click.echo("** Creating connection...")
    try:
        _ = await client.connection_create(params)
    except Exception as e:
        raise CLIConnectionException(
            FeedbackManager.error_connection_create(connection_name=connection_name, error=str(e))
        )

    async with aiofiles.open(conn_file_path, "w") as f:
        await f.write(
            f"""TYPE {service}

"""
        )
    click.echo(FeedbackManager.success_connection_file_created(name=conn_file_name))


def get_ca_pem_content(ca_pem: Optional[str], filename: Optional[str] = None) -> Optional[str]:
    if not ca_pem:
        return None

    def is_valid_content(text_content: str) -> bool:
        return text_content.startswith("-----BEGIN CERTIFICATE-----")

    ca_pem_content = ca_pem
    base_path = Path(getcwd(), filename).parent if filename else Path(getcwd())
    ca_pem_path = Path(base_path, ca_pem)
    path_exists = os.path.exists(ca_pem_path)

    if not path_exists:
        raise CLIConnectionException(FeedbackManager.error_connection_ca_pem_not_found(ca_pem=ca_pem))

    if ca_pem.endswith(".pem") and path_exists:
        with open(ca_pem_path, "r") as f:
            ca_pem_content = f.read()

    if not is_valid_content(ca_pem_content):
        raise CLIConnectionException(FeedbackManager.error_connection_invalid_ca_pem())

    return ca_pem_content
