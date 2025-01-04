# This is a command file for our CLI. Please keep it clean.
#
# - If it makes sense and only when strictly necessary, you can create utility functions in this file.
# - But please, **do not** interleave utility functions and command definitions.

import os
from typing import Any, Dict, List, Optional

import click
import humanfriendly.tables

from tinybird.config import get_display_host
from tinybird.feedback_manager import FeedbackManager
from tinybird.tb_cli_modules.cli import cli
from tinybird.tb_cli_modules.common import (
    configure_connector,
    coro,
    echo_safe_humanfriendly_tables_format_smart_table,
    get_host_from_region,
    get_regions,
    try_authenticate,
    try_update_config_with_remote,
)
from tinybird.tb_cli_modules.config import CLIConfig, ConfigValueOrigin
from tinybird.tb_cli_modules.exceptions import CLIAuthException
from tinybird.tb_cli_modules.regions import Region


@cli.group(invoke_without_command=True)
@click.option("--token", help="Use auth token, defaults to TB_TOKEN envvar, then to the .tinyb file")
@click.option(
    "--host",
    help="Set custom host if it's different than https://api.tinybird.co. Check https://www.tinybird.co/docs/api-reference/overview#regions-and-endpoints for the available list of regions",
)
@click.option(
    "--region", envvar="TB_REGION", help="Set region. Run 'tb auth ls' to show available regions. Overrides host."
)
@click.option(
    "--connector",
    type=click.Choice(["bigquery", "snowflake"], case_sensitive=True),
    help="Set credentials for one of the supported connectors",
)
@click.option(
    "-i",
    "--interactive",
    is_flag=True,
    default=False,
    help="Show available regions and select where to authenticate to",
)
@click.pass_context
@coro
async def auth(ctx: click.Context, token: str, host: str, region: str, connector: str, interactive: bool) -> None:
    """Configure auth."""

    config: CLIConfig = CLIConfig.get_project_config()
    if token:
        config.set_token(token)
    if host:
        config.set_host(host)

    if connector:
        await configure_connector(connector)
        return

    # Only run when doing a bare 'tb auth'
    if ctx.invoked_subcommand:
        return

    assert isinstance(ctx.parent, click.Context)

    env_token = os.environ.get("TB_TOKEN", None)

    # If no token passed, let's clear the current one to
    # do a clean auth
    if not token and not ctx.parent.params.get("token") and not env_token:
        config.set_token(None)
    else:
        if env_token and not token:
            click.echo(FeedbackManager.info_reading_from_env(value="token", envvar="TB_TOKEN"))

    regions: Optional[List[Region]] = None
    try_all_regions = True

    if host:
        try_all_regions = False

    if region:
        regions, host = await get_host_from_region(config, region, config.get_host())
        config.set_host(host)
        if token:
            config.set_token_for_host(token, host)
            try_all_regions = False

    if not await try_authenticate(config, regions=regions, interactive=interactive, try_all_regions=try_all_regions):
        raise CLIAuthException(FeedbackManager.error_invalid_token_for_host(host=config.get_host()))

    config.persist_to_file()


@auth.command(name="login", hidden=True)
@click.option(
    "--host",
    help="Set custom host if it's different than https://api.tinybird.co. Use `tb auth ls` or check https://docs.tinybird.co/cli.html for the available list of regions.",
)
@click.argument("token", required=False)
@coro
async def auth_login(host: Optional[str], token: Optional[str]) -> None:
    """Authenticate with a Tinybird host.

    The authentication mode is token based. After completion, the authentication token
    will be stored internally.

    You need your User Token to log into Tinybird. Please, check the docs at
    https://www.tinybird.co/docs/concepts/auth-tokens.html for more info.

    Alternatively, tb will use the authentication token found in the environment
    variable TB_USER_TOKEN. This method is most suitable for "headless" use of tb
    such as in automations or in a CI environment."""
    config = CLIConfig.get_global_config()

    if host:
        config.set_host(host)

    if not token:
        click.echo(FeedbackManager.info_pre_prompt_auth_login_user_token(host=config.get_host()))
        token = click.prompt(FeedbackManager.prompt_auth_login_user_token(), type=str, hide_input=True)
        if not token:
            raise CLIAuthException(FeedbackManager.error_auth_login_token_expected())
    config.set_user_token(token)

    auth_info: Dict[str, Any] = await config.get_user_client().check_auth_login()
    if not auth_info.get("is_valid", False):
        raise CLIAuthException(FeedbackManager.error_auth_login_not_valid(host=config.get_host()))

    if not auth_info.get("is_user", False):
        raise CLIAuthException(FeedbackManager.error_auth_login_not_user(host=config.get_host()))

    config.persist_to_file()

    # No more output needed as per "The art of UNIX programming"
    # http://www.catb.org/~esr/writings/taoup/html/ch11s09.html
    # > be chatty only about things that deviate from what's normally expected.


@auth.command(name="logout", hidden=True)
@coro
async def auth_logout() -> None:
    """Remove authentication from Tinybird."""
    conf = CLIConfig.get_global_config()
    conf.set_user_token(None)
    conf.persist_to_file()


@auth.command(name="info")
@coro
async def auth_info() -> None:
    """Get information about the authentication that is currently being used"""

    config = CLIConfig.get_project_config()
    _ = await try_update_config_with_remote(config, raise_on_errors=False)

    if "id" in config:
        table = []
        user_email = config.get("user_email", "No user")

        ORIGINS: Dict[ConfigValueOrigin, str] = {
            ConfigValueOrigin.CONFIG: ".tinyb",
            ConfigValueOrigin.DEFAULT: ".tinyb",
            ConfigValueOrigin.ENVIRONMENT: "env",
        }

        token_origin = ORIGINS.get(config.get_value_origin("token"), "")
        token = f"{config.get_token()} ({token_origin})"

        host = config.get("host") or ""
        ui_host = get_display_host(host)

        if config.get_user_token():
            user_token_origin = ORIGINS.get(config.get_value_origin("user_token"), "")
            user_token = f"{config.get_user_token()} ({user_token_origin})"
            columns = ["user", "user_token", "token", "host", "ui", "workspace_name", "workspace_id"]
            table.append([user_email, user_token, token, host, ui_host, config["name"], config["id"]])
        else:
            columns = ["user", "token", "host", "ui", "workspace_name", "workspace_id"]
            table.append([user_email, token, host, ui_host, config["name"], config["id"]])

        click.echo(humanfriendly.tables.format_robust_table(table, column_names=columns))


@auth.command(name="ls")
@coro
async def auth_ls() -> None:
    """List available regions to authenticate."""

    click.echo(FeedbackManager.info_available_regions())

    config = CLIConfig.get_project_config()
    _ = await try_update_config_with_remote(config, raise_on_errors=False)

    table: List[List[Any]] = []
    regions: List[Region] = await get_regions(config)
    if regions:

        def is_current(region: Region) -> bool:
            return region["host"] == config.get("host") or region["api_host"] == config.get("host")

        for index, region in enumerate(regions):
            table.append(
                [
                    index + 1,
                    region["name"].lower(),
                    region.get("provider") or "",
                    region["host"],
                    region["api_host"],
                    is_current(region),
                ]
            )
    else:
        table.append([1, "default", "", config["host"], True])

    columns: List[str] = ["idx", "region", "provider", "ui", "host", "current"]
    echo_safe_humanfriendly_tables_format_smart_table(table, column_names=columns)


@auth.command(name="use")
@click.argument("region_name_or_host_or_id")
@coro
async def auth_use(region_name_or_host_or_id: str) -> None:
    """Switch to a different region.
    You can pass the region name, the region host url, or the region index
    after listing available regions with 'tb auth ls'

    \b
    Example usage:
    \b
    $ tb auth use us-east
    $ tb auth use 1
    $ tb auth use https://ui.us-east.tinybird.co
    """

    config = CLIConfig.get_project_config()
    _ = await try_update_config_with_remote(config, raise_on_errors=False)

    regions, host = await get_host_from_region(config, region_name_or_host_or_id, config.get_host())
    config.set_host(host)

    if not await try_authenticate(config, regions):
        msg = FeedbackManager.error_wrong_config_file(config_file=config._path)
        raise CLIAuthException(msg)

    config.persist_to_file()
    click.echo(FeedbackManager.success_now_using_config(name=config["name"], id=config["id"]))
