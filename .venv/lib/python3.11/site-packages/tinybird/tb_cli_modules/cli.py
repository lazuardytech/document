# This is a command file for our CLI. Please keep it clean.
#
# - If it makes sense and only when strictly necessary, you can create utility functions in this file.
# - But please, **do not** interleave utility functions and command definitions.

import json
import logging
import os
import pprint
import re
import shutil
import sys
from os import environ, getcwd
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import click
import humanfriendly
from click import Context
from packaging import version

import tinybird.context as context
from tinybird.client import (
    AuthException,
    AuthNoTokenException,
    DoesNotExistException,
    OperationCanNotBePerformed,
    TinyB,
)
from tinybird.config import CURRENT_VERSION, SUPPORTED_CONNECTORS, VERSION, FeatureFlags, get_config
from tinybird.datafile import (
    AlreadyExistsException,
    CLIGitRelease,
    CLIGitReleaseException,
    Datafile,
    ParseException,
    build_graph,
    create_release,
    diff_command,
    folder_pull,
    folder_push,
    get_project_filenames,
    get_resource_versions,
    has_internal_datafiles,
    parse_datasource,
    parse_pipe,
    parse_token,
    wait_job,
)
from tinybird.feedback_manager import FeedbackManager
from tinybird.tb_cli_modules.cicd import check_cicd_exists, init_cicd
from tinybird.tb_cli_modules.common import (
    OLDEST_ROLLBACK,
    CatchAuthExceptions,
    CLIException,
    _get_tb_client,
    coro,
    create_tb_client,
    echo_safe_humanfriendly_tables_format_smart_table,
    folder_init,
    get_current_main_workspace,
    getenv_bool,
    is_major_semver,
    is_post_semver,
    load_connector_config,
    remove_release,
    try_update_config_with_remote,
)
from tinybird.tb_cli_modules.config import CLIConfig
from tinybird.tb_cli_modules.telemetry import add_telemetry_event

__old_click_echo = click.echo
__old_click_secho = click.secho
DEFAULT_PATTERNS: List[Tuple[str, Union[str, Callable[[str], str]]]] = [
    (r"p\.ey[A-Za-z0-9-_\.]+", lambda v: f"{v[:4]}...{v[-8:]}")
]


@click.group(cls=CatchAuthExceptions, context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--debug/--no-debug",
    default=False,
    help="Prints internal representation, can be combined with any command to get more information.",
)
@click.option("--token", help="Use auth token, defaults to TB_TOKEN envvar, then to the .tinyb file")
@click.option("--host", help="Use custom host, defaults to TB_HOST envvar, then to https://api.tinybird.co")
@click.option("--semver", help="Semver of a Release to run the command. Example: 1.0.0", hidden=True)
@click.option("--gcp-project-id", help="The Google Cloud project ID", hidden=True)
@click.option(
    "--gcs-bucket", help="The Google Cloud Storage bucket to write temp files when using the connectors", hidden=True
)
@click.option(
    "--google-application-credentials",
    envvar="GOOGLE_APPLICATION_CREDENTIALS",
    help="Set GOOGLE_APPLICATION_CREDENTIALS",
    hidden=True,
)
@click.option("--sf-account", help="The Snowflake Account (e.g. your-domain.west-europe.azure)", hidden=True)
@click.option("--sf-warehouse", help="The Snowflake warehouse name", hidden=True)
@click.option("--sf-database", help="The Snowflake database name", hidden=True)
@click.option("--sf-schema", help="The Snowflake schema name", hidden=True)
@click.option("--sf-role", help="The Snowflake role name", hidden=True)
@click.option("--sf-user", help="The Snowflake user name", hidden=True)
@click.option("--sf-password", help="The Snowflake password", hidden=True)
@click.option(
    "--sf-storage-integration",
    help="The Snowflake GCS storage integration name (leave empty to auto-generate one)",
    hidden=True,
)
@click.option("--sf-stage", help="The Snowflake GCS stage name (leave empty to auto-generate one)", hidden=True)
@click.option(
    "--with-headers", help="Flag to enable connector to export with headers", is_flag=True, default=False, hidden=True
)
@click.option(
    "--version-warning/--no-version-warning",
    envvar="TB_VERSION_WARNING",
    default=True,
    help="Don't print version warning message if there's a new available version. You can use TB_VERSION_WARNING envar",
)
@click.option("--show-tokens", is_flag=True, default=False, help="Enable the output of tokens")
@click.version_option(version=VERSION)
@click.pass_context
@coro
async def cli(
    ctx: Context,
    debug: bool,
    token: str,
    host: str,
    semver: str,
    gcp_project_id: str,
    gcs_bucket: str,
    google_application_credentials: str,
    sf_account: str,
    sf_warehouse: str,
    sf_database: str,
    sf_schema: str,
    sf_role: str,
    sf_user: str,
    sf_password: str,
    sf_storage_integration: str,
    sf_stage,
    with_headers: bool,
    version_warning: bool,
    show_tokens: bool,
) -> None:
    """
    Use `OBFUSCATE_REGEX_PATTERN` and `OBFUSCATE_PATTERN_SEPARATOR` environment variables to define a regex pattern and a separator (in case of a single string with multiple regex) to obfuscate secrets in the CLI output.
    """

    # We need to unpatch for our tests not to break
    if show_tokens:
        __unpatch_click_output()
    else:
        __patch_click_output()

    if getenv_bool("TB_DISABLE_SSL_CHECKS", False):
        click.echo(FeedbackManager.warning_disabled_ssl_checks())

    # ensure that ctx.obj exists and is a dict (in case `cli()` is called)
    # by means other than the `if` block below
    if not environ.get("PYTEST", None) and version_warning and not token:
        from tinybird.check_pypi import CheckPypi

        latest_version = await CheckPypi().get_latest_version()

        if "x.y.z" in CURRENT_VERSION:
            click.echo(FeedbackManager.warning_development_cli())

        if "x.y.z" not in CURRENT_VERSION and latest_version != CURRENT_VERSION:
            click.echo(FeedbackManager.warning_update_version(latest_version=latest_version))
            click.echo(FeedbackManager.warning_current_version(current_version=CURRENT_VERSION))

    if debug:
        logging.basicConfig(level=logging.DEBUG)

    config_temp = CLIConfig.get_project_config()
    if token:
        config_temp.set_token(token)
    if host:
        config_temp.set_host(host)
    if semver:
        config_temp.set_semver(semver)
    if token or host or semver:
        await try_update_config_with_remote(config_temp, auto_persist=False, raise_on_errors=False)

    # Overwrite token and host with env vars manually, without resorting to click.
    #
    # We need this to avoid confusing the new config class about where are
    # token and host coming from (we need to show the proper origin in
    # `tb auth info`)
    if not token and "TB_TOKEN" in os.environ:
        token = os.environ.get("TB_TOKEN", "")
    if not host and "TB_HOST" in os.environ:
        host = os.environ.get("TB_HOST", "")
    if not semver and "TB_SEMVER" in os.environ:
        semver = os.environ.get("TB_SEMVER", "")

    config = await get_config(host, token, semver)
    client = _get_tb_client(config.get("token", None), config["host"])

    # If they have passed a token or host as paramter and it's different that record in .tinyb, refresh the workspace id
    if token or host:
        try:
            workspace = await client.workspace_info()
            config["id"] = workspace.get("id", "")
        # If we can not get this info, we continue with the id on the file
        except (AuthNoTokenException, AuthException):
            pass

    ctx.ensure_object(dict)["config"] = config

    if ctx.invoked_subcommand == "auth":
        return

    from tinybird.connectors import create_connector

    if gcp_project_id and gcs_bucket and google_application_credentials and not sf_account:
        bq_config = {
            "project_id": gcp_project_id,
            "bucket_name": gcs_bucket,
            "service_account": google_application_credentials,
            "with_headers": with_headers,
        }
        ctx.ensure_object(dict)["bigquery"] = create_connector("bigquery", bq_config)
    if (
        sf_account
        and sf_warehouse
        and sf_database
        and sf_schema
        and sf_role
        and sf_user
        and sf_password
        and gcs_bucket
        and google_application_credentials
        and gcp_project_id
    ):
        sf_config = {
            "account": sf_account,
            "warehouse": sf_warehouse,
            "database": sf_database,
            "schema": sf_schema,
            "role": sf_role,
            "user": sf_user,
            "password": sf_password,
            "storage_integration": sf_storage_integration,
            "stage": sf_stage,
            "bucket_name": gcs_bucket,
            "service_account": google_application_credentials,
            "project_id": gcp_project_id,
            "with_headers": with_headers,
        }
        ctx.ensure_object(dict)["snowflake"] = create_connector("snowflake", sf_config)

    logging.debug("debug enabled")

    ctx.ensure_object(dict)["client"] = _get_tb_client(config.get("token", None), config["host"], semver)

    for connector in SUPPORTED_CONNECTORS:
        load_connector_config(ctx, connector, debug, check_uninstalled=True)


@cli.command()
@click.option(
    "--generate-datasources",
    is_flag=True,
    default=False,
    help="Generate datasources based on CSV, NDJSON and Parquet files in this folder",
)
@click.option(
    "--folder",
    default=None,
    type=click.Path(exists=True, file_okay=False),
    help="Folder where datafiles will be placed",
)
@click.option("-f", "--force", is_flag=True, default=False, help="Overrides existing files")
@click.option(
    "-ir",
    "--ignore-remote",
    is_flag=True,
    default=False,
    help="Ignores remote files not present in the local data project on git init",
)
@click.option(
    "--git",
    is_flag=True,
    default=False,
    help="Init workspace with git releases. Generates CI/CD files for your git provider",
)
@click.option(
    "--override-commit",
    default=None,
    help="Use this option to manually override the reference commit of your workspace. This is useful if a commit is not recognized in your git log, such as after a force push (git push -f).",
)
@click.option(
    "--cicd", is_flag=True, default=False, help="Generates only CI/CD files for your git provider", hidden=True
)
@click.pass_context
@coro
async def init(
    ctx: Context,
    generate_datasources: bool,
    folder: Optional[str],
    force: bool,
    ignore_remote: bool,
    git: bool,
    override_commit: Optional[str],
    cicd: Optional[bool],
) -> None:
    """Initialize folder layout."""
    client: TinyB = ctx.ensure_object(dict)["client"]
    config = CLIConfig.get_project_config()
    if config.get("token") is None:
        raise AuthNoTokenException
    folder = folder if folder else getcwd()

    workspaces: List[Dict[str, Any]] = (await client.user_workspaces_and_branches()).get("workspaces", [])
    current_ws: Dict[str, Any] = next(
        (workspace for workspace in workspaces if config and workspace.get("id", ".") == config.get("id", "..")), {}
    )

    if current_ws.get("is_branch"):
        raise CLIException(FeedbackManager.error_not_allowed_in_branch())

    await folder_init(client, folder, generate_datasources, generate_releases=True, force=force)

    if not (git or override_commit or cicd):
        return

    sync_git = git or override_commit
    cli_git_release = None
    error = False
    final_response = None
    try:
        cli_git_release = CLIGitRelease(path=folder)
    except CLIGitReleaseException:
        raise CLIGitReleaseException(FeedbackManager.error_no_git_repo_for_init(repo_path=folder))

    if sync_git:
        if not cli_git_release.is_main_branch() and not override_commit:
            raise CLIGitReleaseException(FeedbackManager.error_no_git_main_branch())

        if not cli_git_release.is_dottinyb_ignored():
            raise CLIGitReleaseException(
                FeedbackManager.error_dottinyb_not_ignored(git_working_dir=f"{cli_git_release.working_dir()}/")
            )
        else:
            click.echo(FeedbackManager.info_dottinyb_already_ignored())

        if (
            os.path.exists(f"{cli_git_release.working_dir()}/.diff_tmp")
            and not cli_git_release.is_dotdifftemp_ignored()
        ):
            raise CLIGitReleaseException(
                FeedbackManager.error_dotdiff_not_ignored(git_working_dir=f"{cli_git_release.working_dir()}/")
            )
        else:
            click.echo(FeedbackManager.info_dotdifftemp_already_ignored())

        if "release" not in current_ws:
            raise CLIGitReleaseException(FeedbackManager.error_no_correct_token_for_init())

        # If we have a release and we are not overriding the commit, we check if we have a release already
        elif current_ws.get("release") and not override_commit:
            final_response = FeedbackManager.error_release_already_set(
                workspace=current_ws["name"], commit=current_ws["release"]["commit"]
            )
            error = True

        elif override_commit:
            if click.confirm(
                FeedbackManager.prompt_init_git_release_force(
                    current_commit=current_ws["release"]["commit"], new_commit=override_commit
                )
            ):
                try:
                    release = await cli_git_release.update_release(client, current_ws, override_commit)
                except Exception as exc:
                    raise CLIGitReleaseException(FeedbackManager.error_exception(error=str(exc)))

                final_response = FeedbackManager.success_init_git_release(
                    workspace_name=current_ws["name"], release_commit=release["commit"]
                )
            else:
                return

        else:
            click.echo(FeedbackManager.info_no_git_release_yet(workspace=current_ws["name"]))
            click.echo(FeedbackManager.info_diff_resources_for_git_init())
            changed = await diff_command(
                [], True, client, with_print=False, verbose=False, clean_up=True, progress_bar=True
            )
            changed = {
                k: v
                for k, v in changed.items()
                if v is not None and (not ignore_remote or v not in ["remote", "shared"])
            }
            if changed:
                tb_pull_command = "tb pull --force"
                click.echo(FeedbackManager.warning_git_release_init_with_diffs())
                if click.confirm(FeedbackManager.prompt_init_git_release_pull(pull_command=tb_pull_command)):
                    await folder_pull(client, folder, auto=True, match=None, force=True)

                else:
                    raise CLIGitReleaseException(
                        FeedbackManager.error_diff_resources_for_git_init(
                            workspace=current_ws["name"], pull_command=tb_pull_command
                        )
                    )
            else:
                click.echo(FeedbackManager.info_git_release_init_without_diffs(workspace=current_ws["name"]))
            if cli_git_release.is_dirty_to_init():
                raise CLIGitReleaseException(
                    FeedbackManager.error_commit_changes_to_init_release(
                        path=cli_git_release.path, git_output=cli_git_release.status()
                    )
                )
            try:
                release = await cli_git_release.update_release(client, current_ws)
            except Exception as exc:
                raise CLIGitReleaseException(FeedbackManager.error_exception(error=str(exc)))

            final_response = FeedbackManager.success_init_git_release(
                workspace_name=current_ws["name"], release_commit=release["commit"]
            )

    if not override_commit:
        cicd_provider = await check_cicd_exists(cli_git_release.working_dir())
        if not force and cicd_provider:
            if cicd:
                final_response = FeedbackManager.error_cicd_already_exists(provider=cicd_provider.name)
                error = True
            else:
                click.echo(FeedbackManager.info_cicd_already_exists(provider=cicd_provider.name))
        else:
            if cicd:
                data_project_dir = os.path.relpath(folder, cli_git_release.working_dir())
                await init_cicd(client, path=cli_git_release.working_dir(), data_project_dir=data_project_dir)

    if final_response:
        if error:
            raise CLIException(final_response)
        click.echo(final_response)


@cli.command()
@click.argument("filenames", type=click.Path(exists=True), nargs=-1, default=None)
@click.option("--debug", is_flag=True, default=False, help="Print internal representation")
def check(filenames: List[str], debug: bool) -> None:
    """Check file syntax."""

    if not filenames:
        filenames = get_project_filenames(".")

    def process(filenames: Iterable):
        parser_matrix = {".pipe": parse_pipe, ".datasource": parse_datasource, ".token": parse_token}
        incl_suffix = ".incl"
        try:
            for filename in filenames:
                if os.path.isdir(filename):
                    process(filenames=get_project_filenames(filename))

                click.echo(FeedbackManager.info_processing_file(filename=filename))

                file_suffix = Path(filename).suffix
                if file_suffix == incl_suffix:
                    click.echo(FeedbackManager.info_ignoring_incl_file(filename=filename))
                    continue

                doc: Datafile
                parser = parser_matrix.get(file_suffix)
                if not parser:
                    raise ParseException(FeedbackManager.error_unsupported_datafile(extension=file_suffix))

                doc = parser(filename)

                click.echo(FeedbackManager.success_processing_file(filename=filename))
                if debug:
                    pp = pprint.PrettyPrinter()
                    for x in doc.nodes:
                        pp.pprint(x)

        except ParseException as e:
            raise CLIException(FeedbackManager.error_exception(error=e))

    process(filenames=filenames)


@cli.command()
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Run the command without creating resources on the Tinybird account or any side effect",
)
@click.option(
    "--check/--no-check", is_flag=True, default=True, help="Enable/Disable output checking, enabled by default"
)
@click.option("--push-deps", is_flag=True, default=False, help="Push dependencies, disabled by default")
@click.option(
    "--only-changes",
    is_flag=True,
    default=False,
    help="Push only the resources that have changed compared to the destination workspace",
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
    help="Prints internal representation, can be combined with any command to get more information.",
)
@click.option("-f", "--force", is_flag=True, default=False, help="Override pipes when they already exist")
@click.option(
    "--override-datasource",
    is_flag=True,
    default=False,
    help="When pushing a pipe with a Materialized node if the target Data Source exists it will try to override it.",
)
@click.option("--populate", is_flag=True, default=False, help="Populate materialized nodes when pushing them")
@click.option(
    "--subset",
    type=float,
    default=None,
    help="Populate with a subset percent of the data (limited to a maximum of 2M rows), this is useful to quickly test a materialized node with some data. The subset must be greater than 0 and lower than 0.1. A subset of 0.1 means a 10 percent of the data in the source Data Source will be used to populate the materialized view. Use it together with --populate, it has precedence over --sql-condition",
)
@click.option(
    "--sql-condition",
    type=str,
    default=None,
    help="Populate with a SQL condition to be applied to the trigger Data Source of the Materialized View. For instance, `--sql-condition='date == toYYYYMM(now())'` it'll populate taking all the rows from the trigger Data Source which `date` is the current month. Use it together with --populate. --sql-condition is not taken into account if the --subset param is present. Including in the ``sql_condition`` any column present in the Data Source ``engine_sorting_key`` will make the populate job process less data.",
)
@click.option(
    "--unlink-on-populate-error",
    is_flag=True,
    default=False,
    help="If the populate job fails the Materialized View is unlinked and new data won't be ingested in the Materialized View. First time a populate job fails, the Materialized View is always unlinked.",
)
@click.option("--fixtures", is_flag=True, default=False, help="Append fixtures to data sources")
@click.option(
    "--wait",
    is_flag=True,
    default=False,
    help="To be used along with --populate command. Waits for populate jobs to finish, showing a progress bar. Disabled by default.",
)
@click.option("--yes", is_flag=True, default=False, help="Do not ask for confirmation")
@click.option(
    "--only-response-times", is_flag=True, default=False, help="Checks only response times, when --force push a pipe"
)
@click.argument("filenames", type=click.Path(exists=True), nargs=-1, default=None)
@click.option("--workspace_map", nargs=2, type=str, multiple=True)
@click.option(
    "--workspace",
    nargs=2,
    type=str,
    multiple=True,
    help="add a workspace path to the list of external workspaces, usage: --workspace name path/to/folder",
)
@click.option(
    "--no-versions",
    is_flag=True,
    default=False,
    help="when set, resource dependency versions are not used, it pushes the dependencies as-is",
)
@click.option(
    "-l", "--limit", type=click.IntRange(0, 100), default=0, required=False, help="Number of requests to validate"
)
@click.option(
    "--sample-by-params",
    type=click.IntRange(1, 100),
    default=1,
    required=False,
    help="When set, we will aggregate the pipe_stats_rt requests by extractURLParameterNames(assumeNotNull(url)) and for each combination we will take a sample of N requests",
)
@click.option(
    "-ff", "--failfast", is_flag=True, default=False, help="When set, the checker will exit as soon one test fails"
)
@click.option(
    "--ignore-order", is_flag=True, default=False, help="When set, the checker will ignore the order of list properties"
)
@click.option(
    "--validate-processed-bytes",
    is_flag=True,
    default=False,
    help="When set, the checker will validate that the new version doesn't process more than 25% than the current version",
)
@click.option(
    "--check-requests-from-main",
    is_flag=True,
    default=False,
    help="When set, the checker will get Main Workspace requests",
    hidden=True,
)
@click.option(
    "--folder",
    default=".",
    help="Folder from where to execute the command. By default the current folder",
    hidden=True,
    type=click.types.STRING,
)
@click.option(
    "--user_token",
    is_flag=False,
    default=None,
    help="The user token is required for sharing a datasource that contains the SHARED_WITH entry.",
    type=click.types.STRING,
)
@click.pass_context
@coro
async def push(
    ctx: Context,
    filenames: Optional[List[str]],
    dry_run: bool,
    check: bool,
    push_deps: bool,
    only_changes: bool,
    debug: bool,
    force: bool,
    override_datasource: bool,
    populate: bool,
    subset: Optional[float],
    sql_condition: Optional[str],
    unlink_on_populate_error: bool,
    fixtures: bool,
    wait: bool,
    yes: bool,
    only_response_times: bool,
    workspace_map,
    workspace,
    no_versions: bool,
    limit: int,
    sample_by_params: int,
    failfast: bool,
    ignore_order: bool,
    validate_processed_bytes: bool,
    check_requests_from_main: bool,
    folder: str,
    user_token: Optional[str],
) -> None:
    """Push files to Tinybird."""

    ignore_sql_errors = FeatureFlags.ignore_sql_errors()
    context.disable_template_security_validation.set(True)

    is_internal = has_internal_datafiles(folder)

    await folder_push(
        create_tb_client(ctx),
        filenames,
        dry_run,
        check,
        push_deps,
        only_changes,
        debug=debug,
        force=force,
        override_datasource=override_datasource,
        populate=populate,
        populate_subset=subset,
        populate_condition=sql_condition,
        unlink_on_populate_error=unlink_on_populate_error,
        upload_fixtures=fixtures,
        wait=wait,
        ignore_sql_errors=ignore_sql_errors,
        skip_confirmation=yes,
        only_response_times=only_response_times,
        workspace_map=dict(workspace_map),
        workspace_lib_paths=workspace,
        no_versions=no_versions,
        run_tests=False,
        tests_to_run=limit,
        tests_sample_by_params=sample_by_params,
        tests_failfast=failfast,
        tests_ignore_order=ignore_order,
        tests_validate_processed_bytes=validate_processed_bytes,
        tests_check_requests_from_branch=check_requests_from_main,
        folder=folder,
        config=CLIConfig.get_project_config(),
        user_token=user_token,
        is_internal=is_internal,
    )
    return


@cli.command()
@click.option(
    "--folder", default=None, type=click.Path(exists=True, file_okay=False), help="Folder where files will be placed"
)
@click.option(
    "--auto/--no-auto",
    is_flag=True,
    default=True,
    help="Saves datafiles automatically into their default directories (/datasources or /pipes). Default is True",
)
@click.option("--match", default=None, help="Retrieve any resourcing matching the pattern. eg --match _test")
@click.option("-f", "--force", is_flag=True, default=False, help="Override existing files")
@click.option("--fmt", is_flag=True, default=False, help="Format files before saving")
@click.pass_context
@coro
async def pull(ctx: Context, folder: str, auto: bool, match: Optional[str], force: bool, fmt: bool) -> None:
    """Retrieve latest versions for project files from Tinybird."""

    client = ctx.ensure_object(dict)["client"]
    folder = folder if folder else getcwd()

    return await folder_pull(client, folder, auto, match, force, fmt=fmt)


@cli.command()
@click.option("--no-deps", is_flag=True, default=False, help="Print only data sources with no pipes using them")
@click.option("--match", default=None, help="Retrieve any resource matching the pattern")
@click.option("--pipe", default=None, help="Retrieve any resource used by pipe")
@click.option("--datasource", default=None, help="Retrieve resources depending on this Data Source")
@click.option(
    "--check-for-partial-replace",
    is_flag=True,
    default=False,
    help="Retrieve dependant Data Sources that will have their data replaced if a partial replace is executed in the Data Source selected",
)
@click.option("--recursive", is_flag=True, default=False, help="Calculate recursive dependencies")
@click.pass_context
@coro
async def dependencies(
    ctx: Context,
    no_deps: bool,
    match: Optional[str],
    pipe: Optional[str],
    datasource: Optional[str],
    check_for_partial_replace: bool,
    recursive: bool,
) -> None:
    """Print all data sources dependencies."""

    client = ctx.ensure_object(dict)["client"]

    response = await client.datasource_dependencies(
        no_deps, match, pipe, datasource, check_for_partial_replace, recursive
    )
    for ds in response["dependencies"]:
        click.echo(FeedbackManager.info_dependency_list(dependency=ds))
        for pipe in response["dependencies"][ds]:
            click.echo(FeedbackManager.info_dependency_list_item(dependency=pipe))
    if "incompatible_datasources" in response and len(response["incompatible_datasources"]):
        click.echo(FeedbackManager.info_no_compatible_dependencies_found())
        for ds in response["incompatible_datasources"]:
            click.echo(FeedbackManager.info_dependency_list(dependency=ds))
        raise CLIException(FeedbackManager.error_partial_replace_cant_be_executed(datasource=datasource))


@cli.command(
    name="diff",
    short_help="Diffs local datafiles to the corresponding remote files in the workspace. For the case of .datasource files it just diffs VERSION and SCHEMA, since ENGINE, KAFKA or other metadata is considered immutable.",
)
@click.argument("filename", type=click.Path(exists=True), nargs=-1, required=False)
@click.option(
    "--fmt/--no-fmt",
    is_flag=True,
    default=True,
    help="Format files before doing the diff, default is True so both files match the format",
)
@click.option("--no-color", is_flag=True, default=False, help="Don't colorize diff")
@click.option(
    "--no-verbose", is_flag=True, default=False, help="List the resources changed not the content of the diff"
)
@click.option(
    "--main",
    is_flag=True,
    default=False,
    help="Diffs local datafiles to the corresponding remote files in the main workspace. Only works when authenticated on a Branch.",
    hidden=True,
)
@click.pass_context
@coro
async def diff(
    ctx: Context, filename: Optional[Tuple], fmt: bool, no_color: bool, no_verbose: bool, main: bool
) -> None:
    only_resources_changed = no_verbose
    client: TinyB = ctx.ensure_object(dict)["client"]

    if not main:
        changed = await diff_command(
            list(filename) if filename else None, fmt, client, no_color, with_print=not only_resources_changed
        )
    else:
        config = CLIConfig.get_project_config()

        response = await client.user_workspaces_and_branches()
        ws_client = None
        for workspace in response["workspaces"]:
            if config["id"] == workspace["id"]:
                if not workspace.get("is_branch"):
                    raise CLIException(FeedbackManager.error_not_a_branch())

                origin = workspace["main"]
                workspace = await get_current_main_workspace(config)

                if not workspace:
                    raise CLIException(FeedbackManager.error_workspace(workspace=origin))

                ws_client = _get_tb_client(workspace["token"], config["host"])
                break

        if not ws_client:
            raise CLIException(FeedbackManager.error_workspace(workspace=origin))
        changed = await diff_command(
            list(filename) if filename else None, fmt, ws_client, no_color, with_print=not only_resources_changed
        )

    if only_resources_changed:
        click.echo("\n")
        for resource, status in dict(sorted(changed.items(), key=lambda item: str(item[1]))).items():
            if status is None:
                continue
            status = "changed" if status not in ["remote", "local", "shared"] else status
            click.echo(f"{status}: {resource}")


@cli.command()
@click.argument("query", required=False)
@click.option("--rows_limit", default=100, help="Max number of rows retrieved")
@click.option("--pipeline", default=None, help="The name of the Pipe to run the SQL Query")
@click.option("--pipe", default=None, help="The path to the .pipe file to run the SQL Query of a specific NODE")
@click.option("--node", default=None, help="The NODE name")
@click.option(
    "--format",
    "format_",
    type=click.Choice(["json", "csv", "human"], case_sensitive=False),
    default="human",
    help="Output format",
)
@click.option("--stats/--no-stats", default=False, help="Show query stats")
@click.pass_context
@coro
async def sql(
    ctx: Context,
    query: str,
    rows_limit: int,
    pipeline: Optional[str],
    pipe: Optional[str],
    node: Optional[str],
    format_: str,
    stats: bool,
) -> None:
    """Run SQL query over data sources and pipes."""

    client = ctx.ensure_object(dict)["client"]
    req_format = "CSVWithNames" if format_ == "csv" else "JSON"
    res = None
    try:
        if query:
            q = query.lower().strip()
            if q.startswith("insert"):
                click.echo(FeedbackManager.info_append_data())
                raise CLIException(FeedbackManager.error_invalid_query())
            if q.startswith("delete"):
                raise CLIException(FeedbackManager.error_invalid_query())
            res = await client.query(
                f"SELECT * FROM ({query}) LIMIT {rows_limit} FORMAT {req_format}", pipeline=pipeline
            )
        elif pipe and node:
            datasources: List[Dict[str, Any]] = await client.datasources()
            pipes: List[Dict[str, Any]] = await client.pipes()

            existing_resources: List[str] = [x["name"] for x in datasources] + [x["name"] for x in pipes]
            resource_versions = get_resource_versions(existing_resources)
            filenames = [pipe]

            # build graph to get new versions for all the files involved in the query
            # dependencies need to be processed always to get the versions
            dependencies_graph = await build_graph(
                filenames,
                client,
                dir_path=".",
                process_dependencies=True,
                skip_connectors=True,
            )

            # update existing versions
            latest_datasource_versions = resource_versions.copy()

            for dep in dependencies_graph.to_run.values():
                ds = dep["resource_name"]
                if dep["version"] is not None:
                    latest_datasource_versions[ds] = dep["version"]

            # build the graph again with the rigth version
            dependencies_graph = await build_graph(
                filenames,
                client,
                dir_path=".",
                resource_versions=latest_datasource_versions,
                verbose=False,
            )
            query = ""
            for _, elem in dependencies_graph.to_run.items():
                for _node in elem["nodes"]:
                    if _node["params"]["name"].lower() == node.lower():
                        query = "".join(_node["sql"])
            pipeline = pipe.split("/")[-1].split(".pipe")[0]
            res = await client.query(
                f"SELECT * FROM ({query}) LIMIT {rows_limit} FORMAT {req_format}", pipeline=pipeline
            )

    except AuthNoTokenException:
        raise
    except Exception as e:
        raise CLIException(FeedbackManager.error_exception(error=str(e)))

    if isinstance(res, dict) and "error" in res:
        raise CLIException(FeedbackManager.error_exception(error=res["error"]))

    if stats:
        stats_query = f"SELECT * FROM ({query}) LIMIT {rows_limit} FORMAT JSON"
        stats_res = await client.query(stats_query, pipeline=pipeline)
        stats_dict = stats_res["statistics"]
        seconds = stats_dict["elapsed"]
        rows_read = humanfriendly.format_number(stats_dict["rows_read"])
        bytes_read = humanfriendly.format_size(stats_dict["bytes_read"])
        click.echo(FeedbackManager.info_query_stats(seconds=seconds, rows=rows_read, bytes=bytes_read))

    if format_ == "csv":
        click.echo(res)
    elif isinstance(res, dict) and "data" in res and res["data"]:
        if format_ == "json":
            click.echo(json.dumps(res, indent=8))
        else:
            dd = []
            for d in res["data"]:
                dd.append(d.values())
            echo_safe_humanfriendly_tables_format_smart_table(dd, column_names=res["data"][0].keys())
    else:
        click.echo(FeedbackManager.info_no_rows())


@cli.command(
    name="materialize",
    short_help="Given a local Pipe datafile (.pipe) and a node name it generates the target Data Source and materialized Pipe ready to be pushed and guides you through the process to create the materialized view",
)
@click.argument("filename", type=click.Path(exists=True))
@click.argument("target_datasource", default=None, required=False)
@click.option("--push-deps", is_flag=True, default=False, help="Push dependencies, disabled by default")
@click.option("--workspace_map", nargs=2, type=str, multiple=True, hidden=True)
@click.option(
    "--workspace",
    nargs=2,
    type=str,
    multiple=True,
    help="add a workspace path to the list of external workspaces, usage: --workspace name path/to/folder",
)
@click.option(
    "--no-versions",
    is_flag=True,
    default=False,
    help="when set, resource dependency versions are not used, it pushes the dependencies as-is",
)
@click.option("--verbose", is_flag=True, default=False, help="Prints more log")
@click.option("--force-populate", default=None, required=False, help="subset or full", hidden=True)
@click.option(
    "--unlink-on-populate-error",
    is_flag=True,
    default=False,
    help="If the populate job fails the Materialized View is unlinked and new data won't be ingested in the Materialized View. First time a populate job fails, the Materialized View is always unlinked.",
)
@click.option("--override-pipe", is_flag=True, default=False, help="Override pipe if exists or prompt", hidden=True)
@click.option(
    "--override-datasource", is_flag=True, default=False, help="Override data source if exists or prompt", hidden=True
)
@click.pass_context
@coro
async def materialize(
    ctx: Context,
    filename: str,
    push_deps: bool,
    workspace_map: List[str],
    workspace: List[str],
    no_versions: bool,
    verbose: bool,
    force_populate: Optional[str],
    unlink_on_populate_error: bool,
    override_pipe: bool,
    override_datasource: bool,
    target_datasource: Optional[str] = None,
) -> None:
    deprecation_notice = FeedbackManager.warning_deprecated(
        warning="'tb materialize' is deprecated. To create a Materialized View in a guided way you can use the UI: `Create Pipe` and use the `Create Materialized View` wizard. Finally download the resulting `.pipe` and `.datasource` files."
    )
    click.echo(deprecation_notice)
    cl = create_tb_client(ctx)

    async def _try_push_pipe_to_analyze(pipe_name):
        try:
            to_run = await folder_push(
                cl,
                filenames=[filename],
                dry_run=False,
                check=False,
                push_deps=push_deps,
                debug=False,
                force=False,
                workspace_map=dict(workspace_map),
                workspace_lib_paths=workspace,
                no_versions=no_versions,
                run_tests=False,
                as_standard=True,
                raise_on_exists=True,
                verbose=verbose,
            )
        except AlreadyExistsException as e:
            if "Datasource" in str(e):
                click.echo(str(e))
                return
            if override_pipe or click.confirm(FeedbackManager.info_pipe_exists(name=pipe_name)):
                to_run = await folder_push(
                    cl,
                    filenames=[filename],
                    dry_run=False,
                    check=False,
                    push_deps=push_deps,
                    debug=False,
                    force=True,
                    workspace_map=dict(workspace_map),
                    workspace_lib_paths=workspace,
                    no_versions=no_versions,
                    run_tests=False,
                    as_standard=True,
                    verbose=verbose,
                )
            else:
                return
        except click.ClickException as ex:
            # HACK: By now, datafile raises click.ClickException instead of
            # CLIException to avoid circular imports. Thats we need to trace
            # the error here.
            #
            # Once we do a big refactor in datafile, we can get rid of this
            # snippet.
            msg: str = str(ex)
            add_telemetry_event("datafile_error", error=msg)
            click.echo(msg)

        return to_run

    def _choose_node_name(pipe):
        node = pipe["nodes"][0]
        materialized_nodes = [node for node in pipe["nodes"] if node["type"].lower() == "materialized"]

        if len(materialized_nodes) == 1:
            node = materialized_nodes[0]

        if len(pipe["nodes"]) > 1 and len(materialized_nodes) != 1:
            for index, node in enumerate(pipe["nodes"], start=1):
                click.echo(f"  [{index}] Materialize node with name => {node['name']}")
            option = click.prompt(FeedbackManager.prompt_choose_node(), default=len(pipe["nodes"]))
            node = pipe["nodes"][option - 1]
        node_name = node["name"]
        return node, node_name

    def _choose_target_datasource_name(pipe, node, node_name):
        return target_datasource or node.get("datasource", None) or f'mv_{pipe["resource_name"]}_{node_name}'

    def _save_local_backup_pipe(pipe):
        pipe_bak = f"{filename}_bak"
        shutil.copyfile(filename, pipe_bak)
        pipe_file_name = f"{pipe['resource_name']}.pipe"
        click.echo(FeedbackManager.info_pipe_backup_created(name=pipe_bak))
        return pipe_file_name

    def _save_local_datasource(datasource_name, ds_datafile):
        base = Path("datasources")
        if not base.exists():
            base = Path()
        file_name = f"{datasource_name}.datasource"
        f = base / file_name
        with open(f"{f}", "w") as file:
            file.write(ds_datafile)

        click.echo(FeedbackManager.success_generated_local_file(file=f))
        return f

    async def _try_push_datasource(datasource_name, f):
        exists = False
        try:
            exists = await cl.get_datasource(datasource_name)
        except Exception:
            pass

        if exists:
            click.echo(FeedbackManager.info_materialize_push_datasource_exists(name=f.name))
            if override_datasource or click.confirm(FeedbackManager.info_materialize_push_datasource_override(name=f)):
                try:
                    await cl.datasource_delete(datasource_name, force=True)
                except DoesNotExistException:
                    pass

        filename = str(f.absolute())
        to_run = await folder_push(
            cl,
            filenames=[filename],
            push_deps=push_deps,
            workspace_map=dict(workspace_map),
            workspace_lib_paths=workspace,
            no_versions=no_versions,
            verbose=verbose,
        )
        return to_run

    def _save_local_pipe(pipe_file_name, pipe_datafile, pipe):
        base = Path("pipes")
        if not base.exists():
            base = Path()
        f_pipe = base / pipe_file_name

        with open(f"{f_pipe}", "w") as file:
            if pipe["version"] is not None and pipe["version"] >= 0:
                pipe_datafile = f"VERSION {pipe['version']} \n {pipe_datafile}"
            matches = re.findall(r"([^\s\.]*__v\d+)", pipe_datafile)
            for match in set(matches):
                m = match.split("__v")[0]
                if m in pipe_datafile:
                    pipe_datafile = pipe_datafile.replace(match, m)
            file.write(pipe_datafile)

        click.echo(FeedbackManager.success_generated_local_file(file=f_pipe))
        return f_pipe

    async def _try_push_pipe(f_pipe):
        if override_pipe:
            option = 2
        else:
            click.echo(FeedbackManager.info_materialize_push_pipe_skip(name=f_pipe.name))
            click.echo(FeedbackManager.info_materialize_push_pipe_override(name=f_pipe.name))
            option = click.prompt(FeedbackManager.prompt_choose(), default=1)
        force = True
        check = option == 1

        filename = str(f_pipe.absolute())
        to_run = await folder_push(
            cl,
            filenames=[filename],
            dry_run=False,
            check=check,
            push_deps=push_deps,
            debug=False,
            force=force,
            unlink_on_populate_error=unlink_on_populate_error,
            workspace_map=dict(workspace_map),
            workspace_lib_paths=workspace,
            no_versions=no_versions,
            run_tests=False,
            verbose=verbose,
        )
        return to_run

    async def _populate(pipe, node_name, f_pipe):
        if force_populate or click.confirm(FeedbackManager.prompt_populate(file=f_pipe)):
            if not force_populate:
                click.echo(FeedbackManager.info_materialize_populate_partial())
                click.echo(FeedbackManager.info_materialize_populate_full())
                option = click.prompt(FeedbackManager.prompt_choose(), default=1)
            else:
                option = 1 if force_populate == "subset" else 2
            populate = False
            populate_subset = False
            if option == 1:
                populate_subset = 0.1
                populate = True
            elif option == 2:
                populate = True

            if populate:
                response = await cl.populate_node(
                    pipe["name"],
                    node_name,
                    populate_subset=populate_subset,
                    unlink_on_populate_error=unlink_on_populate_error,
                )
                if "job" not in response:
                    raise CLIException(response)

                job_url = response.get("job", {}).get("job_url", None)
                job_id = response.get("job", {}).get("job_id", None)
                click.echo(FeedbackManager.info_populate_job_url(url=job_url))
                wait_populate = True
                if wait_populate:
                    await wait_job(cl, job_id, job_url, "Populating")

    click.echo(FeedbackManager.warning_beta_tester())
    pipe_name = os.path.basename(filename).rsplit(".", 1)[0]
    click.echo(FeedbackManager.info_before_push_materialize(name=filename))
    try:
        # extracted the materialize logic to local functions so the workflow is more readable
        to_run = await _try_push_pipe_to_analyze(pipe_name)

        if to_run is None:
            return

        pipe = to_run[pipe_name.split("/")[-1]]
        node, node_name = _choose_node_name(pipe)
        datasource_name = _choose_target_datasource_name(pipe, node, node_name)

        click.echo(FeedbackManager.info_before_materialize(name=pipe["name"]))
        analysis = await cl.analyze_pipe_node(pipe["name"], node, datasource_name=datasource_name)
        ds_datafile = analysis["analysis"]["datasource"]["datafile"]
        pipe_datafile = analysis["analysis"]["pipe"]["datafile"]

        pipe_file_name = _save_local_backup_pipe(pipe)
        f = _save_local_datasource(datasource_name, ds_datafile)
        await _try_push_datasource(datasource_name, f)

        f_pipe = _save_local_pipe(pipe_file_name, pipe_datafile, pipe)
        await _try_push_pipe(f_pipe)
        await _populate(pipe, node_name, f_pipe)
        click.echo(FeedbackManager.success_created_matview(name=datasource_name))
    except AuthNoTokenException:
        raise
    except Exception as e:
        raise CLIException(FeedbackManager.error_exception(error=str(e)))


def __patch_click_output():
    import re

    CUSTOM_PATTERNS: List[str] = []

    _env_patterns = os.getenv("OBFUSCATE_REGEX_PATTERN", None)
    if _env_patterns:
        CUSTOM_PATTERNS = _env_patterns.split(os.getenv("OBFUSCATE_PATTERN_SEPARATOR", "|"))

    def _obfuscate(msg: Any, *args: Any, **kwargs: Any) -> Any:
        for pattern in CUSTOM_PATTERNS:
            msg = re.sub(pattern, "****...****", str(msg))

        for pattern, substitution in DEFAULT_PATTERNS:
            if isinstance(substitution, str):
                msg = re.sub(pattern, substitution, str(msg))
            else:
                msg = re.sub(pattern, lambda m: substitution(m.group(0)), str(msg))  # noqa
        return msg

    def _obfuscate_echo(msg: Any, *args: Any, **kwargs: Any) -> None:
        msg = _obfuscate(msg, *args, **kwargs)
        __old_click_echo(msg, *args, **kwargs)

    def _obfuscate_secho(msg: Any, *args: Any, **kwargs: Any) -> None:
        msg = _obfuscate(msg, *args, **kwargs)
        __old_click_secho(msg, *args, **kwargs)

    click.echo = lambda msg, *args, **kwargs: _obfuscate_echo(msg, *args, **kwargs)
    click.secho = lambda msg, *args, **kwargs: _obfuscate_secho(msg, *args, **kwargs)


def __unpatch_click_output():
    click.echo = __old_click_echo
    click.secho = __old_click_echo


@cli.command(short_help="Learn how to include info about the CLI in your shell PROMPT")
@click.pass_context
@coro
async def prompt(_ctx: Context) -> None:
    click.secho("Follow these instructions => https://www.tinybird.co/docs/cli.html#configure-the-shell-prompt")


@cli.command()
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Run the command without creating resources on the Tinybird account or any side effect",
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
    help="Prints internal representation, can be combined with any command to get more information.",
)
@click.option(
    "--override-datasource",
    is_flag=True,
    default=False,
    help="When pushing a pipe with a Materialized node if the target Data Source exists it will try to override it.",
)
@click.option("--populate", is_flag=True, default=False, help="Populate materialized nodes when pushing them")
@click.option(
    "--subset",
    type=float,
    default=None,
    help="Populate with a subset percent of the data (limited to a maximum of 2M rows), this is useful to quickly test a materialized node with some data. The subset must be greater than 0 and lower than 0.1. A subset of 0.1 means a 10 percent of the data in the source Data Source will be used to populate the materialized view. Use it together with --populate, it has precedence over --sql-condition",
)
@click.option(
    "--sql-condition",
    type=str,
    default=None,
    help="Populate with a SQL condition to be applied to the trigger Data Source of the Materialized View. For instance, `--sql-condition='date == toYYYYMM(now())'` it'll populate taking all the rows from the trigger Data Source which `date` is the current month. Use it together with --populate. --sql-condition is not taken into account if the --subset param is present. Including in the ``sql_condition`` any column present in the Data Source ``engine_sorting_key`` will make the populate job process less data.",
)
@click.option(
    "--unlink-on-populate-error",
    is_flag=True,
    default=False,
    help="If the populate job fails the Materialized View is unlinked and new data won't be ingested in the Materialized View. First time a populate job fails, the Materialized View is always unlinked.",
)
@click.option(
    "--wait",
    is_flag=True,
    default=False,
    help="To be used along with --populate command. Waits for populate jobs to finish, showing a progress bar. Disabled by default.",
)
@click.option("--yes", is_flag=True, default=False, help="Do not ask for confirmation")
@click.option("--workspace_map", nargs=2, type=str, multiple=True)
@click.option(
    "--workspace",
    nargs=2,
    type=str,
    multiple=True,
    help="add a workspace path to the list of external workspaces, usage: --workspace name path/to/folder",
)
@click.option(
    "--folder",
    default=".",
    help="Folder from where to execute the command. By default the current folder",
    hidden=True,
    type=click.types.STRING,
)
@click.option(
    "--user_token",
    is_flag=False,
    default=None,
    help="The user token is required for sharing a datasource that contains the SHARED_WITH entry.",
    type=click.types.STRING,
)
@click.option(
    "--fork-downstream",
    is_flag=True,
    default=False,
    help="Creates new versions of the dependent downstream resources to the changed resources.",
    hidden=True,
)
@click.option(
    "--fork",
    is_flag=True,
    default=False,
    help="Creates new versions of the changed resources. Use --fork-downstream to fork also the downstream dependencies of the changed resources.",
    hidden=True,
)
# this is added to use tb deploy in dry run mode. It's temprary => https://gitlab.com/tinybird/analytics/-/issues/12551
@click.option(
    "--use-main",
    is_flag=True,
    default=False,
    help="Use main commit instead of release commit",
    hidden=True,
)
@click.option(
    "--skip-head-outdated",
    is_flag=True,
    default=False,
    help="Allows to deploy any commit without checking if the branch is rebased to the workspace commit",
    hidden=True,
)
@click.pass_context
@coro
async def deploy(
    ctx: Context,
    dry_run: bool,
    debug: bool,
    override_datasource: bool,
    populate: bool,
    subset: Optional[float],
    sql_condition: Optional[str],
    unlink_on_populate_error: bool,
    wait: bool,
    yes: bool,
    workspace_map,
    workspace,
    folder: str,
    user_token: Optional[str],
    fork_downstream: bool,
    fork: bool,
    use_main: bool,
    skip_head_outdated: bool,
) -> None:
    """Deploy in Tinybird pushing resources changed from last git commit deployed.

    Usage: tb deploy
    """
    try:
        ignore_sql_errors = FeatureFlags.ignore_sql_errors()
        context.disable_template_security_validation.set(True)

        is_internal = has_internal_datafiles(folder)

        client: TinyB = ctx.ensure_object(dict)["client"]
        config = CLIConfig.get_project_config()
        workspaces: List[Dict[str, Any]] = (await client.user_workspaces_and_branches()).get("workspaces", [])
        current_ws: Dict[str, Any] = next(
            (workspace for workspace in workspaces if config and workspace.get("id", ".") == config.get("id", "..")), {}
        )

        semver = config.get("semver")
        auto_promote = getenv_bool("TB_AUTO_PROMOTE", True)
        release = current_ws.get("release", {})
        current_semver: Optional[str] = None
        if release and isinstance(release, dict):
            current_semver = release.get("semver")

        if not current_semver:
            click.echo(FeedbackManager.error_init_release(workspace=current_ws.get("name")))
            sys.exit(1)

        release_created = False
        new_release = False
        check_backfill_required = False

        # semver is now optional, if not sent force the bump
        if not semver:
            semver = current_semver
        else:
            click.echo(FeedbackManager.warning_deprecated_releases())

        if semver and current_semver:
            new_version = version.parse(semver.split("-snapshot")[0])
            current_version = version.parse(current_semver.split("-snapshot")[0])
            show_feedback = new_version != current_version
            if show_feedback:
                if dry_run:
                    click.echo(
                        FeedbackManager.info_dry_releases_detected(current_semver=current_version, semver=new_version)
                    )
                else:
                    click.echo(
                        FeedbackManager.info_releases_detected(current_semver=current_version, semver=new_version)
                    )

            if new_version == current_version:
                if current_version.post is None:
                    semver = str(current_version) + "-1"
                    new_version = version.Version(semver)
                else:
                    semver = f"{current_version.major}.{current_version.minor}.{current_version.micro}-{current_version.post + 1}"
                    new_version = version.Version(semver)

            if is_post_semver(new_version, current_version):
                if show_feedback:
                    if dry_run:
                        click.echo(FeedbackManager.info_dry_local_release(version=new_version))
                    else:
                        click.echo(FeedbackManager.info_local_release(version=new_version))
                yes = True
                auto_promote = False
                new_release = False
            elif is_major_semver(new_version, current_version):
                if show_feedback:
                    if dry_run:
                        click.echo(FeedbackManager.info_dry_major_release(version=new_version))
                    else:
                        click.echo(FeedbackManager.info_major_release(version=new_version))
                auto_promote = False
                new_release = True
            else:
                # allows TB_AUTO_PROMOTE=0 so release is left in preview
                auto_promote = getenv_bool("TB_AUTO_PROMOTE", True)
                if auto_promote:
                    if show_feedback:
                        if dry_run:
                            click.echo(
                                FeedbackManager.info_dry_minor_patch_release_with_autopromote(version=new_version)
                            )
                        else:
                            click.echo(FeedbackManager.info_minor_patch_release_with_autopromote(version=new_version))
                else:
                    if show_feedback:
                        if dry_run:
                            click.echo(FeedbackManager.info_dry_minor_patch_release_no_autopromote(version=new_version))
                        else:
                            click.echo(FeedbackManager.info_minor_patch_release_no_autopromote(version=new_version))
                new_release = True

        if new_release:
            if not dry_run:
                try:
                    await create_release(client, config, semver)
                except OperationCanNotBePerformed as e:
                    raise CLIException(FeedbackManager.error_exception(error=str(e)))
            release_created = True
            fork_downstream = True
            # allows TB_CHECK_BACKFILL_REQUIRED=0 so it is not checked
            check_backfill_required = getenv_bool("TB_CHECK_BACKFILL_REQUIRED", True)
        try:
            tb_client = create_tb_client(ctx)
            if dry_run:
                config.set_semver(None)
                tb_client.semver = None
            await folder_push(
                tb_client=tb_client,
                dry_run=dry_run,
                check=False,
                push_deps=True,
                debug=debug,
                force=True,
                git_release=True,
                override_datasource=override_datasource,
                populate=populate,
                populate_subset=subset,
                populate_condition=sql_condition,
                unlink_on_populate_error=unlink_on_populate_error,
                upload_fixtures=False,
                wait=wait,
                ignore_sql_errors=ignore_sql_errors,
                skip_confirmation=yes,
                workspace_map=dict(workspace_map),
                workspace_lib_paths=workspace,
                run_tests=False,
                folder=folder,
                config=config,
                user_token=user_token,
                fork_downstream=fork_downstream,
                fork=fork,
                is_internal=is_internal,
                release_created=release_created,
                auto_promote=auto_promote,
                check_backfill_required=check_backfill_required,
                use_main=use_main,
                check_outdated=not skip_head_outdated,
            )
        except Exception as e:
            if release_created and not dry_run:
                await client.release_failed(config["id"], semver)
            raise e

        if release_created:
            try:
                if not dry_run:
                    await client.release_preview(config["id"], semver)
                    click.echo(FeedbackManager.success_release_preview(semver=semver))
                else:
                    click.echo(FeedbackManager.success_dry_release_preview(semver=semver))
                if auto_promote:
                    try:
                        force_remove = getenv_bool("TB_FORCE_REMOVE_OLDEST_ROLLBACK", False)
                        if dry_run:
                            click.echo(FeedbackManager.warning_dry_remove_oldest_rollback(semver=semver))
                        else:
                            click.echo(FeedbackManager.warning_remove_oldest_rollback(semver=semver))
                        try:
                            await remove_release(dry_run, config, OLDEST_ROLLBACK, client, force=force_remove)
                        except Exception as e:
                            click.echo(FeedbackManager.error_remove_oldest_rollback(error=str(e), semver=semver))
                            sys.exit(1)

                        if not dry_run:
                            release = await client.release_promote(config["id"], semver)
                            click.echo(FeedbackManager.success_release_promote(semver=semver))
                            click.echo(FeedbackManager.success_git_release(release_commit=release["commit"]))
                        else:
                            click.echo(FeedbackManager.success_dry_release_promote(semver=semver))
                            click.echo(FeedbackManager.success_dry_git_release(release_commit=release["commit"]))
                    except Exception as e:
                        raise CLIException(FeedbackManager.error_exception(error=str(e)))
            except Exception as e:
                raise CLIException(FeedbackManager.error_exception(error=str(e)))

        if not new_release:
            if dry_run:
                if show_feedback:
                    click.echo(FeedbackManager.success_dry_release_update(semver=current_semver, new_semver=semver))
            else:
                client.semver = None
                await client.update_release_semver(config["id"], current_semver, semver)
                if show_feedback:
                    click.echo(FeedbackManager.success_release_update(semver=current_semver, new_semver=semver))
    except AuthNoTokenException:
        raise
    except Exception as e:
        raise CLIException(str(e))
