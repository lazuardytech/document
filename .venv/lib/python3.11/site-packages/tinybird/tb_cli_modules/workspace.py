# This is a command file for our CLI. Please keep it clean.
#
# - If it makes sense and only when strictly necessary, you can create utility functions in this file.
# - But please, **do not** interleave utility functions and command definitions.

from typing import Optional

import click
from click import Context

from tinybird.client import CanNotBeDeletedException, DoesNotExistException, TinyB
from tinybird.config import get_display_host
from tinybird.datafile import PipeTypes
from tinybird.feedback_manager import FeedbackManager
from tinybird.tb_cli_modules.cli import cli
from tinybird.tb_cli_modules.common import (
    _get_workspace_plan_name,
    ask_for_user_token,
    check_user_token,
    coro,
    create_workspace_interactive,
    create_workspace_non_interactive,
    echo_safe_humanfriendly_tables_format_smart_table,
    get_current_main_workspace,
    is_valid_starterkit,
    print_current_workspace,
    switch_workspace,
)
from tinybird.tb_cli_modules.config import CLIConfig
from tinybird.tb_cli_modules.exceptions import CLIWorkspaceException


@cli.group()
@click.pass_context
def workspace(ctx: Context) -> None:
    """Workspace commands"""


@workspace.command(name="ls")
@click.pass_context
@coro
async def workspace_ls(ctx: Context) -> None:
    """List all the workspaces you have access to in the account you're currently authenticated into."""

    config = CLIConfig.get_project_config()
    client = config.get_client()

    response = await client.user_workspaces()

    current_main_workspace = await get_current_main_workspace(config)
    if not current_main_workspace:
        raise CLIWorkspaceException(FeedbackManager.error_unable_to_identify_main_workspace())

    columns = ["name", "id", "role", "plan", "current"]
    table = []
    click.echo(FeedbackManager.info_workspaces())

    for workspace in response["workspaces"]:
        table.append(
            [
                workspace["name"],
                workspace["id"],
                workspace["role"],
                _get_workspace_plan_name(workspace["plan"]),
                current_main_workspace["id"] == workspace["id"],
            ]
        )

    echo_safe_humanfriendly_tables_format_smart_table(table, column_names=columns)


@workspace.command(name="use")
@click.argument("workspace_name_or_id")
@coro
async def workspace_use(workspace_name_or_id: str) -> None:
    """Switch to another workspace. Use 'tb workspace ls' to list the workspaces you have access to."""
    config = CLIConfig.get_project_config()

    await switch_workspace(config, workspace_name_or_id)


@workspace.command(name="current")
@coro
async def workspace_current():
    """Show the workspace you're currently authenticated to"""
    config = CLIConfig.get_project_config()

    await print_current_workspace(config)


@workspace.command(
    name="clear",
    short_help="Drop all the resources inside a project. This command is dangerous because it removes everything, use with care.",
)
@click.option("--yes", is_flag=True, default=False, help="Do not ask for confirmation")
@click.option("--dry-run", is_flag=True, default=False, help="Run the command without removing anything")
@click.pass_context
@coro
async def clear_workspace(ctx: Context, yes: bool, dry_run: bool) -> None:
    """Drop all the resources inside a project. This command is dangerous because it removes everything, use with care."""

    # Get current workspace to add the name to the alert message
    config = CLIConfig.get_project_config()
    client: TinyB = config.get_client()

    response = await client.user_workspaces_and_branches()

    columns = ["name", "id", "role", "plan", "current"]
    table = []

    for workspace in response["workspaces"]:
        if config["id"] == workspace["id"]:
            if workspace.get("is_branch"):
                raise CLIWorkspaceException(FeedbackManager.error_not_allowed_in_branch())
                return
            else:
                click.echo(FeedbackManager.info_current_workspace())
                table.append(
                    [
                        workspace["name"],
                        workspace["id"],
                        workspace["role"],
                        _get_workspace_plan_name(workspace["plan"]),
                        True,
                    ]
                )
            break

    echo_safe_humanfriendly_tables_format_smart_table(table, column_names=columns)

    if yes or click.confirm(FeedbackManager.warning_confirm_clear_workspace()):
        pipes = await client.pipes(dependencies=False, node_attrs="id,name,materialized", attrs="name,type")
        pipe_names = [pipe["name"] for pipe in pipes]

        for pipe in pipes:
            if pipe["type"] == PipeTypes.MATERIALIZED:
                if not dry_run:
                    node_id = None
                    for node in pipe["nodes"]:
                        if "materialized" in node and node["materialized"] is not None:
                            node_id = node["id"]
                            break

                    if node_id:
                        click.echo(FeedbackManager.info_unlinking_materialized_pipe(pipe=pipe["name"]))
                        try:
                            await client.pipe_unlink_materialized(pipe["name"], node_id)
                        except DoesNotExistException:
                            click.echo(FeedbackManager.info_materialized_unlinking_pipe_not_found(pipe=pipe["name"]))
                else:
                    click.echo(FeedbackManager.info_materialized_dry_unlinking_pipe(pipe=pipe["name"]))

        for pipe_name in pipe_names:
            if not dry_run:
                click.echo(FeedbackManager.info_removing_pipe(pipe=pipe_name))
                try:
                    await client.pipe_delete(pipe_name)
                except DoesNotExistException:
                    click.echo(FeedbackManager.info_removing_pipe_not_found(pipe=pipe_name))
            else:
                click.echo(FeedbackManager.info_dry_removing_pipe(pipe=pipe_name))

        datasources = await client.datasources()
        ds_names = [datasource["name"] for datasource in datasources]
        for ds_name in ds_names:
            if not dry_run:
                click.echo(FeedbackManager.info_removing_datasource(datasource=ds_name))
                try:
                    await client.datasource_delete(ds_name, force=True)
                except DoesNotExistException:
                    click.echo(FeedbackManager.info_removing_datasource_not_found(datasource=ds_name))
                except CanNotBeDeletedException as e:
                    raise CLIWorkspaceException(
                        FeedbackManager.error_datasource_can_not_be_deleted(datasource=ds_name, error=e)
                    )
                except Exception as e:
                    if "is a Shared Data Source" in str(e):
                        raise CLIWorkspaceException(FeedbackManager.error_operation_can_not_be_performed(error=e))
                    else:
                        raise CLIWorkspaceException(FeedbackManager.error_exception(error=e))
            else:
                click.echo(FeedbackManager.info_dry_removing_datasource(datasource=ds_name))


@workspace.command(name="create", short_help="Create a new Workspace for your Tinybird user")
@click.argument("workspace_name", required=False)
@click.option("--starter_kit", "starter_kit", type=str, required=False, help="Use a Tinybird starter kit as a template")
@click.option("--starter-kit", "starter_kit", hidden=True)
@click.option("--user_token", is_flag=False, default=None, help="When passed, we won't prompt asking for it")
@click.option(
    "--fork",
    is_flag=True,
    default=False,
    help="When enabled, we will share all datasource from the current workspace to the new created one",
)
@click.pass_context
@coro
async def create_workspace(
    ctx: Context, workspace_name: str, starter_kit: str, user_token: Optional[str], fork: bool
) -> None:
    if starter_kit and not await is_valid_starterkit(ctx, starter_kit):
        raise CLIWorkspaceException(FeedbackManager.error_starterkit_name(starterkit_name=starter_kit))

    if not user_token:
        config = CLIConfig.get_project_config()
        host = config.get_host() or CLIConfig.DEFAULTS["host"]
        ui_host = get_display_host(host)
        user_token = ask_for_user_token("create a new workspace", ui_host)
        if not user_token:
            return
    await check_user_token(ctx, user_token)

    # If we have at least workspace_name, we start the non interactive
    # process, creating an empty workspace
    if workspace_name:
        await create_workspace_non_interactive(ctx, workspace_name, starter_kit, user_token, fork)
    else:
        await create_workspace_interactive(ctx, workspace_name, starter_kit, user_token, fork)


@workspace.command(name="delete", short_help="Delete a Workspace for your Tinybird user")
@click.argument("workspace_name_or_id")
@click.option("--user_token", is_flag=False, default=None, help="When passed, we won't prompt asking for it")
@click.option(
    "--confirm_hard_delete",
    default=None,
    help="Introduce the name of the workspace in order to confirm you want to run a hard delete over the workspace",
    hidden=True,
)
@click.option("--yes", is_flag=True, default=False, help="Do not ask for confirmation")
@click.pass_context
@coro
async def delete_workspace(
    ctx: Context, workspace_name_or_id: str, user_token: Optional[str], confirm_hard_delete: Optional[str], yes: bool
) -> None:
    """Delete a workspace where you are an admin."""

    config = CLIConfig.get_project_config()
    client = config.get_client()
    host = config.get_host() or CLIConfig.DEFAULTS["host"]
    ui_host = get_display_host(host)

    if not user_token:
        user_token = ask_for_user_token("delete a workspace", ui_host)
    await check_user_token(ctx, user_token)

    workspaces = (await client.user_workspaces()).get("workspaces", [])
    workspace_to_delete = next(
        (
            workspace
            for workspace in workspaces
            if workspace["name"] == workspace_name_or_id or workspace["id"] == workspace_name_or_id
        ),
        None,
    )

    if not workspace_to_delete:
        raise CLIWorkspaceException(FeedbackManager.error_workspace(workspace=workspace_name_or_id))

    if yes or click.confirm(
        FeedbackManager.warning_confirm_delete_workspace(workspace_name=workspace_to_delete.get("name"))
    ):
        client.token = user_token

        try:
            await client.delete_workspace(workspace_to_delete["id"], confirm_hard_delete)
            click.echo(FeedbackManager.success_workspace_deleted(workspace_name=workspace_to_delete["name"]))
        except Exception as e:
            raise CLIWorkspaceException(FeedbackManager.error_exception(error=str(e)))
