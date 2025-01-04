# This is a command file for our CLI. Please keep it clean.
#
# - If it makes sense and only when strictly necessary, you can create utility functions in this file.
# - But please, **do not** interleave utility functions and command definitions.

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import click
from click import Context

from tinybird.client import TinyB
from tinybird.config import get_display_host
from tinybird.feedback_manager import FeedbackManager
from tinybird.tb_cli_modules.common import (
    ask_for_user_token,
    check_user_token,
    coro,
    echo_safe_humanfriendly_tables_format_smart_table,
    get_current_workspace,
)
from tinybird.tb_cli_modules.config import CLIConfig
from tinybird.tb_cli_modules.exceptions import CLIWorkspaceMembersException
from tinybird.tb_cli_modules.workspace import workspace

ROLES = ["viewer", "guest", "admin"]


@dataclass
class WorkspaceMemberCommandContext:
    client: TinyB
    config: CLIConfig
    host: str
    ui_host: str
    workspace: Dict[str, Any]


async def get_command_context(ctx: click.Context) -> WorkspaceMemberCommandContext:
    config = CLIConfig.get_project_config()
    client = config.get_client()
    host = config.get_host() or CLIConfig.DEFAULTS["host"]
    ui_host = get_display_host(host)

    workspace = await get_current_workspace(config)

    if not workspace:
        raise CLIWorkspaceMembersException(FeedbackManager.error_unknown_resource(resource=config["d"]))

    return WorkspaceMemberCommandContext(client, config, host, ui_host, workspace)


def get_workspace_users(cmd_ctx: WorkspaceMemberCommandContext) -> Tuple[Any, ...]:
    return tuple(u["email"] for u in cmd_ctx.workspace["members"])


@workspace.group()
@click.pass_context
def members(ctx: Context) -> None:
    """Workspace members management commands."""


@members.command(name="add", short_help="Adds members to the current Workspace")
@click.argument("members_emails")
@click.option("--role", is_flag=False, default=None, help="Role for the members being added", type=click.Choice(ROLES))
@click.option("--user_token", is_flag=False, default=None, help="When passed, we won't prompt asking for it")
@click.pass_context
@coro
async def add_members_to_workspace(
    ctx: Context, members_emails: str, user_token: Optional[str], role: Optional[str]
) -> None:
    """Adds members to the current Workspace."""

    cmd_ctx = await get_command_context(ctx)

    requested_users = [u.strip() for u in members_emails.split(",")]
    users_to_add = [u for u in requested_users if u not in get_workspace_users(cmd_ctx)]

    if len(users_to_add) == 0:
        msg = (
            FeedbackManager.info_user_already_exists(user=requested_users[0], workspace_name=cmd_ctx.workspace["name"])
            if len(requested_users) == 1
            else FeedbackManager.info_users_already_exists(workspace_name=cmd_ctx.workspace["name"])
        )
        click.echo(msg)
    else:
        if not user_token:
            user_token = ask_for_user_token(f"add users to {cmd_ctx.workspace['name']}", cmd_ctx.ui_host)
        await check_user_token(ctx, user_token)

        user_client: TinyB = deepcopy(cmd_ctx.client)
        user_client.token = user_token
        await user_client.add_users_to_workspace(cmd_ctx.workspace, users_to_add, role)
        msg = (
            FeedbackManager.success_workspace_user_added(user=users_to_add[0], workspace_name=cmd_ctx.workspace["name"])
            if len(users_to_add) == 1
            else FeedbackManager.success_workspace_users_added(workspace_name=cmd_ctx.workspace["name"])
        )
        click.echo(msg)


@members.command(name="ls", short_help="List members in the current Workspace")
@click.pass_context
@coro
async def list_members_in_workspace(ctx: Context) -> None:
    """List members in the current Workspace."""

    cmd_ctx = await get_command_context(ctx)
    users = tuple([u] for u in get_workspace_users(cmd_ctx))

    echo_safe_humanfriendly_tables_format_smart_table(users, column_names=["email"])


@members.command(name="rm", short_help="Removes members from the current Workspace")
@click.argument("members_emails")
@click.option("--user_token", is_flag=False, default=None, help="When passed, we won't prompt asking for it")
@click.pass_context
@coro
async def remove_members_from_workspace(ctx: Context, members_emails: str, user_token: Optional[str]) -> None:
    """Removes members from the current Workspace."""

    cmd_ctx = await get_command_context(ctx)

    requested_users = [u.strip() for u in members_emails.split(",")]
    workspace_users = get_workspace_users(cmd_ctx)
    users_to_remove = [u for u in requested_users if u in workspace_users]
    non_valid_users = [u for u in requested_users if u not in workspace_users]

    if len(users_to_remove) == 0:
        msg = (
            FeedbackManager.info_user_not_exists(user=requested_users[0], workspace_name=cmd_ctx.workspace["name"])
            if len(requested_users) == 1
            else FeedbackManager.info_users_not_exists(workspace_name=cmd_ctx.workspace["name"])
        )
        click.echo(msg)
    else:
        if not user_token:
            user_token = ask_for_user_token(f"remove users from {cmd_ctx.workspace['name']}", cmd_ctx.ui_host)
        await check_user_token(ctx, user_token)

        if len(non_valid_users) > 0:
            click.echo(
                FeedbackManager.warning_user_doesnt_exist(user=non_valid_users[0], workspace=cmd_ctx.workspace["name"])
                if len(non_valid_users) == 1
                else FeedbackManager.warning_users_dont_exist(
                    users=", ".join(non_valid_users), workspace=cmd_ctx.workspace["name"]
                )
            )

        user_client: TinyB = deepcopy(cmd_ctx.client)
        user_client.token = user_token
        await user_client.remove_users_from_workspace(cmd_ctx.workspace, users_to_remove)
        msg = (
            FeedbackManager.success_workspace_user_removed(
                user=users_to_remove[0], workspace_name=cmd_ctx.workspace["name"]
            )
            if len(users_to_remove) == 1
            else FeedbackManager.success_workspace_users_removed(workspace_name=cmd_ctx.workspace["name"])
        )
        click.echo(msg)


@members.command(name="set-role", short_help="Sets the role for existing workspace members")
@click.argument("role", required=True, type=click.Choice(ROLES))
@click.argument("members_emails", required=True, type=str)
@click.option("--user_token", is_flag=False, default=None, help="When passed, we won't prompt asking for it")
@click.pass_context
@coro
async def set_workspace_member_role(ctx: Context, role: str, members_emails: str, user_token: Optional[str]) -> None:
    """Sets the role for existing workspace members."""

    cmd_ctx = await get_command_context(ctx)

    requested_users = [u.strip() for u in members_emails.split(",")]
    workspace_users = get_workspace_users(cmd_ctx)
    users_to_change = [u for u in requested_users if u in workspace_users]
    non_valid_users = [u for u in requested_users if u not in workspace_users]

    if len(users_to_change) == 0:
        msg = (
            FeedbackManager.info_user_not_exists(user=requested_users[0], workspace_name=cmd_ctx.workspace["name"])
            if len(requested_users) == 1
            else FeedbackManager.info_users_not_exists(workspace_name=cmd_ctx.workspace["name"])
        )
        click.echo(msg)
    else:
        if not user_token:
            user_token = ask_for_user_token(f"change user roles in {cmd_ctx.workspace['name']}", cmd_ctx.ui_host)
        await check_user_token(ctx, user_token)

        if len(non_valid_users) > 0:
            click.echo(
                FeedbackManager.warning_user_doesnt_exist(user=non_valid_users[0], workspace=cmd_ctx.workspace["name"])
                if len(non_valid_users) == 1
                else FeedbackManager.warning_users_dont_exist(
                    users=", ".join(non_valid_users), workspace=cmd_ctx.workspace["name"]
                )
            )

        user_client: TinyB = deepcopy(cmd_ctx.client)
        user_client.token = user_token
        await user_client.set_role_for_users_in_workspace(cmd_ctx.workspace, users_to_change, role=role)
        msg = (
            FeedbackManager.success_workspace_user_changed_role(
                user=users_to_change[0], role=role, workspace_name=cmd_ctx.workspace["name"]
            )
            if len(users_to_change) == 1
            else FeedbackManager.success_workspace_users_changed_role(
                role=role, workspace_name=cmd_ctx.workspace["name"]
            )
        )
        click.echo(msg)
