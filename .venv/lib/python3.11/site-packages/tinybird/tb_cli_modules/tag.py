from typing import Optional

import click
from click import Context

from tinybird.feedback_manager import FeedbackManager
from tinybird.tb_cli_modules.cli import cli
from tinybird.tb_cli_modules.common import coro, echo_safe_humanfriendly_tables_format_smart_table


@cli.group()
@click.pass_context
def tag(ctx: Context) -> None:
    """Tag commands"""


@tag.command(name="ls")
@click.argument("tag_name", required=False)
@click.pass_context
@coro
async def tag_ls(ctx: Context, tag_name: Optional[str]) -> None:
    """List all the tags of the current Workspace or the resources associated to a specific tag."""

    client = ctx.ensure_object(dict)["client"]
    response = await client.get_all_tags()

    if tag_name:
        the_tag = [tag for tag in response["tags"] if tag["name"] == tag_name]

        columns = ["name", "id", "type"]
        table = []

        if len(the_tag) > 0:
            for resource in the_tag[0]["resources"]:
                table.append([resource["name"], resource["id"], resource["type"]])

        click.echo(FeedbackManager.info_tag_resources(tag_name=tag_name))
        echo_safe_humanfriendly_tables_format_smart_table(table, column_names=columns)
        return

    columns = ["tag", "resources"]
    table = []

    for tag in response["tags"]:
        unique_resources = []
        for resource in tag["resources"]:
            if resource.get("name", "") not in unique_resources:
                unique_resources.append(resource)  # Reducing by name in case there are duplicates.
        table.append([tag["name"], len(unique_resources)])

    click.echo(FeedbackManager.info_tag_list())
    echo_safe_humanfriendly_tables_format_smart_table(table, column_names=columns)


@tag.command(name="create")
@click.argument("tag_name")
@click.pass_context
@coro
async def tag_create(ctx: Context, tag_name: str) -> None:
    """Create a tag in the current Workspace."""

    client = ctx.ensure_object(dict)["client"]
    await client.create_tag(name=tag_name)

    click.echo(FeedbackManager.success_tag_created(tag_name=tag_name))


@tag.command(name="rm")
@click.argument("tag_name")
@click.option("--yes", is_flag=True, default=False, help="Do not ask for confirmation to delete the tag.")
@click.pass_context
@coro
async def tag_rm(ctx: Context, tag_name: str, yes: bool) -> None:
    """Remove a tag from the current Workspace."""

    client = ctx.ensure_object(dict)["client"]
    remove_tag = True

    if not yes:
        all_tags = await client.get_all_tags()
        the_tag = [tag for tag in all_tags["tags"] if tag["name"] == tag_name]
        if len(the_tag) > 0:
            unique_resources = []
            for resource in the_tag[0]["resources"]:
                if resource.get("name", "") not in unique_resources:
                    unique_resources.append(resource)  # Reducing by name in case there are duplicates.

            if len(unique_resources) > 0:
                remove_tag = click.confirm(
                    FeedbackManager.warning_tag_remove(tag_name=tag_name, resources_len=len(unique_resources))
                )
            else:
                remove_tag = click.confirm(FeedbackManager.warning_tag_remove_no_resources(tag_name=tag_name))
        else:
            remove_tag = False
            click.echo(FeedbackManager.error_tag_not_found(tag_name=tag_name))

    if remove_tag:
        await client.delete_tag(tag_name)
        click.echo(FeedbackManager.success_tag_removed(tag_name=tag_name))
