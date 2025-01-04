# This is a command file for our CLI. Please keep it clean.
#
# - If it makes sense and only when strictly necessary, you can create utility functions in this file.
# - But please, **do not** interleave utility functions and command definitions.

import click
from click import Context

from tinybird.client import DoesNotExistException, TinyB
from tinybird.feedback_manager import FeedbackManager
from tinybird.tb_cli_modules.cli import cli
from tinybird.tb_cli_modules.common import coro, echo_safe_humanfriendly_tables_format_smart_table
from tinybird.tb_cli_modules.exceptions import CLIException


@cli.group()
@click.pass_context
def job(ctx: Context) -> None:
    """Jobs commands."""


@job.command(name="ls")
@click.option(
    "-s",
    "--status",
    help="Show only jobs with this status",
    type=click.Choice(["waiting", "working", "done", "error"], case_sensitive=False),
    multiple=True,
    default=None,
)
@click.pass_context
@coro
async def jobs_ls(ctx: Context, status: str) -> None:
    """List jobs, up to 100 in the last 48h"""
    client: TinyB = ctx.ensure_object(dict)["client"]
    jobs = await client.jobs(status=status)
    columns = ["id", "kind", "status", "created at", "updated at", "job url"]
    click.echo(FeedbackManager.info_jobs())
    table = []
    for j in jobs:
        table.append([j[c.replace(" ", "_")] for c in columns])
    echo_safe_humanfriendly_tables_format_smart_table(table, column_names=columns)
    click.echo("\n")


@job.command(name="details")
@click.argument("job_id")
@click.pass_context
@coro
async def job_details(ctx: Context, job_id: str) -> None:
    """Get details for any job created in the last 48h"""
    client: TinyB = ctx.ensure_object(dict)["client"]
    job = await client.job(job_id)
    columns = []
    click.echo(FeedbackManager.info_job(job=job_id))
    table = []
    columns = job.keys()
    table = [job.values()]
    echo_safe_humanfriendly_tables_format_smart_table(table, column_names=columns)
    click.echo("\n")


@job.command(name="cancel")
@click.argument("job_id")
@click.pass_context
@coro
async def job_cancel(ctx: Context, job_id: str) -> None:
    """Try to cancel a job"""
    client = ctx.ensure_object(dict)["client"]

    try:
        result = await client.job_cancel(job_id)
    except DoesNotExistException:
        raise CLIException(FeedbackManager.error_job_does_not_exist(job_id=job_id))
    except Exception as e:
        raise CLIException(FeedbackManager.error_exception(error=e))
    else:
        current_job_status = result["status"]
        if current_job_status == "cancelling":
            click.echo(FeedbackManager.success_job_cancellation_cancelling(job_id=job_id))
        elif current_job_status == "cancelled":
            click.echo(FeedbackManager.success_job_cancellation_cancelled(job_id=job_id))
        else:
            raise CLIException(FeedbackManager.error_job_cancelled_but_status_unknown(job_id=job_id))
    click.echo("\n")
