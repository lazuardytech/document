# This is a command file for our CLI. Please keep it clean.
#
# - If it makes sense and only when strictly necessary, you can create utility functions in this file.
# - But please, **do not** interleave utility functions and command definitions.

import glob
from typing import Any, Dict, Iterable, List, Tuple

import click

from tinybird.client import AuthNoTokenException
from tinybird.feedback_manager import FeedbackManager
from tinybird.tb_cli_modules.cli import cli
from tinybird.tb_cli_modules.common import coro, create_tb_client, gather_with_concurrency
from tinybird.tb_cli_modules.config import CLIConfig
from tinybird.tb_cli_modules.exceptions import CLIException
from tinybird.tb_cli_modules.tinyunit.tinyunit import (
    TestSummaryResults,
    generate_file,
    parse_file,
    run_test_file,
    test_run_summary,
)


@cli.group()
@click.pass_context
def test(ctx: click.Context) -> None:
    """Test commands."""


@test.command(
    name="run",
    help="Run the test suite, a file, or a test. To skip test to run in branches and CI put them in a 'skip_in_branch' folder.",
)
@click.argument("file", nargs=-1)
@click.option("-v", "--verbose", is_flag=True, default=False, help="Enable verbose (show results)", type=bool)
@click.option("--fail", "only_fail", is_flag=True, default=False, help="Showy onl failed/error tests", type=bool)
@click.option("-c", "--concurrency", help="How many test to run concurrently", default=1, type=click.IntRange(1, 10))
@click.pass_context
@coro
async def test_run(ctx: click.Context, file: Tuple[str, ...], verbose: bool, only_fail: bool, concurrency: int) -> None:
    results: List[TestSummaryResults] = []

    try:
        tb_client = create_tb_client(ctx)
        config = CLIConfig.get_project_config()
        if config.get("token") is None:
            raise AuthNoTokenException
        workspaces: List[Dict[str, Any]] = (await tb_client.user_workspaces_and_branches()).get("workspaces", [])
        current_ws: Dict[str, Any] = next(
            (workspace for workspace in workspaces if config and workspace.get("id", ".") == config.get("id", "..")), {}
        )
    except Exception as e:
        raise CLIException(FeedbackManager.error_exception(error=e))

    file_list: Iterable[str] = file if len(file) > 0 else glob.glob("./tests/**/*.y*ml", recursive=True)
    click.echo(FeedbackManager.info_skipping_resource(resource="regression.yaml"))
    file_list = [f for f in file_list if not f.endswith("regression.yaml")]
    final_file_list = []
    for f in file_list:
        if "skip_in_branch" in f and current_ws and current_ws.get("is_branch"):
            click.echo(FeedbackManager.info_skipping_resource(resource=f))
        else:
            final_file_list.append(f)
    file_list = final_file_list

    async def run_test(tb_client, test_file, results):
        try:
            test_result = await run_test_file(tb_client, test_file)
            results.append(TestSummaryResults(filename=test_file, results=test_result, semver=tb_client.semver))
        except Exception as e:
            if verbose:
                click.echo(FeedbackManager.error_exception(error=e))
            raise CLIException(FeedbackManager.error_running_test(file=test_file))

    test_tasks = [run_test(tb_client, test_file, results) for test_file in file_list]
    await gather_with_concurrency(concurrency, *test_tasks)

    if len(results) <= 0:
        click.echo(FeedbackManager.warning_no_test_results())
    else:
        test_run_summary(results, only_fail=only_fail, verbose_level=int(verbose))


@test.command(name="init", help="Initialize a file list with a simple test suite.")
@click.argument("files", nargs=-1)
@click.option("--force", is_flag=True, default=False, help="Override existing files")
@click.pass_context
@coro
async def test_init(ctx: click.Context, files: Tuple[str, ...], force: bool) -> None:
    if len(files) == 0:
        files = ("tests/default.yaml",)

    for file in files:
        generate_file(file, overwrite=force)


@test.command(name="parse", help="Read the contents of a test file list.")
@click.argument("files", nargs=-1)
@click.pass_context
@coro
async def test_parse(ctx: click.Context, files: Tuple[str, ...]) -> None:
    for f in files:
        click.echo(f"\nFile: {f}")
        for test in parse_file(f):
            click.echo(test)
