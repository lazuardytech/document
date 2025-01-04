# This is a command file for our CLI. Please keep it clean.
#
# - If it makes sense and only when strictly necessary, you can create utility functions in this file.
# - But please, **do not** interleave utility functions and command definitions.

import os
from os import getcwd
from typing import List, Optional, Tuple

import aiofiles
import click
import yaml

from tinybird.datafile import create_release, wait_job
from tinybird.feedback_manager import FeedbackManager
from tinybird.tb_cli_modules.cli import cli
from tinybird.tb_cli_modules.common import (
    MAIN_BRANCH,
    OLDEST_ROLLBACK,
    coro,
    create_workspace_branch,
    echo_safe_humanfriendly_tables_format_smart_table,
    get_current_main_workspace,
    get_current_workspace,
    get_current_workspace_branches,
    get_oldest_rollback,
    get_workspace_member_email,
    getenv_bool,
    print_branch_regression_tests_summary,
    print_current_branch,
    print_current_workspace,
    print_data_branch_summary,
    print_release_summary,
    remove_release,
    switch_to_workspace_by_user_workspace_data,
    switch_workspace,
    try_update_config_with_remote,
)
from tinybird.tb_cli_modules.config import CLIConfig
from tinybird.tb_cli_modules.exceptions import CLIBranchException, CLIException, CLIReleaseException


@cli.group(hidden=True)
def release() -> None:
    """Release commands"""


@release.command(name="ls", short_help="Lists Releases for the current Workspace")
@coro
async def release_ls() -> None:
    """List current available Releases in the Workspace"""
    config = CLIConfig.get_project_config()
    _ = await try_update_config_with_remote(config, only_if_needed=True)

    await print_releases(config)


async def print_releases(config: CLIConfig):
    response = await config.get_client().releases(config["id"])

    table: List[Tuple[str, str, str, str, str]] = []
    for release in response["releases"]:
        table.append(
            (release["created_at"], release["semver"], release["status"], release["commit"], release["rollback"])
        )

    columns = ["created_at", "semver", "status", "commit", "rollback release"]
    click.echo(FeedbackManager.info_releases())
    echo_safe_humanfriendly_tables_format_smart_table(table, column_names=columns)


@release.command(name="generate", short_help="Generates a custom deployment for a Release")
@click.option(
    "--semver",
    is_flag=False,
    required=True,
    type=str,
    help="Semver of the new Release. Example: 1.0.0",
)
@coro
async def release_generate(semver: str) -> None:
    click.echo(FeedbackManager.warning_deprecated_releases())
    if os.path.exists(".tinyenv"):
        async with aiofiles.open(".tinyenv", "r") as env_file:
            lines = await env_file.readlines()

        updated_lines = []
        for line in lines:
            if line.startswith("VERSION="):
                updated_lines.append(f"VERSION={semver}\n")
            else:
                updated_lines.append(line)

        async with aiofiles.open(".tinyenv", "w") as env_file:
            await env_file.writelines(updated_lines)
    else:
        async with aiofiles.open(".tinyenv", "w") as env_file:
            await env_file.write(f"VERSION={semver}\n")

    deploy_dir = os.path.join("deploy", semver)
    os.makedirs(deploy_dir, exist_ok=True)

    deploy_file = os.path.join(deploy_dir, "deploy.sh")
    async with aiofiles.open(deploy_file, "w") as deploy:
        await deploy.write(
            """\
#!/bin/bash
set -euxo pipefail

# tb --semver $VERSION deploy
"""
        )

    os.chmod(deploy_file, 0o755)

    post_deploy_file = os.path.join(deploy_dir, "postdeploy.sh")
    async with aiofiles.open(post_deploy_file, "w") as post_deploy:
        await post_deploy.write(
            """\
#!/bin/bash
set -euxo pipefail

# tb --semver $VERSION pipe populate <pipe_name> --node <node_name> --sql-condition <sql> --wait
"""
        )

    os.chmod(post_deploy_file, 0o755)

    click.echo(FeedbackManager.info_release_generated(semver=semver))


@release.command(name="create", short_help="Create a new Release in deploying status")
@click.option(
    "--semver",
    is_flag=False,
    required=True,
    type=str,
    help="Semver of the new Release. Example: 1.0.0",
)
@coro
async def release_create(semver: str) -> None:
    click.echo(FeedbackManager.warning_deprecated_releases())
    config = CLIConfig.get_project_config()
    _ = await try_update_config_with_remote(config, only_if_needed=True)

    client = config.get_client()
    folder = getcwd()
    await create_release(client, config, semver, folder)


@release.command(name="promote", short_help="Promotes to live status a preview Release")
@click.option(
    "--semver", required=True, type=str, help="Semver of a preview Release to promote to live. Example: 1.0.0"
)
@coro
async def release_promote(semver: str) -> None:
    """
    The oldest rollback Release will be automatically removed if no usage, otherwise export TB_FORCE_REMOVE_OLDEST_ROLLBACK="1" to force deletion
    """
    click.echo(FeedbackManager.warning_deprecated_releases())
    config = CLIConfig.get_project_config()
    _ = await try_update_config_with_remote(config, only_if_needed=True)

    client = config.get_client()

    try:
        force_remove = getenv_bool("TB_FORCE_REMOVE_OLDEST_ROLLBACK", False)
        click.echo(FeedbackManager.warning_remove_oldest_rollback(semver=semver))
        try:
            await remove_release(False, config, OLDEST_ROLLBACK, client, force=force_remove)
        except Exception as e:
            click.echo(FeedbackManager.error_remove_oldest_rollback(error=str(e), semver=semver))
        release = await client.release_promote(config["id"], semver)
        click.echo(FeedbackManager.success_release_promote(semver=semver))
        click.echo(FeedbackManager.success_git_release(release_commit=release["commit"]))
    except Exception as e:
        raise CLIReleaseException(FeedbackManager.error_exception(error=str(e)))


@release.command(name="preview", short_help="Updates the status of a deploying Release to preview")
@click.option(
    "--semver", is_flag=False, required=True, type=str, help="Semver of a preview Release to preview. Example: 1.0.0"
)
@coro
async def release_preview(semver: str) -> None:
    click.echo(FeedbackManager.warning_deprecated_releases())
    config = CLIConfig.get_project_config()
    _ = await try_update_config_with_remote(config, only_if_needed=True)

    client = config.get_client()

    try:
        await client.release_preview(config["id"], semver)
        click.echo(FeedbackManager.success_release_preview(semver=semver))
    except Exception as e:
        raise CLIReleaseException(FeedbackManager.error_exception(error=str(e)))


@release.command(name="rollback", short_help="Rollbacks to a previous Release")
@click.option("--yes", is_flag=True, default=False, help="Do not ask for confirmation")
@coro
async def release_rollback(yes: bool) -> None:
    click.echo(FeedbackManager.warning_deprecated_releases())
    config = CLIConfig.get_project_config()
    _ = await try_update_config_with_remote(config, only_if_needed=False)

    client = config.get_client()

    releases_response = await config.get_client().releases(config["id"])

    try:
        release_to_rollback = next(
            (release for release in releases_response["releases"] if release["status"] == "live"), None
        )
        if not release_to_rollback:
            raise CLIBranchException(FeedbackManager.error_release_rollback_live_not_found())

        await print_releases(config)
        semver = release_to_rollback.get("semver")
        rollback_version = release_to_rollback.get("rollback")
        await print_release_summary(config, rollback_version)
        if yes or click.confirm(
            FeedbackManager.warning_confirm_rollback_release(semver=semver, rollback=rollback_version)
        ):
            release = await client.release_rollback(config["id"], semver=semver)
            click.echo(FeedbackManager.success_release_rollback(semver=release["semver"]))
    except Exception as e:
        raise CLIReleaseException(FeedbackManager.error_exception(error=str(e)))


@release.command(name="rm", short_help="Removes a preview or failed Release. This action is irreversible")
@click.option(
    "--semver", required=False, type=str, help="Semver of a preview or failed Release to delete. Example: 1.0.0"
)
@click.option(
    "--oldest-rollback", is_flag=True, default=False, help="Removes the oldest rollback Release by creation date"
)
@click.option(
    "--force", is_flag=True, default=False, help="USE WITH CAUTION! Allows to delete a Release that is currently in use"
)
@click.option("--yes", is_flag=True, default=False, help="Do not ask for confirmation")
@click.option(
    "--dry-run", is_flag=True, default=False, help="Checks the Release could be deleted without actually deleting it"
)
@coro
async def release_rm(semver: str, oldest_rollback: bool, force: bool, yes: bool, dry_run: bool) -> None:
    click.echo(FeedbackManager.warning_deprecated_releases())
    if (not semver and not oldest_rollback) or (semver and oldest_rollback):
        raise CLIException(FeedbackManager.error_release_rm_param())

    config = CLIConfig.get_project_config()
    client = config.get_client()
    _ = await try_update_config_with_remote(config)

    if oldest_rollback:
        oldest_rollback_semver = await get_oldest_rollback(config, client)
        if not oldest_rollback_semver:
            click.echo(FeedbackManager.info_release_no_rollback())
            return
        else:
            semver = oldest_rollback_semver
    await print_release_summary(config, semver, info=True, dry_run=dry_run)
    if dry_run or yes or click.confirm(FeedbackManager.warning_confirm_delete_release(semver=semver)):
        try:
            await remove_release(dry_run, config, semver, client, force, show_print=False)
        except Exception as e:
            raise CLIReleaseException(FeedbackManager.simple_error_exception(error=str(e)))


@cli.group()
def branch() -> None:
    """Branch commands. Branches are an experimental feature only available in beta. Running branch commands without activation will return an error"""
    pass


@branch.command(name="ls")
@click.option("--sort/--no-sort", default=False, help="Sort the table rows by name")
@coro
async def branch_ls(sort: bool) -> None:
    """List all the branches available using the workspace token"""

    config = CLIConfig.get_project_config()
    _ = await try_update_config_with_remote(config, only_if_needed=True)

    client = config.get_client()

    current_main_workspace = await get_current_main_workspace(config)
    assert isinstance(current_main_workspace, dict)

    if current_main_workspace["id"] != config["id"]:
        client = config.get_client(token=current_main_workspace["token"])

    current_main_owner_email = get_workspace_member_email(current_main_workspace, current_main_workspace["owner"])

    response = await client.branches()

    columns = ["name", "id", "created_at", "owner", "current"]

    table: List[Tuple[str, str, str, str, bool]] = [
        (
            MAIN_BRANCH,
            current_main_workspace["id"],
            current_main_workspace["created_at"],
            current_main_owner_email,
            config["id"] == current_main_workspace["id"],
        )
    ]

    for branch in response["environments"]:
        branch_owner_email = get_workspace_member_email(branch, branch["owner"])

        table.append(
            (branch["name"], branch["id"], branch["created_at"], branch_owner_email, config["id"] == branch["id"])
        )

    current_branch = [row for row in table if row[4]]
    other_branches = [row for row in table if not row[4]]

    if sort:
        other_branches.sort(key=lambda x: x[0])

    sorted_table = current_branch + other_branches

    await print_current_workspace(config)

    click.echo(FeedbackManager.info_branches())
    echo_safe_humanfriendly_tables_format_smart_table(sorted_table, column_names=columns)


@branch.command(name="use")
@click.argument("branch_name_or_id")
@coro
async def branch_use(branch_name_or_id: str) -> None:
    """Switch to another Branch (requires an admin token associated with a user). Use 'tb branch ls' to list the Branches you can access"""

    config = CLIConfig.get_project_config()
    _ = await try_update_config_with_remote(config, only_if_needed=True)

    if branch_name_or_id == MAIN_BRANCH:
        current_main_workspace = await get_current_main_workspace(config)
        assert isinstance(current_main_workspace, dict)
        await switch_to_workspace_by_user_workspace_data(config, current_main_workspace)
    else:
        await switch_workspace(config, branch_name_or_id, only_environments=True)


@branch.command(name="current")
@coro
async def branch_current() -> None:
    """Show the Branch you're currently authenticated to"""
    config = CLIConfig.get_project_config()
    await print_current_branch(config)


@branch.command(name="create", short_help="Create a new Branch in the current 'main' Workspace")
@click.argument("branch_name", required=False)
@click.option(
    "--last-partition",
    is_flag=True,
    default=False,
    help="Attach the last modified partition from 'main' to the new Branch",
)
@click.option(
    "--all",
    is_flag=True,
    default=False,
    help="Attach all data from 'main' to the new Branch. Use only if you actually need all the data in the Branch",
    hidden=True,
)
@click.option(
    "-i",
    "--ignore-datasource",
    "ignore_datasources",
    type=str,
    multiple=True,
    help="Ignore specified data source partitions",
)
@click.option(
    "--wait/--no-wait",
    is_flag=True,
    default=True,
    help="Wait for data branch jobs to finish, showing a progress bar. Disabled by default.",
)
@coro
async def create_branch(
    branch_name: Optional[str], last_partition: bool, all: bool, ignore_datasources: List[str], wait: bool
) -> None:
    if last_partition and all:
        raise CLIException(FeedbackManager.error_exception(error="Use --last-partition or --all but not both"))
    await create_workspace_branch(branch_name, last_partition, all, list(ignore_datasources), wait)


@branch.command(name="rm", short_help="Removes a Branch from the Workspace. It can't be recovered.")
@click.argument("branch_name_or_id")
@click.option("--yes", is_flag=True, default=False, help="Do not ask for confirmation")
@coro
async def delete_branch(branch_name_or_id: str, yes: bool) -> None:
    """Remove an Branch (not Main)"""

    config = CLIConfig.get_project_config()
    _ = await try_update_config_with_remote(config)

    client = config.get_client()

    if branch_name_or_id == MAIN_BRANCH:
        raise CLIException(FeedbackManager.error_not_allowed_in_main_branch())

    try:
        workspace_branches = await get_current_workspace_branches(config)
        workspace_to_delete = next(
            (
                workspace
                for workspace in workspace_branches
                if workspace["name"] == branch_name_or_id or workspace["id"] == branch_name_or_id
            ),
            None,
        )
    except Exception as e:
        raise CLIBranchException(FeedbackManager.error_exception(error=str(e)))

    if not workspace_to_delete:
        raise CLIBranchException(FeedbackManager.error_branch(branch=branch_name_or_id))

    if yes or click.confirm(FeedbackManager.warning_confirm_delete_branch(branch=workspace_to_delete["name"])):
        need_to_switch_to_main = workspace_to_delete.get("main") and config["id"] == workspace_to_delete["id"]
        # get origin workspace if deleting current branch
        if need_to_switch_to_main:
            try:
                workspaces = (await client.user_workspaces()).get("workspaces", [])
                workspace_main = next(
                    (workspace for workspace in workspaces if workspace["id"] == workspace_to_delete["main"]), None
                )
            except Exception:
                workspace_main = None
        try:
            await client.delete_branch(workspace_to_delete["id"])
            click.echo(FeedbackManager.success_branch_deleted(branch_name=workspace_to_delete["name"]))
        except Exception as e:
            raise CLIBranchException(FeedbackManager.error_exception(error=str(e)))
        else:
            if need_to_switch_to_main:
                if workspace_main:
                    await switch_to_workspace_by_user_workspace_data(config, workspace_main)
                else:
                    raise CLIException(FeedbackManager.error_switching_to_main())


@branch.command(
    name="data",
    short_help="Perform a data branch operation to bring data into the current Branch. Check flags for details",
)
@click.option(
    "--last-partition",
    is_flag=True,
    default=False,
    help="Attach the last modified partition from 'main' to the new Branch",
)
@click.option(
    "--all",
    is_flag=True,
    default=False,
    help="Attach all data from 'main' to the new Branch. Use only if you actually need all the data in the Branch",
    hidden=True,
)
@click.option(
    "-i",
    "--ignore-datasource",
    "ignore_datasources",
    type=str,
    multiple=True,
    help="Ignore specified data source partitions",
)
@click.option(
    "--wait",
    is_flag=True,
    default=False,
    help="Wait for data branch jobs to finish, showing a progress bar. Disabled by default.",
)
@coro
async def data_branch(last_partition: bool, all: bool, ignore_datasources: List[str], wait: bool) -> None:
    if last_partition and all:
        raise CLIException(FeedbackManager.error_exception(error="Use --last-partition or --all but not both"))

    if not last_partition and not all:
        raise CLIException(FeedbackManager.error_exception(error="Use --last-partition or --all"))

    config = CLIConfig.get_project_config()
    client = config.get_client()

    current_main_workspace = await get_current_main_workspace(config)
    assert isinstance(current_main_workspace, dict)

    if current_main_workspace["id"] == config["id"]:
        raise CLIException(FeedbackManager.error_not_allowed_in_main_branch())

    try:
        response = await client.branch_workspace_data(config["id"], last_partition, all, ignore_datasources)

        is_job: bool = "job" in response
        is_summary: bool = "partitions" in response

        if not is_job and not is_summary:
            raise CLIBranchException(str(response))

        if all and not is_job:
            raise CLIBranchException(str(response))

        if wait and is_job:
            job_id = response["job"]["job_id"]
            job_url = response["job"]["job_url"]
            click.echo(FeedbackManager.info_data_branch_job_url(url=job_url))
            job_response = await wait_job(client, job_id, job_url, "Branch creation")
            response = job_response["result"]
            is_job = False
            is_summary = "partitions" in response

        if is_job:
            click.echo(FeedbackManager.success_workspace_data_branch_in_progress(job_url=response["job"]["job_url"]))
        else:
            if not is_job and not is_summary:
                FeedbackManager.warning_unknown_response(response=response)
            elif is_summary and (bool(last_partition) or bool(all)):
                await print_data_branch_summary(client, None, response)
            click.echo(FeedbackManager.success_workspace_data_branch())

    except Exception as e:
        raise CLIBranchException(FeedbackManager.error_exception(error=str(e)))


@branch.group("regression-tests", invoke_without_command=True)
@click.option(
    "-f",
    "--filename",
    type=click.Path(exists=True),
    required=False,
    help="The yaml file with the regression-tests definition",
)
@click.option(
    "--wait",
    is_flag=True,
    default=False,
    help="Wait for regression job to finish, showing a progress bar. Disabled by default.",
)
@click.option(
    "--skip-regression-tests/--no-skip-regression-tests",
    envvar="TB_SKIP_REGRESSION",
    default=False,
    help="Flag to skip execution of regression tests. This is handy for CI branches where regression might be flaky",
)
@click.option(
    "--main",
    is_flag=True,
    default=False,
    help="Run regression tests in the main Branch. For this flag to work all the resources in the Branch pipe endpoints need to exist in the main Branch.",
)
@click.pass_context
@coro
async def regression_tests(
    ctx, filename: str, wait: bool, skip_regression_tests: Optional[bool] = False, main: Optional[bool] = False
):
    """Regression test commands for Branches"""
    if skip_regression_tests:
        click.echo(FeedbackManager.warning_regression_skipped())
        return

    if filename:
        try:
            with open(filename, "r") as file:  # noqa: ASYNC230
                regression_tests_commands = yaml.safe_load(file)
        except Exception as exc:
            raise CLIBranchException(FeedbackManager.error_regression_yaml_not_valid(filename=filename, error=exc))
        if not isinstance(regression_tests_commands, List):
            raise CLIBranchException(
                FeedbackManager.error_regression_yaml_not_valid(filename=filename, error="not a list of pipes")
            )

        config = CLIConfig.get_project_config()
        client = config.get_client()

        current_main_workspace = await get_current_main_workspace(config)
        assert isinstance(current_main_workspace, dict)

        if current_main_workspace["id"] == config["id"]:
            raise CLIException(FeedbackManager.error_not_allowed_in_main_branch())
            return
        try:
            response = await client.branch_regression_tests_file(
                config["id"], regression_tests_commands, run_in_main=main
            )
            if "job" not in response:
                raise CLIBranchException(str(response))
            job_id = response["job"]["job_id"]
            job_url = response["job"]["job_url"]
            click.echo(FeedbackManager.info_regression_tests_branch_job_url(url=job_url))
            if wait:
                await wait_job(client, job_id, job_url, "Regression tests")
                await print_branch_regression_tests_summary(client, job_id, config["host"])
        except Exception as e:
            raise CLIBranchException(FeedbackManager.error_exception(error=str(e)))
    else:
        if not ctx.invoked_subcommand:
            await _run_regression(type="coverage", wait=wait, run_in_main=main)


async def _run_regression(
    type: str,
    pipe_name: Optional[str] = None,
    assert_result: Optional[bool] = True,
    assert_result_no_error: Optional[bool] = True,
    assert_result_rows_count: Optional[bool] = True,
    assert_result_ignore_order: Optional[bool] = False,
    assert_time_increase_percentage: Optional[int] = 25,
    assert_bytes_read_increase_percentage: Optional[int] = 25,
    assert_max_time: Optional[float] = 0.3,
    failfast: Optional[bool] = False,
    wait: Optional[bool] = False,
    skip: Optional[bool] = False,
    run_in_main: Optional[bool] = False,
    **kwargs,
):
    if skip:
        click.echo(FeedbackManager.warning_regression_skipped())
        return

    config = CLIConfig.get_project_config()
    client = config.get_client()

    current_main_workspace = await get_current_main_workspace(config)
    assert isinstance(current_main_workspace, dict)

    if current_main_workspace["id"] == config["id"]:
        raise CLIException(FeedbackManager.error_not_allowed_in_main_branch())
    try:
        response = await client.branch_regression_tests(
            config["id"],
            pipe_name,
            type,
            failfast=failfast,
            assert_result=assert_result,
            assert_result_no_error=assert_result_no_error,
            assert_result_rows_count=assert_result_rows_count,
            assert_result_ignore_order=assert_result_ignore_order,
            assert_time_increase_percentage=assert_time_increase_percentage,
            assert_bytes_read_increase_percentage=assert_bytes_read_increase_percentage,
            assert_max_time=assert_max_time,
            run_in_main=run_in_main,
            **kwargs,
        )
        if "job" not in response:
            raise CLIBranchException(str(response))
        job_id = response["job"]["job_id"]
        job_url = response["job"]["job_url"]
        click.echo(FeedbackManager.info_regression_tests_branch_job_url(url=job_url))
        if wait:
            await wait_job(client, job_id, job_url, "Regression tests")
            await print_branch_regression_tests_summary(client, job_id, config["host"])
    except Exception as e:
        raise CLIBranchException(FeedbackManager.error_exception(error=str(e)))


@regression_tests.command(
    name="coverage",
    short_help="Run regression tests using coverage requests for Branch vs Main Workspace. It creates a regression-tests job. The argument pipe_name supports regular expressions. Using '.*' if no pipe_name is provided",
)
@click.argument("pipe_name", required=False)
@click.option(
    "--assert-result/--no-assert-result",
    is_flag=True,
    default=True,
    help="Whether to perform an assertion on the results returned by the endpoint. Enabled by default. Use --no-assert-result if you expect the endpoint output is different from current version",
)
@click.option(
    "--assert-result-no-error/--no-assert-result-no-error",
    is_flag=True,
    default=True,
    help="Whether to verify that the endpoint does not return errors. Enabled by default. Use --no-assert-result-no-error if you expect errors from the endpoint",
)
@click.option(
    "--assert-result-rows-count/--no-assert-result-rows-count",
    is_flag=True,
    default=True,
    help="Whether to verify that the correct number of elements are returned in the results. Enabled by default. Use --no-assert-result-rows-count if you expect the numbers of elements in the endpoint output is different from current version",
)
@click.option(
    "--assert-result-ignore-order/--no-assert-result-ignore-order",
    is_flag=True,
    default=False,
    help="Whether to ignore the order of the elements in the results. Disabled by default. Use --assert-result-ignore-order if you expect the endpoint output is returning same elements but in different order",
)
@click.option(
    "--assert-time-increase-percentage",
    type=int,
    required=False,
    default=25,
    help="Allowed percentage increase in endpoint response time. Default value is 25%. Use -1 to disable assert.",
)
@click.option(
    "--assert-bytes-read-increase-percentage",
    type=int,
    required=False,
    default=25,
    help="Allowed percentage increase in the amount of bytes read by the endpoint. Default value is 25%. Use -1 to disable assert",
)
@click.option(
    "--assert-max-time",
    type=float,
    required=False,
    default=0.3,
    help="Max time allowed for the endpoint response time. If the response time is lower than this value then the --assert-time-increase-percentage is not taken into account.",
)
@click.option(
    "-ff", "--failfast", is_flag=True, default=False, help="When set, the checker will exit as soon one test fails"
)
@click.option(
    "--wait",
    is_flag=True,
    default=False,
    help="Waits for regression job to finish, showing a progress bar. Disabled by default.",
)
@click.option(
    "--skip-regression-tests/--no-skip-regression-tests",
    envvar="TB_SKIP_REGRESSION",
    default=False,
    help="Flag to skip execution of regression tests. This is handy for CI branches where regression might be flaky",
)
@click.option(
    "--main",
    is_flag=True,
    default=False,
    help="Run regression tests in the main Branch. For this flag to work all the resources in the Branch pipe endpoints need to exist in the main Branch.",
)
@coro
async def coverage(
    pipe_name: str,
    assert_result: bool,
    assert_result_no_error: bool,
    assert_result_rows_count: bool,
    assert_result_ignore_order: bool,
    assert_time_increase_percentage: int,
    assert_bytes_read_increase_percentage: int,
    assert_max_time: float,
    failfast: bool,
    wait: bool,
    skip_regression_tests: Optional[bool] = False,
    main: Optional[bool] = False,
):
    await _run_regression(
        "coverage",
        pipe_name,
        assert_result,
        assert_result_no_error,
        assert_result_rows_count,
        assert_result_ignore_order,
        assert_time_increase_percentage,
        assert_bytes_read_increase_percentage,
        assert_max_time,
        failfast,
        wait,
        skip_regression_tests,
        run_in_main=main,
    )


@regression_tests.command(
    name="last",
    short_help="Run regression tests using last requests for Branch vs Main Workspace. It creates a regression-tests job. The argument pipe_name supports regular expressions. Using '.*' if no pipe_name is provided",
)
@click.argument("pipe_name", required=False)
@click.option(
    "-l",
    "--limit",
    type=click.IntRange(1, 100),
    default=10,
    required=False,
    help="Number of requests to validate. Default is 10",
)
@click.option(
    "--assert-result/--no-assert-result",
    is_flag=True,
    default=True,
    help="Whether to perform an assertion on the results returned by the endpoint. Enabled by default. Use --no-assert-result if you expect the endpoint output is different from current version",
)
@click.option(
    "--assert-result-no-error/--no-assert-result-no-error",
    is_flag=True,
    default=True,
    help="Whether to verify that the endpoint does not return errors. Enabled by default. Use --no-assert-result-no-error if you expect errors from the endpoint",
)
@click.option(
    "--assert-result-rows-count/--no-assert-result-rows-count",
    is_flag=True,
    default=True,
    help="Whether to verify that the correct number of elements are returned in the results. Enabled by default. Use --no-assert-result-rows-count if you expect the numbers of elements in the endpoint output is different from current version",
)
@click.option(
    "--assert-result-ignore-order/--no-assert-result-ignore-order",
    is_flag=True,
    default=False,
    help="Whether to ignore the order of the elements in the results. Disabled by default. Use --assert-result-ignore-order if you expect the endpoint output is returning same elements but in different order",
)
@click.option(
    "--assert-time-increase-percentage",
    type=int,
    required=False,
    default=25,
    help="Allowed percentage increase in endpoint response time. Default value is 25%. Use -1 to disable assert.",
)
@click.option(
    "--assert-bytes-read-increase-percentage",
    type=int,
    required=False,
    default=25,
    help="Allowed percentage increase in the amount of bytes read by the endpoint. Default value is 25%. Use -1 to disable assert",
)
@click.option(
    "--assert-max-time",
    type=float,
    required=False,
    default=0.3,
    help="Max time allowed for the endpoint response time. If the response time is lower than this value then the --assert-time-increase-percentage is not taken into account.",
)
@click.option(
    "-ff", "--failfast", is_flag=True, default=False, help="When set, the checker will exit as soon one test fails"
)
@click.option(
    "--wait",
    is_flag=True,
    default=False,
    help="Waits for regression job to finish, showing a progress bar. Disabled by default.",
)
@click.option(
    "--skip-regression-tests/--no-skip-regression-tests",
    envvar="TB_SKIP_REGRESSION",
    default=False,
    help="Flag to skip execution of regression tests. This is handy for CI branches where regression might be flaky",
)
@coro
async def last(
    pipe_name: str,
    limit: int,
    assert_result: bool,
    assert_result_no_error: bool,
    assert_result_rows_count: bool,
    assert_result_ignore_order: bool,
    assert_time_increase_percentage: int,
    assert_bytes_read_increase_percentage: int,
    assert_max_time: float,
    failfast: bool,
    wait: bool,
    skip_regression_tests: Optional[bool] = False,
):
    await _run_regression(
        "last",
        pipe_name,
        assert_result,
        assert_result_no_error,
        assert_result_rows_count,
        assert_result_ignore_order,
        assert_time_increase_percentage,
        assert_bytes_read_increase_percentage,
        assert_max_time,
        failfast,
        wait,
        skip_regression_tests,
        limit=limit,
    )


@regression_tests.command(
    name="manual",
    short_help="Run regression tests using manual requests for Branch vs Main Workspace. It creates a regression-tests job. The argument pipe_name supports regular expressions. Using '.*' if no pipe_name is provided",
    context_settings=dict(allow_extra_args=True, ignore_unknown_options=True),
)
@click.argument("pipe_name", required=False)
@click.option(
    "--assert-result/--no-assert-result",
    is_flag=True,
    default=True,
    help="Whether to perform an assertion on the results returned by the endpoint. Enabled by default. Use --no-assert-result if you expect the endpoint output is different from current version",
)
@click.option(
    "--assert-result-no-error/--no-assert-result-no-error",
    is_flag=True,
    default=True,
    help="Whether to verify that the endpoint does not return errors. Enabled by default. Use --no-assert-result-no-error if you expect errors from the endpoint",
)
@click.option(
    "--assert-result-rows-count/--no-assert-result-rows-count",
    is_flag=True,
    default=True,
    help="Whether to verify that the correct number of elements are returned in the results. Enabled by default. Use --no-assert-result-rows-count if you expect the numbers of elements in the endpoint output is different from current version",
)
@click.option(
    "--assert-result-ignore-order/--no-assert-result-ignore-order",
    is_flag=True,
    default=False,
    help="Whether to ignore the order of the elements in the results. Disabled by default. Use --assert-result-ignore-order if you expect the endpoint output is returning same elements but in different order",
)
@click.option(
    "--assert-time-increase-percentage",
    type=int,
    required=False,
    default=25,
    help="Allowed percentage increase in endpoint response time. Default value is 25%. Use -1 to disable assert.",
)
@click.option(
    "--assert-bytes-read-increase-percentage",
    type=int,
    required=False,
    default=25,
    help="Allowed percentage increase in the amount of bytes read by the endpoint. Default value is 25%. Use -1 to disable assert",
)
@click.option(
    "--assert-max-time",
    type=float,
    required=False,
    default=0.3,
    help="Max time allowed for the endpoint response time. If the response time is lower than this value then the --assert-time-increase-percentage is not taken into account.",
)
@click.option(
    "-ff", "--failfast", is_flag=True, default=False, help="When set, the checker will exit as soon one test fails"
)
@click.option(
    "--wait",
    is_flag=True,
    default=False,
    help="Waits for regression job to finish, showing a progress bar. Disabled by default.",
)
@click.option(
    "--skip-regression-tests/--no-skip-regression-tests",
    envvar="TB_SKIP_REGRESSION",
    default=False,
    help="Flag to skip execution of regression tests. This is handy for CI branches where regression might be flaky",
)
@click.pass_context
@coro
async def manual(
    ctx: click.Context,
    pipe_name: str,
    assert_result: bool,
    assert_result_no_error: bool,
    assert_result_rows_count: bool,
    assert_result_ignore_order: bool,
    assert_time_increase_percentage: int,
    assert_bytes_read_increase_percentage: int,
    assert_max_time: float,
    failfast: bool,
    wait: bool,
    skip_regression_tests: Optional[bool] = False,
):
    params = [{ctx.args[i][2:]: ctx.args[i + 1] for i in range(0, len(ctx.args), 2)}]
    await _run_regression(
        "manual",
        pipe_name,
        assert_result,
        assert_result_no_error,
        assert_result_rows_count,
        assert_result_ignore_order,
        assert_time_increase_percentage,
        assert_bytes_read_increase_percentage,
        assert_max_time,
        failfast,
        wait,
        skip_regression_tests,
        params=params,
    )


@branch.group()
def datasource() -> None:
    """Branch data source commands."""


@datasource.command(name="copy")
@click.argument("datasource_name")
@click.option(
    "--sql",
    default=None,
    help="Freeform SQL query to select what is copied from Main into the Branch Data Source",
    required=False,
)
@click.option(
    "--sql-from-main",
    is_flag=True,
    default=False,
    help="SQL query selecting * from the same Data Source in Main",
    required=False,
)
@click.option("--wait", is_flag=True, default=False, help="Wait for copy job to finish, disabled by default")
@coro
async def datasource_copy_from_main(datasource_name: str, sql: str, sql_from_main: bool, wait: bool) -> None:
    """Copy data source from Main."""

    if sql and sql_from_main:
        raise CLIException(FeedbackManager.error_exception(error="Use --sql or --sql-from-main but not both"))

    if not sql and not sql_from_main:
        raise CLIException(FeedbackManager.error_exception(error="Use --sql or --sql-from-main"))

    config = CLIConfig.get_project_config()

    current_main_workspace = await get_current_main_workspace(config)
    assert isinstance(current_main_workspace, dict)

    if current_main_workspace["id"] == config["id"] and sql_from_main:
        raise CLIException(FeedbackManager.error_not_allowed_in_main_branch())

    client = config.get_client()

    response = await client.datasource_query_copy(
        datasource_name, sql if sql else f"SELECT * FROM main.{datasource_name}"
    )
    if "job" not in response:
        raise CLIBranchException(response)
    job_id = response["job"]["job_id"]
    job_url = response["job"]["job_url"]
    if sql:
        click.echo(FeedbackManager.info_copy_with_sql_job_url(sql=sql, datasource_name=datasource_name, url=job_url))
    else:
        click.echo(FeedbackManager.info_copy_from_main_job_url(datasource_name=datasource_name, url=job_url))
    if wait:
        base_msg = "Copy from Main Workspace" if sql_from_main else f"Copy from {sql}"
        await wait_job(client, job_id, job_url, f"{base_msg} to {datasource_name}")


async def warn_if_in_live(semver: str) -> None:
    if not semver:
        return

    config = CLIConfig.get_project_config()
    current_workspace = await get_current_workspace(config)
    assert isinstance(current_workspace, dict)

    is_branch = current_workspace.get("is_branch", False)
    release = current_workspace.get("release", {})
    live_semver = release and release.get("semver", None)

    if is_branch:
        return

    if live_semver and live_semver == semver:
        click.echo(FeedbackManager.warning_release_risky_operation_in_live())
        click.echo("")
