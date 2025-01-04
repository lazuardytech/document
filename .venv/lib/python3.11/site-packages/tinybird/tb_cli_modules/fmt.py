import difflib
import sys
from pathlib import Path
from typing import List, Optional

import aiofiles
import click
from click import Context

from tinybird.datafile import color_diff, format_datasource, format_pipe, is_file_a_datasource, peek
from tinybird.feedback_manager import FeedbackManager
from tinybird.tb_cli_modules.cli import cli
from tinybird.tb_cli_modules.common import coro


@cli.command()
@click.argument("filenames", type=click.Path(exists=True), nargs=-1, required=True)
@click.option(
    "--line-length",
    is_flag=False,
    default=100,
    help="A number indicating the maximum characters per line in the node SQL, lines will be splitted based on the SQL syntax and the number of characters passed as a parameter",
)
@click.option("--dry-run", is_flag=True, default=False, help="Don't ask to override the local file")
@click.option("--yes", is_flag=True, default=False, help="Do not ask for confirmation to overwrite the local file")
@click.option(
    "--diff",
    is_flag=True,
    default=False,
    help="Formats local file, prints the diff and exits 1 if different, 0 if equal",
)
@click.pass_context
@coro
async def fmt(
    ctx: Context, filenames: List[str], line_length: int, dry_run: bool, yes: bool, diff: bool
) -> Optional[str]:
    """
    Formats a .datasource, .pipe or .incl file

    This command removes comments starting with # from the file, use DESCRIPTION instead.

    The format command tries to parse the datafile so syntax errors might rise.

    .incl files must contain a NODE definition
    """

    result = ""
    failed = []
    for filename in filenames:
        if not diff:
            click.echo(filename)
        extensions = Path(filename).suffixes
        if is_file_a_datasource(filename):
            result = await format_datasource(filename, skip_eval=True)
        elif (".pipe" in extensions) or (".incl" in extensions):
            result = await format_pipe(filename, line_length, skip_eval=True)
        else:
            click.echo("Unsupported file type. Supported files types are: .pipe, .incl and .datasource")
            return None

        if diff:
            result = result.rstrip("\n")
            lines_fmt = [f"{line}\n" for line in result.split("\n")]
            async with aiofiles.open(filename, "r") as file:
                lines_file = await file.readlines()
            diff_result = difflib.unified_diff(
                lines_file, lines_fmt, fromfile=f"{Path(filename).name} local", tofile="fmt datafile"
            )
            diff_result = color_diff(diff_result)
            not_empty, diff_lines = peek(diff_result)
            if not_empty:
                sys.stdout.writelines(diff_lines)
                failed.append(filename)
                click.echo("")
        else:
            click.echo(result)
            if dry_run:
                return None

            if yes or click.confirm(FeedbackManager.prompt_override_local_file(name=filename)):
                async with aiofiles.open(f"{filename}", "w") as file:
                    await file.write(result)

                click.echo(FeedbackManager.success_generated_local_file(file=filename))

    if len(failed):
        click.echo(FeedbackManager.error_failed_to_format_files(number=len(failed)))
        for f in failed:
            click.echo(f"tb fmt {f} --yes")
        sys.exit(1)
    return result
