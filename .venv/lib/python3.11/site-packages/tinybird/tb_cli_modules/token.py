from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import click
import pyperclip
from click import Context
from humanfriendly import parse_timespan

from tinybird.client import AuthNoTokenException, TinyB
from tinybird.feedback_manager import FeedbackManager
from tinybird.tb_cli_modules.cli import cli
from tinybird.tb_cli_modules.common import (
    DoesNotExistException,
    coro,
    echo_safe_humanfriendly_tables_format_smart_table,
)
from tinybird.tb_cli_modules.exceptions import CLITokenException


@cli.group()
@click.pass_context
def token(ctx: Context) -> None:
    """Token commands."""


@token.command(name="ls")
@click.option("--match", default=None, help="Retrieve any token matching the pattern. eg --match _test")
@click.pass_context
@coro
async def token_ls(
    ctx: Context,
    match: Optional[str] = None,
) -> None:
    """List Static Tokens."""

    obj: Dict[str, Any] = ctx.ensure_object(dict)
    client: TinyB = obj["client"]

    try:
        tokens = await client.token_list(match)
        columns = ["id", "name", "description"]
        table = list(map(lambda token: [token.get(key, "") for key in columns], tokens))

        click.echo(FeedbackManager.info_tokens())
        echo_safe_humanfriendly_tables_format_smart_table(table, column_names=columns)
        click.echo("\n")
    except AuthNoTokenException:
        raise
    except Exception as e:
        raise CLITokenException(FeedbackManager.error_exception(error=e))


@token.command(name="rm")
@click.argument("token_id")
@click.option("--yes", is_flag=True, default=False, help="Do not ask for confirmation")
@click.pass_context
@coro
async def token_rm(ctx: Context, token_id: str, yes: bool) -> None:
    """Remove a static token."""

    obj: Dict[str, Any] = ctx.ensure_object(dict)
    client: TinyB = obj["client"]
    if yes or click.confirm(FeedbackManager.warning_confirm_delete_token(token=token_id)):
        try:
            await client.token_delete(token_id)
        except AuthNoTokenException:
            raise
        except DoesNotExistException:
            raise CLITokenException(FeedbackManager.error_token_does_not_exist(token_id=token_id))
        except Exception as e:
            raise CLITokenException(FeedbackManager.error_exception(error=e))
        click.echo(FeedbackManager.success_delete_token(token=token_id))


@token.command(name="refresh")
@click.argument("token_id")
@click.option("--yes", is_flag=True, default=False, help="Do not ask for confirmation")
@click.pass_context
@coro
async def token_refresh(ctx: Context, token_id: str, yes: bool) -> None:
    """Refresh a Static Token."""

    obj: Dict[str, Any] = ctx.ensure_object(dict)
    client: TinyB = obj["client"]
    if yes or click.confirm(FeedbackManager.warning_confirm_refresh_token(token=token_id)):
        try:
            await client.token_refresh(token_id)
        except AuthNoTokenException:
            raise
        except DoesNotExistException:
            raise CLITokenException(FeedbackManager.error_token_does_not_exist(token_id=token_id))
        except Exception as e:
            raise CLITokenException(FeedbackManager.error_exception(error=e))
        click.echo(FeedbackManager.success_refresh_token(token=token_id))


@token.command(name="scopes")
@click.argument("token_id")
@click.pass_context
@coro
async def token_scopes(ctx: Context, token_id: str) -> None:
    """List Static Token scopes."""

    obj: Dict[str, Any] = ctx.ensure_object(dict)
    client: TinyB = obj["client"]

    try:
        scopes = await client.token_scopes(token_id)
        columns = ["type", "resource", "filter"]
        table = list(map(lambda scope: [scope.get(key, "") for key in columns], scopes))
        click.echo(FeedbackManager.info_token_scopes(token=token_id))
        echo_safe_humanfriendly_tables_format_smart_table(table, column_names=columns)
        click.echo("\n")
    except AuthNoTokenException:
        raise
    except Exception as e:
        raise CLITokenException(FeedbackManager.error_exception(error=e))


@token.command(name="copy")
@click.argument("token_id")
@click.pass_context
@coro
async def token_copy(ctx: Context, token_id: str) -> None:
    """Copy a Static Token."""

    obj: Dict[str, Any] = ctx.ensure_object(dict)
    client: TinyB = obj["client"]

    try:
        token = await client.token_get(token_id)
        pyperclip.copy(token["token"].strip())
    except AuthNoTokenException:
        raise
    except DoesNotExistException:
        raise CLITokenException(FeedbackManager.error_token_does_not_exist(token_id=token_id))
    except Exception as e:
        raise CLITokenException(FeedbackManager.error_exception(error=e))
    click.echo(FeedbackManager.success_copy_token(token=token_id))


def parse_ttl(ctx, param, value):
    if value is None:
        return None
    try:
        seconds = parse_timespan(value)
        return timedelta(seconds=seconds)
    except ValueError:
        raise click.BadParameter(f"Invalid time to live format: {value}")


def parse_fixed_params(fixed_params_list):
    parsed_params = []
    for fixed_param in fixed_params_list:
        param_dict = {}
        for param in fixed_param.split(","):
            key, value = param.split("=")
            param_dict[key] = value
        parsed_params.append(param_dict)
    return parsed_params


@token.group()
@click.pass_context
def create(ctx: Context) -> None:
    """Token creation commands.

    You can create two types of tokens: JWT or Static.

    * JWT tokens have a TTL and can only have the PIPES:READ scope.Their main use case is allow your users to call your endpoints without exposing your API key.

    * Static Tokens do not have a TTL and can have any valid scope (DATASOURCES:READ, DATASOURCES:APPEND, DATASOURCES:CREATE, DATASOURCES:DROP, PIPES:CREATE, PIPES:READ, PIPES:DROP).

    Examples:

    tb token create jwt my_jwt_token --ttl 1h --scope PIPES:READ --resource my_pipe

    tb token create static my_static_token --scope PIPES:READ --resource my_pipe

    tb token create static my_static_token --scope DATASOURCES:READ --resource my_datasource

    tb token create static my_static_token --scope DATASOURCES:READ --resource my_datasource --filters "column_name=value"

    """


@create.command(name="jwt")
@click.argument("name")
@click.option("--ttl", type=str, callback=parse_ttl, required=True, help="Time to live (e.g., '1h', '30min', '1d')")
@click.option(
    "--scope",
    multiple=True,
    type=click.Choice(["PIPES:READ"]),
    required=True,
    help="Scope of the token (only PIPES:READ is allowed for JWT tokens)",
)
@click.option("--resource", multiple=True, required=True, help="Resource associated with the scope")
@click.option(
    "--fixed-params", multiple=True, help="Fixed parameters in key=value format, multiple values separated by commas"
)
@click.pass_context
@coro
async def create_jwt_token(ctx: Context, name: str, ttl: timedelta, scope, resource, fixed_params) -> None:
    """Create a JWT token with a TTL specify."""

    obj: Dict[str, Any] = ctx.ensure_object(dict)
    client: TinyB = obj["client"]

    expiration_time = int((ttl + datetime.now(timezone.utc)).timestamp())
    if len(scope) != len(resource):
        raise CLITokenException(FeedbackManager.error_number_of_scopes_and_resources_mismatch())

    # Ensure the number of fixed-params does not exceed the number of scope/resource pairs
    if fixed_params and len(fixed_params) > len(scope):
        raise CLITokenException(FeedbackManager.error_number_of_fixed_params_and_resources_mismatch())

    # Parse fixed params
    parsed_fixed_params = parse_fixed_params(fixed_params) if fixed_params else []

    # Create a list of fixed params for each scope/resource pair, defaulting to empty dict if not provided
    fixed_params_list: List[Dict[str, Any]] = [{}] * len(scope)
    for i, params in enumerate(parsed_fixed_params):
        fixed_params_list[i] = params

    scopes = []
    for sc, res, fparams in zip(scope, resource, fixed_params_list):
        scopes.append(
            {
                "type": sc,
                "resource": res,
                "fixed_params": fparams,
            }
        )

    try:
        response = await client.create_jwt_token(name, expiration_time, scopes)
    except AuthNoTokenException:
        raise
    except Exception as e:
        raise CLITokenException(FeedbackManager.error_exception(error=e))

    click.echo("The token has been generated successfully.")
    click.echo(
        f"The token will expire at: {datetime.fromtimestamp(expiration_time).strftime('%Y-%m-%d %H:%M:%S')} UTC "
    )
    click.echo(f"The token is: {response['token']}")


# Valid scopes for Static Tokens
valid_scopes = [
    "DATASOURCES:READ",
    "DATASOURCES:APPEND",
    "DATASOURCES:CREATE",
    "DATASOURCES:DROP",
    "PIPES:CREATE",
    "PIPES:READ",
    "PIPES:DROP",
]


# As we are passing dynamic options to the command, we need to create a custom class to handle the help message
class DynamicOptionsCommand(click.Command):
    def get_help(self, ctx):
        # Usage
        usage = "Usage: tb token create static [OPTIONS] NAME\n\n"
        dynamic_options_help = usage

        # Description
        dynamic_options_help += "  Create a Static Token that will live forever.\n\n"

        # Options
        dynamic_options_help += "Options:\n"
        dynamic_options_help += f"  --scope [{','.join(valid_scopes)}]   Scope for the token [Required]\n"
        dynamic_options_help += "  --resource TEXT    Resource you want to associate the scope with\n"
        dynamic_options_help += "  --filter TEXT  SQL condition used to filter the values when calling with this token (eg. --filter=value > 0) \n"
        dynamic_options_help += "  -h, --help            Show this message and exit.\n"

        return dynamic_options_help


@create.command(
    name="static", context_settings=dict(ignore_unknown_options=True, allow_extra_args=True), cls=DynamicOptionsCommand
)
@click.argument("name")
@click.pass_context
@coro
async def create_static_token(ctx, name: str):
    """Create a Static Token."""
    obj: Dict[str, Any] = ctx.ensure_object(dict)
    client: TinyB = obj["client"]

    args = ctx.args
    scopes: List[Dict[str, str]] = []
    current_scope = None

    # We parse the arguments to get the scopes, resources and filters
    # The arguments should be in the format --scope <scope> --resource <resource> --filter <filter>
    i = 0
    while i < len(args):
        if args[i] == "--scope":
            if current_scope:
                scopes.append(current_scope)
                current_scope = {}
            current_scope = {"scope": args[i + 1]}
            i += 2
        elif args[i] == "--resource":
            if current_scope is None:
                raise click.BadParameter("Resource must follow a scope")
            if "resource" in current_scope:
                raise click.BadParameter(
                    "Resource already defined for this scope. The format is --scope <scope> --resource <resource> --filter <filter>"
                )
            current_scope["resource"] = args[i + 1]
            i += 2
        elif args[i] == "--filter":
            if current_scope is None:
                raise click.BadParameter("Filter must follow a scope")
            if "filter" in current_scope:
                raise click.BadParameter(
                    "Filter already defined for this scope. The format is --scope <scope> --resource <resource> --filter <filter>"
                )
            current_scope["filter"] = args[i + 1]
            i += 2
        else:
            raise click.BadParameter(f"Unknown parameter {args[i]}")

    if current_scope:
        scopes.append(current_scope)

    # Parse the scopes like `SCOPE:RESOURCE:FILTER` or `SCOPE:RESOURCE` or `SCOPE` as that's what the API expsects
    scoped_parsed: List[str] = []
    for scope in scopes:
        if scope.get("resource") and scope.get("filter"):
            scoped_parsed.append(f"{scope.get('scope')}:{scope.get('resource')}:{scope.get('filter')}")
        elif scope.get("resource"):
            scoped_parsed.append(f"{scope.get('scope')}:{scope.get('resource')}")
        elif "scope" in scope:
            scoped_parsed.append(scope.get("scope", ""))
        else:
            raise CLITokenException("Unknown error")

    try:
        await client.create_token(name, scoped_parsed, origin_code=None)
    except AuthNoTokenException:
        raise
    except Exception as e:
        raise CLITokenException(FeedbackManager.error_exception(error=e))

    click.echo("The token has been generated successfully.")
