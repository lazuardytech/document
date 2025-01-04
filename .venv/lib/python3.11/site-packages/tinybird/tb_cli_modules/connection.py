# This is a command file for our CLI. Please keep it clean.
#
# - If it makes sense and only when strictly necessary, you can create utility functions in this file.
# - But please, **do not** interleave utility functions and command definitions.

import os
from os import getcwd
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiofiles
import click
from click import Context

from tinybird.client import DoesNotExistException, TinyB
from tinybird.feedback_manager import FeedbackManager
from tinybird.tb_cli_modules.cli import cli
from tinybird.tb_cli_modules.common import (
    ConnectionReplacements,
    DataConnectorType,
    _get_setting_value,
    coro,
    create_aws_iamrole_connection,
    echo_safe_humanfriendly_tables_format_smart_table,
    get_ca_pem_content,
    validate_aws_iamrole_connection_name,
    validate_aws_iamrole_integration,
    validate_connection_name,
    validate_kafka_auto_offset_reset,
    validate_kafka_bootstrap_servers,
    validate_kafka_key,
    validate_kafka_schema_registry_url,
    validate_kafka_secret,
    validate_string_connector_param,
)
from tinybird.tb_cli_modules.exceptions import CLIConnectionException
from tinybird.tb_cli_modules.telemetry import is_ci_environment

DATA_CONNECTOR_SETTINGS: Dict[DataConnectorType, List[str]] = {
    DataConnectorType.KAFKA: [
        "kafka_bootstrap_servers",
        "kafka_sasl_plain_username",
        "kafka_sasl_plain_password",
        "cli_version",
        "endpoint",
        "kafka_security_protocol",
        "kafka_sasl_mechanism",
        "kafka_schema_registry_url",
        "kafka_ssl_ca_pem",
    ],
    DataConnectorType.GCLOUD_SCHEDULER: ["gcscheduler_region"],
    DataConnectorType.SNOWFLAKE: [
        "account",
        "username",
        "password",
        "role",
        "warehouse",
        "warehouse_size",
        "stage",
        "integration",
    ],
    DataConnectorType.BIGQUERY: ["account"],
    DataConnectorType.GCLOUD_STORAGE: [
        "gcs_private_key_id",
        "gcs_client_x509_cert_url",
        "gcs_project_id",
        "gcs_client_id",
        "gcs_client_email",
        "gcs_private_key",
    ],
    DataConnectorType.GCLOUD_STORAGE_HMAC: [
        "gcs_hmac_access_id",
        "gcs_hmac_secret",
    ],
    DataConnectorType.GCLOUD_STORAGE_SA: ["account_email"],
    DataConnectorType.AMAZON_S3: [
        "s3_access_key_id",
        "s3_secret_access_key",
        "s3_region",
    ],
    DataConnectorType.AMAZON_S3_IAMROLE: [
        "s3_iamrole_arn",
        "s3_iamrole_region",
        "s3_iamrole_external_id",
    ],
    DataConnectorType.AMAZON_DYNAMODB: [
        "dynamodb_iamrole_arn",
        "dynamodb_iamrole_region",
        "dynamodb_iamrole_external_id",
    ],
}

SENSITIVE_CONNECTOR_SETTINGS = {
    DataConnectorType.KAFKA: ["kafka_sasl_plain_password"],
    DataConnectorType.GCLOUD_SCHEDULER: [
        "gcscheduler_target_url",
        "gcscheduler_job_name",
        "gcscheduler_region",
    ],
    DataConnectorType.GCLOUD_STORAGE_HMAC: ["gcs_hmac_secret"],
    DataConnectorType.AMAZON_S3: ["s3_secret_access_key"],
    DataConnectorType.AMAZON_S3_IAMROLE: ["s3_iamrole_arn"],
    DataConnectorType.AMAZON_DYNAMODB: ["dynamodb_iamrole_arn"],
}


@cli.group()
@click.pass_context
def connection(ctx: Context) -> None:
    """Connection commands."""


@connection.group(name="create")
@click.pass_context
def connection_create(ctx: Context) -> None:
    """Connection Create commands."""


@connection_create.command(name="kafka", short_help="Add a Kafka connection")
@click.option("--bootstrap-servers", help="Kafka Bootstrap Server in form mykafka.mycloud.com:9092")
@click.option("--key", help="Key")
@click.option("--secret", help="Secret")
@click.option(
    "--connection-name",
    default=None,
    help="The name of your Kafka connection. If not provided, it's set as the bootstrap server",
)
@click.option(
    "--auto-offset-reset", default=None, help="Offset reset, can be 'latest' or 'earliest'. Defaults to 'latest'."
)
@click.option("--schema-registry-url", default=None, help="Avro Confluent Schema Registry URL")
@click.option(
    "--sasl-mechanism",
    default="PLAIN",
    help="Authentication method for connection-based protocols. Defaults to 'PLAIN'",
)
@click.option("--ssl-ca-pem", default=None, help="Path or content of the CA Certificate file in PEM format")
@click.pass_context
@coro
async def connection_create_kafka(
    ctx: Context,
    bootstrap_servers: str,
    key: str,
    secret: str,
    connection_name: Optional[str],
    auto_offset_reset: Optional[str],
    schema_registry_url: Optional[str],
    sasl_mechanism: Optional[str],
    ssl_ca_pem: Optional[str],
) -> None:
    """
    Add a Kafka connection

    \b
    $ tb connection create kafka --bootstrap-servers google.com:80 --key a --secret b --connection-name c
    """

    bootstrap_servers and validate_kafka_bootstrap_servers(bootstrap_servers)
    key and validate_kafka_key(key)
    secret and validate_kafka_secret(secret)
    schema_registry_url and validate_kafka_schema_registry_url(schema_registry_url)
    auto_offset_reset and validate_kafka_auto_offset_reset(auto_offset_reset)

    if not bootstrap_servers:
        bootstrap_servers = click.prompt("Kafka Bootstrap Server")
        validate_kafka_bootstrap_servers(bootstrap_servers)
    if not key:
        key = click.prompt("Key")
        validate_kafka_key(key)
    if not secret:
        secret = click.prompt("Secret", hide_input=True)
        validate_kafka_secret(secret)
    if not connection_name:
        connection_name = click.prompt(
            f"Connection name (optional, current: {bootstrap_servers})", default=bootstrap_servers
        )

    obj: Dict[str, Any] = ctx.ensure_object(dict)
    client: TinyB = obj["client"]

    result = await client.connection_create_kafka(
        bootstrap_servers,
        key,
        secret,
        connection_name,
        auto_offset_reset,
        schema_registry_url,
        sasl_mechanism,
        get_ca_pem_content(ssl_ca_pem),
    )

    id = result["id"]
    click.echo(FeedbackManager.success_connection_created(id=id))


@connection_create.command(name="snowflake", short_help="Creates a Snowflake connection in the current workspace")
@click.option("--account", help="The account identifier of your Snowflake account (e.g. myorg-account123)")
@click.option("--username", help="The Snowflake user you want to use for the connection")
@click.option("--password", help="The Snowflake password of the chosen user")
@click.option(
    "--warehouse",
    default=None,
    help="If not provided, it's set to your Snowflake user default. Warehouse to run the export sentences.",
)
@click.option(
    "--role",
    default=None,
    help="If not provided, it's set to your Snowflake user default. Snowflake role use in the export process.",
)
@click.option(
    "--connection-name",
    default=None,
    help="The name of your Snowflake connection. If not provided, it's set as the account identifier",
)
@click.option(
    "--integration-name",
    default=None,
    help="The name of your Snowflake integration. If not provided, we will create one.",
)
@click.option(
    "--stage-name", default=None, help="The name of your Snowflake stage. If not provided, we will create one."
)
@click.option("--no-validate", is_flag=True, help="Do not validate Snowflake permissions during connection creation")
@click.pass_context
@coro
async def connection_create_snowflake(
    ctx: Context,
    account: Optional[str],
    username: Optional[str],
    password: Optional[str],
    warehouse: Optional[str],
    role: Optional[str],
    connection_name: Optional[str],
    integration_name: Optional[str],
    stage_name: Optional[str],
    no_validate: Optional[bool],
) -> None:
    """
    Creates a Snowflake connection in the current workspace

    \b
    $ tb connection create snowflake
    """

    obj: Dict[str, Any] = ctx.ensure_object(dict)
    client: TinyB = obj["client"]

    is_connection_valid: bool = True

    if not username:
        username = click.prompt("User (must have created stage and create integration in Snowflake)")
    assert isinstance(username, str)

    if not password:
        password = click.prompt("Password", hide_input=True)
    assert isinstance(password, str)

    if not account:
        account = click.prompt("Account identifier")
    assert isinstance(account, str)

    account_parts = account.split(".", maxsplit=1)
    if len(account_parts) == 2:
        account = "-".join(account_parts)

    if not role:
        roles = await client.get_snowflake_roles(account, username, password) or []
        default_role = roles[0] if len(roles) else ""
        role = click.prompt(
            "Role (optional)",
            type=click.types.Choice(roles, case_sensitive=False),
            show_choices=True,
            default=default_role,
            show_default=True,
        )
    assert isinstance(role, str)

    if not warehouse:
        warehouses = await client.get_snowflake_warehouses(account, username, password, role) or []
        warehouses_names = [w["name"] for w in warehouses]
        default_warehouse = warehouses_names[0] if len(warehouses_names) else ""
        warehouse = click.prompt(
            "Warehouse (optional)",
            type=click.types.Choice(warehouses_names, case_sensitive=False),
            default=default_warehouse,
            show_default=False,
        )
    assert isinstance(warehouse, str)

    if connection_name and no_validate is False:
        if await client.get_connector(connection_name, "snowflake") is not None:
            raise CLIConnectionException(FeedbackManager.info_connection_already_exists(name=connection_name))
    else:
        while not connection_name:
            connection_name = click.prompt(
                f"Connection name (optional, current: {account})", default=account, show_default=False
            )
            assert isinstance(connection_name, str)

            if no_validate is False and await client.get_connector(connection_name, "snowflake") is not None:
                click.echo(FeedbackManager.info_connection_already_exists(name=connection_name))
                connection_name = None
    assert isinstance(connection_name, str)

    show_instructions: bool = not is_ci_environment()

    if show_instructions:
        instructions = await client.get_snowflake_integration_query(role, stage_name, integration_name)
        if instructions:
            for step in instructions.get("steps", []):
                click.echo(step.get("description"))
                click.echo("\n------")
                click.echo(step.get("action"))
                click.echo("------\n")

            while True:
                ans: str = click.prompt(
                    "Ready?", type=click.types.Choice(["Y", "n"], case_sensitive=False), default="Y", show_default=True
                )
                if ans.lower() == "y":
                    break

    conn_file_name = f"{connection_name}.connection"
    conn_file_path = Path(getcwd(), conn_file_name)
    if os.path.isfile(conn_file_path):
        raise CLIConnectionException(FeedbackManager.error_connection_file_already_exists(name=conn_file_name))

    if no_validate is False:
        click.echo("** Validating connection...")
        is_connection_valid = await client.validate_snowflake_connection(account, username, password)

    if not is_connection_valid:
        raise CLIConnectionException(FeedbackManager.error_snowflake_improper_permissions())

    _ = await client.connection_create_snowflake(
        account, username, password, warehouse, role, connection_name, integration_name, stage_name
    )

    async with aiofiles.open(conn_file_path, "w") as f:
        await f.write(
            f"""TYPE snowflake

USERNAME='{username}'
ACCOUNT='{account}'
WAREHOUSE='{warehouse}'
ROLE='{role}'
"""
        )
    click.echo(FeedbackManager.success_connection_file_created(name=conn_file_name))


@connection_create.command(name="bigquery", short_help="Add a BigQuery connection")
@click.option("--no-validate", is_flag=True, help="Do not validate GCP permissions during connection creation")
@click.pass_context
@coro
async def connection_create_bigquery(ctx: Context, no_validate: bool) -> None:
    """
    Add a BigQuery connection

    \b
    $ tb connection create bigquery
    """

    obj: Dict[str, Any] = ctx.ensure_object(dict)
    client: TinyB = obj["client"]

    gcp_account_details: Dict[str, Any] = await client.get_gcp_service_account_details()

    connection_created: bool = False

    while True:
        response = click.prompt(
            FeedbackManager.prompt_bigquery_account(service_account=gcp_account_details["account"]),
            type=click.Choice(["y", "N"], case_sensitive=False),
            default="N",
            show_default=True,
            show_choices=True,
        )

        if response in ("n", "N"):
            click.echo(FeedbackManager.info_cancelled_by_user())
            break

        if no_validate or await client.check_gcp_read_permissions():
            connection_created = True
            break
        else:
            click.echo("\n")
            click.echo(FeedbackManager.error_bigquery_improper_permissions())

    if connection_created:
        async with aiofiles.open(Path(getcwd(), "bigquery.connection"), "w") as f:
            await f.write("TYPE bigquery\n")
        click.echo(FeedbackManager.success_connection_created(id="bigquery"))


@connection.command(name="rm")
@click.argument("connection_id_or_name")
@click.option(
    "--force", default=False, help="Force connection removal even if there are datasources currently using it"
)
@click.pass_context
@coro
async def connection_rm(ctx: Context, connection_id_or_name: str, force: bool) -> None:
    """Remove a connection."""

    obj: Dict[str, Any] = ctx.ensure_object(dict)
    client: TinyB = obj["client"]

    try:
        await client.connector_delete(connection_id_or_name)
    except DoesNotExistException:
        connections = await client.connections()
        connection = next(
            (connection for connection in connections if connection["name"] == connection_id_or_name), None
        )
        if connection:
            try:
                await client.connector_delete(connection["id"])
            except DoesNotExistException:
                raise CLIConnectionException(
                    FeedbackManager.error_connection_does_not_exists(connection_id=connection_id_or_name)
                )
        else:
            raise CLIConnectionException(
                FeedbackManager.error_connection_does_not_exists(connection_id=connection_id_or_name)
            )
    except Exception as e:
        raise CLIConnectionException(FeedbackManager.error_exception(error=e))
    click.echo(FeedbackManager.success_delete_connection(connection_id=connection_id_or_name))


@connection.command(name="ls")
@click.option("--connector", help="Filter by connector")
@click.pass_context
@coro
async def connection_ls(ctx: Context, connector: Optional[DataConnectorType] = None) -> None:
    """List connections."""
    obj: Dict[str, Any] = ctx.ensure_object(dict)
    client: TinyB = obj["client"]

    connections = await client.connections(connector=connector)
    columns = []
    table = []

    click.echo(FeedbackManager.info_connections())

    if not connector:
        sensitive_settings = []
        columns = ["service", "name", "id", "connected_datasources"]
    else:
        sensitive_settings = SENSITIVE_CONNECTOR_SETTINGS.get(connector, [])
        columns = ["service", "name", "id", "connected_datasources"]
        if connector_settings := DATA_CONNECTOR_SETTINGS.get(connector):
            columns += connector_settings

    for connection in connections:
        row = [_get_setting_value(connection, setting, sensitive_settings) for setting in columns]
        table.append(row)

    column_names = [c.replace("kafka_", "") for c in columns]
    echo_safe_humanfriendly_tables_format_smart_table(table, column_names=column_names)
    click.echo("\n")


@connection_create.command(name="s3", short_help="Creates a AWS S3 connection in the current workspace")
@click.option("--key", help="Your Amazon S3 key with access to the buckets")
@click.option("--secret", help="The Amazon S3 secret for the key")
@click.option("--region", help=" The Amazon S3 region where you buckets are located")
@click.option("--connection-name", default=None, help="The name of the connection to identify it in Tinybird")
@click.option("--no-validate", is_flag=True, help="Do not validate S3 permissions during connection creation")
@click.pass_context
@coro
async def connection_create_s3(
    ctx: Context,
    key: Optional[str],
    secret: Optional[str],
    region: Optional[str],
    connection_name: Optional[str],
    no_validate: Optional[bool],
) -> None:
    """
    Creates a S3 connection in the current workspace

    \b
    $ tb connection create s3
    """

    obj: Dict[str, Any] = ctx.ensure_object(dict)
    client: TinyB = obj["client"]

    is_connection_valid = True
    service = "s3"

    if not key:
        key = click.prompt("Key")
        validate_string_connector_param("Key", key)

    if not secret:
        secret = click.prompt("Secret", hide_input=True)
        validate_string_connector_param("Secret", secret)

    if not region:
        region = click.prompt("Region")
        validate_string_connector_param("Region", region)

    if not connection_name:
        connection_name = click.prompt(f"Connection name (optional, current: {key})", default=key)
        await validate_connection_name(client, connection_name, service)

    conn_file_name = f"{connection_name}.connection"
    conn_file_path = Path(getcwd(), conn_file_name)

    if os.path.isfile(conn_file_path):
        raise CLIConnectionException(FeedbackManager.error_connection_file_already_exists(name=conn_file_name))

    params = ConnectionReplacements.map_api_params_from_prompt_params(
        service, key=key, secret=secret, region=region, connection_name=connection_name
    )

    if not no_validate:
        click.echo("** Validating connection...")
        is_connection_valid = await client.validate_preview_connection(service, params)

        if not is_connection_valid:
            raise CLIConnectionException(FeedbackManager.error_connection_improper_permissions())

    click.echo("** Creating connection...")
    _ = await client.connection_create(params)

    async with aiofiles.open(conn_file_path, "w") as f:
        await f.write(
            """TYPE s3

"""
        )
    click.echo(FeedbackManager.success_connection_file_created(name=conn_file_name))


@connection_create.command(
    name="gcs_hmac", short_help="Creates a GCS HMAC connection in the current workspace", hidden=True
)
@click.option("--key", help="Your GCS key with access to the buckets")
@click.option("--secret", help="The GCS secret for the key")
@click.option("--region", help=" The GCS region where you buckets are located")
@click.option("--connection-name", default=None, help="The name of the connection to identify it in Tinybird")
@click.pass_context
@coro
async def connection_create_gcs_hmac(
    ctx: Context,
    key: Optional[str],
    secret: Optional[str],
    region: Optional[str],
    connection_name: Optional[str],
) -> None:
    """
    Creates a GCS HMAC connection in the current workspace

    \b
    $ tb connection create gcs_hmac
    """

    obj: Dict[str, Any] = ctx.ensure_object(dict)
    client: TinyB = obj["client"]
    service = "gcs_hmac"

    if not key:
        key = click.prompt("Key")
        validate_string_connector_param("Key", key)

    if not secret:
        secret = click.prompt("Secret", hide_input=True)
        validate_string_connector_param("Secret", secret)

    if not region:
        region = click.prompt("Region")
        validate_string_connector_param("Region", region)

    if not connection_name:
        connection_name = click.prompt(f"Connection name (optional, current: {key})", default=key)
        await validate_connection_name(client, connection_name, service)

    conn_file_name = f"{connection_name}.connection"
    conn_file_path = Path(getcwd(), conn_file_name)

    if os.path.isfile(conn_file_path):
        raise CLIConnectionException(FeedbackManager.error_connection_file_already_exists(name=conn_file_name))

    params = ConnectionReplacements.map_api_params_from_prompt_params(
        service, key=key, secret=secret, region=region, connection_name=connection_name
    )

    click.echo("** Creating connection...")
    try:
        _ = await client.connection_create(params)
    except Exception as e:
        raise CLIConnectionException(
            FeedbackManager.error_connection_create(connection_name=connection_name, error=str(e))
        )

    async with aiofiles.open(conn_file_path, "w") as f:
        await f.write(
            """TYPE gcs_hmac

"""
        )
    click.echo(FeedbackManager.success_connection_file_created(name=conn_file_name))


@connection_create.command(name="gcs", short_help="Creates a GCS connection in the current workspace", hidden=True)
@click.option("--client-id", help="Your GCS client id")
@click.option("--client-email", help="Your GCS client email")
@click.option("--client-x509-cert-url", help="Your GCS cert url")
@click.option("--project-id", help="The GCS client project id with access to the buckets")
@click.option("--private-key", help="Your GCS private key with access to the buckets")
@click.option("--private-key-id", help="Your GCS private key id with access to the buckets")
@click.option("--connection-name", default=None, help="The name of the connection to identify it in Tinybird")
@click.option("--no-validate", is_flag=True, help="Do not validate Snowflake permissions during connection creation")
@click.pass_context
@coro
async def connection_create_gcs(
    ctx: Context,
    client_id: Optional[str],
    client_email: Optional[str],
    client_x509_cert_url: Optional[str],
    project_id: Optional[str],
    private_key: Optional[str],
    private_key_id: Optional[str],
    connection_name: Optional[str],
    no_validate: Optional[bool],
) -> None:
    """
    Creates a GCS connection in the current workspace

    \b
    $ tb connection create gcs
    """

    obj: Dict[str, Any] = ctx.ensure_object(dict)
    client: TinyB = obj["client"]
    service = "gcs"

    if not project_id:
        project_id = click.prompt("Project id")
        validate_string_connector_param("Project id", project_id)

    if not private_key_id:
        private_key_id = click.prompt("private_key_id")
        validate_string_connector_param("Private key id", private_key_id)

    if not private_key:
        private_key = click.prompt("private_key")
        validate_string_connector_param("Private key", private_key)

    if not client_email:
        client_email = click.prompt("Client email")
        validate_string_connector_param("Client email", client_email)

    if not client_id:
        client_id = click.prompt("Client id")
        validate_string_connector_param("Client id", client_id)

    if not client_x509_cert_url:
        client_x509_cert_url = click.prompt("Client x509 cert url")
        validate_string_connector_param("Client x509 cert url", client_x509_cert_url)

    if not connection_name:
        connection_name = click.prompt(f"Connection name (optional, current: {client_id})", default=client_id)
        await validate_connection_name(client, connection_name, service)

    conn_file_name = f"{connection_name}.connection"
    conn_file_path = Path(getcwd(), conn_file_name)

    if os.path.isfile(conn_file_path):
        raise CLIConnectionException(FeedbackManager.error_connection_file_already_exists(name=conn_file_name))

    params = ConnectionReplacements.map_api_params_from_prompt_params(
        service,
        client_id=client_id,
        client_email=client_email,
        client_x509_cert_url=client_x509_cert_url,
        project_id=project_id,
        private_key=private_key,
        private_key_id=private_key_id,
        connection_name=connection_name,
    )

    if not no_validate:
        click.echo("** Validating connection...")
        is_connection_valid = await client.validate_preview_connection(service, params)

        if not is_connection_valid:
            raise CLIConnectionException(FeedbackManager.error_connection_improper_permissions())

    click.echo("** Creating connection...")
    try:
        _ = await client.connection_create(params)
    except Exception as e:
        raise CLIConnectionException(
            FeedbackManager.error_connection_create(connection_name=connection_name, error=str(e))
        )

    async with aiofiles.open(conn_file_path, "w") as f:
        await f.write(
            """TYPE {service}

"""
        )
    click.echo(FeedbackManager.success_connection_file_created(name=conn_file_name))


@connection_create.command(name="s3_iamrole", short_help="Creates a AWS S3 connection using IAM role authentication")
@click.option("--connection-name", default=None, help="The name of the connection to identify it in Tinybird")
@click.option("--role-arn", default=None, help="The ARN of the IAM role to use for the connection")
@click.option("--region", default=None, help="The Amazon S3 region where the bucket is located")
@click.option("--policy", default="write", help="The Amazon S3 access policy: write or read")
@click.option(
    "--no-validate", is_flag=True, default=False, help="Do not validate S3 permissions during connection creation"
)
@click.pass_context
@coro
async def connection_create_s3_iamrole(
    ctx: Context,
    connection_name: Optional[str] = "",
    role_arn: Optional[str] = "",
    region: Optional[str] = "",
    policy: str = "write",
    no_validate: Optional[bool] = False,
) -> None:
    """
    Creates a S3 connection using IAM role authentication in the current workspace

    \b
    $ tb connection create s3_iamrole
    """

    obj: Dict[str, Any] = ctx.ensure_object(dict)
    client: TinyB = obj["client"]
    service = DataConnectorType.AMAZON_S3_IAMROLE
    role_arn, region, external_id = await validate_aws_iamrole_integration(
        client,
        service=service,
        role_arn=role_arn,
        region=region,
        policy=policy,
        no_validate=no_validate,
    )
    connection_name = await validate_aws_iamrole_connection_name(client, connection_name, no_validate)
    await create_aws_iamrole_connection(
        client, service=service, connection_name=connection_name, role_arn=role_arn, region=region
    )
    if external_id:
        click.echo(
            FeedbackManager.success_s3_iam_connection_created(
                connection_name=connection_name, external_id=external_id, role_arn=role_arn
            )
        )


@connection_create.command(
    name="dynamodb", short_help="Creates a AWS DynamoDB connection using IAM role authentication", hidden=True
)
@click.option("--connection-name", default=None, help="The name of the connection to identify it in Tinybird")
@click.option("--role-arn", default=None, help="The ARN of the IAM role to use for the connection")
@click.option("--region", default=None, help="The AWS region where DynamoDB is located")
@click.option("--no-validate", is_flag=True, default=False, help="Do not validate DynamoDB connection during creation")
@click.pass_context
@coro
async def connection_create_dynamodb(
    ctx: Context,
    connection_name: Optional[str] = "",
    role_arn: Optional[str] = "",
    region: Optional[str] = "",
    no_validate: Optional[bool] = False,
) -> None:
    """
    Creates a DynamoDB connection using IAM role authentication in the current workspace

    \b
    $ tb connection create dynamodb
    """

    obj: Dict[str, Any] = ctx.ensure_object(dict)
    client: TinyB = obj["client"]

    service = DataConnectorType.AMAZON_DYNAMODB
    role_arn, region, _external_id = await validate_aws_iamrole_integration(
        client,
        service=service,
        role_arn=role_arn,
        region=region,
        policy="read",
        no_validate=no_validate,
    )
    connection_name = await validate_aws_iamrole_connection_name(client, connection_name, no_validate)
    await create_aws_iamrole_connection(
        client, service=service, connection_name=connection_name, role_arn=role_arn, region=region
    )
    click.echo(
        FeedbackManager.success_dynamodb_connection_created(
            connection_name=connection_name, region=region, role_arn=role_arn
        )
    )
