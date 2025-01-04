import json
from os import environ, getcwd
from pathlib import Path
from typing import Any, Dict, Optional

import aiofiles
import click

from tinybird import __cli__
from tinybird.feedback_manager import FeedbackManager

try:
    from tinybird.__cli__ import __revision__
except Exception:
    __revision__ = ""

DEFAULT_API_HOST = "https://api.tinybird.co"
DEFAULT_LOCALHOST = "http://localhost:8001"
CURRENT_VERSION = f"{__cli__.__version__}"
VERSION = f"{__cli__.__version__} (rev {__revision__})"
DEFAULT_UI_HOST = "https://app.tinybird.co"
SUPPORTED_CONNECTORS = ["bigquery", "snowflake"]
PROJECT_PATHS = ["datasources", "datasources/fixtures", "endpoints", "pipes", "tests", "scripts", "deploy"]
DEPRECATED_PROJECT_PATHS = ["endpoints"]
MIN_WORKSPACE_ID_LENGTH = 36
LEGACY_HOSTS = {
    "https://api.tinybird.co": "https://app.tinybird.co/gcp/europe-west3",
    "https://api.us-east.tinybird.co": "https://app.tinybird.co/gcp/us-east4",
    "https://api.us-east.aws.tinybird.co": "https://app.tinybird.co/aws/us-east-1",
    "https://api.us-west-2.aws.tinybird.co": "https://app.tinybird.co/aws/us-west-2",
    "https://api.eu-central-1.aws.tinybird.co": "https://app.tinybird.co/aws/eu-central-1",
    "https://api.ap-east.aws.tinybird.co": "https://app.tinybird.co/aws/ap-east",
    "https://api.wadus1.gcp.tinybird.co": "https://app.wadus.tinybird.co/gcp/wadus1",
    "https://api.wadus2.gcp.tinybird.co": "https://app.wadus.tinybird.co/gcp/wadus2",
    "https://api.wadus3.gcp.tinybird.co": "https://app.wadus.tinybird.co/gcp/wadus3",
    "https://api.wadus4.gcp.tinybird.co": "https://app.wadus.tinybird.co/gcp/wadus4",
    "https://api.wadus5.gcp.tinybird.co": "https://app.wadus.tinybird.co/gcp/wadus5",
    "https://api.wadus6.gcp.tinybird.co": "https://app.wadus.tinybird.co/gcp/wadus6",
    "https://api.wadus1.aws.tinybird.co": "https://app.wadus.tinybird.co/aws/wadus1",
    "https://api.wadus2.aws.tinybird.co": "https://app.wadus.tinybird.co/aws/wadus2",
    "https://api.wadus3.aws.tinybird.co": "https://app.wadus.tinybird.co/aws/wadus3",
    "https://api.wadus4.aws.tinybird.co": "https://app.wadus.tinybird.co/aws/wadus4",
    "https://api.wadus5.aws.tinybird.co": "https://app.wadus.tinybird.co/aws/wadus5",
    "https://api.wadus6.aws.tinybird.co": "https://app.wadus.tinybird.co/aws/wadus6",
    "https://ui.tinybird.co": "https://app.tinybird.co/gcp/europe-west3",
    "https://ui.us-east.tinybird.co": "https://app.tinybird.co/gcp/us-east4",
    "https://ui.us-east.aws.tinybird.co": "https://app.tinybird.co/aws/us-east-1",
    "https://ui.us-west-2.aws.tinybird.co": "https://app.tinybird.co/aws/us-west-2",
    "https://ui.eu-central-1.aws.tinybird.co": "https://app.tinybird.co/aws/eu-central-1",
    "https://ui.ap-east.aws.tinybird.co": "https://app.tinybird.co/aws/ap-east",
    "https://inditex-tech.tinybird.co": "https://app.inditex.tinybird.co/gcp/inditex-tech",
    "https://inditex-c-stg.tinybird.co": "https://app.inditex.tinybird.co/gcp/inditex-c-stg",
    "https://inditex-c-pro.tinybird.co": "https://app.inditex.tinybird.co/gcp/inditex-c-pro",
    "https://inditex-z-stg.tinybird.co": "https://app.inditex.tinybird.co/gcp/inditex-z-stg",
    "https://inditex-rt-pro.tinybird.co": "https://app.inditex.tinybird.co/gcp/inditex-rt-pro",
    "https://inditex-pro.tinybird.co": "https://app.inditex.tinybird.co/gcp/inditex-pro",
    "https://ui.split.tinybird.co": "https://app.tinybird.co/aws/split-us-east",
    "https://ui.split.us-west-2.aws.tinybird.co": "https://app.tinybird.co/aws/split-us-west-2",
    "https://api.split.tinybird.co": "https://app.tinybird.co/aws/split-us-east",
    "https://api.split.us-west-2.aws.tinybird.co": "https://app.tinybird.co/aws/split-us-west-2",
    "https://ui.wadus1.gcp.tinybird.co": "https://app.wadus.tinybird.co/gcp/wadus1",
    "https://ui.wadus2.gcp.tinybird.co": "https://app.wadus.tinybird.co/gcp/wadus2",
    "https://ui.wadus3.gcp.tinybird.co": "https://app.wadus.tinybird.co/gcp/wadus3",
    "https://ui.wadus4.gcp.tinybird.co": "https://app.wadus.tinybird.co/gcp/wadus4",
    "https://ui.wadus5.gcp.tinybird.co": "https://app.wadus.tinybird.co/gcp/wadus5",
    "https://ui.wadus6.gcp.tinybird.co": "https://app.wadus.tinybird.co/gcp/wadus6",
    "https://ui.wadus1.aws.tinybird.co": "https://app.wadus.tinybird.co/aws/wadus1",
    "https://ui.wadus2.aws.tinybird.co": "https://app.wadus.tinybird.co/aws/wadus2",
    "https://ui.wadus3.aws.tinybird.co": "https://app.wadus.tinybird.co/aws/wadus3",
    "https://ui.wadus4.aws.tinybird.co": "https://app.wadus.tinybird.co/aws/wadus4",
    "https://ui.wadus5.aws.tinybird.co": "https://app.wadus.tinybird.co/aws/wadus5",
    "https://ui.wadus6.aws.tinybird.co": "https://app.wadus.tinybird.co/aws/wadus6",
}


async def get_config(host: str, token: Optional[str], semver: Optional[str] = None) -> Dict[str, Any]:
    if host:
        host = host.rstrip("/")

    config_file = Path(getcwd()) / ".tinyb"
    config = {}
    try:
        async with aiofiles.open(config_file) as file:
            res = await file.read()
            config = json.loads(res)
    except IOError:
        pass
    except json.decoder.JSONDecodeError:
        click.echo(FeedbackManager.error_load_file_config(config_file=config_file))
        return config

    config["token_passed"] = token
    config["token"] = token or config.get("token", None)
    config["semver"] = semver or config.get("semver", None)
    config["host"] = host or config.get("host", DEFAULT_API_HOST)
    config["workspaces"] = config.get("workspaces", [])
    return config


async def write_config(config: Dict[str, Any], dest_file: str = ".tinyb"):
    config_file = Path(getcwd()) / dest_file
    async with aiofiles.open(config_file, "w") as file:
        await file.write(json.dumps(config, indent=4, sort_keys=True))


def get_display_host(ui_host: str):
    return LEGACY_HOSTS.get(ui_host, ui_host)


class FeatureFlags:
    @classmethod
    def ignore_sql_errors(cls) -> bool:  # Context: #1155
        return "TB_IGNORE_SQL_ERRORS" in environ

    @classmethod
    def is_localhost(cls) -> bool:
        return "SET_LOCALHOST" in environ
