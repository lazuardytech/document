import json
import os
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

from packaging import version

import tinybird.client as tbc
from tinybird.config import CURRENT_VERSION, DEFAULT_API_HOST, DEFAULT_LOCALHOST

APP_CONFIG_NAME = "tinybird"


class FeatureFlags:
    @classmethod
    def ignore_sql_errors(cls) -> bool:  # Context: #1155
        return "TB_IGNORE_SQL_ERRORS" in os.environ

    @classmethod
    def is_localhost(cls) -> bool:
        return "SET_LOCALHOST" in os.environ

    @classmethod
    def ignore_ssl_errors(cls) -> bool:
        return os.environ.get("TB_DISABLE_SSL_CHECKS", "0") == "1"

    @classmethod
    def send_telemetry(cls) -> bool:
        if os.environ.get("TB_CLI_TELEMETRY_OPTOUT", "0") == "1":
            return False
        if "x.y.z" in CURRENT_VERSION and os.environ.get("TB_CLI_TELEMETRY_SEND_IN_LOCAL", "0") == "0":
            return False
        return True


def compare_versions(a: str, b: str) -> int:
    va = version.parse(a)
    vb = version.parse(b)
    return -1 if va < vb else (1 if va > vb else 0)


class ConfigValueOrigin(Enum):
    # Sources for config values (environment variables, .tinyb file or default value)

    ENVIRONMENT: str = "env"
    CONFIG: str = "conf"
    DEFAULT: str = "default"
    NONE: str = ""


@dataclass
class ConfigValue:
    name: str
    value: Any
    origin: ConfigValueOrigin

    def as_tuple(self) -> Tuple[str, str]:
        return (self.name, self.value)


def write_json_file(data: Dict[str, Any], path: str) -> None:
    with open(path, "w") as file:
        file.write(json.dumps(data, indent=4, sort_keys=True))


class CLIConfig:
    # Mapping between environment variables and config values
    ENV_KEYS: Dict[str, str] = {
        "token": "TB_TOKEN",
        "user_token": "TB_USER_TOKEN",
        "host": "TB_HOST",
        "semver": "TB_SEMVER",
    }

    DEFAULTS: Dict[str, str] = {"host": DEFAULT_API_HOST if not FeatureFlags.is_localhost() else DEFAULT_LOCALHOST}

    _global: Optional["CLIConfig"] = None
    _projects: Dict[str, "CLIConfig"] = {}

    def __init__(self, path: Optional[str], parent: Optional["CLIConfig"] = None) -> None:
        self._path = path
        self._parent = parent
        self._values: Dict[str, ConfigValue] = {}
        self._values["version"] = ConfigValue("version", CURRENT_VERSION, ConfigValueOrigin.DEFAULT)
        self._workspaces: List[Dict[str, Any]] = []

        if self._path:
            self.override_with_file(self._path)
        self.override_with_environment()
        self.override_with_defaults()

    def __str__(self) -> str:
        return str(self.to_dict())

    def to_dict(self) -> Dict[str, Any]:
        """Helper to ease"""
        result: Dict[str, Any] = self._parent.to_dict() if self._parent else {}
        result.update(dict((v.name, deepcopy(v.value)) for v in self._values.values()))
        return result

    def __getitem__(self, key) -> Any:
        """Gets a config key value in this order:
        - Environment
        - Internal dict (by host)
        - Parent dict (if has a parent)
        - Default values
        """
        if key in self._values:
            return self._values[key].value
        if self._parent:
            return self._parent[key]
        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        self._values[key] = ConfigValue(key, value, ConfigValueOrigin.CONFIG)

    def __contains__(self, key: str) -> bool:
        return self.get_value_origin(key) != ConfigValueOrigin.NONE

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def get_value_origin(self, key: str) -> ConfigValueOrigin:
        if key in self._values:
            return self._values[key].origin
        if self._parent:
            return self._parent.get_value_origin(key)
        else:
            return ConfigValueOrigin.NONE

    def persist_to_file(self, override_with_values: Optional["CLIConfig"] = None) -> None:
        if not self._path:
            raise ValueError("Cannot persist configuration: `path` is None")

        if override_with_values:
            self.update(override_with_values)

        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        values = dict(v.as_tuple() for v in self._values.values())
        write_json_file(values, self._path)

    def override_with_file(self, path: str) -> bool:
        """Loads the contents of the passed file."""
        try:
            with open(path) as file:
                values: Dict[str, Any] = json.loads(file.read())
                for k, v in values.items():
                    self[k] = v
            return True
        except IOError:
            return False

    def override_with_environment(self) -> None:
        """Loads environment variables."""
        for config_key, env_key in CLIConfig.ENV_KEYS.items():
            env_value = os.environ.get(env_key, None)
            if env_value:
                self._values[config_key] = ConfigValue(config_key, env_value, ConfigValueOrigin.ENVIRONMENT)

    def override_with_defaults(self) -> None:
        """Loads default values."""
        for key, default_value in CLIConfig.DEFAULTS.items():
            if key not in self._values:
                self._values[key] = ConfigValue(key, default_value, ConfigValueOrigin.DEFAULT)

    def set_token(self, token: Optional[str]) -> None:
        self["token"] = token

    def get_token(self) -> Optional[str]:
        try:
            return self["token"]
        except KeyError:
            return None

    def set_semver(self, semver: Optional[str]) -> None:
        self["semver"] = semver

    def get_semver(self) -> Optional[str]:
        try:
            return self["semver"]
        except KeyError:
            return None

    def set_token_for_host(self, token: Optional[str], host: Optional[str]) -> None:
        """Sets the token for the specified host.

        Here we ask for the host explicitly to avoid mistakenly setting the token
        for the wrong host.
        """
        if not token and not host:
            return

        assert isinstance(host, str)

        tokens: Dict[str, Optional[str]] = self.get("tokens", {})
        tokens[host] = token
        self["tokens"] = tokens

    def get_token_for_host(self, host: str) -> Optional[str]:
        try:
            return self["tokens"][host]
        except KeyError:
            return None

    def set_user_token(self, token: Optional[str]) -> None:
        self["user_token"] = token

    def get_user_token(self) -> Optional[str]:
        try:
            return self["user_token"]
        except KeyError:
            return None

    def set_host(self, host: Optional[str]) -> None:
        url_info = urlparse(host)
        scheme: str = url_info.scheme.decode() if isinstance(url_info.scheme, bytes) else url_info.scheme
        netloc: str = url_info.netloc.decode() if isinstance(url_info.netloc, bytes) else url_info.netloc
        self["host"] = f"{scheme}://{netloc}"

    def get_host(self, use_defaults_if_needed: bool = False) -> Optional[str]:
        result: Optional[str] = self.get("host", None)
        if result:
            return result
        if use_defaults_if_needed:
            return CLIConfig.DEFAULTS["host"]
        return None

    def get_client(self, token: Optional[str] = None, host: Optional[str] = None) -> tbc.TinyB:
        """Returns a new TinyB client configured with:

        - token:
          - passed token
          - OR the user token
          - OR the current admin token

        - host:
          - passed host
          - OR the current host
        """
        host = host or self.get_host()
        assert isinstance(host, str)
        token = token or self.get_token() or self.get_token_for_host(host) or ""
        assert isinstance(token, str)

        return tbc.TinyB(
            token,
            host,
            version=CURRENT_VERSION,
            disable_ssl_checks=FeatureFlags.ignore_ssl_errors(),
            send_telemetry=FeatureFlags.send_telemetry(),
        )

    def get_user_client(self, host: Optional[str] = None) -> tbc.TinyB:
        return self.get_client(self.get_user_token(), host)

    def set_workspace_token(self, workspace_id: str, token: str) -> None:
        pass

    async def get_workspace_token(
        self, workspace_id: Optional[str] = None, host: Optional[str] = None
    ) -> Optional[str]:
        """Returns the token for the specific workspace on a host.
        - If no workspace passed, it uses the current active workspace.
        - If no host is passed, it uses the current one.
        """

        # First, try to get any saved token for the host
        if host:
            try:
                return self["tokens"][host]
            except KeyError:
                pass

        # If we don't have an user_token, can't continue
        if not self.get_user_token():
            return None

        if not workspace_id:
            workspace_id = self.get("id", None)
            if not workspace_id:
                return None

        client: tbc.TinyB = self.get_client(token=self.get_user_token(), host=host)

        info: Dict[str, Any] = await client.user_workspaces_and_branches()
        workspaces: List[Dict[str, Any]] = info["workspaces"]

        result: Optional[str] = next(
            (w["token"] for w in workspaces if workspace_id in (w.get("id"), w.get("name"))), None
        )
        if host:
            self.set_token_for_host(result, host)

        return result

    def spawn(self) -> "CLIConfig":
        return CLIConfig(path=None, parent=self)

    def update(self, other: Union["CLIConfig", Dict[str, Any]]):
        values = other if isinstance(other, dict) else other._values
        for k, v in values.items():
            self[k] = v

    @staticmethod
    def get_global_config(_path: Optional[str] = None) -> "CLIConfig":
        """Returns the user-specific config.

        The data is cached between calls, so feel free to use it freely instead
        of saving a reference.

        Note: the `_path` argument is mainly intended to help during testing.
        """
        if not CLIConfig._global:
            path: Optional[str] = _path or os.environ.get("XDG_CONFIG_HOME", None)
            if not path:
                path = os.path.join(os.environ.get("HOME", "~"), ".config")
            path = os.path.join(path, APP_CONFIG_NAME, ".tinyb")
            CLIConfig._global = CLIConfig(path, parent=None)
        return CLIConfig._global

    @staticmethod
    def get_project_config(working_dir: Optional[str] = None) -> "CLIConfig":
        """Returns the project-specific config located at `working_dir` (defaults to `os.getcwd()`)

        The data is cached between calls, given the same `working_dir`.
        """
        working_dir = working_dir or os.getcwd()
        result = CLIConfig._projects.get(working_dir)
        if not result:
            path: str = os.path.join(working_dir, ".tinyb")
            result = CLIConfig(path, parent=CLIConfig.get_global_config())
            CLIConfig._projects[working_dir] = result
        return result

    @staticmethod
    def reset() -> None:
        CLIConfig._global = None
        CLIConfig._projects.clear()
