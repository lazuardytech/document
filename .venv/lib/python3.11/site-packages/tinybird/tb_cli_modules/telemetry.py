import functools
import json
import os
import platform
import re
import sys
import threading
import uuid
from copy import deepcopy
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests

from tinybird.config import CURRENT_VERSION

TELEMETRY_TIMEOUT: int = 1
TELEMETRY_DATASOURCE: str = "tb_cli_telemetry"


def get_ci_product_name() -> Optional[str]:
    if _is_env_true("TB_DISABLE_CI_DETECTION"):
        return None

    CI_CHECKS: List[Tuple[str, Callable[[], bool]]] = [
        ("Azure pipelines", lambda: _is_env_true("TF_BUILD")),
        ("GitHub Actions", lambda: _is_env_true("GITHUB_ACTIONS")),
        ("Appveyor", lambda: _is_env_true("APPVEYOR")),
        ("Travis CI", lambda: _is_env_true("TRAVIS")),
        ("Circle CI", lambda: _is_env_true("CIRCLECI")),
        ("Amazon Web Services CodeBuild", lambda: _is_env_present(["CODEBUILD_BUILD_ID", "AWS_REGION"])),
        ("Jenkins", lambda: _is_env_present(["BUILD_ID", "BUILD_URL"])),
        ("Google Cloud Build", lambda: _is_env_present(["BUILD_ID", "PROJECT_ID"])),
        ("TeamCity", lambda: _is_env_present(["TEAMCITY_VERSION"])),
        ("JetBrains Space", lambda: _is_env_present(["JB_SPACE_API_URL"])),
        ("Generic CI", lambda: _is_env_true("CI")),
    ]

    return next((check[0] for check in CI_CHECKS if check[1]()), None)


def is_ci_environment() -> bool:
    ci_product: Optional[str] = get_ci_product_name()
    return ci_product is not None


def silence_errors(f: Callable) -> Callable:
    """Decorator to silence all errors in the decorated
    function.
    """

    @functools.wraps(f)
    def wrapper(*args, **kwargs) -> Any:
        try:
            return f(*args, **kwargs)
        except Exception:
            return None

    return wrapper


def _is_env_true(env_var: str) -> bool:
    """Checks if `env_var` is `true` or `1`."""
    return os.getenv(env_var, "").lower() in ("true", "1")


def _is_env_present(envs: List[str]) -> bool:
    """Checks if all of the variables passed in `envs`
    are defined (ie: not empty)
    """
    return all(os.getenv(env_var, None) is not None for env_var in envs)


def _hide_tokens(text: str) -> str:
    """Cuts any token in a way that they get unusable if leaked,
    but we still can use them for debugging if needed.
    """
    return re.sub(r"p\.ey[A-Za-z0-9-_\.]+", lambda s: f"{s[0][:10]}...{s[0][-10:]}", text)


class TelemetryHelper:
    def __init__(self, tb_host: Optional[str] = None, max_enqueued_events: int = 5) -> None:
        self.tb_host = tb_host or os.getenv("TB_CLI_TELEMETRY_HOST", "https://api.tinybird.co")
        self.max_enqueued_events: int = max_enqueued_events

        self.enabled: bool = True
        self.events: List[Dict[str, Any]] = []
        self.telemetry_token: Optional[str] = None

        run_id = str(uuid.uuid4())

        self._defaults: Dict[str, Any] = {
            # Per-event values
            "event": "<the event>",
            "event_data": "<the event data>",
            "timestamp": "<the timestamp>",
            # Static values
            "run_id": run_id,
        }

        self._threads: List[threading.Thread] = []
        self.log(f"Telemetry initialized with run_id: {run_id}")

    @silence_errors
    def add_event(self, event: str, event_data: Dict[str, Any]) -> None:
        if not self.enabled:
            self.log("Helper is disabled")
            return

        if "x.y.z" in CURRENT_VERSION and not _is_env_true("TB_CLI_TELEMETRY_SEND_IN_LOCAL"):
            self.log("Not sending events in local development mode")
            return

        # Let's save deep copies to not interfere with original objects
        event_dict: Dict[str, Any] = deepcopy(self._defaults)
        event_dict["event"] = event
        event_dict["event_data"] = json.dumps(event_data)
        event_dict["timestamp"] = datetime.utcnow().isoformat()

        self.events.append(event_dict)
        if len(self.events) >= self.max_enqueued_events:
            self.flush()

    @silence_errors
    def flush(self, wait: bool = False) -> None:
        if self.enabled and len(self.events) > 0:
            # Take the ownership for the pending events.
            #
            # We need this because the proper flush() is done in
            # a thread to avoid blocking  the user and we could send
            # the same event twice if we maintain the same list after
            # during the sending.

            events: List[Dict[str, Any]] = self.events
            self.events = []

            self.log(f"Flusing {len(events)} events in a new thread...")
            thread: threading.Thread = threading.Thread(target=self._flush, args=[events])
            self._threads.append(thread)
            thread.start()

        if wait:
            for t in self._threads:
                t.join()
                if t.is_alive():
                    self.log(f"Couldn't wait for the end of the thread {t.name}")
            self._threads.clear()

    @silence_errors
    def _flush(self, events: List[Dict[str, Any]]) -> None:
        """Actual flush. This is where we use HFI to ingest events."""

        timeout: int
        try:
            timeout = int(os.getenv("TB_CLI_TELEMETRY_TIMEOUT", TELEMETRY_TIMEOUT))
            timeout = max(TELEMETRY_TIMEOUT, timeout)
        except ValueError:
            timeout = TELEMETRY_TIMEOUT

        if not self.telemetry_token:
            self.telemetry_token = os.getenv("TB_CLI_TELEMETRY_TOKEN")
            if self.telemetry_token:
                self.log("Got telemetry token from environment TB_CLI_TELEMETRY_TOKEN")

        with requests.Session() as session:
            if not self.telemetry_token:
                url: str = f"{self.tb_host}/v0/regions"
                self.log(f"Requesting token from {url}...")
                try:
                    r = session.get(url, timeout=timeout)
                    regions: List[Dict[str, Any]] = json.loads(r.content.decode())["regions"]
                    self.telemetry_token = next(
                        (r.get("telemetry_token", None) for r in regions if r["api_host"] == self.tb_host), None
                    )
                    if self.telemetry_token:
                        self.log(f"Got telemetry token from {url}")
                except requests.exceptions.Timeout:
                    self.log(f"Disabling due to timeout after {timeout} seconds")
                    self.enabled = False
                    return
                except Exception as ex:
                    self.log(str(ex))

            if not self.telemetry_token:
                self.log("Disabling due to lack of token")
                self.enabled = False
                return

            self.log(f"token={self.telemetry_token}")

            data: str = _hide_tokens("\n".join(json.dumps(e) for e in events))

            # Note we don't use `wait` as this telemetry isn't a critical
            # operation to support and we don't want to generate overhead
            params: Dict[str, Any] = {"name": TELEMETRY_DATASOURCE, "token": self.telemetry_token}
            url = f"{self.tb_host}/v0/events?{urlencode(params)}"

            try:
                self.log(f"Sending data to {url}...")
                r = session.post(url, data=data, timeout=timeout)
            except requests.exceptions.Timeout:
                self.log(f"Disabling due to timeout after {timeout} seconds")
                self.enabled = False
                return

            self.log(f"Received status {r.status_code}: {r.text}")

            if r.status_code == 200 or r.status_code == 202:
                self.log(f"Successfully sent {len(events)} events to {self.tb_host}")
                self.events.clear()
                return

            if r.status_code in (403, 404):
                self.log(f"Disabling due to {r.status_code} errors")
                self.enabled = False
                return

            if r.status_code >= 500:
                self.log(f"Disabling telemetry and discarding {len(events)} events")
                self.enabled = False

    @silence_errors
    def log(self, msg: str) -> None:
        """Internal logging function to help with development and debugging."""
        if not _is_env_true("TB_CLI_TELEMETRY_DEBUG"):
            return
        print(f"> Telemetry: {msg}")  # noqa: T201


_helper_instance: Optional[TelemetryHelper] = None


@silence_errors
def init_telemetry() -> None:
    """Setups the telemetry helper with the config present in `config`.
    If no config is provided, it tries to get it from the passed Click context.

    We need to call this method any time we suspect the config changes any value.
    """

    telemetry = _get_helper()
    if telemetry:
        telemetry.log("Initialized")


@silence_errors
def add_telemetry_event(event: str, **kw_event_data: Any) -> None:
    """Adds a new telemetry event."""

    telemetry = _get_helper()
    if not telemetry:
        return

    try:
        telemetry.add_event(event, dict(**kw_event_data))
    except Exception as ex:
        telemetry.log(str(ex))


@silence_errors
def add_telemetry_sysinfo_event() -> None:
    """Collects system info and sends a `system_info` event
    with the data.
    """

    ci_product: Optional[str] = get_ci_product_name()

    add_telemetry_event(
        "system_info",
        platform=platform.platform(),
        system=platform.system(),
        arch=platform.machine(),
        processor=platform.processor(),
        python_runtime=platform.python_implementation(),
        python_version=platform.python_version(),
        is_ci=ci_product is not None,
        ci_product=ci_product,
        cli_version=CURRENT_VERSION,
        cli_args=sys.argv[1:] if len(sys.argv) > 1 else [],
    )


@silence_errors
def flush_telemetry(wait: bool = False) -> None:
    """Flushes all pending telemetry events."""

    telemetry = _get_helper()
    if not telemetry:
        return

    try:
        telemetry.flush(wait=wait)
    except Exception as ex:
        telemetry.log(str(ex))


@silence_errors
def _get_helper() -> Optional[TelemetryHelper]:
    """Returns the shared TelemetryHelper instance."""

    if _is_env_true("TB_CLI_TELEMETRY_OPTOUT"):
        return None

    global _helper_instance

    if not _helper_instance:
        _helper_instance = TelemetryHelper()

    return _helper_instance
