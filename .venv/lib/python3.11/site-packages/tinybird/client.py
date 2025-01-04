import asyncio
import json
import logging
import ssl
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Union
from urllib.parse import quote, urlencode

import aiofiles
import requests
import requests.adapters
from requests import Response
from urllib3 import Retry

from tinybird.ch_utils.constants import COPY_ENABLED_TABLE_FUNCTIONS
from tinybird.syncasync import sync_to_async
from tinybird.tb_cli_modules.telemetry import add_telemetry_event

HOST = "https://api.tinybird.co"
LIMIT_RETRIES = 10
LAST_PARTITION = "last_partition"
ALL_PARTITIONS = "all_partitions"


class AuthException(Exception):
    pass


class AuthNoTokenException(AuthException):
    pass


class DoesNotExistException(Exception):
    pass


class CanNotBeDeletedException(Exception):
    pass


class OperationCanNotBePerformed(Exception):
    pass


class TimeoutException(Exception):
    pass


class ReachRetryLimit(Exception):
    pass


class ConnectorNothingToLoad(Exception):
    pass


class JobException(Exception):
    pass


def connector_equals(connector, datafile_params):
    if not connector:
        return False
    if connector["name"] == datafile_params["kafka_connection_name"]:
        return True
    return False


def parse_error_response(response: Response) -> str:
    try:
        content: Dict = response.json()
        if content.get("error", None):
            error = content["error"]
            if content.get("errors", None):
                error += f' -> errors: {content.get("errors")}'
        else:
            error = json.dumps(response, indent=4)
        return error
    except json.decoder.JSONDecodeError:
        return f"Server error, cannot parse response. {response.content.decode('utf-8')}"


class TinyB:
    MAX_GET_LENGTH = 4096

    def __init__(
        self,
        token: str,
        host: str = HOST,
        version: Optional[str] = None,
        disable_ssl_checks: bool = False,
        send_telemetry: bool = False,
        semver: Optional[str] = None,
    ):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        self.token = token
        self.host = host
        self.version = version
        self.disable_ssl_checks = disable_ssl_checks
        self.send_telemetry = send_telemetry
        self.semver = semver

    async def _req(
        self,
        endpoint: str,
        data=None,
        files=None,
        method: str = "GET",
        retries: int = LIMIT_RETRIES,
        use_token: Optional[str] = None,
        **kwargs,
    ):
        url = f"{self.host.strip('/')}/{endpoint.strip('/')}"

        token_to_use = use_token if use_token else self.token
        if token_to_use:
            url += ("&" if "?" in endpoint else "?") + "token=" + token_to_use
        if self.version:
            url += ("&" if "?" in url else "?") + "cli_version=" + quote(self.version)
        if self.semver:
            url += ("&" if "?" in url else "?") + "__tb__semver=" + self.semver

        verify_ssl = not self.disable_ssl_checks
        try:
            with requests.Session() as session:
                if retries > 0:
                    retry = Retry(
                        total=retries,
                        status_forcelist=[429] if method in ("POST", "PUT", "DELETE") else [504, 502, 598, 599, 429],
                        allowed_methods=frozenset({method}),
                    )
                    session.mount("https://", requests.adapters.HTTPAdapter(max_retries=retry))
                    session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retry))
                if method == "POST":
                    if files:
                        response = await sync_to_async(session.post, thread_sensitive=False)(
                            url, files=files, verify=verify_ssl, **kwargs
                        )
                    else:
                        response = await sync_to_async(session.post, thread_sensitive=False)(
                            url, data=data, verify=verify_ssl, **kwargs
                        )
                elif method == "PUT":
                    response = await sync_to_async(session.put, thread_sensitive=False)(
                        url, data=data, verify=verify_ssl, **kwargs
                    )
                elif method == "DELETE":
                    response = await sync_to_async(session.delete, thread_sensitive=False)(
                        url, data=data, verify=verify_ssl, **kwargs
                    )
                else:
                    response = await sync_to_async(session.get, thread_sensitive=False)(
                        url, verify=verify_ssl, **kwargs
                    )

        except Exception as e:
            raise e

        logging.debug("== server response ==")
        logging.debug(response.content)
        logging.debug("==      end        ==")

        if self.send_telemetry:
            try:
                add_telemetry_event("api_request", endpoint=url, token=self.token, status_code=response.status_code)
            except Exception as ex:
                logging.exception(f"Can't send telemetry: {ex}")

        if response.status_code == 403:
            error = parse_error_response(response)
            if not token_to_use:
                raise AuthNoTokenException(f"Forbidden: {error}")
            raise AuthException(f"Forbidden: {error}")
        if response.status_code == 204 or response.status_code == 205:
            return None
        if response.status_code == 404:
            error = parse_error_response(response)
            raise DoesNotExistException(error)
        if response.status_code == 400:
            error = parse_error_response(response)
            raise OperationCanNotBePerformed(error)
        if response.status_code == 409:
            error = parse_error_response(response)
            raise CanNotBeDeletedException(error)
        if response.status_code == 599:
            raise TimeoutException("timeout")
        if "Content-Type" in response.headers and (
            response.headers["Content-Type"] == "text/plain" or "text/csv" in response.headers["Content-Type"]
        ):
            return response.content.decode("utf-8")
        if response.status_code >= 400 and response.status_code not in [400, 403, 404, 409, 429]:
            error = parse_error_response(response)
            raise Exception(error)
        if response.content:
            try:
                return response.json()
            except json.decoder.JSONDecodeError:
                raise Exception(f"Server error, cannot parse response. {response.content.decode('utf-8')}")

        return response

    async def tokens(self):
        response = await self._req("/v0/tokens")
        return response["tokens"]

    async def starterkits(self) -> List[Dict[str, Any]]:
        data = await self._req("/v0/templates")
        return data.get("templates", [])

    async def get_token_by_name(self, name: str):
        tokens = await self.tokens()
        for tk in tokens:
            if tk["name"] == name:
                return tk
        return None

    async def create_token(
        self, name: str, scope: List[str], origin_code: Optional[str], origin_resource_name_or_id: Optional[str] = None
    ):
        origin = origin_code or "C"  # == Origins.CUSTOM  if none specified
        params = {
            "name": name,
            "origin": origin,
        }
        if origin_resource_name_or_id:
            params["resource_id"] = origin_resource_name_or_id

        # TODO: We should support sending multiple scopes in the body of the request
        url = f"/v0/tokens?{urlencode(params)}"
        url = url + "&" + "&".join([f"scope={scope}" for scope in scope])
        return await self._req(
            url,
            method="POST",
            data="",
        )

    async def alter_tokens(self, name: str, scopes: List[str]):
        if not scopes:
            return
        scopes_url: str = "&".join([f"scope={scope}" for scope in scopes])
        url = f"/v0/tokens/{name}"
        if len(url + "?" + scopes_url) > TinyB.MAX_GET_LENGTH:
            return await self._req(url, method="PUT", data=scopes_url)
        else:
            url = url + "?" + scopes_url
            return await self._req(url, method="PUT", data="")

    async def datasources(self, branch: Optional[str] = None, used_by: bool = False):
        params = {
            "attrs": "used_by" if used_by else "",
        }
        response = await self._req(f"/v0/datasources?{urlencode(params)}")
        ds = response["datasources"]

        if branch:
            ds = [x for x in ds if x["name"].startswith(branch)]
        return ds

    async def get_connections(self, service: Optional[str] = None):
        params = {}

        if service:
            params["service"] = service

        response = await self._req(f"/v0/connectors?{urlencode(params)}")
        return response["connectors"]

    async def connections(self, connector: Optional[str] = None, skip_bigquery: Optional[bool] = False):
        response = await self._req("/v0/connectors")
        connectors = response["connectors"]
        bigquery_connection = None
        if not skip_bigquery:
            bigquery_connection = (
                await self.bigquery_connection() if connector == "bigquery" or connector is None else None
            )
        connectors = [*connectors, bigquery_connection] if bigquery_connection else connectors
        if connector:
            return [
                {
                    "id": c["id"],
                    "service": c["service"],
                    "name": c["name"],
                    "connected_datasources": len(c["linkers"]),
                    **c["settings"],
                }
                for c in connectors
                if c["service"] == connector
            ]
        return [
            {
                "id": c["id"],
                "service": c["service"],
                "name": c["name"],
                "connected_datasources": len(c["linkers"]),
                **c["settings"],
            }
            for c in connectors
        ]

    async def bigquery_connection(self):
        bigquery_resources = await self.list_gcp_resources()
        if len(bigquery_resources) == 0:
            return None

        gcp_account_details: Dict[str, Any] = await self.get_gcp_service_account_details()
        datasources = await self.datasources()
        bigquery_datasources = [ds["name"] for ds in datasources if ds["type"] == "bigquery"]
        return {
            "id": gcp_account_details["account"].split("@")[0],
            "service": "bigquery",
            "name": "bigquery",
            "linkers": bigquery_datasources,
            "settings": gcp_account_details,
        }

    async def get_datasource(self, ds_name: str, used_by: bool = False) -> Dict[str, Any]:
        params = {
            "attrs": "used_by" if used_by else "",
        }
        return await self._req(f"/v0/datasources/{ds_name}?{urlencode(params)}")

    async def alter_datasource(
        self,
        ds_name: str,
        new_schema: Optional[str] = None,
        description: Optional[str] = None,
        ttl: Optional[str] = None,
        dry_run: bool = False,
        indexes: Optional[str] = None,
    ):
        params = {"dry": "true" if dry_run else "false"}
        if new_schema:
            params.update({"schema": new_schema})
        if description:
            params.update({"description": description})
        if ttl:
            params.update({"ttl": ttl})
        if indexes:
            params.update({"indexes": indexes})
        res = await self._req(f"/v0/datasources/{ds_name}/alter", method="POST", data=params)

        if "Error" in res:
            raise Exception(res["error"])

        return res

    async def update_datasource(self, ds_name: str, data: Dict[str, Any]):
        res = await self._req(f"/v0/datasources/{ds_name}", method="PUT", data=data)

        if "Error" in res:
            raise Exception(res["error"])

        return res

    async def pipe_file(self, pipe: str):
        return await self._req(f"/v0/pipes/{pipe}.pipe")

    async def datasource_file(self, datasource: str):
        try:
            return await self._req(f"/v0/datasources/{datasource}.datasource")
        except DoesNotExistException:
            raise Exception(f"Data Source {datasource} not found.")

    async def datasource_analyze(self, url):
        params = {"url": url}
        return await self._req(f"/v0/analyze?{urlencode(params)}", method="POST", data="")

    async def datasource_analyze_file(self, data):
        return await self._req("/v0/analyze", method="POST", data=data)

    async def datasource_create_from_definition(self, parameter_definition: Dict[str, str]):
        return await self._req("/v0/datasources", method="POST", data=parameter_definition)

    async def datasource_create_from_url(
        self,
        table_name: str,
        url: str,
        mode: str = "create",
        status_callback=None,
        sql_condition: Optional[str] = None,
        format: str = "csv",
        replace_options: Optional[Set[str]] = None,
    ):
        params = {"name": table_name, "url": url, "mode": mode, "debug": "blocks_block_log", "format": format}

        if sql_condition:
            params["replace_condition"] = sql_condition
        if replace_options:
            for option in list(replace_options):
                params[option] = "true"

        req_url = f"/v0/datasources?{urlencode(params, safe='')}"
        res = await self._req(req_url, method="POST", data=b"")

        if "error" in res:
            raise Exception(res["error"])

        return await self.wait_for_job(res["id"], status_callback, backoff_multiplier=1.5, maximum_backoff_seconds=20)

    async def datasource_delete(self, datasource_name: str, force: bool = False, dry_run: bool = False):
        params = {"force": "true" if force else "false", "dry_run": "true" if dry_run else "false"}
        return await self._req(f"/v0/datasources/{datasource_name}?{urlencode(params)}", method="DELETE")

    async def datasource_append_data(
        self,
        datasource_name: str,
        file: Union[str, Path],
        mode: str = "append",
        status_callback=None,
        sql_condition: Optional[str] = None,
        format: str = "csv",
        replace_options: Optional[Set[str]] = None,
    ):
        params = {"name": datasource_name, "mode": mode, "format": format, "debug": "blocks_block_log"}

        if sql_condition:
            params["replace_condition"] = sql_condition
        if replace_options:
            for option in list(replace_options):
                params[option] = "true"

        async with aiofiles.open(file, "rb") as content:
            file_content = await content.read()
            if format == "csv":
                files = {"csv": ("csv", file_content)}
            else:
                files = {"ndjson": ("ndjson", file_content)}

            res = await self._req(
                f"v0/datasources?{urlencode(params, safe='')}",
                files=files,
                method="POST",
            )
            if status_callback:
                status_callback(res)

            return res

    async def datasource_truncate(self, datasource_name: str):
        return await self._req(f"/v0/datasources/{datasource_name}/truncate", method="POST", data="")

    async def datasource_delete_rows(self, datasource_name: str, delete_condition: str, dry_run: bool = False):
        params = {"delete_condition": delete_condition}
        if dry_run:
            params.update({"dry_run": "true"})
        return await self._req(f"/v0/datasources/{datasource_name}/delete", method="POST", data=params)

    async def datasource_dependencies(
        self, no_deps: bool, match: str, pipe: str, datasource: str, check_for_partial_replace: bool, recursive: bool
    ):
        params = {
            "no_deps": "true" if no_deps else "false",
            "check_for_partial_replace": "true" if check_for_partial_replace else "false",
            "recursive": "true" if recursive else "false",
        }
        if match:
            params["match"] = match
        if pipe:
            params["pipe"] = pipe
        if datasource:
            params["datasource"] = datasource

        return await self._req(f"/v0/dependencies?{urlencode(params)}", timeout=60)

    async def datasource_share(self, datasource_id: str, current_workspace_id: str, destination_workspace_id: str):
        params = {"origin_workspace_id": current_workspace_id, "destination_workspace_id": destination_workspace_id}
        return await self._req(f"/v0/datasources/{datasource_id}/share", method="POST", data=params)

    async def datasource_unshare(self, datasource_id: str, current_workspace_id: str, destination_workspace_id: str):
        params = {"origin_workspace_id": current_workspace_id, "destination_workspace_id": destination_workspace_id}
        return await self._req(f"/v0/datasources/{datasource_id}/share", method="DELETE", data=params)

    async def datasource_sync(self, datasource_id: str):
        return await self._req(f"/v0/datasources/{datasource_id}/scheduling/runs", method="POST", data="")

    async def datasource_scheduling_state(self, datasource_id: str):
        response = await self._req(f"/v0/datasources/{datasource_id}/scheduling/state", method="GET")
        return response["state"]

    async def datasource_scheduling_pause(self, datasource_id: str):
        return await self._req(
            f"/v0/datasources/{datasource_id}/scheduling/state",
            method="PUT",
            data='{"state": "paused"}',
        )

    async def datasource_scheduling_resume(self, datasource_id: str):
        return await self._req(
            f"/v0/datasources/{datasource_id}/scheduling/state",
            method="PUT",
            data='{"state": "running"}',
        )

    async def datasource_exchange(self, datasource_a: str, datasource_b: str):
        payload = {"datasource_a": datasource_a, "datasource_b": datasource_b}
        return await self._req("/v0/datasources/exchange", method="POST", data=payload)

    async def analyze_pipe_node(
        self, pipe_name: str, node: Dict[str, Any], dry_run: str = "false", datasource_name: Optional[str] = None
    ):
        params = {**{"include_datafile": "true", "dry_run": dry_run}, **node.get("params", node)}
        if "mode" in params:
            params.pop("mode")
        node_name = node["params"]["name"] if node.get("params", None) else node["name"]
        if datasource_name:
            params["datasource"] = datasource_name
        response = await self._req(f"/v0/pipes/{pipe_name}/nodes/{node_name}/analysis?{urlencode(params)}")
        return response

    async def populate_node(
        self,
        pipe_name: str,
        node_name: str,
        populate_subset: bool = False,
        populate_condition: Optional[str] = None,
        truncate: bool = True,
        unlink_on_populate_error: bool = False,
    ):
        params: Dict[str, Any] = {
            "truncate": "true" if truncate else "false",
            "unlink_on_populate_error": "true" if unlink_on_populate_error else "false",
        }
        if populate_subset:
            params.update({"populate_subset": populate_subset})
        if populate_condition:
            params.update({"populate_condition": populate_condition})
        response = await self._req(
            f"/v0/pipes/{pipe_name}/nodes/{node_name}/population?{urlencode(params)}", method="POST"
        )
        return response

    async def pipes(self, branch=None, dependencies: bool = False, node_attrs=None, attrs=None):
        params = {
            "dependencies": "true" if dependencies else "false",
            "attrs": attrs if attrs else "",
            "node_attrs": node_attrs if node_attrs else "",
        }
        response = await self._req(f"/v0/pipes?{urlencode(params)}")
        pipes = response["pipes"]
        if branch:
            pipes = [x for x in pipes if x["name"].startswith(branch)]
        return pipes

    async def pipe(self, pipe: str):
        return await self._req(f"/v0/pipes/{pipe}")

    async def pipe_data(
        self,
        pipe_name_or_uid: str,
        sql: Optional[str] = None,
        format: Optional[str] = "json",
        params: Optional[Mapping[str, Any]] = None,
    ):
        params = {**params} if params else {}
        if sql:
            params["q"] = sql

        url = f"/v0/pipes/{pipe_name_or_uid}.{format}"
        query_string = urlencode(params)
        if len(url + "?" + query_string) > TinyB.MAX_GET_LENGTH:
            return await self._req(f"/v0/pipes/{pipe_name_or_uid}.{format}", method="POST", data=params)
        else:
            url = url + "?" + query_string
            return await self._req(url)

    async def pipe_create(self, pipe_name: str, sql: str):
        return await self._req(
            f"/v0/pipes?name={pipe_name}&sql={quote(sql, safe='')}", method="POST", data=sql.encode()
        )

    async def pipe_delete(self, pipe_name: str):
        return await self._req(f"/v0/pipes/{pipe_name}", method="DELETE")

    async def pipe_append_node(self, pipe_name_or_uid: str, sql: str):
        return await self._req(f"/v0/pipes/{pipe_name_or_uid}/nodes", method="POST", data=sql.encode())

    async def pipe_set_endpoint(self, pipe_name_or_uid: str, published_node_uid: str):
        return await self._req(f"/v0/pipes/{pipe_name_or_uid}/nodes/{published_node_uid}/endpoint", method="POST")

    async def pipe_remove_endpoint(self, pipe_name_or_uid: str, published_node_uid: str):
        return await self._req(f"/v0/pipes/{pipe_name_or_uid}/nodes/{published_node_uid}/endpoint", method="DELETE")

    async def pipe_update_copy(
        self,
        pipe_name_or_id: str,
        node_id: str,
        target_datasource: Optional[str] = None,
        schedule_cron: Optional[str] = None,
        mode: Optional[str] = None,
    ):
        data = {"schedule_cron": schedule_cron}

        if target_datasource:
            data["target_datasource"] = target_datasource

        if mode:
            data["mode"] = mode

        return await self._req(f"/v0/pipes/{pipe_name_or_id}/nodes/{node_id}/copy", method="PUT", data=data)

    async def pipe_remove_copy(self, pipe_name_or_id: str, node_id: str):
        return await self._req(f"/v0/pipes/{pipe_name_or_id}/nodes/{node_id}/copy", method="DELETE")

    async def pipe_run_copy(
        self, pipe_name_or_id: str, params: Optional[Dict[str, str]] = None, mode: Optional[str] = None
    ):
        params = {**params} if params else {}
        if mode:
            params["_mode"] = mode
        return await self._req(f"/v0/pipes/{pipe_name_or_id}/copy?{urlencode(params)}", method="POST")

    async def pipe_resume_copy(self, pipe_name_or_id: str):
        return await self._req(f"/v0/pipes/{pipe_name_or_id}/copy/resume", method="POST")

    async def pipe_pause_copy(self, pipe_name_or_id: str):
        return await self._req(f"/v0/pipes/{pipe_name_or_id}/copy/pause", method="POST")

    async def pipe_create_sink(self, pipe_name_or_id: str, node_id: str, params: Optional[Dict[str, str]] = None):
        params = {**params} if params else {}
        return await self._req(
            f"/v0/pipes/{pipe_name_or_id}/nodes/{node_id}/sink?{urlencode(params)}", method="POST", data=""
        )

    async def pipe_remove_sink(self, pipe_name_or_id: str, node_id: str):
        return await self._req(f"/v0/pipes/{pipe_name_or_id}/nodes/{node_id}/sink", method="DELETE")

    async def pipe_remove_stream(self, pipe_name_or_id: str, node_id: str):
        return await self._req(f"/v0/pipes/{pipe_name_or_id}/nodes/{node_id}/stream", method="DELETE")

    async def pipe_run_sink(self, pipe_name_or_id: str, params: Optional[Dict[str, str]] = None):
        params = {**params} if params else {}
        return await self._req(f"/v0/pipes/{pipe_name_or_id}/sink?{urlencode(params)}", method="POST")

    async def pipe_unlink_materialized(self, pipe_name: str, node_id: str):
        return await self._req(f"/v0/pipes/{pipe_name}/nodes/{node_id}/materialization", method="DELETE")

    async def query(self, sql: str, pipeline: Optional[str] = None):
        params = {}
        if pipeline:
            params = {"pipeline": pipeline}
        params.update({"release_replacements": "true"})

        if len(sql) > TinyB.MAX_GET_LENGTH:
            return await self._req(f"/v0/sql?{urlencode(params)}", data=sql, method="POST")
        else:
            return await self._req(f"/v0/sql?q={quote(sql, safe='')}&{urlencode(params)}")

    async def jobs(self, status=None):
        jobs = (await self._req("/v0/jobs"))["jobs"]
        if status:
            status = [status] if isinstance(status, str) else status
            jobs = [j for j in jobs if j["status"] in status]
        return jobs

    async def job(self, job_id: str):
        return await self._req(f"/v0/jobs/{job_id}")

    async def job_cancel(self, job_id: str):
        return await self._req(f"/v0/jobs/{job_id}/cancel", method="POST", data=b"")

    async def user_workspaces(self):
        return await self._req("/v0/user/workspaces/?with_environments=false")

    async def user_workspaces_and_branches(self):
        return await self._req("/v0/user/workspaces/?with_environments=true")

    async def user_workspace_branches(self):
        return await self._req("/v0/user/workspaces/?with_environments=true&only_environments=true")

    async def branches(self):
        return await self._req("/v0/environments")

    async def releases(self, workspace_id):
        return await self._req(f"/v0/workspaces/{workspace_id}/releases")

    async def create_workspace(self, name: str, template: Optional[str]):
        url = f"/v0/workspaces?name={name}"
        if template:
            url += f"&starter_kit={template}"
        return await self._req(url, method="POST", data=b"")

    async def create_workspace_branch(
        self,
        branch_name: str,
        last_partition: Optional[bool],
        all: Optional[bool],
        ignore_datasources: Optional[List[str]],
    ):
        params = {
            "name": branch_name,
            "data": LAST_PARTITION if last_partition else (ALL_PARTITIONS if all else ""),
        }
        if ignore_datasources:
            params["ignore_datasources"] = ",".join(ignore_datasources)
        return await self._req(f"/v0/environments?{urlencode(params)}", method="POST", data=b"")

    async def branch_workspace_data(
        self,
        workspace_id: str,
        last_partition: bool,
        all: bool,
        ignore_datasources: Optional[List[str]] = None,
    ):
        params = {}
        if last_partition:
            params["mode"] = LAST_PARTITION

        if all:
            params["mode"] = ALL_PARTITIONS
        if ignore_datasources:
            params["ignore_datasources"] = ",".join(ignore_datasources)
        url = f"/v0/environments/{workspace_id}/data?{urlencode(params)}"
        return await self._req(url, method="POST", data=b"")

    async def branch_regression_tests(
        self,
        branch_id: str,
        pipe_name: Optional[str],
        test_type: str,
        failfast: Optional[bool] = False,
        limit: Optional[int] = None,
        sample_by_params: Optional[int] = None,
        match: Optional[List[str]] = None,
        params: Optional[List[Dict[str, Any]]] = None,
        assert_result: Optional[bool] = True,
        assert_result_no_error: Optional[bool] = True,
        assert_result_rows_count: Optional[bool] = True,
        assert_result_ignore_order: Optional[bool] = False,
        assert_time_increase_percentage: Optional[float] = None,
        assert_bytes_read_increase_percentage: Optional[float] = None,
        assert_max_time: Optional[float] = None,
        run_in_main: Optional[bool] = False,
    ):
        test: Dict[str, Any] = {
            test_type: {
                "config": {
                    "assert_result_ignore_order": assert_result_ignore_order,
                    "assert_result": assert_result,
                    "assert_result_no_error": assert_result_no_error,
                    "assert_result_rows_count": assert_result_rows_count,
                    "failfast": failfast,
                    "assert_time_increase_percentage": assert_time_increase_percentage,
                    "assert_bytes_read_increase_percentage": assert_bytes_read_increase_percentage,
                    "assert_max_time": assert_max_time,
                }
            }
        }
        if limit is not None:
            test[test_type].update({"limit": limit})
        if sample_by_params is not None:
            test[test_type].update({"samples_by_params": sample_by_params})
        if match is not None:
            test[test_type].update({"matches": match})
        if params is not None:
            test[test_type].update({"params": params})

        regression_commands: List[Dict[str, Any]] = [
            {"pipe": ".*" if pipe_name is None else pipe_name, "tests": [test]}
        ]

        data = json.dumps(regression_commands)
        if run_in_main:
            url = f"/v0/environments/{branch_id}/regression/main"
        else:
            url = f"/v0/environments/{branch_id}/regression"
        return await self._req(url, method="POST", data=data, headers={"Content-Type": "application/json"})

    async def branch_regression_tests_file(
        self, branch_id: str, regression_commands: List[Dict[str, Any]], run_in_main: Optional[bool] = False
    ):
        data = json.dumps(regression_commands)
        if run_in_main:
            url = f"/v0/environments/{branch_id}/regression/main"
        else:
            url = f"/v0/environments/{branch_id}/regression"
        return await self._req(url, method="POST", data=data, headers={"Content-Type": "application/json"})

    async def delete_workspace(self, id: str, hard_delete_confirmation: Optional[str]):
        data = {"confirmation": hard_delete_confirmation}
        return await self._req(f"/v0/workspaces/{id}", data, method="DELETE")

    async def delete_branch(self, id: str):
        return await self._req(f"/v0/environments/{id}", method="DELETE")

    async def add_users_to_workspace(self, workspace: Dict[str, Any], users_emails: List[str], role: Optional[str]):
        users = ",".join(users_emails)
        return await self._req(
            f"/v0/workspaces/{workspace['id']}/users/",
            method="PUT",
            data={"operation": "add", "users": users, "role": role},
        )

    async def remove_users_from_workspace(self, workspace: Dict[str, Any], users_emails: List[str]):
        users = ",".join(users_emails)
        return await self._req(
            f"/v0/workspaces/{workspace['id']}/users/", method="PUT", data={"operation": "remove", "users": users}
        )

    async def set_role_for_users_in_workspace(self, workspace: Dict[str, Any], users_emails: List[str], role: str):
        users = ",".join(users_emails)
        return await self._req(
            f"/v0/workspaces/{workspace['id']}/users/",
            method="PUT",
            data={"operation": "change_role", "users": users, "new_role": role},
        )

    async def workspace(self, workspace_id: str, with_token: bool = False):
        params = {"with_token": "true" if with_token else "false"}
        return await self._req(f"/v0/workspaces/{workspace_id}?{urlencode(params)}")

    async def workspace_info(self):
        return await self._req("/v0/workspace")

    async def wait_for_job(
        self,
        job_id: str,
        status_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        backoff_seconds: float = 2.0,
        backoff_multiplier: float = 1,
        maximum_backoff_seconds: float = 2.0,
    ) -> Dict[str, Any]:
        res: Dict[str, Any] = {}
        done: bool = False
        while not done:
            params = {"debug": "blocks,block_log"}
            res = await self._req(f"/v0/jobs/{job_id}?{urlencode(params)}")

            if res["status"] == "error":
                error_message = "There has been an error"
                if not isinstance(res.get("error", True), bool):
                    error_message = str(res["error"])
                if "errors" in res:
                    error_message += f": {res['errors']}"
                raise JobException(error_message)

            if res["status"] == "cancelled":
                raise JobException("Job has been cancelled")

            done = res["status"] == "done"

            if status_callback:
                status_callback(res)

            if not done:
                backoff_seconds = min(backoff_seconds * backoff_multiplier, maximum_backoff_seconds)
                await asyncio.sleep(backoff_seconds)

        return res

    async def datasource_kafka_connect(self, connection_id, datasource_name, topic, group, auto_offset_reset):
        return await self._req(
            f"/v0/datasources?connector={connection_id}&name={datasource_name}&"
            f"kafka_topic={topic}&kafka_group_id={group}&kafka_auto_offset_reset={auto_offset_reset}",
            method="POST",
            data=b"",
        )

    async def connection_create_kafka(
        self,
        kafka_bootstrap_servers,
        kafka_key,
        kafka_secret,
        kafka_connection_name,
        kafka_auto_offset_reset=None,
        kafka_schema_registry_url=None,
        kafka_sasl_mechanism="PLAIN",
        kafka_ssl_ca_pem=None,
    ):
        params = {
            "service": "kafka",
            "kafka_security_protocol": "SASL_SSL",
            "kafka_sasl_mechanism": kafka_sasl_mechanism,
            "kafka_bootstrap_servers": kafka_bootstrap_servers,
            "kafka_sasl_plain_username": kafka_key,
            "kafka_sasl_plain_password": kafka_secret,
            "name": kafka_connection_name,
        }

        if kafka_schema_registry_url:
            params["kafka_schema_registry_url"] = kafka_schema_registry_url
        if kafka_auto_offset_reset:
            params["kafka_auto_offset_reset"] = kafka_auto_offset_reset
        if kafka_ssl_ca_pem:
            params["kafka_ssl_ca_pem"] = kafka_ssl_ca_pem
        connection_params = {key: value for key, value in params.items() if value is not None}

        return await self._req(
            "/v0/connectors",
            method="POST",
            headers={"Content-Type": "application/json"},
            data=json.dumps(connection_params),
        )

    async def kafka_list_topics(self, connection_id: str, timeout=5):
        resp = await self._req(
            f"/v0/connectors/{connection_id}/preview?preview_activity=false",
            connect_timeout=timeout,
            request_timeout=timeout,
        )
        return [x["topic"] for x in resp["preview"]]

    async def get_gcp_service_account_details(self) -> Dict[str, Any]:
        return await self._req("/v0/datasources-bigquery-credentials")

    async def list_connectors(self, service: Optional[str] = None) -> List[Dict[str, Any]]:
        try:
            params: str = f"?service={service}" if service else ""
            result = await self._req(f"/v0/connections/{params}")
            if not result:
                return []

            return result.get("connectors", [])
        except Exception:
            return []

    async def get_connector(
        self,
        name_or_id: str,
        service: Optional[str] = None,
        key: Optional[str] = "name",
        skip_bigquery: Optional[bool] = False,
    ) -> Optional[Dict[str, Any]]:
        return next(
            (c for c in await self.connections(connector=service, skip_bigquery=skip_bigquery) if c[key] == name_or_id),
            None,
        )

    async def get_connector_by_id(self, connector_id: Optional[str] = None):
        return await self._req(f"/v0/connectors/{connector_id}")

    async def get_snowflake_integration_query(
        self, role: str, stage: Optional[str], integration: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        try:
            params = {
                "role": role,
            }
            if stage:
                params["stage"] = stage
            if integration:
                params["integration"] = integration

            return await self._req(f"/v0/connectors/snowflake/instructions?{urlencode(params)}")
        except Exception:
            return None

    async def list_gcp_resources(self) -> List[Dict[str, Any]]:
        try:
            resources = await self._req("/v0/connections/bigquery")
            if not resources:
                return []

            return resources.get("items", [])
        except Exception:
            return []

    async def check_gcp_read_permissions(self) -> bool:
        """Returns `True` if our service account (see `TinyB::get_gcp_service_account_details()`)
        has the proper permissions in GCP.

        Here we assume that we have permissions if we can list resources but currently this
        logic is wrong under some circumstances.

        See https://gitlab.com/tinybird/analytics/-/issues/6485.
        """
        try:
            items = await self.list_gcp_resources()
            if not items:
                return False
            return len(items) > 0
        except Exception:
            return False

    async def connector_delete(self, connection_id):
        return await self._req(f"/v0/connectors/{connection_id}", method="DELETE")

    async def connection_create_snowflake(
        self,
        account_identifier: str,
        user: str,
        password: str,
        warehouse: str,
        role: str,
        connection_name: str,
        integration: Optional[str],
        stage: Optional[str],
    ) -> Dict[str, Any]:
        params = {
            "service": "snowflake",
            "name": connection_name,
            "account": account_identifier,
            "username": user,
            "password": password,
            "role": role,
            "warehouse": warehouse,
        }

        if integration:
            params["integration"] = integration
        if stage:
            params["stage"] = stage

        return await self._req(f"/v0/connectors?{urlencode(params)}", method="POST", data="")

    async def validate_snowflake_connection(self, account_identifier: str, user: str, password: str) -> bool:
        try:
            roles = await self.get_snowflake_roles(account_identifier, user, password)
            if not roles:
                return False
            return len(roles) > 0
        except Exception:
            return False

    async def validate_preview_connection(self, service: str, params: Dict[str, Any]) -> bool:
        params = {"service": service, "dry_run": "true", **params}
        bucket_list = None
        try:
            bucket_list = await self._req(f"/v0/connectors?{urlencode(params)}", method="POST", data="")
            if not bucket_list:
                return False
            return len(bucket_list) > 0
        except Exception:
            return False

    async def preview_bucket(self, connector: str, bucket_uri: str):
        params = {"bucket_uri": bucket_uri, "service": "s3", "summary": "true"}
        return await self._req(f"/v0/connectors/{connector}/preview?{urlencode(params)}", method="GET")

    async def connection_create(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return await self._req(f"/v0/connectors?{urlencode(params)}", method="POST", data="")

    async def get_snowflake_roles(self, account_identifier: str, user: str, password: str) -> Optional[List[str]]:
        params = {"account": account_identifier, "username": user, "password": password}

        response = await self._req(f"/v0/connectors/snowflake/roles?{urlencode(params)}", method="POST", data="")
        return response["roles"]

    async def get_snowflake_warehouses(
        self, account_identifier: str, user: str, password: str, role: str
    ) -> Optional[List[Dict[str, Any]]]:
        params = {
            "account": account_identifier,
            "username": user,
            "password": password,
            "role": role,
        }

        response = await self._req(f"/v0/connectors/snowflake/warehouses?{urlencode(params)}", method="POST", data="")
        return response["warehouses"]

    async def get_trust_policy(self, service: str) -> Dict[str, Any]:
        return await self._req(f"/v0/integrations/{service}/policies/trust-policy")

    async def get_access_write_policy(self, service: str) -> Dict[str, Any]:
        return await self._req(f"/v0/integrations/{service}/policies/write-access-policy")

    async def get_access_read_policy(self, service: str) -> Dict[str, Any]:
        return await self._req(f"/v0/integrations/{service}/policies/read-access-policy")

    async def sql_get_format(self, sql: str, with_clickhouse_format: bool = False) -> str:
        try:
            if with_clickhouse_format:
                from tinybird.sql_toolset import format_sql

                return format_sql(sql)
            else:
                return await self._sql_get_format_remote(sql, with_clickhouse_format)
        except ModuleNotFoundError:
            return await self._sql_get_format_remote(sql, with_clickhouse_format)

    async def _sql_get_format_remote(self, sql: str, with_clickhouse_format: bool = False) -> str:
        params = {"with_clickhouse_format": "true" if with_clickhouse_format else "false"}
        result = await self._req(f"/v0/sql_format?q={quote(sql, safe='')}&{urlencode(params)}")
        return result["q"]

    @staticmethod
    def _sql_get_used_tables_local(sql: str, raising: bool = False, is_copy: Optional[bool] = False) -> List[str]:
        from tinybird.sql_toolset import sql_get_used_tables

        tables = sql_get_used_tables(
            sql, raising, table_functions=False, function_allow_list=COPY_ENABLED_TABLE_FUNCTIONS if is_copy else None
        )
        return [t[1] if t[0] == "" else f"{t[0]}.{t[1]}" for t in tables]

    async def _sql_get_used_tables_remote(
        self, sql: str, raising: bool = False, is_copy: Optional[bool] = False
    ) -> List[str]:
        params = {
            "q": sql,
            "raising": "true" if raising else "false",
            "table_functions": "false",
            "is_copy": "true" if is_copy else "false",
        }
        result = await self._req("/v0/sql_tables", data=params, method="POST")
        return [t[1] if t[0] == "" else f"{t[0]}.{t[1]}" for t in result["tables"]]

    # Get used tables from a query. Does not include table functions
    async def sql_get_used_tables(self, sql: str, raising: bool = False, is_copy: Optional[bool] = False) -> List[str]:
        try:
            return self._sql_get_used_tables_local(sql, raising, is_copy)
        except ModuleNotFoundError:
            return await self._sql_get_used_tables_remote(sql, raising, is_copy)

    @staticmethod
    def _replace_tables_local(q: str, replacements):
        from tinybird.sql_toolset import replace_tables, replacements_to_tuples

        return replace_tables(q, replacements_to_tuples(replacements))

    async def _replace_tables_remote(self, q: str, replacements):
        params = {
            "q": q,
            "replacements": json.dumps({k[1] if isinstance(k, tuple) else k: v for k, v in replacements.items()}),
        }
        result = await self._req("/v0/sql_replace", data=params, method="POST")
        return result["query"]

    async def replace_tables(self, q: str, replacements):
        try:
            return self._replace_tables_local(q, replacements)
        except ModuleNotFoundError:
            return await self._replace_tables_remote(q, replacements)

    async def get_connection(self, **kwargs):
        result = await self._req("/v0/connectors")
        return next((connector for connector in result["connectors"] if connector_equals(connector, kwargs)), None)

    async def regions(self):
        regions = await self._req("/v0/regions")
        return regions

    async def datasource_query_copy(self, datasource_name: str, sql_query: str):
        params = {"copy_to": datasource_name}
        return await self._req(f"/v0/sql_copy?{urlencode(params)}", data=sql_query, method="POST")

    async def workspace_commit_update(self, workspace_id: str, commit: str):
        return await self._req(
            f"/v0/workspaces/{workspace_id}/releases/?commit={commit}&force=true", method="POST", data=""
        )

    async def update_release_semver(self, workspace_id: str, semver: str, new_semver: str):
        return await self._req(f"/v0/workspaces/{workspace_id}/releases/{semver}?new_semver={new_semver}", method="PUT")

    async def release_new(self, workspace_id: str, semver: str, commit: str):
        params = {
            "commit": commit,
            "semver": semver,
        }
        return await self._req(f"/v0/workspaces/{workspace_id}/releases/?{urlencode(params)}", method="POST", data="")

    async def release_failed(self, workspace_id: str, semver: str):
        return await self._req(f"/v0/workspaces/{workspace_id}/releases/{semver}?status=failed", method="PUT")

    async def release_preview(self, workspace_id: str, semver: str):
        return await self._req(f"/v0/workspaces/{workspace_id}/releases/{semver}?status=preview", method="PUT")

    async def release_promote(self, workspace_id: str, semver: str):
        return await self._req(f"/v0/workspaces/{workspace_id}/releases/{semver}?status=live", method="PUT")

    async def release_rollback(self, workspace_id: str, semver: str):
        return await self._req(f"/v0/workspaces/{workspace_id}/releases/{semver}?status=rollback", method="PUT")

    async def release_rm(
        self,
        workspace_id: str,
        semver: str,
        confirmation: Optional[str] = None,
        dry_run: bool = False,
        force: bool = False,
    ):
        params = {"force": "true" if force else "false", "dry_run": "true" if dry_run else "false"}
        if confirmation:
            params["confirmation"] = confirmation
        return await self._req(f"/v0/workspaces/{workspace_id}/releases/{semver}?{urlencode(params)}", method="DELETE")

    async def release_oldest_rollback(
        self,
        workspace_id: str,
    ):
        return await self._req(f"/v0/workspaces/{workspace_id}/releases/oldest-rollback", method="GET")

    async def token_list(self, match: Optional[str] = None):
        tokens = await self.tokens()
        return [token for token in tokens if (not match or token["name"].find(match) != -1) and "token" in token]

    async def token_delete(self, token_id: str):
        return await self._req(f"/v0/tokens/{token_id}", method="DELETE")

    async def token_refresh(self, token_id: str):
        return await self._req(f"/v0/tokens/{token_id}/refresh", method="POST", data="")

    async def token_get(self, token_id: str):
        return await self._req(f"/v0/tokens/{token_id}", method="GET")

    async def token_scopes(self, token_id: str):
        token = await self.token_get(token_id)
        return token["scopes"]

    def _token_to_params(self, token: Dict[str, Any]) -> str:
        params = urlencode(
            {
                "name": token["name"],
                "description": token.get("description", ""),
                "origin": token.get("origin", "C"),
            }
        )

        if "scopes" in token:
            for scope_dict in token["scopes"]:
                scope_types = scope_dict["name"].split(",")
                for scope_type in scope_types:
                    scope = scope_type.strip()
                    if "resource" in scope_dict:
                        resource = scope_dict["resource"]
                        scope += f":{resource}"
                        if "filter" in scope_dict:
                            scope += f":{scope_dict['filter']}"
                    params += f"&scope={scope}"
        return params

    async def token_create(self, token: Dict[str, Any]):
        params = self._token_to_params(token)
        return await self._req(f"/v0/tokens?{params}", method="POST", data="")

    async def create_jwt_token(self, name: str, expiration_time: int, scopes: List[Dict[str, Any]]):
        url_params = {"name": name, "expiration_time": expiration_time}
        body = json.dumps({"scopes": scopes})
        return await self._req(f"/v0/tokens?{urlencode(url_params)}", method="POST", data=body)

    async def token_update(self, token: Dict[str, Any]):
        name = token["name"]
        params = self._token_to_params(token)
        return await self._req(f"/v0/tokens/{name}?{params}", method="PUT", data="")

    async def token_file(self, token_id: str):
        return await self._req(f"/v0/tokens/{token_id}.token")

    async def check_auth_login(self) -> Dict[str, Any]:
        return await self._req("/v0/auth")

    async def get_all_tags(self) -> Dict[str, Any]:
        return await self._req("/v0/tags")

    async def create_tag_with_resource(self, name: str, resource_id: str, resource_name: str, resource_type: str):
        return await self._req(
            "/v0/tags",
            method="POST",
            headers={"Content-Type": "application/json"},
            data=json.dumps(
                {
                    "name": name,
                    "resources": [{"id": resource_id, "name": resource_name, "type": resource_type}],
                }
            ),
        )

    async def create_tag(self, name: str):
        return await self._req(
            "/v0/tags",
            method="POST",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"name": name}),
        )

    async def update_tag(self, name: str, resources: List[Dict[str, Any]]):
        await self._req(
            f"/v0/tags/{name}",
            method="PUT",
            headers={"Content-Type": "application/json"},
            data=json.dumps(
                {
                    "resources": resources,
                }
            ),
        )

    async def delete_tag(self, name: str):
        await self._req(f"/v0/tags/{name}", method="DELETE")
