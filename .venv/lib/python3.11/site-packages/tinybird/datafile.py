"""
Datafile is like a Dockerfile but to describe ETL processes
"""

from asyncio import Semaphore, gather
from datetime import datetime

import aiofiles
from requests import Response

try:
    from colorama import Back, Fore, Style, init

    init()
except ImportError:  # fallback so that the imported classes always exist

    class ColorFallback:
        def __getattr__(self, name):
            return ""

    Fore = Back = Style = ColorFallback()

import difflib
import glob
import itertools
import json
import logging
import math
import os
import os.path
import pprint
import re
import shlex
import shutil
import sys
import textwrap
import traceback
import unittest
import urllib.parse
from collections import namedtuple
from copy import deepcopy
from dataclasses import asdict, dataclass
from enum import Enum
from io import StringIO
from operator import itemgetter
from os import getcwd
from pathlib import Path
from statistics import mean, median
from string import Template
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Set, Tuple, Union, cast
from urllib.parse import parse_qs, urlencode, urlparse

import click
import requests
from croniter import croniter
from humanfriendly import format_size
from humanfriendly.tables import format_pretty_table
from mypy_extensions import KwArg, VarArg
from toposort import toposort

from tinybird.config import PROJECT_PATHS
from tinybird.sql_template_fmt import DEFAULT_FMT_LINE_LENGTH, format_sql_template
from tinybird.syncasync import sync_to_async
from tinybird.tb_cli_modules.common import (
    _get_tb_client,
    get_ca_pem_content,
    get_current_main_workspace,
    getenv_bool,
    wait_job,
)
from tinybird.tb_cli_modules.config import CLIConfig
from tinybird.tb_cli_modules.exceptions import CLIGitReleaseException, CLIPipeException

from .ch_utils.engine import ENABLED_ENGINES
from .client import AuthException, AuthNoTokenException, CanNotBeDeletedException, DoesNotExistException, TinyB
from .feedback_manager import FeedbackManager
from .sql import parse_indexes_structure, parse_table_structure, schema_to_sql_columns
from .sql_template import get_template_and_variables, get_used_tables_in_template, render_sql_template
from .tornado_template import UnClosedIfError

os.environ["GIT_PYTHON_REFRESH"] = "quiet"
from git import HEAD, Diff, GitCommandError, InvalidGitRepositoryError, Repo

INTERNAL_TABLES: Tuple[str, ...] = (
    "datasources_ops_log",
    "pipe_stats",
    "pipe_stats_rt",
    "block_log",
    "data_connectors_log",
    "kafka_ops_log",
    "datasources_storage",
    "endpoint_errors",
    "bi_stats_rt",
    "bi_stats",
)


PIPE_CHECKER_RETRIES: int = 3


class PipeTypes:
    MATERIALIZED = "materialized"
    ENDPOINT = "endpoint"
    COPY = "copy"
    DATA_SINK = "sink"
    STREAM = "stream"
    DEFAULT = "default"


class PipeNodeTypes:
    MATERIALIZED = "materialized"
    ENDPOINT = "endpoint"
    STANDARD = "standard"
    DEFAULT = "default"
    DATA_SINK = "sink"
    COPY = "copy"
    STREAM = "stream"


class DataFileExtensions:
    PIPE = ".pipe"
    DATASOURCE = ".datasource"
    INCL = ".incl"
    TOKEN = ".token"


class CopyParameters:
    TARGET_DATASOURCE = "target_datasource"
    COPY_SCHEDULE = "copy_schedule"
    COPY_MODE = "copy_mode"


class CopyModes:
    APPEND = "append"
    REPLACE = "replace"

    valid_modes = (APPEND, REPLACE)

    @staticmethod
    def is_valid(node_mode):
        return node_mode.lower() in CopyModes.valid_modes


PREVIEW_CONNECTOR_SERVICES = ["s3", "s3_iamrole", "gcs"]

ON_DEMAND = "@on-demand"
DEFAULT_CRON_PERIOD: int = 60


class ImportReplacements:
    _REPLACEMENTS: Tuple[Tuple[str, str, Optional[str]], ...] = (
        ("import_service", "service", None),
        ("import_strategy", "mode", "replace"),
        ("import_connection_name", "connection", None),
        ("import_schedule", "cron", ON_DEMAND),
        ("import_query", "query", None),
        ("import_connector", "connector", None),
        ("import_external_datasource", "external_data_source", None),
        ("import_bucket_uri", "bucket_uri", None),
        ("import_from_timestamp", "from_time", None),
        ("import_table_arn", "dynamodb_table_arn", None),
        ("import_export_bucket", "dynamodb_export_bucket", None),
    )

    @staticmethod
    def get_datafile_parameter_keys() -> List[str]:
        return [x[0] for x in ImportReplacements._REPLACEMENTS]

    @staticmethod
    def get_api_param_for_datafile_param(connector_service: str, key: str) -> Tuple[Optional[str], Optional[str]]:
        """Returns the API parameter name and default value for a given
        datafile parameter.
        """
        key = key.lower()
        for datafile_k, linker_k, value in ImportReplacements._REPLACEMENTS:
            if datafile_k == key:
                return linker_k, value
        return None, None

    @staticmethod
    def get_datafile_param_for_linker_param(connector_service: str, linker_param: str) -> Optional[str]:
        """Returns the datafile parameter name for a given linter parameter."""
        linker_param = linker_param.lower()
        for datafile_k, linker_k, _ in ImportReplacements._REPLACEMENTS:
            if linker_k == linker_param:
                return datafile_k
        return None

    @staticmethod
    def get_datafile_value_for_linker_value(
        connector_service: str, linker_param: str, linker_value: str
    ) -> Optional[str]:
        """Map linker values to datafile values."""
        linker_param = linker_param.lower()
        if linker_param != "cron":
            return linker_value
        if linker_value == "@once":
            return ON_DEMAND
        if connector_service in PREVIEW_CONNECTOR_SERVICES:
            return "@auto"
        return linker_value


class ExportReplacements:
    SERVICES = ("gcs_hmac", "s3", "s3_iamrole", "kafka")
    NODE_TYPES = (PipeNodeTypes.DATA_SINK, PipeNodeTypes.STREAM)
    _REPLACEMENTS = (
        ("export_service", "service", None),
        ("export_connection_name", "connection", None),
        ("export_schedule", "schedule_cron", ""),
        ("export_bucket_uri", "path", None),
        ("export_file_template", "file_template", None),
        ("export_format", "format", "csv"),
        ("export_compression", "compression", None),
        ("export_write_strategy", "write_strategy", None),
        ("export_strategy", "strategy", "@new"),
        ("export_kafka_topic", "kafka_topic", None),
        ("kafka_connection_name", "connection", None),
        ("kafka_topic", "kafka_topic", None),
    )

    @staticmethod
    def get_export_service(node: Dict[str, Optional[str]]) -> str:
        if (node.get("type", "standard") or "standard").lower() == PipeNodeTypes.STREAM:
            return "kafka"
        return (node.get("export_service", "") or "").lower()

    @staticmethod
    def get_node_type(node: Dict[str, Optional[str]]) -> str:
        return (node.get("type", "standard") or "standard").lower()

    @staticmethod
    def is_export_node(node: Dict[str, Optional[str]]) -> bool:
        export_service = ExportReplacements.get_export_service(node)
        node_type = (node.get("type", "standard") or "standard").lower()
        if not export_service:
            return False
        if export_service not in ExportReplacements.SERVICES:
            raise CLIPipeException(f"Invalid export service: {export_service}")
        if node_type not in ExportReplacements.NODE_TYPES:
            raise CLIPipeException(f"Invalid export node type: {node_type}")
        return True

    @staticmethod
    def get_params_from_datafile(node: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
        """Returns the export parameters for a given node."""
        params = {}
        node_type = ExportReplacements.get_node_type(node)
        for datafile_key, export_key, default_value in ExportReplacements._REPLACEMENTS:
            if node_type != PipeNodeTypes.STREAM and datafile_key.startswith("kafka_"):
                continue
            if node_type == PipeNodeTypes.STREAM and datafile_key.startswith("export_"):
                continue
            if datafile_key == "export_schedule" and node.get(datafile_key, None) == ON_DEMAND:
                node[datafile_key] = ""
            params[export_key] = node.get(datafile_key, default_value)
        return params

    @staticmethod
    def get_datafile_key(param: str, node: Dict[str, Optional[str]]) -> Optional[str]:
        """Returns the datafile key for a given export parameter."""
        node_type = ExportReplacements.get_node_type(node)
        for datafile_key, export_key, _ in ExportReplacements._REPLACEMENTS:
            if node_type != PipeNodeTypes.STREAM and datafile_key.startswith("kafka_"):
                continue
            if node_type == PipeNodeTypes.STREAM and datafile_key.startswith("export_"):
                continue
            if export_key == param.lower():
                return datafile_key.upper()
        return None


requests_get = sync_to_async(requests.get, thread_sensitive=False)
requests_delete = sync_to_async(requests.delete, thread_sensitive=False)

pp = pprint.PrettyPrinter()


class AlreadyExistsException(click.ClickException):
    pass


class ParseException(Exception):
    def __init__(self, err: str, lineno: int = -1):
        self.lineno: int = lineno
        super().__init__(err)


class IncludeFileNotFoundException(Exception):
    def __init__(self, err: str, lineno: int = -1):
        self.lineno: int = lineno
        super().__init__(err)


class ValidationException(Exception):
    def __init__(self, err: str, lineno: int = -1) -> None:
        self.lineno: int = lineno
        super().__init__(err)


def sizeof_fmt(num: Union[int, float], suffix: str = "b") -> str:
    """Readable file size
    :param num: Bytes value
    :type num: int
    :param suffix: Unit suffix (optionnal) default = o
    :type suffix: str
    :rtype: str
    """
    for unit in ["", "k", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)


def is_shared_datasource(ds_name: str) -> bool:
    """just looking for a dot in the name is fine, dot are not allowed in regular datasources"""
    return "." in ds_name


def format_filename(filename: str, hide_folders: bool = False):
    return os.path.basename(filename) if hide_folders else filename


class CLIGitRelease:
    NO_DATAFILES_PATHS = ["vendor/", "tests/", "scripts/", ".diff_tmp/"]
    DATAFILES_SUFFIXES = [".datasource", ".pipe", ".incl", ".token"]

    class ChangeType(Enum):
        # change type = T
        NEW_FILE = "A"
        COPIED = "C"
        MODIFIED = "M"
        RENAMED = "R"
        DELETED = "D"
        NONE = None

        def as_feedback_message(self, diff: Diff) -> str:
            change_type = self.name.lower().replace("_", " ")
            changed = f"{diff.a_path} -> {diff.b_path}" if self.value == self.RENAMED.value else f"{diff.a_path}"  # type: ignore
            return FeedbackManager.info_git_release_diff(change_type=change_type, datafile_changed=changed)

    def __init__(self, path=None):
        try:
            self.repo = Repo(path, search_parent_directories=True)
            self.path = path or os.getcwd()
            incl_path = os.getenv("TB_INCL_RELATIVE_PATH", None)
            if incl_path:
                incl_path = os.path.abspath(incl_path)
                if (
                    os.path.exists(incl_path)
                    and os.path.isdir(incl_path)
                    and Repo(incl_path, search_parent_directories=True)
                ):
                    self.paths = [self.path, incl_path]
                else:
                    FeedbackManager.info_incl_relative_path(path=incl_path)
                    self.paths = [self.path]
            else:
                self.paths = [self.path]
        except InvalidGitRepositoryError as exc:
            raise CLIGitReleaseException(FeedbackManager.error_no_git_repo_for_release(path=str(exc)))

    def working_dir(self):
        return self.repo.working_dir

    def status(self):
        return self.repo.git.status(self.path, "-s")

    def head(self) -> HEAD:
        return self.repo.head

    def head_commit(self) -> str:
        return self.head().commit.hexsha

    def is_main_branch(self) -> bool:
        return self.repo.active_branch.name in ["main", "master", "develop", "dev", "prod"]

    def is_head_empty(self) -> bool:
        return not self.repo.head.is_valid()

    # FIXME: Temporary. Delete this => https://gitlab.com/tinybird/analytics/-/issues/12551
    def get_main_branch_commit(self) -> Optional[str]:
        try:
            origin = self.repo.remotes.origin
            origin.fetch("main")
            click.echo(self.repo.references)
            for reference in self.repo.references:
                click.echo(reference.name)
                if reference.name == "origin/main":
                    return reference.commit.hexsha
        except Exception:
            pass
        return None

    def is_head_outdated(self, current_ws_commit: str) -> bool:
        try:
            return self.repo.is_ancestor(current_ws_commit, self.head_commit())
        except GitCommandError:
            raise CLIGitReleaseException(
                FeedbackManager.error_in_git_ancestor_commits(
                    head_commit=self.head().commit.hexsha, workspace_commit=current_ws_commit
                )
            )

    def has_untracked_files(self) -> bool:
        current_pwd = os.getcwd()
        working_dir = f"{self.repo.working_dir}/" if os.getcwd() != self.repo.working_dir else self.repo.working_dir
        project_path = current_pwd[current_pwd.startswith(working_dir) and len(working_dir) :]
        for untracked_file in self.repo.untracked_files:
            for path in PROJECT_PATHS:
                if untracked_file.startswith(f"{project_path}/{path}/" if project_path else f"{path}/"):
                    return True
        return False

    def is_dirty_to_release(self, use_include_dir: bool = True) -> bool:
        if use_include_dir:
            return any([self.repo.is_dirty(path=p) for p in self.paths]) or self.has_untracked_files()
        else:
            return self.repo.is_dirty(path=self.path) or self.has_untracked_files()

    def is_dirty_to_init(self) -> bool:
        return self.is_dirty_to_release(use_include_dir=False) or self.is_head_empty()

    def is_new_release(self, current_ws_commit: str) -> bool:
        return self.head().commit.hexsha != current_ws_commit

    def diff(self, current_ws_commit: str) -> List[Diff]:
        return self.repo.commit(current_ws_commit).diff(self.head(), paths=self.paths)

    def diff_datafiles(self, current_ws_commit: str) -> List[Diff]:
        # ignore diffs if:
        # not having a_path
        # not in data files paths
        # not expected suffixes
        return list(
            filter(
                lambda d: d.change_type in [c.value for c in self.ChangeType]
                and d.a_path
                and d.a_path not in self.NO_DATAFILES_PATHS
                and Path(d.a_path).resolve().suffix in self.DATAFILES_SUFFIXES,
                self.diff(current_ws_commit),
            )
        )

    def is_dottinyb_ignored(self) -> bool:
        return bool(self.repo.ignored(".tinyb"))

    def is_dotdifftemp_ignored(self) -> bool:
        return bool(self.repo.ignored(".diff_tmp")) or bool(self.repo.ignored(".diff_tmp/"))

    def validate_local_for_release(
        self,
        current_release: Dict[str, Any],
        check_dirty: bool = True,
        check_outdated: bool = True,
        check_new: bool = True,
    ):
        if self.is_dirty_to_release() and check_dirty:
            raise CLIGitReleaseException(
                FeedbackManager.error_commit_changes_to_release(path=self.path, git_output=self.status())
            )
        try:
            if not self.is_head_outdated(current_release["commit"]):
                if check_outdated:
                    raise CLIGitReleaseException(FeedbackManager.error_head_outdated(commit=self.head_commit()))
                else:
                    click.echo(FeedbackManager.warning_head_outdated(commit=self.head_commit()))
        except CLIGitReleaseException as e:
            if check_outdated:
                raise e
        if not self.is_new_release(current_release["commit"]) and check_new:
            raise CLIGitReleaseException(FeedbackManager.error_head_already_released(commit=current_release["commit"]))

    def get_changes_from_diffs(self, diffs: List[Diff], filenames: List[str]):
        changed = {}
        parsed_resources: Dict[str, Datafile] = {}

        def _is_include(include_path, resource_path):
            try:
                if filename.endswith(".pipe"):
                    parsed_func = parse_pipe
                else:
                    parsed_func = parse_datasource
                parsed_resource = parsed_resources.get(resource_path, parsed_func(resource_path))
                parsed_resources[filename] = parsed_resource
                for include in parsed_resource.includes.keys():
                    if Path(include_path).resolve().name in include.strip('"').strip("'"):
                        return True
                return False
            except IncludeFileNotFoundException as e:
                raise click.ClickException(
                    FeedbackManager.error_deleted_include(include_file=str(e), filename=filename)
                )

        for diff in diffs:
            changes_in_includes = False
            # should be already filtered in diff_datafiles. make mypy happy
            if diff.a_path:
                if ".incl" in diff.a_path:
                    if not changes_in_includes:
                        changes_in_includes = True
                        click.echo(FeedbackManager.info_detected_changes_from_includes())
                    for filename in filenames:
                        if _is_include(diff.a_path, filename):
                            changed[Path(filename).resolve().stem] = diff.change_type
                            click.echo(
                                FeedbackManager.info_processing_from_include(
                                    include_filename=diff.a_path, filename=filename
                                )
                            )
                else:
                    changed[Path(diff.a_path).resolve().stem] = diff.change_type

        # 'D' deleted are ignored
        return {
            Path(filename).resolve().stem: changed.get(Path(filename).resolve().stem, None) for filename in filenames
        }

    def get_deleted_from_diffs(self, diffs: List[Diff]) -> List[str]:
        return [diff.a_path for diff in diffs if diff.change_type == self.ChangeType.DELETED.value and diff.a_path]

    async def update_release(
        self, tb_client: TinyB, current_ws: Dict[str, Any], commit: Optional[str] = None
    ) -> Dict[str, Any]:
        release = await tb_client.workspace_commit_update(
            current_ws["id"], self.head().commit.hexsha if not commit else commit
        )
        return release


class Deployment:
    def __init__(
        self, current_ws: Dict[str, Any], is_git_release: bool, tb_client: TinyB, dry_run: bool, only_changes: bool
    ) -> None:
        self.current_ws = current_ws
        self.is_git_release = is_git_release
        self.current_release: Optional[Dict[str, Any]] = self.current_ws.get("release")
        self.cli_git_release: Optional[CLIGitRelease] = None
        self.tb_client = tb_client
        self.dry_run = dry_run
        self.only_changes = only_changes

    def _filenames_from_changed_modified(self, changed: Dict[str, Optional[str]], filenames: List[str]) -> List[str]:
        filenames_from_changed = []
        for change, type in changed.items():
            if (
                self.cli_git_release
                and type
                and self.cli_git_release.ChangeType(type) == self.cli_git_release.ChangeType.MODIFIED
            ):
                filenames_from_changed += [
                    filename
                    for filename in filenames
                    if filename.endswith(f"{change}.pipe") or filename.endswith(f"{change}.datasource")
                ]
        return filenames_from_changed

    def check_changes(
        self,
        changed_resources,
        pipes: List[Dict[str, Any]],
        release_created: Optional[bool] = False,
    ):
        # If there are copy pipes changed and is not a post-release throws an exception
        if changed_resources and pipes and self.cli_git_release and release_created:
            copy_pipe_names = {pipe["name"] for pipe in pipes if pipe["type"] == PipeTypes.COPY}

            copy_pipes_changed = {
                key
                for key in changed_resources
                if key in copy_pipe_names
                and self.cli_git_release.ChangeType(changed_resources[key]) == self.cli_git_release.ChangeType.MODIFIED
            }

            if copy_pipes_changed:
                click.echo(FeedbackManager.warning_copy_pipes_changed())

    async def detect_changes(
        self,
        filenames: List[str],
        only_changes: bool = False,
        config: Optional[CLIConfig] = None,
        use_main: bool = False,
        check_outdated: bool = True,
    ):
        if self.is_git_release:
            if not self.current_release:
                raise CLIGitReleaseException(FeedbackManager.error_init_release(workspace=self.current_ws["name"]))
            self.cli_git_release = CLIGitRelease()
            if not use_main:
                self.cli_git_release.validate_local_for_release(self.current_release, check_outdated=check_outdated)
            click.echo(FeedbackManager.info_deployment_detecting_changes_header())
            commit = self.cli_git_release.get_main_branch_commit() if use_main else self.current_release["commit"]
            diffs = self.cli_git_release.diff_datafiles(commit)  # type: ignore
            click.echo(
                FeedbackManager.info_git_release_diffs(
                    workspace=self.current_ws["name"],
                    current_release_commit=commit,
                    new_release_commit=self.cli_git_release.head_commit(),
                )
            )
            for d in diffs:
                click.echo(self.cli_git_release.ChangeType(d.change_type).as_feedback_message(d))
            # error until we support it https://gitlab.com/tinybird/analytics/-/issues/9655
            for d in diffs:
                if self.cli_git_release.ChangeType(d.change_type) == self.cli_git_release.ChangeType.RENAMED:
                    raise CLIGitReleaseException(FeedbackManager.error_unsupported_diff())
            if not diffs:
                click.echo(FeedbackManager.info_git_release_no_diffs())
            changed = self.cli_git_release.get_changes_from_diffs(diffs, filenames)
            deleted = self.cli_git_release.get_deleted_from_diffs(diffs)

            if getenv_bool("TB_DIFF_ON_DEPLOY", True):
                filenames_to_diff = self._filenames_from_changed_modified(changed, filenames)

                if filenames_to_diff:
                    diff_command_output = await diff_command(
                        filenames_to_diff,
                        fmt=True,
                        client=self.tb_client,
                        with_print=False,
                        for_deploy=True,
                        clean_up=True,
                        verbose=False,
                    )
                    found_ignored_by_format = None
                    for change, type in changed.items():
                        if (
                            type
                            and self.cli_git_release.ChangeType(type) == self.cli_git_release.ChangeType.MODIFIED
                            and change in diff_command_output
                            and diff_command_output.get(change) is None
                        ):
                            if not found_ignored_by_format:
                                click.echo(FeedbackManager.info_detected_changes_only_format())
                                found_ignored_by_format = True
                            click.echo(FeedbackManager.info_ignored_only_format(resource=change))
                            changed[change] = None

        else:
            # only changes flow
            changed = (
                await get_changes_from_main(only_changes, self.tb_client, config, self.current_ws, filenames=filenames)
                if config
                else None
            )
            deleted = []
        return changed, deleted

    def preparing_release(self):
        if not self.is_git_release:
            return
        click.echo(FeedbackManager.info_deployment_preparing_release_header())

    def preparing_dependencies(
        self, changed: Dict[str, str], dep_map: Dict[str, Set[str]], latest_datasource_versions: Dict[str, str]
    ) -> None:
        if not self.is_git_release:
            return
        deps_to_deploy = False

        if changed:
            for change, change_type in changed.items():
                if change_type:
                    for k, v in dep_map.items():
                        if k in changed and changed[k] is None and change in v:
                            name = (
                                f"'{change}' (v{latest_datasource_versions.get(change)})"
                                if latest_datasource_versions.get(change)
                                else f"'{change}'"
                            )
                            if not deps_to_deploy:
                                click.echo(FeedbackManager.info_deployment_deploy_deps())
                                deps_to_deploy = True
                            click.echo(FeedbackManager.info_deps_for_resource(resource=name, dep=k))

    def deploying_dry_run(self):
        click.echo(FeedbackManager.info_deployment_deploying_release_dry_run_header())

    def deploying(self):
        click.echo(FeedbackManager.info_deployment_deploying_release_header())

    async def update_release(
        self, commit: Optional[str] = None, has_semver: Optional[bool] = False, release_created: Optional[bool] = False
    ):
        if self.dry_run or has_semver:
            return
        if self.is_git_release or self.only_changes:
            try:
                self.cli_git_release = self.cli_git_release or CLIGitRelease()
                if self.current_release:
                    release = await self.cli_git_release.update_release(self.tb_client, self.current_ws, commit)
                    click.echo(FeedbackManager.success_git_release(release_commit=release["commit"]))
                else:
                    click.echo(FeedbackManager.warning_no_release())
            except Exception as e:
                if self.only_changes:
                    click.echo(FeedbackManager.warning_no_release())
                else:
                    raise e

    async def delete_resources(self, deleted: List[str], remote_pipes: List[Dict[str, Any]], dry_run: bool = False):
        async def delete_pipe(resource_name_or_id: str, dry_run: bool = False):
            if dry_run:
                click.echo(
                    FeedbackManager.info_deleting_resource(dry_run="[DRY RUN] ", resource_name=resource_name_or_id)
                )
            else:
                click.echo(FeedbackManager.info_deleting_resource(dry_run="", resource_name=resource_name_or_id))
                try:
                    await self.tb_client.pipe_delete(resource_name_or_id)
                except AuthException as e:
                    raise CLIGitReleaseException(str(e))
                else:
                    click.echo(FeedbackManager.success_delete(name=resource_name_or_id))

        async def delete_datasource(resource_name_or_id: str, dry_run: bool = False):
            if dry_run:
                click.echo(
                    FeedbackManager.info_deleting_resource(dry_run="[DRY RUN] ", resource_name=resource_name_or_id)
                )
            else:
                click.echo(FeedbackManager.info_deleting_resource(dry_run="", resource_name=resource_name_or_id))
                try:
                    await self.tb_client.datasource_delete(resource_name_or_id)
                except CanNotBeDeletedException as e:
                    raise CLIGitReleaseException(str(e))
                else:
                    click.echo(FeedbackManager.success_delete(name=resource_name_or_id))

        resource_extension_processor = {".datasource": delete_datasource, ".pipe": delete_pipe}

        # builds pipes deps the other way around from API to use with toposort
        # i.e {'analytics_hits': ['analytics_pages', 'trend', 'analytics_sources', 'analytics_sessions']}
        pipes_deps: Dict[str, List[str]] = {}
        remote_pipes_type: Dict[str, str] = {}
        for pipe in remote_pipes:
            pipes_deps[pipe["name"]] = []
            remote_pipes_type[pipe["name"]] = pipe["type"]
            for node in pipe["nodes"]:
                for dep in node["dependencies"]:
                    if dep in [pipe["name"] for pipe in remote_pipes]:
                        pipes_deps.setdefault(dep, []).append(pipe["name"])
        deleted_resources: Dict[str, str] = {
            Path(resource).resolve().stem: resource for resource in deleted if ".incl" not in resource
        }

        for group in toposort(pipes_deps):
            # for each level keep materialized for the end
            for pipe_name in sorted(group, key=lambda x: remote_pipes_type[x] == "materialized"):
                if pipe_name in deleted_resources:
                    resource_path = deleted_resources.pop(pipe_name)
                    try:
                        await resource_extension_processor[Path(resource_path).suffix](pipe_name, dry_run)
                    except KeyError:
                        raise CLIGitReleaseException(FeedbackManager.error_file_extension(filename=resource_path))

        # Delete pending resources (datasources)
        for name, path in deleted_resources.items():
            try:
                await resource_extension_processor[Path(path).suffix](name, dry_run)
            except KeyError:
                raise CLIGitReleaseException(FeedbackManager.error_file_extension(filename=path))


class Datafile:
    def __init__(self) -> None:
        self.maintainer: Optional[str] = None
        self.sources: List[str] = []
        self.nodes: List[Dict[str, Any]] = []
        self.tokens: List[Dict[str, Any]] = []
        self.version: Optional[int] = None
        self.description: Optional[str] = None
        self.raw: Optional[List[str]] = None
        self.includes: Dict[str, Any] = {}
        self.shared_with: List[str] = []
        self.warnings: List[str] = []
        self.filtering_tags: Optional[List[str]] = None

    def validate(self) -> None:
        for x in self.nodes:
            if not x["name"].strip():
                raise ValidationException("invalid node name, can't be empty")
            if "sql" not in x:
                raise ValidationException("node %s must have a SQL query" % x["name"])
        if self.version is not None and (not isinstance(self.version, int) or self.version < 0):
            raise ValidationException("version must be a positive integer")

    def is_equal(self, other):
        if len(self.nodes) != len(other.nodes):
            return False

        return all(self.nodes[i] == other.nodes[i] for i, _ in enumerate(self.nodes))


def parse_datasource(
    filename: str,
    replace_includes: bool = True,
    content: Optional[str] = None,
    skip_eval: bool = False,
    hide_folders: bool = False,
) -> Datafile:
    basepath = ""
    if not content:
        with open(filename) as file:
            s = file.read()
        basepath = os.path.dirname(filename)
    else:
        s = content

    filename = format_filename(filename, hide_folders)
    try:
        doc = parse(s, "default", basepath, replace_includes=replace_includes, skip_eval=skip_eval)
    except ParseException as e:
        raise click.ClickException(
            FeedbackManager.error_parsing_file(filename=filename, lineno=e.lineno, error=e)
        ) from None

    if len(doc.nodes) > 1:
        raise ValueError(f"{filename}: datasources can't have more than one node")

    return doc


def parse_pipe(
    filename: str,
    replace_includes: bool = True,
    content: Optional[str] = None,
    skip_eval: bool = False,
    hide_folders: bool = False,
) -> Datafile:
    basepath = ""
    if not content:
        with open(filename) as file:
            s = file.read()
        basepath = os.path.dirname(filename)
    else:
        s = content

    filename = format_filename(filename, hide_folders)
    try:
        sql = ""
        doc = parse(s, basepath=basepath, replace_includes=replace_includes, skip_eval=skip_eval)
        for node in doc.nodes:
            sql = node.get("sql", "")
            if sql.strip()[0] == "%":
                sql, _, variable_warnings = render_sql_template(sql[1:], test_mode=True, name=node["name"])
                doc.warnings = variable_warnings
            # it'll fail with a ModuleNotFoundError when the toolset is not available but it returns the parsed doc
            from tinybird.sql_toolset import format_sql as toolset_format_sql

            toolset_format_sql(sql)
    except ParseException as e:
        raise click.ClickException(
            FeedbackManager.error_parsing_file(
                filename=filename, lineno=e.lineno, error=f"{str(e)} + SQL(parse exception): {sql}"
            )
        )
    except ValueError as e:
        t, template_variables, _ = get_template_and_variables(sql, name=node["name"])

        if sql.strip()[0] != "%" and len(template_variables) > 0:
            raise click.ClickException(FeedbackManager.error_template_start(filename=filename))
        raise click.ClickException(
            FeedbackManager.error_parsing_file(
                filename=filename, lineno="", error=f"{str(e)} + SQL(value error): {sql}"
            )
        )
    except UnClosedIfError as e:
        raise click.ClickException(
            FeedbackManager.error_parsing_node_with_unclosed_if(node=e.node, pipe=filename, lineno=e.lineno, sql=e.sql)
        )
    except IncludeFileNotFoundException as e:
        raise click.ClickException(FeedbackManager.error_not_found_include(filename=e, lineno=e.lineno))
    except ModuleNotFoundError:
        pass
    return doc


def parse_token(
    filename: str,
    replace_includes: bool = True,
    content: Optional[str] = None,
    skip_eval: bool = False,
    hide_folders: bool = False,
) -> Datafile:
    if not content:
        with open(filename) as file:
            s = file.read()
        basepath = os.path.dirname(filename)
    else:
        s = content

    filename = format_filename(filename, hide_folders)
    try:
        sql = ""
        doc = parse(s, basepath=basepath, replace_includes=replace_includes, skip_eval=skip_eval)
    except ParseException as e:
        raise click.ClickException(
            FeedbackManager.error_parsing_file(
                filename=filename, lineno=e.lineno, error=f"{str(e)} + SQL(parse exception): {sql}"
            )
        )
    except ValueError as e:
        raise click.ClickException(
            FeedbackManager.error_parsing_file(
                filename=filename, lineno="", error=f"{str(e)} + SQL(value error): {sql}"
            )
        )
    except UnClosedIfError as e:
        raise click.ClickException(
            FeedbackManager.error_parsing_node_with_unclosed_if(node=e.node, pipe=filename, lineno=e.lineno, sql=e.sql)
        )
    except ModuleNotFoundError:
        pass

    return doc


def _unquote(x: str):
    QUOTES = ('"', "'")
    if x[0] in QUOTES and x[-1] in QUOTES:
        x = x[1:-1]
    return x


def eval_var(s: str, skip: bool = False) -> str:
    if skip:
        return s
    # replace ENV variables
    # it's probably a bad idea to allow to get any env var
    return Template(s).safe_substitute(os.environ)


def parse_tags(tags: str) -> Tuple[str, List[str]]:
    """
    Parses a string of tags into:
    - kv_tags: a string of key-value tags: the previous tags we have for operational purposes. It
        has the format key=value&key2=value2 (with_staging=true&with_last_date=true)
    - filtering_tags: a list of tags that are used for filtering.

    Example: "with_staging=true&with_last_date=true,billing,stats" ->
        kv_tags = {"with_staging": "true", "with_last_date": "true"}
        filtering_tags = ["billing", "stats"]
    """
    kv_tags = []
    filtering_tags = []

    entries = tags.split(",")
    for entry in entries:
        trimmed_entry = entry.strip()
        if "=" in trimmed_entry:
            kv_tags.append(trimmed_entry)
        else:
            filtering_tags.append(trimmed_entry)

    all_kv_tags = "&".join(kv_tags)

    return all_kv_tags, filtering_tags


def parse(
    s: str,
    default_node: Optional[str] = None,
    basepath: str = ".",
    replace_includes: bool = True,
    skip_eval: bool = False,
) -> Datafile:
    """
    Parses `s` string into a document
    >>> d = parse("FROM SCRATCH\\nSOURCE 'https://example.com'\\n#this is a comment\\nMAINTAINER 'rambo' #this is me\\nNODE \\"test_01\\"\\n    DESCRIPTION this is a node that does whatever\\nSQL >\\n\\n        SELECT * from test_00\\n\\n\\nNODE \\"test_02\\"\\n    DESCRIPTION this is a node that does whatever\\nSQL >\\n\\n    SELECT * from test_01\\n    WHERE a > 1\\n    GROUP by a\\n")
    >>> d.maintainer
    'rambo'
    >>> d.sources
    ['https://example.com']
    >>> len(d.nodes)
    2
    >>> d.nodes[0]
    {'name': 'test_01', 'description': 'this is a node that does whatever', 'sql': 'SELECT * from test_00'}
    >>> d.nodes[1]
    {'name': 'test_02', 'description': 'this is a node that does whatever', 'sql': 'SELECT * from test_01\\nWHERE a > 1\\nGROUP by a'}
    """
    lines = list(StringIO(s, newline=None))

    doc = Datafile()
    doc.raw = list(StringIO(s, newline=None))

    parser_state = namedtuple("parser_state", ["multiline", "current_node", "command", "multiline_string", "is_sql"])

    parser_state.multiline = False
    parser_state.current_node = False

    def assign(attr):
        def _fn(x, **kwargs):
            setattr(doc, attr, _unquote(x))

        return _fn

    def schema(*args, **kwargs):
        s = _unquote("".join(args))
        try:
            sh = parse_table_structure(s)
        except Exception as e:
            raise ParseException(FeedbackManager.error_parsing_schema(line=kwargs["lineno"], error=e))

        parser_state.current_node["schema"] = ",".join(schema_to_sql_columns(sh))
        parser_state.current_node["columns"] = sh

    def indexes(*args, **kwargs):
        s = _unquote("".join(args))
        if not s:
            return
        try:
            indexes = parse_indexes_structure(s.splitlines())
        except Exception as e:
            raise ParseException(FeedbackManager.error_parsing_indices(line=kwargs["lineno"], error=e))

        parser_state.current_node["indexes"] = indexes

    def assign_var(v: str) -> Callable[[VarArg(str), KwArg(Any)], None]:
        def _f(*args: str, **kwargs: Any):
            s = _unquote((" ".join(args)).strip())
            parser_state.current_node[v.lower()] = eval_var(s, skip=skip_eval)

        return _f

    def sources(x: str, **kwargs: Any) -> None:
        doc.sources.append(_unquote(x))

    def node(*args: str, **kwargs: Any) -> None:
        node = {"name": eval_var(_unquote(args[0]))}
        doc.nodes.append(node)
        parser_state.current_node = node

    def scope(*args: str, **kwargs: Any) -> None:
        scope = {"name": eval_var(_unquote(args[0]))}
        doc.nodes.append(scope)
        parser_state.current_node = scope

    def description(*args: str, **kwargs: Any) -> None:
        description = (" ".join(args)).strip()

        if parser_state.current_node:
            parser_state.current_node["description"] = description
            if parser_state.current_node.get("name", "") == "default":
                doc.description = description
        else:
            doc.description = description

    def sql(var_name: str, **kwargs: Any) -> Callable[[str, KwArg(Any)], None]:
        def _f(sql: str, **kwargs: Any) -> None:
            if not parser_state.current_node:
                raise ParseException("SQL must be called after a NODE command")
            _sql = textwrap.dedent(sql).rstrip() if "%" not in sql.strip()[0] else sql.strip()
            _sql = eval_var(_sql)
            parser_state.current_node[var_name] = _sql

        # HACK this cast is needed because Mypy
        return cast(Callable[[str, KwArg(Any)], None], _f)

    def assign_node_var(v: str) -> Callable[[VarArg(str), KwArg(Any)], None]:
        def _f(*args: str, **kwargs: Any) -> None:
            if not parser_state.current_node:
                raise ParseException("%s must be called after a NODE command" % v)
            return assign_var(v)(*args, **kwargs)

        return _f

    def add_token(*args: str, **kwargs: Any) -> None:  # token_name, permissions):
        if len(args) < 2:
            raise ParseException('TOKEN gets two params, token name and permissions e.g TOKEN "read api token" READ')
        doc.tokens.append({"token_name": _unquote(args[0]), "permissions": args[1]})

    def test(*args: str, **kwargs: Any) -> None:
        # TODO: Should be removed?
        print("test", args, kwargs)  # noqa: T201

    def include(*args: str, **kwargs: Any) -> None:
        f = _unquote(args[0])
        f = eval_var(f)
        attrs = dict(_unquote(x).split("=", 1) for x in args[1:])
        nonlocal lines
        lineno = kwargs["lineno"]
        replace_includes = kwargs["replace_includes"]
        n = lineno
        args_with_attrs = " ".join(args)

        try:
            while True:
                n += 1
                if len(lines) <= n:
                    break
                if "NODE" in lines[n]:
                    doc.includes[args_with_attrs] = lines[n]
                    break
            if args_with_attrs not in doc.includes:
                doc.includes[args_with_attrs] = ""
        except Exception:
            pass

        # If this parse was triggered by format, we don't want to replace the file
        if not replace_includes:
            return

        # be sure to replace the include line
        p = Path(basepath)

        try:
            with open(p / f) as file:
                try:
                    ll = list(StringIO(file.read(), newline=None))
                    node_line = [line for line in ll if "NODE" in line]
                    if node_line and doc.includes[args_with_attrs]:
                        doc.includes[node_line[0].split("NODE")[-1].split("\n")[0].strip()] = ""
                except Exception:
                    pass
                finally:
                    file.seek(0)
                lines[lineno : lineno + 1] = [
                    "",
                    *list(StringIO(Template(file.read()).safe_substitute(attrs), newline=None)),
                ]
        except FileNotFoundError:
            raise IncludeFileNotFoundException(f, lineno)

    def version(*args: str, **kwargs: Any) -> None:
        if len(args) < 1:
            raise ParseException("VERSION gets one positive integer param")
        try:
            version = int(args[0])
            if version < 0:
                raise ValidationException("version must be a positive integer e.g VERSION 2")
            doc.version = version
        except ValueError:
            raise ValidationException("version must be a positive integer e.g VERSION 2")

    def shared_with(*args: str, **kwargs: Any) -> None:
        for entries in args:
            # In case they specify multiple workspaces
            doc.shared_with += [workspace.strip() for workspace in entries.splitlines()]

    def __init_engine(v: str):
        if not parser_state.current_node:
            raise Exception(f"{v} must be called after a NODE command")
        if "engine" not in parser_state.current_node:
            parser_state.current_node["engine"] = {"type": None, "args": []}

    def set_engine(*args: str, **kwargs: Any) -> None:
        __init_engine("ENGINE")
        engine_type = _unquote((" ".join(args)).strip())
        parser_state.current_node["engine"]["type"] = eval_var(engine_type, skip=skip_eval)

    def add_engine_var(v: str) -> Callable[[VarArg(str), KwArg(Any)], None]:
        def _f(*args: str, **kwargs: Any):
            __init_engine(f"ENGINE_{v}".upper())
            engine_arg = eval_var(_unquote((" ".join(args)).strip()), skip=skip_eval)
            parser_state.current_node["engine"]["args"].append((v, engine_arg))

        return _f

    def tags(*args: str, **kwargs: Any) -> None:
        raw_tags = _unquote((" ".join(args)).strip())
        operational_tags, filtering_tags = parse_tags(raw_tags)

        # Pipe nodes or Data Sources
        if parser_state.current_node and operational_tags:
            operational_tags_args = (operational_tags,)
            assign_node_var("tags")(*operational_tags_args, **kwargs)

        if filtering_tags:
            if doc.filtering_tags is None:
                doc.filtering_tags = filtering_tags
            else:
                doc.filtering_tags += filtering_tags

    cmds = {
        "from": assign("from"),
        "source": sources,
        "maintainer": assign("maintainer"),
        "schema": schema,
        "indexes": indexes,
        # TODO: Added to be able to merge MR 11347, let's remove it afterwards
        "indices": indexes,
        "engine": set_engine,
        "partition_key": assign_var("partition_key"),
        "sorting_key": assign_var("sorting_key"),
        "primary_key": assign_var("primary_key"),
        "sampling_key": assign_var("sampling_key"),
        "ttl": assign_var("ttl"),
        "settings": assign_var("settings"),
        "node": node,
        "scope": scope,
        "description": description,
        "type": assign_node_var("type"),
        "datasource": assign_node_var("datasource"),
        "tags": tags,
        "target_datasource": assign_node_var("target_datasource"),
        "copy_schedule": assign_node_var(CopyParameters.COPY_SCHEDULE),
        "copy_mode": assign_node_var("mode"),
        "mode": assign_node_var("mode"),
        "resource": assign_node_var("resource"),
        "filter": assign_node_var("filter"),
        "token": add_token,
        "test": test,
        "include": include,
        "sql": sql("sql"),
        "version": version,
        "kafka_connection_name": assign_var("kafka_connection_name"),
        "kafka_topic": assign_var("kafka_topic"),
        "kafka_group_id": assign_var("kafka_group_id"),
        "kafka_bootstrap_servers": assign_var("kafka_bootstrap_servers"),
        "kafka_key": assign_var("kafka_key"),
        "kafka_secret": assign_var("kafka_secret"),
        "kafka_schema_registry_url": assign_var("kafka_schema_registry_url"),
        "kafka_target_partitions": assign_var("kafka_target_partitions"),
        "kafka_auto_offset_reset": assign_var("kafka_auto_offset_reset"),
        "kafka_store_raw_value": assign_var("kafka_store_raw_value"),
        "kafka_store_headers": assign_var("kafka_store_headers"),
        "kafka_store_binary_headers": assign_var("kafka_store_binary_headers"),
        "kafka_key_avro_deserialization": assign_var("kafka_key_avro_deserialization"),
        "kafka_ssl_ca_pem": assign_var("kafka_ssl_ca_pem"),
        "kafka_sasl_mechanism": assign_var("kafka_sasl_mechanism"),
        "import_service": assign_var("import_service"),
        "import_connection_name": assign_var("import_connection_name"),
        "import_schedule": assign_var("import_schedule"),
        "import_strategy": assign_var("import_strategy"),
        "import_external_datasource": assign_var("import_external_datasource"),
        "import_bucket_uri": assign_var("import_bucket_uri"),
        "import_from_timestamp": assign_var("import_from_timestamp"),
        "import_query": assign_var("import_query"),
        "import_table_arn": assign_var("import_table_arn"),
        "import_export_bucket": assign_var("import_export_bucket"),
        "shared_with": shared_with,
        "export_service": assign_var("export_service"),
        "export_connection_name": assign_var("export_connection_name"),
        "export_schedule": assign_var("export_schedule"),
        "export_bucket_uri": assign_var("export_bucket_uri"),
        "export_file_template": assign_var("export_file_template"),
        "export_format": assign_var("export_format"),
        "export_strategy": assign_var("export_strategy"),
        "export_compression": assign_var("export_compression"),
        "export_write_strategy": assign_var("export_write_strategy"),
        "export_kafka_topic": assign_var("export_kafka_topic"),
    }

    engine_vars = set()

    for _engine, (params, options) in ENABLED_ENGINES:
        for p in params:
            engine_vars.add(p.name)
        for o in options:
            engine_vars.add(o.name)
    for v in engine_vars:
        cmds[f"engine_{v}"] = add_engine_var(v)

    if default_node:
        node(default_node)

    lineno = 0
    try:
        while lineno < len(lines):
            line = lines[lineno]
            try:
                sa = shlex.shlex(line)
                sa.whitespace_split = True
                lexer = list(sa)
            except ValueError:
                sa = shlex.shlex(shlex.quote(line))
                sa.whitespace_split = True
                lexer = list(sa)
            if lexer:
                cmd, args = lexer[0], lexer[1:]
                if (
                    parser_state.multiline
                    and cmd.lower() in cmds
                    and not (line.startswith(" ") or line.startswith("\t") or line.lower().startswith("from"))
                ):
                    parser_state.multiline = False
                    cmds[parser_state.command](
                        parser_state.multiline_string, lineno=lineno, replace_includes=replace_includes
                    )

                if not parser_state.multiline:
                    if len(args) >= 1 and args[0] == ">":
                        parser_state.multiline = True
                        parser_state.command = cmd.lower()
                        parser_state.multiline_string = ""
                    else:
                        if cmd.lower() == "settings":
                            raise click.ClickException(FeedbackManager.error_settings_not_allowed())
                        if cmd.lower() in cmds:
                            cmds[cmd.lower()](*args, lineno=lineno, replace_includes=replace_includes)
                        else:
                            raise click.ClickException(FeedbackManager.error_option(option=cmd.upper()))
                else:
                    parser_state.multiline_string += line
            lineno += 1
        # close final state
        if parser_state.multiline:
            cmds[parser_state.command](parser_state.multiline_string, lineno=lineno, replace_includes=replace_includes)
    except ParseException as e:
        raise ParseException(str(e), lineno=lineno)
    except ValidationException as e:
        raise ValidationException(str(e), lineno=lineno)
    except IndexError as e:
        if "node" in line.lower():
            raise click.ClickException(FeedbackManager.error_missing_node_name())
        elif "sql" in line.lower():
            raise click.ClickException(FeedbackManager.error_missing_sql_command())
        elif "datasource" in line.lower():
            raise click.ClickException(FeedbackManager.error_missing_datasource_name())
        else:
            raise ValidationException(f"Validation error, found {line} in line {str(lineno)}: {str(e)}", lineno=lineno)
    except IncludeFileNotFoundException as e:
        raise IncludeFileNotFoundException(str(e), lineno=lineno)
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        raise ParseException(f"Unexpected error: {e}", lineno=lineno)

    return doc


async def process_file(
    filename: str,
    tb_client: TinyB,
    resource_versions: Optional[Dict] = None,
    skip_connectors: bool = False,
    workspace_map: Optional[Dict] = None,
    workspace_lib_paths: Optional[List[Tuple[str, str]]] = None,
    current_ws: Optional[Dict[str, Any]] = None,
):
    if workspace_map is None:
        workspace_map = {}

    if resource_versions is None:
        resource_versions = {}
    resource_versions_string = {k: f"__v{v}" for k, v in resource_versions.items() if v >= 0}

    def get_engine_params(node: Dict[str, Any]) -> Dict[str, Any]:
        params = {}

        if "engine" in node:
            engine = node["engine"]["type"]
            params["engine"] = engine
            args = node["engine"]["args"]
            for k, v in args:
                params[f"engine_{k}"] = v
        return params

    async def get_kafka_params(node: Dict[str, Any]):
        params = {key: value for key, value in node.items() if key.startswith("kafka")}

        if not skip_connectors:
            try:
                connector_params = {
                    "kafka_bootstrap_servers": params.get("kafka_bootstrap_servers", None),
                    "kafka_key": params.get("kafka_key", None),
                    "kafka_secret": params.get("kafka_secret", None),
                    "kafka_connection_name": params.get("kafka_connection_name", None),
                    "kafka_auto_offset_reset": params.get("kafka_auto_offset_reset", None),
                    "kafka_schema_registry_url": params.get("kafka_schema_registry_url", None),
                    "kafka_ssl_ca_pem": get_ca_pem_content(params.get("kafka_ssl_ca_pem", None), filename),
                    "kafka_sasl_mechanism": params.get("kafka_sasl_mechanism", None),
                }

                connector = await tb_client.get_connection(**connector_params)
                if not connector:
                    click.echo(
                        FeedbackManager.info_creating_kafka_connection(connection_name=params["kafka_connection_name"])
                    )
                    required_params = [
                        connector_params["kafka_bootstrap_servers"],
                        connector_params["kafka_key"],
                        connector_params["kafka_secret"],
                    ]

                    if not all(required_params):
                        raise click.ClickException(FeedbackManager.error_unknown_kafka_connection(datasource=name))

                    connector = await tb_client.connection_create_kafka(**connector_params)
            except Exception as e:
                raise click.ClickException(
                    FeedbackManager.error_connection_create(
                        connection_name=params["kafka_connection_name"], error=str(e)
                    )
                )

            click.echo(FeedbackManager.success_connection_using(connection_name=connector["name"]))

            params.update(
                {
                    "connector": connector["id"],
                    "service": "kafka",
                }
            )

        return params

    async def get_import_params(datasource: Dict[str, Any], node: Dict[str, Any]) -> Dict[str, Any]:
        params: Dict[str, Any] = {key: value for key, value in node.items() if key.startswith("import_")}

        if len(params) == 0 or skip_connectors:
            return params

        service: Optional[str] = node.get("import_service", None)

        if service and service.lower() == "bigquery":
            if not await tb_client.check_gcp_read_permissions():
                raise click.ClickException(FeedbackManager.error_unknown_bq_connection(datasource=datasource["name"]))

            # Bigquery doesn't have a datalink, so we can stop here
            return params

        # Rest of connectors

        connector_id: Optional[str] = node.get("import_connector", None)
        connector_name: Optional[str] = node.get("import_connection_name", None)
        if not connector_name and not connector_id:
            raise click.ClickException(FeedbackManager.error_missing_connection_name(datasource=datasource["name"]))

        if not connector_id:
            assert isinstance(connector_name, str)

            connector: Optional[Dict[str, Any]] = await tb_client.get_connector(connector_name, service)

            if not connector:
                raise Exception(
                    FeedbackManager.error_unknown_connection(datasource=datasource["name"], connection=connector_name)
                )
            connector_id = connector["id"]
            service = connector["service"]

        # The API needs the connector ID to create the datasource.
        params["import_connector"] = connector_id
        if service:
            params["import_service"] = service

        if import_from_timestamp := params.get("import_from_timestamp", None):
            try:
                str(datetime.fromisoformat(import_from_timestamp).isoformat())
            except ValueError:
                raise click.ClickException(
                    FeedbackManager.error_invalid_import_from_timestamp(datasource=datasource["name"])
                )

        if service in PREVIEW_CONNECTOR_SERVICES:
            if not params.get("import_bucket_uri", None):
                raise click.ClickException(FeedbackManager.error_missing_bucket_uri(datasource=datasource["name"]))
        elif service == "dynamodb":
            if not params.get("import_table_arn", None):
                raise click.ClickException(FeedbackManager.error_missing_table_arn(datasource=datasource["name"]))
            if not params.get("import_export_bucket", None):
                raise click.ClickException(FeedbackManager.error_missing_export_bucket(datasource=datasource["name"]))
        else:
            if not params.get("import_external_datasource", None):
                raise click.ClickException(
                    FeedbackManager.error_missing_external_datasource(datasource=datasource["name"])
                )

        return params

    if DataFileExtensions.DATASOURCE in filename:
        doc = parse_datasource(filename)
        node = doc.nodes[0]
        deps: List[str] = []
        # reemplace tables on materialized columns
        columns = parse_table_structure(node["schema"])

        _format = "csv"
        for x in columns:
            if x["default_value"] and x["default_value"].lower().startswith("materialized"):
                # turn expression to a select query to sql_get_used_tables can get the used tables
                q = "select " + x["default_value"][len("materialized") :]
                tables = await tb_client.sql_get_used_tables(q)
                # materialized columns expressions could have joins so we need to add them as a dep
                deps += tables
                # generate replacements and replace the query
                replacements = {t: t + resource_versions_string.get(t, "") for t in tables}

                replaced_results = await tb_client.replace_tables(q, replacements)
                x["default_value"] = replaced_results.replace("SELECT", "materialized", 1)
            if x.get("jsonpath", None):
                _format = "ndjson"

        schema = ",".join(schema_to_sql_columns(columns))

        name = os.path.basename(filename).rsplit(".", 1)[0]

        if workspace_lib_paths:
            for wk_name, wk_path in workspace_lib_paths:
                try:
                    Path(filename).relative_to(wk_path)
                    name = f"{workspace_map.get(wk_name, wk_name)}.{name}"
                except ValueError:
                    # the path was not relative, not inside workspace
                    pass

        version = f"__v{doc.version}" if doc.version is not None else ""

        def append_version_to_name(name: str, version: str) -> str:
            if version != "":
                name = name.replace(".", "_")
                return name + version
            return name

        description = node.get("description", "")
        indexes_list = node.get("indexes", [])
        indexes = None
        if indexes_list:
            indexes = "\n".join([index.to_sql() for index in indexes_list])
        params = {
            "name": append_version_to_name(name, version),
            "description": description,
            "schema": schema,
            "indexes": indexes,
            "indexes_list": indexes_list,
            "format": _format,
        }

        params.update(get_engine_params(node))

        if "import_service" in node or "import_connection_name" in node:
            VALID_SERVICES: Tuple[str, ...] = ("bigquery", "snowflake", "s3", "s3_iamrole", "gcs", "dynamodb")

            import_params = await get_import_params(params, node)

            service = import_params.get("import_service", None)
            if service and service not in VALID_SERVICES:
                raise Exception(f"Unknown import service: {service}")

            if service in PREVIEW_CONNECTOR_SERVICES:
                ON_DEMAND_CRON = ON_DEMAND
                AUTO_CRON = "@auto"
                ON_DEMAND_CRON_EXPECTED_BY_THE_API = "@once"
                VALID_CRONS: Tuple[str, ...] = (ON_DEMAND_CRON, AUTO_CRON)
                cron = node.get("import_schedule", ON_DEMAND_CRON)

                if cron not in VALID_CRONS:
                    valid_values = ", ".join(VALID_CRONS)
                    raise Exception(f"Invalid import schedule: '{cron}'. Valid values are: {valid_values}")

                if cron == ON_DEMAND_CRON:
                    import_params["import_schedule"] = ON_DEMAND_CRON_EXPECTED_BY_THE_API
                if cron == AUTO_CRON:
                    period: int = DEFAULT_CRON_PERIOD

                    if current_ws:
                        workspaces = (await tb_client.user_workspaces()).get("workspaces", [])
                        workspace_rate_limits: Dict[str, Dict[str, int]] = next(
                            (w.get("rate_limits", {}) for w in workspaces if w["id"] == current_ws["id"]), {}
                        )
                        period = workspace_rate_limits.get("api_datasources_create_append_replace", {}).get(
                            "period", DEFAULT_CRON_PERIOD
                        )

                    def seconds_to_cron_expression(seconds: int) -> str:
                        minutes = seconds // 60
                        hours = minutes // 60
                        days = hours // 24
                        if days > 0:
                            return f"0 0 */{days} * *"
                        if hours > 0:
                            return f"0 */{hours} * * *"
                        if minutes > 0:
                            return f"*/{minutes} * * * *"
                        return f"*/{seconds} * * * *"

                    import_params["import_schedule"] = seconds_to_cron_expression(period)

            # Include all import_ parameters in the datasource params
            params.update(import_params)

            # Substitute the import parameters with the ones used by the
            # import API:
            # - If an import parameter is not present and there's a default
            #   value, use the default value.
            # - If the resulting value is None, do not add the parameter.
            #
            # Note: any unknown import_ parameter is leaved as is.
            for key in ImportReplacements.get_datafile_parameter_keys():
                replacement, default_value = ImportReplacements.get_api_param_for_datafile_param(service, key)
                if not replacement:
                    continue  # We should not reach this never, but just in case...

                value: Any
                try:
                    value = params[key]
                    del params[key]
                except KeyError:
                    value = default_value

                if value:
                    params[replacement] = value

        if "kafka_connection_name" in node:
            kafka_params = await get_kafka_params(node)
            params.update(kafka_params)
            del params["format"]

        if "tags" in node:
            tags = {k: v[0] for k, v in urllib.parse.parse_qs(node["tags"]).items()}
            params.update(tags)

        resources: List[Dict[str, Any]] = []

        resources.append(
            {
                "resource": "datasources",
                "resource_name": name,
                "version": doc.version,
                "params": params,
                "filename": filename,
                "deps": deps,
                "tokens": doc.tokens,
                "shared_with": doc.shared_with,
                "filtering_tags": doc.filtering_tags,
            }
        )

        return resources

    elif DataFileExtensions.PIPE in filename:
        doc = parse_pipe(filename)
        version = f"__v{doc.version}" if doc.version is not None else ""
        name = os.path.basename(filename).split(".")[0]
        description = doc.description if doc.description is not None else ""

        deps = []
        nodes: List[Dict[str, Any]] = []

        is_copy = any([node for node in doc.nodes if node.get("type", "standard").lower() == PipeNodeTypes.COPY])
        for node in doc.nodes:
            sql = node["sql"]
            node_type = node.get("type", "standard").lower()
            params = {
                "name": node["name"],
                "type": node_type,
                "description": node.get("description", ""),
                "target_datasource": node.get("target_datasource", None),
                "copy_schedule": node.get(CopyParameters.COPY_SCHEDULE, None),
                "mode": node.get("mode", CopyModes.APPEND),
            }

            is_export_node = ExportReplacements.is_export_node(node)
            export_params = ExportReplacements.get_params_from_datafile(node) if is_export_node else None

            sql = sql.strip()
            is_template = False
            if sql[0] == "%":
                try:
                    sql_rendered, _, _ = render_sql_template(sql[1:], test_mode=True)
                except Exception as e:
                    raise click.ClickException(
                        FeedbackManager.error_parsing_node(node=node["name"], pipe=name, error=str(e))
                    )
                is_template = True
            else:
                sql_rendered = sql

            try:
                dependencies = await tb_client.sql_get_used_tables(sql_rendered, raising=True, is_copy=is_copy)
                deps += [t for t in dependencies if t not in [n["name"] for n in doc.nodes]]

            except Exception as e:
                raise click.ClickException(
                    FeedbackManager.error_parsing_node(node=node["name"], pipe=name, error=str(e))
                )

            if is_template:
                deps += get_used_tables_in_template(sql[1:])

            is_neither_copy_nor_materialized = "datasource" not in node and "target_datasource" not in node
            if "engine" in node and is_neither_copy_nor_materialized:
                raise ValueError("Defining ENGINE options in a node requires a DATASOURCE")

            if "datasource" in node:
                params["datasource"] = node["datasource"] + resource_versions_string.get(node["datasource"], "")
                deps += [node["datasource"]]

            if "target_datasource" in node:
                params["target_datasource"] = node["target_datasource"] + resource_versions_string.get(
                    node["target_datasource"], ""
                )
                deps += [node["target_datasource"]]

            params.update(get_engine_params(node))

            def create_replacement_for_resource(name: str) -> str:
                for old_ws, new_ws in workspace_map.items():
                    name = name.replace(f"{old_ws}.", f"{new_ws}.")
                return name + resource_versions_string.get(name, "")

            replacements = {
                x: create_replacement_for_resource(x) for x in deps if x not in [n["name"] for n in doc.nodes]
            }

            # FIXME: Ideally we should use await tb_client.replace_tables(sql, replacements)
            for old, new in replacements.items():
                sql = re.sub("([\t \\n']+|^)" + old + "([\t \\n'\\)]+|$)", "\\1" + new + "\\2", sql)

            if "tags" in node:
                tags = {k: v[0] for k, v in urllib.parse.parse_qs(node["tags"]).items()}
                params.update(tags)

            nodes.append(
                {
                    "sql": sql,
                    "params": params,
                    "export_params": export_params,
                }
            )

        return [
            {
                "resource": "pipes",
                "resource_name": name,
                "version": doc.version,
                "filename": filename,
                "name": name + version,
                "nodes": nodes,
                "deps": [x for x in set(deps)],
                "tokens": doc.tokens,
                "description": description,
                "warnings": doc.warnings,
                "filtering_tags": doc.filtering_tags,
            }
        ]
    elif DataFileExtensions.TOKEN in filename:
        doc = parse_token(filename)
        name = os.path.basename(filename).split(DataFileExtensions.TOKEN)[0]
        description = doc.description or ""
        version = f"__v{doc.version}" if doc.version is not None else ""
        scopes = doc.nodes or []
        return [
            {
                "resource": "tokens",
                "resource_name": name,
                "version": doc.version,
                "filename": filename,
                "name": name + version,
                "scopes": scopes,
                "description": description,
            }
        ]
    else:
        raise click.ClickException(FeedbackManager.error_file_extension(filename=filename))


def full_path_by_name(
    folder: str, name: str, workspace_lib_paths: Optional[List[Tuple[str, str]]] = None
) -> Optional[Path]:
    f = Path(folder)
    ds = name + ".datasource"
    if os.path.isfile(os.path.join(folder, ds)):
        return f / ds
    if os.path.isfile(f / "datasources" / ds):
        return f / "datasources" / ds

    pipe = name + ".pipe"
    if os.path.isfile(os.path.join(folder, pipe)):
        return f / pipe

    if os.path.isfile(f / "endpoints" / pipe):
        return f / "endpoints" / pipe

    if os.path.isfile(f / "pipes" / pipe):
        return f / "pipes" / pipe

    token = name + ".token"
    if os.path.isfile(f / "tokens" / token):
        return f / "tokens" / token

    if workspace_lib_paths:
        for wk_name, wk_path in workspace_lib_paths:
            if name.startswith(f"{wk_name}."):
                r = full_path_by_name(wk_path, name.replace(f"{wk_name}.", ""))
                if r:
                    return r
    return None


def find_file_by_name(
    folder: str,
    name: str,
    verbose: bool = False,
    is_raw: bool = False,
    workspace_lib_paths: Optional[List[Tuple[str, str]]] = None,
    resource: Optional[Dict] = None,
):
    f = Path(folder)
    ds = name + ".datasource"
    if os.path.isfile(os.path.join(folder, ds)):
        return ds, None
    if os.path.isfile(f / "datasources" / ds):
        return ds, None

    pipe = name + ".pipe"
    if os.path.isfile(os.path.join(folder, pipe)):
        return pipe, None

    if os.path.isfile(f / "endpoints" / pipe):
        return pipe, None

    if os.path.isfile(f / "pipes" / pipe):
        return pipe, None

    token = name + ".token"
    if os.path.isfile(f / "tokens" / token):
        return token, None

    # look for the file in subdirectories if it's not found in datasources folder
    if workspace_lib_paths:
        _resource = None
        for wk_name, wk_path in workspace_lib_paths:
            file = None
            if name.startswith(f"{wk_name}."):
                file, _resource = find_file_by_name(
                    wk_path, name.replace(f"{wk_name}.", ""), verbose, is_raw, resource=resource
                )
            if file:
                return file, _resource

    if not is_raw:
        f, raw = find_file_by_name(
            folder,
            name,
            verbose=verbose,
            is_raw=True,
            workspace_lib_paths=workspace_lib_paths,
            resource=resource,
        )
        return f, raw

    # materialized node with DATASOURCE definition
    if resource and "nodes" in resource:
        for node in resource["nodes"]:
            params = node.get("params", {})
            if (
                params.get("type", None) == "materialized"
                and params.get("engine", None)
                and params.get("datasource", None)
            ):
                pipe = resource["resource_name"] + ".pipe"
                pipe_file_exists = (
                    os.path.isfile(os.path.join(folder, pipe))
                    or os.path.isfile(f / "endpoints" / pipe)
                    or os.path.isfile(f / "pipes" / pipe)
                )
                is_target_datasource = params["datasource"] == name
                if pipe_file_exists and is_target_datasource:
                    return pipe, {"resource_name": params.get("datasource")}

    if verbose:
        click.echo(FeedbackManager.warning_file_not_found_inside(name=name, folder=folder))

    return None, None


def drop_token(url: str) -> str:
    """
    drops token param from the url query string
    >>> drop_token('https://api.tinybird.co/v0/pipes/aaa.json?token=abcd&a=1')
    'https://api.tinybird.co/v0/pipes/aaa.json?a=1'
    >>> drop_token('https://api.tinybird.co/v0/pipes/aaa.json?a=1')
    'https://api.tinybird.co/v0/pipes/aaa.json?a=1'
    """
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    qs_simplify = {k: v[0] for k, v in qs.items()}  # change several arguments to single one
    if "token" in qs_simplify:
        del qs_simplify["token"]
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{urlencode(qs_simplify)}"


def normalize_array(items: List[Dict[str, Optional[Any]]]) -> List[Dict]:
    """
        Sorted() doesn't not support values with different types for the same column like None vs str.
        So, we need to cast all None to default value of the type of the column if exist and if all the values are None, we can leave them as None
    >>> normalize_array([{'x': 'hello World'}, {'x': None}])
    [{'x': 'hello World'}, {'x': ''}]
    >>> normalize_array([{'x': 3}, {'x': None}])
    [{'x': 3}, {'x': 0}]
    >>> normalize_array([{'x': {'y': [1,2,3,4]}}, {'x': {'z': "Hello" }}])
    [{'x': {'y': [1, 2, 3, 4]}}, {'x': {'z': 'Hello'}}]
    """
    types: Dict[str, type] = {}
    if len(items) == 0:
        return items

    columns = items[0].keys()
    for column in columns:
        for object in items:
            if object[column] is not None:
                types[column] = type(object[column])
                break

    for object in items:
        for column in columns:
            if object[column] is not None:
                continue

            # If None, we replace it for the default value
            if types.get(column, None):
                object[column] = types[column]()

    return items


class PipeChecker(unittest.TestCase):
    RETRIES_LIMIT = PIPE_CHECKER_RETRIES

    current_response_time: float = 0
    checker_response_time: float = 0

    current_read_bytes: int = 0
    checker_read_bytes: int = 0

    def __init__(
        self,
        request: Dict[str, Any],
        pipe_name: str,
        checker_pipe_name: str,
        token: str,
        only_response_times: bool,
        ignore_order: bool,
        validate_processed_bytes: bool,
        relative_change: float,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        if request.get("http_method") == "POST":
            self.http_method = "POST"
            self.current_pipe_url, self.pipe_request_params = self._prepare_current_pipe_for_post_request(request)
        else:
            self.http_method = "GET"
            self.current_pipe_url, self.pipe_request_params = self._prepare_current_pipe_url_for_get_request(request)

        self._process_params()
        self.checker_pipe_name = checker_pipe_name
        self.pipe_name = pipe_name
        self.token = token
        self.only_response_times = only_response_times
        self.ignore_order = ignore_order
        self.validate_processed_bytes = validate_processed_bytes
        self.relative_change = relative_change

        parsed = urlparse(self.current_pipe_url)
        self.checker_pipe_url = f"{parsed.scheme}://{parsed.netloc}/v0/pipes/{self.checker_pipe_name}.json"
        self.checker_pipe_url += f"?{parsed.query}" if parsed.query is not None and parsed.query != "" else ""

    def _process_params(self) -> None:
        for key in self.pipe_request_params.keys():
            try:
                self.pipe_request_params[key] = json.loads(self.pipe_request_params[key])
            except Exception:
                pass

    def _prepare_current_pipe_url_for_get_request(self, request) -> Tuple[str, Dict[str, str]]:
        current_pipe_url = request.get("endpoint_url", "")
        current_pipe_url = (
            current_pipe_url.replace(".ndjson", ".json").replace(".csv", ".json").replace(".parquet", ".json")
        )
        current_pipe_url = drop_token(current_pipe_url)
        current_pipe_url += ("&" if "?" in current_pipe_url else "?") + "pipe_checker=true"
        return current_pipe_url, request.get("pipe_request_params", {})

    def _prepare_current_pipe_for_post_request(self, request) -> Tuple[str, Dict[str, str]]:
        current_pipe_url = request.get("endpoint_url", "")
        current_pipe_url = (
            current_pipe_url.replace(".ndjson", ".json").replace(".csv", ".json").replace(".parquet", ".json")
        )
        all_parameters = request.get("pipe_request_params")
        all_parameters.pop("token", None)
        all_parameters["pipe_checker"] = "true"

        return current_pipe_url, all_parameters

    def __str__(self):
        post_values = f" - POST Body: {self.pipe_request_params}" if self.http_method == "POST" else ""

        return f"current {self.current_pipe_url}{post_values}\n    new {self.checker_pipe_url}{post_values}"

    def diff(self, a: Dict[str, Any], b: Dict[str, Any]) -> str:
        a_properties = list(map(lambda x: f"{x}:{a[x]}\n", a.keys()))
        b_properties = list(map(lambda x: f"{x}:{b[x]}\n", b.keys()))

        return "".join(difflib.context_diff(a_properties, b_properties, self.pipe_name, self.checker_pipe_name))

    def _do_request_to_pipe(self, pipe_url: str) -> Response:
        headers = {"Authorization": f"Bearer {self.token}"}
        if self.http_method == "GET":
            return requests.get(pipe_url, headers=headers, verify=not getenv_bool("TB_DISABLE_SSL_CHECKS", False))
        else:
            return requests.post(
                pipe_url,
                headers=headers,
                verify=not getenv_bool("TB_DISABLE_SSL_CHECKS", False),
                data=self.pipe_request_params,
            )

    def _write_performance(self):
        return ""

    def _runTest(self) -> None:
        current_r = self._do_request_to_pipe(self.current_pipe_url)
        checker_r = self._do_request_to_pipe(self.checker_pipe_url)

        try:
            self.current_response_time = current_r.elapsed.total_seconds()
            self.checker_response_time = checker_r.elapsed.total_seconds()
        except Exception:
            pass

        current_response: Dict[str, Any] = current_r.json()
        checker_response: Dict[str, Any] = checker_r.json()

        current_data: List[Dict[str, Any]] = current_response.get("data", [])
        checker_data: List[Dict[str, Any]] = checker_response.get("data", [])

        self.current_read_bytes = current_response.get("statistics", {}).get("bytes_read", 0)
        self.checker_read_bytes = checker_response.get("statistics", {}).get("bytes_read", 0)

        error_check_fixtures_data: Optional[str] = checker_response.get("error", None)
        self.assertIsNone(
            error_check_fixtures_data,
            "You are trying to push a pipe with errors, please check the output or run with --no-check",
        )

        increase_response_time = (
            checker_r.elapsed.total_seconds() - current_r.elapsed.total_seconds()
        ) / current_r.elapsed.total_seconds()
        if self.only_response_times:
            self.assertLess(
                increase_response_time, 0.25, msg=f"response time has increased {round(increase_response_time * 100)}%"
            )
            return

        self.assertEqual(len(current_data), len(checker_data), "Number of elements does not match")

        if self.validate_processed_bytes:
            increase_read_bytes = (self.checker_read_bytes - self.current_read_bytes) / self.current_read_bytes
            self.assertLess(
                round(increase_read_bytes, 2),
                0.25,
                msg=f"The number of processed bytes has increased {round(increase_read_bytes * 100)}%",
            )

        if self.ignore_order:
            current_data = (
                sorted(normalize_array(current_data), key=itemgetter(*[k for k in current_data[0].keys()]))
                if len(current_data) > 0
                else current_data
            )
            checker_data = (
                sorted(normalize_array(checker_data), key=itemgetter(*[k for k in checker_data[0].keys()]))
                if len(checker_data) > 0
                else checker_data
            )

        for _, (current_data_e, check_fixtures_data_e) in enumerate(zip(current_data, checker_data)):
            self.assertEqual(list(current_data_e.keys()), list(check_fixtures_data_e.keys()))
            for x in current_data_e.keys():
                if isinstance(current_data_e[x], (float, int)):
                    d = abs(current_data_e[x] - check_fixtures_data_e[x])

                    try:
                        self.assertLessEqual(
                            d / current_data_e[x],
                            self.relative_change,
                            f"key {x}. old value: {current_data_e[x]}, new value: {check_fixtures_data_e[x]}\n{self.diff(current_data_e, check_fixtures_data_e)}",
                        )
                    except ZeroDivisionError:
                        self.assertEqual(
                            d,
                            0,
                            f"key {x}. old value: {current_data_e[x]}, new value: {check_fixtures_data_e[x]}\n{self.diff(current_data_e, check_fixtures_data_e)}",
                        )
                elif (
                    not isinstance(current_data_e[x], (str, bytes))
                    and isinstance(current_data_e[x], Iterable)
                    and self.ignore_order
                ):

                    def flatten(items):
                        """Yield items from any nested iterable; see Reference."""
                        output = []
                        for x in items:
                            if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
                                output.extend(flatten(x))
                            else:
                                output.append(x)
                        return output

                    self.assertEqual(
                        flatten(current_data_e[x]).sort(),
                        flatten(check_fixtures_data_e[x]).sort(),
                        "\n" + self.diff(current_data_e, check_fixtures_data_e),
                    )
                else:
                    self.assertEqual(
                        current_data_e[x],
                        check_fixtures_data_e[x],
                        "\n" + self.diff(current_data_e, check_fixtures_data_e),
                    )

    def runTest(self) -> None:
        if "debug" in self.pipe_request_params or (
            "from" in self.pipe_request_params and self.pipe_request_params["from"] == "ui"
        ):
            self.skipTest("found debug param")

        # Let's retry the validation to avoid false alerts when dealing with endpoints that have continuos ingestion
        retries = 0
        while retries < self.RETRIES_LIMIT:
            try:
                self._runTest()
            except AssertionError as e:
                retries += 1
                if retries >= self.RETRIES_LIMIT:
                    raise e
            else:
                break


@dataclass
class PipeCheckerRunnerResponse:
    pipe_name: str
    test_type: str
    output: str
    metrics_summary: Optional[Dict[str, Any]]
    metrics_timing: Dict[str, Tuple[float, float, float]]
    failed: List[Dict[str, str]]
    was_successfull: bool


class PipeCheckerRunner:
    checker_stream_result_class = unittest.runner._WritelnDecorator

    def __init__(self, pipe_name: str, host: str):
        self.pipe_name = pipe_name
        self.host = host

    def get_sqls_for_requests_to_check(
        self,
        matches: List[str],
        sample_by_params: int,
        limit: int,
        pipe_stats_rt_table: str = "",
        extra_where_clause: str = "",
    ):
        pipe_stats_rt = pipe_stats_rt_table or "tinybird.pipe_stats_rt"
        # TODO it may not be needed to extract token, pipe_checker, form or debug. They may be used in next steps
        # TODO extractURLParameter(assumeNotNull(url), 'from') <> 'ui' should read from request_param_names.
        sql_for_coverage = f"""
                    SELECT
                        groupArraySample({sample_by_params if sample_by_params > 0 else 1})(url) as endpoint_url,
                        groupArraySample({sample_by_params if sample_by_params > 0 else 1})(pipe_request_params) as pipe_request_params,
                        http_method
                    FROM
                        (
                        Select
                            url,
                            mapFilter((k, v) -> (k not IN ('token', 'pipe_checker', 'from', 'debug')), parameters) AS pipe_request_params,
                            mapKeys(pipe_request_params) request_param_names,
                            extractURLParameterNames(assumeNotNull(url)) as url_param_names,
                            method as http_method
                        FROM {pipe_stats_rt}
                        WHERE
                            pipe_name = '{self.pipe_name}'
                            AND url IS NOT NULL
                            AND extractURLParameter(assumeNotNull(url), 'from') <> 'ui'
                            AND extractURLParameter(assumeNotNull(url), 'pipe_checker') <> 'true'
                            AND extractURLParameter(assumeNotNull(url), 'debug') <> 'query'
                            AND error = 0
                            AND not mapContains(parameters, '__tb__semver')
                            {" AND " + " AND ".join([f"mapContains(pipe_request_params, '{match}')" for match in matches]) if matches and len(matches) > 0 else ''}
                            { extra_where_clause }
                        Limit 5000000 -- Enough to bring data while not processing all requests from highly used pipes
                        )
                    group by request_param_names, http_method
                    FORMAT JSON
                    """
        sql_latest_requests = f"""
                                SELECT
                                    [first_value(url)] as endpoint_url,
                                    [pipe_request_params] as pipe_request_params,
                                    http_method
                                FROM (
                                    SELECT assumeNotNull(url) as url,
                                        mapFilter((k, v) -> (k not IN ('token', 'pipe_checker', 'from', 'debug')), parameters) AS pipe_request_params,
                                        mapKeys(pipe_request_params) request_param_names,
                                        extractURLParameterNames(assumeNotNull(url)) as url_param_names,
                                        method as http_method
                                    FROM {pipe_stats_rt}
                                    WHERE
                                        pipe_name = '{self.pipe_name}'
                                        AND url IS NOT NULL
                                        AND extractURLParameter(assumeNotNull(url), 'from') <> 'ui'
                                        AND extractURLParameter(assumeNotNull(url), 'pipe_checker') <> 'true'
                                        AND extractURLParameter(assumeNotNull(url), 'debug') <> 'query'
                                        AND error = 0
                                        AND not mapContains(parameters, '__tb__semver')
                                        {" AND " + " AND ".join([f"mapContains(pipe_request_params, '{match}')" for match in matches]) if matches and len(matches) > 0 else ''}
                                        {extra_where_clause}
                                    LIMIT {limit}
                                )
                                GROUP BY pipe_request_params, http_method
                                FORMAT JSON
                            """
        return sql_for_coverage, sql_latest_requests

    def _get_checker(
        self,
        request: Dict[str, Any],
        checker_pipe_name: str,
        token: str,
        only_response_times: bool,
        ignore_order: bool,
        validate_processed_bytes: bool,
        relative_change: float,
    ) -> PipeChecker:
        return PipeChecker(
            request,
            self.pipe_name,
            checker_pipe_name,
            token,
            only_response_times,
            ignore_order,
            validate_processed_bytes,
            relative_change,
        )

    def _delta_percentage(self, checker: float, current: float) -> float:
        try:
            if current == 0.0:
                return 0.0
            return round(((checker - current) / current) * 100, 2)
        except Exception as exc:
            logging.warning(f"Error calculating delta: {exc}")
            return 0.0

    def run_pipe_checker(
        self,
        pipe_requests_to_check: List[Dict[str, Any]],
        checker_pipe_name: str,
        token: str,
        only_response_times: bool,
        ignore_order: bool,
        validate_processed_bytes: bool,
        relative_change: float,
        failfast: bool,
        custom_output: bool = False,
        debug: bool = False,
    ) -> PipeCheckerRunnerResponse:
        class PipeCheckerTextTestResult(unittest.TextTestResult):
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self.custom_output = kwargs.pop("custom_output", False)
                super().__init__(*args, **kwargs)
                self.success: List[PipeChecker] = []

            def addSuccess(self, test: PipeChecker):  # type: ignore
                super().addSuccess(test)
                self.success.append(test)

            def startTest(self, test):
                if not self.custom_output:
                    super().startTest(test)
                else:
                    super(unittest.TextTestResult, self).startTest(test)

            def _write_status(self, test, status):
                if self.custom_output:
                    self.stream.write(status.upper())
                    self.stream.write(" - ")
                    self.stream.write(str(test))
                    self.stream.write(" - ")
                    self.stream.writeln(test._write_performance())

                else:
                    self.stream.writeln(status)
                self.stream.flush()
                self._newline = True

        suite = unittest.TestSuite()

        for _, request in enumerate(pipe_requests_to_check):
            suite.addTest(
                self._get_checker(
                    request,
                    checker_pipe_name,
                    token,
                    only_response_times,
                    ignore_order,
                    validate_processed_bytes,
                    relative_change,
                )
            )

        result = PipeCheckerTextTestResult(
            self.checker_stream_result_class(sys.stdout),  # type: ignore
            descriptions=True,
            verbosity=2,
            custom_output=custom_output,
        )
        result.failfast = failfast
        suite.run(result)

        metrics_summary: Optional[Dict[str, Any]] = None
        metrics_timing: Dict[str, Tuple[float, float, float]] = {}

        try:
            current_response_times: List[float] = []
            checker_response_times: List[float] = []

            current_read_bytes: List[int] = []
            checker_read_bytes: List[int] = []
            if result.success:
                for test in result.success:
                    current_response_times.append(test.current_response_time)
                    checker_response_times.append(test.checker_response_time)

                    current_read_bytes.append(test.current_read_bytes)
                    checker_read_bytes.append(test.checker_read_bytes)

                for test, _ in result.failures:  # type: ignore
                    current_response_times.append(test.current_response_time)
                    checker_response_times.append(test.checker_response_time)

                    current_read_bytes.append(test.current_read_bytes)
                    checker_read_bytes.append(test.checker_read_bytes)
            else:
                # if we do not have any successful execution, let's just return a table with dummy metrics https://gitlab.com/tinybird/analytics/-/issues/10875
                current_response_times = [0]
                checker_response_times = [0]

                current_read_bytes = [0]
                checker_read_bytes = [0]

            metrics_summary = {
                "run": result.testsRun,
                "passed": len(result.success),
                "failed": len(result.failures),
                "percentage_passed": len(result.success) * 100 / result.testsRun,
                "percentage_failed": len(result.failures) * 100 / result.testsRun,
            }
            metrics_timing = {
                "min response time": (
                    min(current_response_times),
                    min(checker_response_times),
                    self._delta_percentage(min(checker_response_times), min(current_response_times)),
                ),
                "max response time": (
                    max(current_response_times),
                    max(checker_response_times),
                    self._delta_percentage(max(checker_response_times), max(current_response_times)),
                ),
                "mean response time": (
                    float(format(mean(current_response_times), ".6f")),
                    float(format(mean(checker_response_times), ".6f")),
                    self._delta_percentage(
                        float(format(mean(checker_response_times), ".6f")),
                        float(format(mean(current_response_times), ".6f")),
                    ),
                ),
                "median response time": (
                    median(current_response_times),
                    median(checker_response_times),
                    self._delta_percentage(median(checker_response_times), median(current_response_times)),
                ),
                "p90 response time": (
                    sorted(current_response_times)[math.ceil(len(current_response_times) * 0.9) - 1],
                    sorted(checker_response_times)[math.ceil(len(checker_response_times) * 0.9) - 1],
                    self._delta_percentage(
                        sorted(checker_response_times)[math.ceil(len(checker_response_times) * 0.9) - 1],
                        sorted(current_response_times)[math.ceil(len(current_response_times) * 0.9) - 1],
                    ),
                ),
                "min read bytes": (
                    format_size(min(current_read_bytes)),
                    format_size(min(checker_read_bytes)),
                    self._delta_percentage(min(checker_read_bytes), min(current_read_bytes)),
                ),
                "max read bytes": (
                    format_size(max(current_read_bytes)),
                    format_size(max(checker_read_bytes)),
                    self._delta_percentage(max(checker_read_bytes), max(current_read_bytes)),
                ),
                "mean read bytes": (
                    format_size(mean(current_read_bytes)),
                    format_size(mean(checker_read_bytes)),
                    self._delta_percentage(mean(checker_read_bytes), mean(current_read_bytes)),
                ),
                "median read bytes": (
                    format_size(median(current_read_bytes)),
                    format_size(median(checker_read_bytes)),
                    self._delta_percentage(median(checker_read_bytes), median(current_read_bytes)),
                ),
                "p90 read bytes": (
                    format_size(sorted(current_read_bytes)[math.ceil(len(current_read_bytes) * 0.9) - 1]),
                    format_size(sorted(checker_read_bytes)[math.ceil(len(checker_read_bytes) * 0.9) - 1]),
                    self._delta_percentage(
                        sorted(checker_read_bytes)[math.ceil(len(checker_read_bytes) * 0.9) - 1],
                        sorted(current_read_bytes)[math.ceil(len(current_read_bytes) * 0.9) - 1],
                    ),
                ),
            }
        except Exception as e:
            if debug:
                logging.exception(e)

        failures = []
        if not result.wasSuccessful():
            for _test, err in result.failures:
                try:
                    i = err.index("AssertionError") + len("AssertionError :")
                    failures.append({"name": str(_test), "error": err[i:]})
                except Exception as e:
                    if debug:
                        logging.exception(e)

        return PipeCheckerRunnerResponse(
            pipe_name=checker_pipe_name,
            test_type=getattr(self, "test_type", ""),
            output=getattr(result.stream, "_buffer", ""),
            metrics_summary=metrics_summary,
            metrics_timing=metrics_timing,
            failed=failures,
            was_successfull=result.wasSuccessful(),
        )


async def check_pipe(
    pipe,
    host: str,
    token: str,
    populate: bool,
    cl: TinyB,
    limit: int = 0,
    relative_change: float = 0.01,
    sample_by_params: int = 0,
    only_response_times=False,
    matches: Optional[List[str]] = None,
    failfast: bool = False,
    validate_processed_bytes: bool = False,
    ignore_order: bool = False,
    token_for_requests_to_check: Optional[str] = None,
    current_pipe: Optional[Dict[str, Any]] = None,
):
    checker_pipe = deepcopy(pipe)
    checker_pipe["name"] = f"{checker_pipe['name']}__checker"

    if current_pipe:
        pipe_type = current_pipe["type"]
        if pipe_type == PipeTypes.COPY:
            await cl.pipe_remove_copy(current_pipe["id"], current_pipe["copy_node"])
        if pipe_type == PipeTypes.DATA_SINK:
            await cl.pipe_remove_sink(current_pipe["id"], current_pipe["sink_node"])
        if pipe_type == PipeTypes.STREAM:
            await cl.pipe_remove_stream(current_pipe["id"], current_pipe["stream_node"])

    # In case of doing --force for a materialized view, checker is being created as standard pipe
    for node in checker_pipe["nodes"]:
        node["params"]["type"] = PipeNodeTypes.STANDARD

    if populate:
        raise click.ClickException(FeedbackManager.error_check_pipes_populate())

    runner = PipeCheckerRunner(pipe["name"], host)
    headers = (
        {"Authorization": f"Bearer {token_for_requests_to_check}"}
        if token_for_requests_to_check
        else {"Authorization": f"Bearer {token}"}
    )

    sql_for_coverage, sql_latest_requests = runner.get_sqls_for_requests_to_check(
        matches or [], sample_by_params, limit
    )

    params = {"q": sql_for_coverage if limit == 0 and sample_by_params > 0 else sql_latest_requests}
    r: requests.Response = await requests_get(
        f"{host}/v0/sql?{urlencode(params)}", headers=headers, verify=not getenv_bool("TB_DISABLE_SSL_CHECKS", False)
    )

    # If we get a timeout, fallback to just the last requests

    if not r or r.status_code == 408:
        params = {"q": sql_latest_requests}
        r = await requests_get(
            f"{host}/v0/sql?{urlencode(params)}",
            headers=headers,
            verify=not getenv_bool("TB_DISABLE_SSL_CHECKS", False),
        )

    if not r or r.status_code != 200:
        raise click.ClickException(FeedbackManager.error_check_pipes_api(pipe=pipe["name"]))

    pipe_requests_to_check: List[Dict[str, Any]] = []
    for row in r.json().get("data", []):
        for i in range(len(row["endpoint_url"])):
            pipe_requests_to_check += [
                {
                    "endpoint_url": f"{host}{row['endpoint_url'][i]}",
                    "pipe_request_params": row["pipe_request_params"][i],
                    "http_method": row["http_method"],
                }
            ]

    if not pipe_requests_to_check:
        return

    await new_pipe(checker_pipe, cl, force=True, check=False, populate=populate)

    runner_response = runner.run_pipe_checker(
        pipe_requests_to_check,
        checker_pipe["name"],
        token,
        only_response_times,
        ignore_order,
        validate_processed_bytes,
        relative_change,
        failfast,
    )

    try:
        if runner_response.metrics_summary and runner_response.metrics_timing:
            column_names_tests = ["Test Run", "Test Passed", "Test Failed", "% Test Passed", "% Test Failed"]
            click.echo("\n==== Test Metrics ====\n")
            click.echo(
                format_pretty_table(
                    [
                        [
                            runner_response.metrics_summary["run"],
                            runner_response.metrics_summary["passed"],
                            runner_response.metrics_summary["failed"],
                            runner_response.metrics_summary["percentage_passed"],
                            runner_response.metrics_summary["percentage_failed"],
                        ]
                    ],
                    column_names=column_names_tests,
                )
            )

            column_names_timing = ["Timing Metric (s)", "Current", "New"]
            click.echo("\n==== Response Time Metrics ====\n")
            click.echo(
                format_pretty_table(
                    [
                        [metric, runner_response.metrics_timing[metric][0], runner_response.metrics_timing[metric][1]]
                        for metric in [
                            "min response time",
                            "max response time",
                            "mean response time",
                            "median response time",
                            "p90 response time",
                            "min read bytes",
                            "max read bytes",
                            "mean read bytes",
                            "median read bytes",
                            "p90 read bytes",
                        ]
                    ],
                    column_names=column_names_timing,
                )
            )
    except Exception:
        pass

    if not runner_response.was_successfull:
        for failure in runner_response.failed:
            try:
                click.echo("==== Test FAILED ====\n")
                click.echo(failure["name"])
                click.echo(FeedbackManager.error_check_pipe(error=failure["error"]))
                click.echo("=====================\n\n\n")
            except Exception:
                pass
        raise RuntimeError("Invalid results, you can bypass checks by running push with the --no-check flag")

    # Only delete if no errors, so we can check results after failure
    headers = {"Authorization": f"Bearer {token}"}
    r = await requests_delete(f"{host}/v0/pipes/{checker_pipe['name']}", headers=headers)
    if r.status_code != 204:
        click.echo(FeedbackManager.warning_check_pipe(content=r.content))


async def check_materialized(pipe, host, token, cl, override_datasource=False, current_pipe=None):
    checker_pipe = deepcopy(pipe)
    checker_pipe["name"] = f"{checker_pipe['name']}__checker"
    headers = {"Authorization": f"Bearer {token}"}

    if current_pipe:
        from_copy_to_materialized = current_pipe["type"] == "copy"
        if from_copy_to_materialized:
            await cl.pipe_remove_copy(current_pipe["id"], current_pipe["copy_node"])

    materialized_node = None
    for node in checker_pipe["nodes"]:
        if node["params"]["type"] == "materialized":
            materialized_node = deepcopy(node)
            materialized_node["params"]["override_datasource"] = "true" if override_datasource else "false"
        node["params"]["type"] = "standard"

    try:
        pipe_created = False
        await new_pipe(
            checker_pipe, cl, force=True, check=False, populate=False, skip_tokens=True, ignore_sql_errors=False
        )
        pipe_created = True
        response = await cl.analyze_pipe_node(checker_pipe["name"], materialized_node, dry_run="true")
        if response.get("warnings"):
            show_materialized_view_warnings(response["warnings"])

    except Exception as e:
        raise click.ClickException(FeedbackManager.error_while_check_materialized(error=str(e)))
    finally:
        if pipe_created:
            r = await requests_delete(f"{host}/v0/pipes/{checker_pipe['name']}", headers=headers)
            if r.status_code != 204:
                click.echo(FeedbackManager.warning_check_pipe(content=r.content))


async def check_copy_pipe(pipe, copy_node, tb_client: TinyB):
    target_datasource = copy_node["params"].get("target_datasource", None)
    if not target_datasource:
        raise CLIPipeException(FeedbackManager.error_creating_copy_pipe_target_datasource_required())

    try:
        await tb_client.get_datasource(target_datasource)
    except DoesNotExistException:
        raise CLIPipeException(
            FeedbackManager.error_creating_copy_pipe_target_datasource_not_found(target_datasource=target_datasource)
        )
    except Exception as e:
        raise CLIPipeException(FeedbackManager.error_exception(error=e))

    schedule_cron = copy_node["params"].get(CopyParameters.COPY_SCHEDULE, None)
    is_valid_cron = not schedule_cron or (
        schedule_cron and (schedule_cron == ON_DEMAND or croniter.is_valid(schedule_cron))
    )

    if not is_valid_cron:
        raise CLIPipeException(FeedbackManager.error_creating_copy_pipe_invalid_cron(schedule_cron=schedule_cron))

    mode = copy_node["params"].get("mode", CopyModes.APPEND)
    is_valid_mode = CopyModes.is_valid(mode)

    if not is_valid_mode:
        raise CLIPipeException(FeedbackManager.error_creating_copy_pipe_invalid_mode(mode=mode))

    if not pipe:
        return

    pipe_name = pipe["name"]
    pipe_type = pipe["type"]

    if pipe_type == PipeTypes.ENDPOINT:
        await tb_client.pipe_remove_endpoint(pipe_name, pipe["endpoint"])

    if pipe_type == PipeTypes.DATA_SINK:
        await tb_client.pipe_remove_sink(pipe_name, pipe["sink_node"])

    if pipe_type == PipeTypes.STREAM:
        await tb_client.pipe_remove_stream(pipe_name, pipe["stream_node"])


async def check_sink_pipe(pipe, sink_node, tb_client: TinyB):
    if not sink_node["export_params"]:
        return

    if not pipe:
        return

    pipe_name = pipe["name"]
    pipe_type = pipe["type"]

    schedule_cron = sink_node["export_params"].get("schedule_cron", "")
    is_valid_cron = not schedule_cron or (schedule_cron and croniter.is_valid(schedule_cron))

    if not is_valid_cron:
        raise CLIPipeException(FeedbackManager.error_creating_sink_pipe_invalid_cron(schedule_cron=schedule_cron))

    if pipe_type == PipeTypes.ENDPOINT:
        await tb_client.pipe_remove_endpoint(pipe_name, pipe["endpoint"])

    if pipe_type == PipeTypes.COPY:
        await tb_client.pipe_remove_copy(pipe_name, pipe["copy_node"])

    if pipe_type == PipeTypes.STREAM:
        await tb_client.pipe_remove_stream(pipe_name, pipe["stream_node"])


async def check_stream_pipe(pipe, stream_node, tb_client: TinyB):
    if not stream_node["params"]:
        return

    if not pipe:
        return

    pipe_name = pipe["name"]
    pipe_type = pipe["type"]

    if pipe_type == PipeTypes.ENDPOINT:
        await tb_client.pipe_remove_endpoint(pipe_name, pipe["endpoint"])

    if pipe_type == PipeTypes.COPY:
        await tb_client.pipe_remove_copy(pipe_name, pipe["copy_node"])

    if pipe_type == PipeTypes.DATA_SINK:
        await tb_client.pipe_remove_sink(pipe_name, pipe["sink_node"])


def show_materialized_view_warnings(warnings):
    """
    >>> show_materialized_view_warnings([{'code': 'SIM', 'weight': 1}])

    >>> show_materialized_view_warnings([{'code': 'SIM', 'weight': 1}, {'code': 'HUGE_JOIN', 'weight': 2}, {'text': "Column 'number' is present in the GROUP BY but not in the SELECT clause. This might indicate a not valid Materialized View, please make sure you aggregate and GROUP BY in the topmost query.", 'code': 'GROUP_BY', 'weight': 100, 'documentation': 'https://tinybird.co/docs/guides/materialized-views.html#use-the-same-alias-in-select-and-group-by'}])
    ⚠️  Column 'number' is present in the GROUP BY but not in the SELECT clause. This might indicate a not valid Materialized View, please make sure you aggregate and GROUP BY in the topmost query. For more information read https://tinybird.co/docs/guides/materialized-views.html#use-the-same-alias-in-select-and-group-by or contact us at support@tinybird.co
    >>> show_materialized_view_warnings([{'code': 'SINGLE_JOIN', 'weight': 300}, {'text': "Column 'number' is present in the GROUP BY but not in the SELECT clause. This might indicate a not valid Materialized View, please make sure you aggregate and GROUP BY in the topmost query.", 'code': 'GROUP_BY', 'weight': 100, 'documentation': 'https://tinybird.co/docs/guides/materialized-views.html#use-the-same-alias-in-select-and-group-by'}])
    ⚠️  Column 'number' is present in the GROUP BY but not in the SELECT clause. This might indicate a not valid Materialized View, please make sure you aggregate and GROUP BY in the topmost query. For more information read https://tinybird.co/docs/guides/materialized-views.html#use-the-same-alias-in-select-and-group-by or contact us at support@tinybird.co
    """
    excluded_warnings = ["SIM", "SIM_UNKNOWN", "HUGE_JOIN"]
    sorted_warnings = sorted(warnings, key=lambda warning: warning["weight"])
    most_important_warning = {}
    for warning in sorted_warnings:
        if warning.get("code") and warning["code"] not in excluded_warnings:
            most_important_warning = warning
            break
    if most_important_warning:
        click.echo(
            FeedbackManager.single_warning_materialized_pipe(
                content=most_important_warning["text"], docs_url=most_important_warning["documentation"]
            )
        )


async def update_tags(resource_id: str, resource_name: str, resource_type: str, tags: List[str], tb_client: TinyB):
    def get_tags_for_resource(all_tags: dict, resource_id: str, resource_name: str) -> List[str]:
        tag_names = []

        for tag in all_tags.get("tags", []):
            for resource in tag.get("resources", []):
                if resource.get("id") == resource_id or resource.get("name") == resource_name:
                    tag_names.append(tag.get("name"))
                    break  # No need to check other resources in this tag

        return tag_names

    def get_tag(all_tags: dict, tag_name: str) -> Optional[dict]:
        for tag in all_tags.get("tags", []):
            if tag.get("name") == tag_name:
                return tag
        return None

    def compare_tags(current_tags: List[str], new_tags: List[str]) -> Tuple[List[str], List[str]]:
        tags_to_add = list(set(new_tags) - set(current_tags))
        tags_to_remove = list(set(current_tags) - set(new_tags))
        return tags_to_add, tags_to_remove

    try:
        all_tags = await tb_client.get_all_tags()
    except Exception as e:
        raise Exception(FeedbackManager.error_getting_tags(error=str(e)))

    # Get all tags of that resource
    current_tags = get_tags_for_resource(all_tags, resource_id, resource_name)

    # Get the tags to add and remove
    tags_to_add, tags_to_remove = compare_tags(current_tags, tags)

    # Tags to add
    for tag_name in tags_to_add:
        tag = get_tag(all_tags, tag_name)

        if not tag:
            # Create new tag
            try:
                await tb_client.create_tag_with_resource(
                    name=tag_name,
                    resource_id=resource_id,
                    resource_name=resource_name,
                    resource_type=resource_type,
                )
            except Exception as e:
                raise Exception(FeedbackManager.error_creating_tag(error=str(e)))
        else:
            # Update tag with new resource
            resources = tag.get("resources", [])
            resources.append({"id": resource_id, "name": resource_name, "type": resource_type})
            try:
                await tb_client.update_tag(tag.get("name", tag_name), resources)
            except Exception as e:
                raise Exception(FeedbackManager.error_updating_tag(error=str(e)))

    # Tags to delete
    for tag_name in tags_to_remove:
        tag = get_tag(all_tags, tag_name)

        if tag:
            resources = tag.get("resources", [])
            resources = [resource for resource in resources if resource.get("name") != resource_name]
            try:
                await tb_client.update_tag(tag.get("name", tag_name), resources)
            except Exception as e:
                raise Exception(FeedbackManager.error_updating_tag(error=str(e)))


async def update_tags_in_resource(rs: Dict[str, Any], resource_type: str, client: TinyB):
    filtering_tags = rs.get("filtering_tags", [])

    if not filtering_tags:
        return

    resource_id = ""
    resource_name = ""

    if resource_type == "datasource":
        ds_name = rs["params"]["name"]
        try:
            persisted_ds = await client.get_datasource(ds_name)
            resource_id = persisted_ds.get("id", "")
            resource_name = persisted_ds.get("name", "")
        except DoesNotExistException:
            click.echo(
                FeedbackManager.error_tag_generic("Could not get the latest Data Source info for updating its tags.")
            )
    elif resource_type == "pipe":
        pipe_name = rs["name"]
        try:
            persisted_pipe = await client.pipe(pipe_name)
            resource_id = persisted_pipe.get("id", "")
            resource_name = persisted_pipe.get("name", "")
        except DoesNotExistException:
            click.echo(FeedbackManager.error_tag_generic("Could not get the latest Pipe info for updating its tags."))

    if resource_id and resource_name:
        try:
            await update_tags(
                resource_id=resource_id,
                resource_name=resource_name,
                resource_type=resource_type,
                tags=filtering_tags,
                tb_client=client,
            )
        except Exception as e:
            click.echo(FeedbackManager.error_tag_generic(error=str(e)))


async def get_token_from_main_branch(branch_tb_client: TinyB) -> Optional[str]:
    token_from_main_branch = None
    current_workspace = await branch_tb_client.workspace_info()
    # current workspace is a branch
    if current_workspace.get("main"):
        response = await branch_tb_client.user_workspaces()
        workspaces = response["workspaces"]
        prod_workspace = next(
            (workspace for workspace in workspaces if workspace["id"] == current_workspace["main"]), None
        )
        if prod_workspace:
            token_from_main_branch = prod_workspace.get("token")
    return token_from_main_branch


async def new_pipe(
    p,
    tb_client: TinyB,
    force: bool = False,
    check: bool = True,
    populate: bool = False,
    populate_subset=None,
    populate_condition=None,
    unlink_on_populate_error: bool = False,
    wait_populate: bool = False,
    skip_tokens: bool = False,
    ignore_sql_errors: bool = False,
    only_response_times: bool = False,
    run_tests: bool = False,
    as_standard: bool = False,
    tests_to_run: int = 0,
    tests_relative_change: float = 0.01,
    tests_to_sample_by_params: int = 0,
    tests_filter_by: Optional[List[str]] = None,
    tests_failfast: bool = False,
    tests_ignore_order: bool = False,
    tests_validate_processed_bytes: bool = False,
    override_datasource: bool = False,
    tests_check_requests_from_branch: bool = False,
    config: Any = None,
    fork_downstream: Optional[bool] = False,
    fork: Optional[bool] = False,
):
    # TODO use tb_client instead of calling the urls directly.
    host = tb_client.host
    token = tb_client.token

    headers = {"Authorization": f"Bearer {token}"}

    cli_params = {}
    cli_params["cli_version"] = tb_client.version
    cli_params["description"] = p.get("description", "")
    cli_params["ignore_sql_errors"] = "true" if ignore_sql_errors else "false"

    r: requests.Response = await requests_get(f"{host}/v0/pipes/{p['name']}?{urlencode(cli_params)}", headers=headers)

    current_pipe = r.json() if r.status_code == 200 else None
    pipe_exists = current_pipe is not None

    is_materialized = any([node.get("params", {}).get("type", None) == "materialized" for node in p["nodes"]])
    copy_node = next((node for node in p["nodes"] if node.get("params", {}).get("type", None) == "copy"), None)
    sink_node = next((node for node in p["nodes"] if node.get("params", {}).get("type", None) == "sink"), None)
    stream_node = next((node for node in p["nodes"] if node.get("params", {}).get("type", None) == "stream"), None)

    for node in p["nodes"]:
        if node["params"]["name"] == p["name"]:
            raise click.ClickException(FeedbackManager.error_pipe_node_same_name(name=p["name"]))

    if pipe_exists:
        if force or run_tests:
            # TODO: this should create a different node and rename it to the final one on success
            if check and not populate:
                if not is_materialized and not copy_node and not sink_node and not stream_node:
                    await check_pipe(
                        p,
                        host,
                        token,
                        populate,
                        tb_client,
                        only_response_times=only_response_times,
                        limit=tests_to_run,
                        relative_change=tests_relative_change,
                        sample_by_params=tests_to_sample_by_params,
                        matches=tests_filter_by,
                        failfast=tests_failfast,
                        validate_processed_bytes=tests_validate_processed_bytes,
                        ignore_order=tests_ignore_order,
                        token_for_requests_to_check=(
                            await get_token_from_main_branch(tb_client)
                            if not tests_check_requests_from_branch
                            else None
                        ),
                        current_pipe=current_pipe,
                    )
                else:
                    if is_materialized:
                        await check_materialized(
                            p,
                            host,
                            token,
                            tb_client,
                            override_datasource=override_datasource,
                            current_pipe=current_pipe,
                        )
                    if copy_node:
                        await check_copy_pipe(pipe=current_pipe, copy_node=copy_node, tb_client=tb_client)
                    if sink_node:
                        await check_sink_pipe(pipe=current_pipe, sink_node=sink_node, tb_client=tb_client)
                    if stream_node:
                        await check_stream_pipe(pipe=current_pipe, stream_node=stream_node, tb_client=tb_client)
            if run_tests:
                logging.info(f"skipping force override of {p['name']}")
                return
        else:
            raise click.ClickException(FeedbackManager.error_pipe_already_exists(pipe=p["name"]))
    elif not pipe_exists and check:
        if is_materialized:
            await check_materialized(
                p, host, token, tb_client, override_datasource=override_datasource, current_pipe=current_pipe
            )
        if copy_node:
            await check_copy_pipe(pipe=current_pipe, copy_node=copy_node, tb_client=tb_client)

    params = {}
    params.update(cli_params)
    if force:
        params["force"] = "true"
    if populate:
        params["populate"] = "true"
    if populate_condition:
        params["populate_condition"] = populate_condition
    if populate_subset:
        params["populate_subset"] = populate_subset
    params["unlink_on_populate_error"] = "true" if unlink_on_populate_error else "false"
    params["branch_mode"] = "fork" if fork_downstream or fork else "None"

    body = {"name": p["name"], "description": p.get("description", "")}

    def parse_node(node):
        if "params" in node:
            node.update(node["params"])
            if node.get("type", "") == "materialized" and override_datasource:
                node["override_datasource"] = "true"
            del node["params"]
        return node

    if p["nodes"]:
        body["nodes"] = [parse_node(n) for n in p["nodes"]]

    if copy_node:
        body["target_datasource"] = copy_node.get("target_datasource", None)
        # We will update the schedule cron later
        body["schedule_cron"] = None

    if sink_node:
        body.update(sink_node.get("export_params", {}))

    if stream_node:
        body.update(stream_node.get("export_params", {}))

    post_headers = {"Content-Type": "application/json"}

    post_headers.update(headers)

    try:
        data = await tb_client._req(
            f"/v0/pipes?{urlencode(params)}", method="POST", headers=post_headers, data=json.dumps(body)
        )
    except Exception as e:
        raise click.ClickException(FeedbackManager.error_pushing_pipe(pipe=p["name"], error=str(e)))

    datasource = data.get("datasource", None)
    created_datasource = data.get("created_datasource", None)

    if datasource and created_datasource:
        if copy_node:
            click.echo(FeedbackManager.info_copy_datasource_created(pipe=p["name"], datasource=datasource["name"]))
        else:
            click.echo(
                FeedbackManager.info_materialized_datasource_created(pipe=p["name"], datasource=datasource["name"])
            )
    elif datasource and not created_datasource:
        if copy_node:
            click.echo(FeedbackManager.info_copy_datasource_used(pipe=p["name"], datasource=datasource["name"]))
        else:
            click.echo(FeedbackManager.info_materialized_datasource_used(pipe=p["name"], datasource=datasource["name"]))

    if datasource and populate and not copy_node:
        job_url = data.get("job", {}).get("job_url", None)
        job_id = data.get("job", {}).get("job_id", None)
        if populate_subset:
            click.echo(FeedbackManager.info_populate_subset_job_url(url=job_url, subset=populate_subset))
        elif populate_condition:
            click.echo(
                FeedbackManager.info_populate_condition_job_url(url=job_url, populate_condition=populate_condition)
            )
        else:
            click.echo(FeedbackManager.info_populate_job_url(url=job_url))

        if wait_populate:
            result = await wait_job(tb_client, job_id, job_url, "Populating")
            click.echo(FeedbackManager.info_populate_job_result(result=result))
    else:
        if data.get("type") == "default" and not skip_tokens and not as_standard and not copy_node and not sink_node:
            # FIXME: set option to add last node as endpoint in the API
            endpoint_node = next(
                (node for node in data.get("nodes", []) if node.get("type") == "endpoint"), data.get("nodes", [])[-1]
            )
            try:
                data = await tb_client._req(
                    f"/v0/pipes/{p['name']}/nodes/{endpoint_node.get('id')}/endpoint?{urlencode(cli_params)}",
                    method="POST",
                    headers=headers,
                )
            except Exception as e:
                raise Exception(
                    FeedbackManager.error_creating_endpoint(
                        node=endpoint_node.get("name"), pipe=p["name"], error=str(e)
                    )
                )

            click.echo(FeedbackManager.success_test_endpoint_no_token(host=host, pipe=p["name"]))

    if copy_node:
        pipe_id = data["id"]
        node = next((node for node in data["nodes"] if node["node_type"] == "copy"), None)
        if node:
            copy_params = {"pipe_name_or_id": pipe_id, "node_id": node["id"]}
            try:
                target_datasource = copy_node.get(CopyParameters.TARGET_DATASOURCE, None)
                schedule_cron = copy_node.get(CopyParameters.COPY_SCHEDULE, None)
                mode = copy_node.get("mode", CopyModes.APPEND)
                schedule_cron = None if schedule_cron == ON_DEMAND else schedule_cron
                current_target_datasource_id = data["copy_target_datasource"]
                target_datasource_response = await tb_client.get_datasource(target_datasource)
                target_datasource_to_send = (
                    target_datasource
                    if target_datasource_response.get("id", target_datasource) != current_target_datasource_id
                    else None
                )
                copy_params[CopyParameters.TARGET_DATASOURCE] = target_datasource_to_send
                current_schedule = data.get("schedule", {})
                current_schedule_cron = current_schedule.get("cron", None) if current_schedule else None
                schedule_cron_should_be_removed = current_schedule_cron and not schedule_cron
                copy_params["schedule_cron"] = "None" if schedule_cron_should_be_removed else schedule_cron
                copy_params["mode"] = mode
                await tb_client.pipe_update_copy(**copy_params)
            except Exception as e:
                raise Exception(
                    FeedbackManager.error_setting_copy_node(node=copy_node.get("name"), pipe=p["name"], error=str(e))
                )

    if p["tokens"] and not skip_tokens and not as_standard and data.get("type") in ["endpoint", "copy"]:
        # search for token with specified name and adds it if not found or adds permissions to it
        t = None
        for tk in p["tokens"]:
            token_name = tk["token_name"]
            t = await tb_client.get_token_by_name(token_name)
            if t:
                click.echo(FeedbackManager.info_create_found_token(token=token_name))
                scopes = [f"PIPES:{tk['permissions']}:{p['name']}"]
                for x in t["scopes"]:
                    sc = x["type"] if "resource" not in x else f"{x['type']}:{x['resource']}"
                    scopes.append(sc)
                try:
                    r = await tb_client.alter_tokens(token_name, scopes)
                    token = r["token"]  # type: ignore
                except Exception as e:
                    raise click.ClickException(FeedbackManager.error_creating_pipe(error=e))
            else:
                token_name = tk["token_name"]
                click.echo(FeedbackManager.info_create_not_found_token(token=token_name))
                try:
                    r = await tb_client.create_token(
                        token_name, [f"PIPES:{tk['permissions']}:{p['name']}"], "P", p["name"]
                    )
                    token = r["token"]  # type: ignore
                except Exception as e:
                    raise click.ClickException(FeedbackManager.error_creating_pipe(error=e))

        if data.get("type") == "endpoint":
            click.echo(FeedbackManager.success_test_endpoint_no_token(host=host, pipe=p["name"]))


async def share_and_unshare_datasource(
    client: TinyB,
    datasource: Dict[str, Any],
    user_token: str,
    workspaces_current_shared_with: List[str],
    workspaces_to_share: List[str],
    current_ws: Optional[Dict[str, Any]],
) -> None:
    datasource_name = datasource.get("name", "")
    datasource_id = datasource.get("id", "")
    workspaces: List[Dict[str, Any]]

    # In case we are pushing to a branch, we don't share the datasource
    # FIXME: Have only once way to get the current workspace
    if current_ws:
        # Force to get all the workspaces the user can access
        workspace = current_ws
        workspaces = (await client.user_workspaces()).get("workspaces", [])
    else:
        workspace = await client.user_workspace_branches()
        workspaces = workspace.get("workspaces", [])

    if workspace.get("is_branch", False):
        click.echo(FeedbackManager.info_skipping_sharing_datasources_branch(datasource=datasource["name"]))
        return

    # We duplicate the client to use the user_token
    user_client: TinyB = deepcopy(client)
    user_client.token = user_token
    if not workspaces_current_shared_with:
        for workspace_to_share in workspaces_to_share:
            w: Optional[Dict[str, Any]] = next((w for w in workspaces if w["name"] == workspace_to_share), None)
            if not w:
                raise Exception(
                    f"Unable to share datasource with the workspace {workspace_to_share}. Review that you have the admin permissions on this workspace"
                )

            await user_client.datasource_share(
                datasource_id=datasource_id,
                current_workspace_id=workspace.get("id", ""),
                destination_workspace_id=w.get("id", ""),
            )
            click.echo(
                FeedbackManager.success_datasource_shared(datasource=datasource_name, workspace=w.get("name", ""))
            )
    else:
        shared_with = [
            w
            for w in workspaces
            if next((ws for ws in workspaces_current_shared_with if ws == w["id"] or ws == w["name"]), None)
        ]
        defined_to_share_with = [
            w for w in workspaces if next((ws for ws in workspaces_to_share if ws == w["id"] or ws == w["name"]), None)
        ]
        workspaces_need_to_share = [w for w in defined_to_share_with if w not in shared_with]
        workspaces_need_to_unshare = [w for w in shared_with if w not in defined_to_share_with]

        for w in workspaces_need_to_share:
            await user_client.datasource_share(
                datasource_id=datasource_id,
                current_workspace_id=workspace.get("id", ""),
                destination_workspace_id=w.get("id", ""),
            )
            click.echo(
                FeedbackManager.success_datasource_shared(datasource=datasource["name"], workspace=w.get("name", ""))
            )

        for w in workspaces_need_to_unshare:
            await user_client.datasource_unshare(
                datasource_id=datasource_id,
                current_workspace_id=workspace.get("id", ""),
                destination_workspace_id=w.get("id", ""),
            )
            click.echo(
                FeedbackManager.success_datasource_unshared(datasource=datasource_name, workspace=w.get("name", ""))
            )


async def new_ds(
    ds: Dict[str, Any],
    client: TinyB,
    user_token: Optional[str],
    force: bool = False,
    skip_confirmation: bool = False,
    current_ws=None,
    fork_downstream: Optional[bool] = False,
    fork: Optional[bool] = False,
    git_release: Optional[bool] = False,
):
    ds_name = ds["params"]["name"]

    async def manage_tokens():
        # search for token with specified name and adds it if not found or adds permissions to it
        t = None
        for tk in ds["tokens"]:
            token_name = tk["token_name"]
            t = await client.get_token_by_name(token_name)
            if not t:
                token_name = tk["token_name"]
                click.echo(FeedbackManager.info_create_not_found_token(token=token_name))
                # DS == token_origin.Origins.DATASOURCE
                await client.create_token(token_name, [f"DATASOURCES:{tk['permissions']}:{ds_name}"], "DS", ds_name)
            else:
                click.echo(FeedbackManager.info_create_found_token(token=token_name))
                scopes = [f"DATASOURCES:{tk['permissions']}:{ds_name}"]
                for x in t["scopes"]:
                    sc = x["type"] if "resource" not in x else f"{x['type']}:{x['resource']}"
                    scopes.append(sc)
                await client.alter_tokens(token_name, scopes)

    try:
        existing_ds = await client.get_datasource(ds_name)
        datasource_exists = True
    except DoesNotExistException:
        datasource_exists = False

    engine_param = ds["params"].get("engine", "")

    if (
        ds["params"].get("service") == "dynamodb"
        and engine_param != ""
        and engine_param.lower() != "replacingmergetree"
    ):
        raise click.ClickException(FeedbackManager.error_dynamodb_engine_not_supported(engine=engine_param))

    if engine_param.lower() == "join":
        deprecation_notice = FeedbackManager.warning_deprecated(
            warning="Data Sources with Join engine are deprecated and will be removed in the next major release of tinybird-cli. Use MergeTree instead."
        )
        click.echo(deprecation_notice)

    if not datasource_exists or fork_downstream or fork:
        params = ds["params"]
        params["branch_mode"] = "fork" if fork_downstream or fork else "None"

        try:
            if (
                params.get("service") in PREVIEW_CONNECTOR_SERVICES
                and params.get("connector")
                and params.get("bucket_uri")
            ):
                bucket_uri = params.get("bucket_uri")
                extension = bucket_uri.split(".")[-1]
                if extension == "gz":
                    extension = bucket_uri.split(".")[-2]
                valid_formats = ["csv", "json", "jsonl", "ndjson", "parquet"]
                if extension not in valid_formats:
                    raise Exception(FeedbackManager.error_format(extension=extension, valid_formats=valid_formats))
                params["format"] = extension
            datasource_response = await client.datasource_create_from_definition(params)
            datasource = datasource_response.get("datasource", {})

            if datasource.get("service") == "dynamodb":
                job_id = datasource_response.get("import_id", None)
                if job_id:
                    jobs = await client.jobs(status=["waiting", "working"])
                    job_url = next((job["job_url"] for job in jobs if job["id"] == job_id), None)
                    if job_url:
                        click.echo(FeedbackManager.success_dynamodb_initial_load(job_url=job_url))

            if ds.get("tokens"):
                await manage_tokens()

            if ds.get("shared_with"):
                if not user_token:
                    click.echo(FeedbackManager.info_skipping_shared_with_entry())
                else:
                    await share_and_unshare_datasource(
                        client,
                        datasource,
                        user_token,
                        workspaces_current_shared_with=[],
                        workspaces_to_share=ds["shared_with"],
                        current_ws=current_ws,
                    )

        except Exception as e:
            raise click.ClickException(FeedbackManager.error_creating_datasource(error=str(e)))
        return

    if not force:
        raise click.ClickException(FeedbackManager.error_datasource_already_exists(datasource=ds_name))

    if ds.get("shared_with", []) or existing_ds.get("shared_with", []):
        if not user_token:
            click.echo(FeedbackManager.info_skipping_shared_with_entry())
        else:
            await share_and_unshare_datasource(
                client,
                existing_ds,
                user_token,
                existing_ds.get("shared_with", []),
                ds.get("shared_with", []),
                current_ws,
            )

    alter_response = None
    alter_error_message = None
    new_description = None
    new_schema = None
    new_indices = None
    new_ttl = None

    try:
        if datasource_exists and ds["params"]["description"] != existing_ds["description"]:
            new_description = ds["params"]["description"]

        if datasource_exists and ds["params"].get("engine_ttl") != existing_ds["engine"].get("ttl"):
            new_ttl = ds["params"].get("engine_ttl", "false")

        # Schema fixed by the kafka connector
        if datasource_exists and (
            ds["params"]["schema"].replace(" ", "") != existing_ds["schema"]["sql_schema"].replace(" ", "")
        ):
            new_schema = ds["params"]["schema"]

        if datasource_exists:
            new = [asdict(index) for index in ds.get("params", {}).get("indexes_list", [])]
            existing = existing_ds.get("indexes", [])
            new.sort(key=lambda x: x["name"])
            existing.sort(key=lambda x: x["name"])
            if len(existing) != len(new) or any([(d, d2) for d, d2 in zip(new, existing) if d != d2]):
                new_indices = ds.get("params", {}).get("indexes") or "0"
        if (
            new_description
            or new_schema
            or new_ttl
            or ((new_indices is not None) and (not fork_downstream or not fork))
        ):
            alter_response = await client.alter_datasource(
                ds_name,
                new_schema=new_schema,
                description=new_description,
                ttl=new_ttl,
                dry_run=True,
                indexes=new_indices,
            )
    except Exception as e:
        if "There were no operations to perform" in str(e):
            pass
        else:
            alter_error_message = str(e)

    if alter_response:
        if git_release and not skip_confirmation:
            click.echo(FeedbackManager.info_custom_deployment())
            click.echo("***************************************")
            click.echo("***************************************")
        click.echo(FeedbackManager.info_datasource_doesnt_match(datasource=ds_name))
        for operation in alter_response["operations"]:
            click.echo(f"**   -  {operation}")
        if alter_response["operations"] and alter_response.get("dependencies", []):
            click.echo(FeedbackManager.info_datasource_alter_dependent_pipes())
            for dependency in alter_response.get("dependencies", []):
                click.echo(f"**   -  {dependency}")

        if skip_confirmation:
            make_changes = True
        else:
            make_changes = click.prompt(FeedbackManager.info_ask_for_alter_confirmation()).lower() == "y"

        if make_changes:
            await client.alter_datasource(
                ds_name,
                new_schema=new_schema,
                description=new_description,
                ttl=new_ttl,
                dry_run=False,
                indexes=new_indices,
            )
            click.echo(FeedbackManager.success_datasource_alter())
        else:
            alter_error_message = "Alter datasource cancelled"

    if alter_error_message:
        raise click.ClickException(
            FeedbackManager.error_datasource_already_exists_and_alter_failed(
                datasource=ds_name, alter_error_message=alter_error_message
            )
        )

    if datasource_exists and ds["params"].get("backfill_column") != existing_ds["tags"].get("backfill_column"):
        params = {
            "backfill_column": ds["params"].get("backfill_column"),
        }

        try:
            click.echo(FeedbackManager.info_update_datasource(datasource=ds_name, params=params))
            await client.update_datasource(ds_name, params)
            click.echo(FeedbackManager.success_update_datasource(datasource=ds_name, params=params))
            make_changes = True
            alter_response = True
        except Exception as e:
            raise click.ClickException(FeedbackManager.error_updating_datasource(datasource=ds_name, error=str(e)))

    connector_data = None
    promote_error_message = None

    ds_params = ds["params"]
    service = ds_params.get("service")
    DATASOURCE_VALID_SERVICES_TO_UPDATE = ["bigquery", "snowflake"]
    if datasource_exists and service and service in [*DATASOURCE_VALID_SERVICES_TO_UPDATE, *PREVIEW_CONNECTOR_SERVICES]:
        connector_required_params = {
            "bigquery": ["service", "cron", "external_data_source"],
            "snowflake": ["connector", "service", "cron", "external_data_source"],
            "s3": ["connector", "service", "cron", "bucket_uri"],
            "s3_iamrole": ["connector", "service", "cron", "bucket_uri"],
            "gcs": ["connector", "service", "cron", "bucket_uri"],
        }.get(service, [])

        if not all(key in ds_params for key in connector_required_params):
            return

        connector = ds_params.get("connector", None)

        if service in PREVIEW_CONNECTOR_SERVICES:
            connector_id = existing_ds.get("connector", "")
            if not connector_id:
                return

            current_connector = await client.get_connector_by_id(existing_ds.get("connector", ""))
            if not current_connector:
                return

            if current_connector["name"] != ds_params["connection"]:
                param = "connection"
                datafile_param = ImportReplacements.get_datafile_param_for_linker_param(service, param) or param
                raise click.ClickException(FeedbackManager.error_updating_connector_not_supported(param=datafile_param))

            linkers = current_connector.get("linkers", [])
            linker = next((linker for linker in linkers if linker["datasource_id"] == existing_ds["id"]), None)
            if not linker:
                return

            linker_settings = linker.get("settings", {})
            for param, value in linker_settings.items():
                ds_params_value = ds_params.get(param, None)
                if ds_params_value and ds_params_value != value:
                    datafile_param = ImportReplacements.get_datafile_param_for_linker_param(service, param) or param
                    raise Exception(
                        FeedbackManager.error_updating_connector_not_supported(param=datafile_param.upper())
                    )
            return

        connector_data = {
            "connector": connector,
            "service": service,
            "cron": ds_params.get("cron", None),
            "external_data_source": ds_params.get("external_data_source", None),
            "bucket_uri": ds_params.get("bucket_uri", None),
            "mode": ds_params.get("mode", "replace"),
            "query": ds_params.get("query", None),
            "ingest_now": ds_params.get("ingest_now", False),
        }

        try:
            await client.update_datasource(ds_name, connector_data)
            click.echo(FeedbackManager.success_promoting_datasource(datasource=ds_name))
            return
        except Exception as e:
            promote_error_message = str(e)

    if alter_response and make_changes:
        # alter operation finished
        pass
    else:
        # removed replacing by default. When a datasource is removed data is
        # removed and all the references needs to be updated
        if (
            os.getenv("TB_I_KNOW_WHAT_I_AM_DOING")
            and click.prompt(FeedbackManager.info_ask_for_datasource_confirmation()) == ds_name
        ):  # TODO move to CLI
            try:
                await client.datasource_delete(ds_name)
                click.echo(FeedbackManager.success_delete_datasource(datasource=ds_name))
            except Exception:
                raise click.ClickException(FeedbackManager.error_removing_datasource(datasource=ds_name))
            return
        else:
            if alter_error_message:
                raise click.ClickException(
                    FeedbackManager.error_datasource_already_exists_and_alter_failed(
                        datasource=ds_name, alter_error_message=alter_error_message
                    )
                )
            if promote_error_message:
                raise click.ClickException(
                    FeedbackManager.error_promoting_datasource(datasource=ds_name, error=promote_error_message)
                )
            else:
                click.echo(FeedbackManager.warning_datasource_already_exists(datasource=ds_name))


async def new_token(token: Dict[str, Any], client: TinyB, force: bool = False):
    existing_token = None
    try:
        existing_token = await client.token_get(token["name"])
    except Exception:
        pass

    if not existing_token:
        await client.token_create(token)
        return

    if force:
        ADMIN_SCOPES = ["ADMIN", "ADMIN_USER"]
        if any([scope["type"] in ADMIN_SCOPES for scope in existing_token["scopes"]]):
            raise click.ClickException(FeedbackManager.error_token_cannot_be_overriden(token=token["name"]))

        await client.token_update(token)
        return

    raise click.ClickException(FeedbackManager.error_token_already_exists(token=token["name"]))


async def exec_file(
    r: Dict[str, Any],
    tb_client: TinyB,
    force: bool,
    check: bool,
    debug: bool,
    populate: bool,
    populate_subset,
    populate_condition,
    unlink_on_populate_error,
    wait_populate,
    user_token: Optional[str],
    override_datasource: bool = False,
    ignore_sql_errors: bool = False,
    skip_confirmation: bool = False,
    only_response_times: bool = False,
    run_tests=False,
    as_standard=False,
    tests_to_run: int = 0,
    tests_relative_change: float = 0.01,
    tests_to_sample_by_params: int = 0,
    tests_filter_by: Optional[List[str]] = None,
    tests_failfast: bool = False,
    tests_ignore_order: bool = False,
    tests_validate_processed_bytes: bool = False,
    tests_check_requests_from_branch: bool = False,
    current_ws: Optional[Dict[str, Any]] = None,
    fork_downstream: Optional[bool] = False,
    fork: Optional[bool] = False,
    git_release: Optional[bool] = False,
):
    if debug:
        click.echo(FeedbackManager.debug_running_file(file=pp.pformat(r)))
    if r["resource"] == "pipes":
        await new_pipe(
            r,
            tb_client,
            force,
            check,
            populate,
            populate_subset,
            populate_condition,
            unlink_on_populate_error,
            wait_populate,
            ignore_sql_errors=ignore_sql_errors,
            only_response_times=only_response_times,
            run_tests=run_tests,
            as_standard=as_standard,
            tests_to_run=tests_to_run,
            tests_relative_change=tests_relative_change,
            tests_to_sample_by_params=tests_to_sample_by_params,
            tests_filter_by=tests_filter_by,
            tests_failfast=tests_failfast,
            tests_ignore_order=tests_ignore_order,
            tests_validate_processed_bytes=tests_validate_processed_bytes,
            override_datasource=override_datasource,
            tests_check_requests_from_branch=tests_check_requests_from_branch,
            fork_downstream=fork_downstream,
            fork=fork,
        )
        await update_tags_in_resource(r, "pipe", tb_client)
    elif r["resource"] == "datasources":
        await new_ds(
            r,
            tb_client,
            user_token,
            force,
            skip_confirmation=skip_confirmation,
            current_ws=current_ws,
            fork_downstream=fork_downstream,
            fork=fork,
            git_release=git_release,
        )
        await update_tags_in_resource(r, "datasource", tb_client)
    elif r["resource"] == "tokens":
        await new_token(r, tb_client, force)
    else:
        raise click.ClickException(FeedbackManager.error_unknown_resource(resource=r["resource"]))


def get_name_version(ds: str) -> Dict[str, Any]:
    """
    Given a name like "name__dev__v0" returns ['name', 'dev', 'v0']
    >>> get_name_version('dev__name__v0')
    {'name': 'dev__name', 'version': 0}
    >>> get_name_version('name__v0')
    {'name': 'name', 'version': 0}
    >>> get_name_version('dev__name')
    {'name': 'dev__name', 'version': None}
    >>> get_name_version('name')
    {'name': 'name', 'version': None}
    >>> get_name_version('horario__3__pipe')
    {'name': 'horario__3__pipe', 'version': None}
    >>> get_name_version('horario__checker')
    {'name': 'horario__checker', 'version': None}
    >>> get_name_version('dev__horario__checker')
    {'name': 'dev__horario__checker', 'version': None}
    >>> get_name_version('tg__dActividades__v0_pipe_3907')
    {'name': 'tg__dActividades', 'version': 0}
    >>> get_name_version('tg__dActividades__va_pipe_3907')
    {'name': 'tg__dActividades__va_pipe_3907', 'version': None}
    >>> get_name_version('tg__origin_workspace.shared_ds__v3907')
    {'name': 'tg__origin_workspace.shared_ds', 'version': 3907}
    >>> get_name_version('tmph8egtl__')
    {'name': 'tmph8egtl__', 'version': None}
    >>> get_name_version('tmph8egtl__123__')
    {'name': 'tmph8egtl__123__', 'version': None}
    >>> get_name_version('dev__name__v0')
    {'name': 'dev__name', 'version': 0}
    >>> get_name_version('name__v0')
    {'name': 'name', 'version': 0}
    >>> get_name_version('dev__name')
    {'name': 'dev__name', 'version': None}
    >>> get_name_version('name')
    {'name': 'name', 'version': None}
    >>> get_name_version('horario__3__pipe')
    {'name': 'horario__3__pipe', 'version': None}
    >>> get_name_version('horario__checker')
    {'name': 'horario__checker', 'version': None}
    >>> get_name_version('dev__horario__checker')
    {'name': 'dev__horario__checker', 'version': None}
    >>> get_name_version('tg__dActividades__v0_pipe_3907')
    {'name': 'tg__dActividades', 'version': 0}
    >>> get_name_version('tg__origin_workspace.shared_ds__v3907')
    {'name': 'tg__origin_workspace.shared_ds', 'version': 3907}
    >>> get_name_version('tmph8egtl__')
    {'name': 'tmph8egtl__', 'version': None}
    >>> get_name_version('tmph8egtl__123__')
    {'name': 'tmph8egtl__123__', 'version': None}
    """
    tk = ds.rsplit("__", 2)
    if len(tk) == 1:
        return {"name": tk[0], "version": None}
    elif len(tk) == 2:
        if len(tk[1]):
            if tk[1][0] == "v" and re.match("[0-9]+$", tk[1][1:]):
                return {"name": tk[0], "version": int(tk[1][1:])}
            else:
                return {"name": tk[0] + "__" + tk[1], "version": None}
    elif len(tk) == 3 and len(tk[2]):
        if tk[2] == "checker":
            return {"name": tk[0] + "__" + tk[1] + "__" + tk[2], "version": None}
        if tk[2][0] == "v":
            parts = tk[2].split("_")
            try:
                return {"name": tk[0] + "__" + tk[1], "version": int(parts[0][1:])}
            except ValueError:
                return {"name": tk[0] + "__" + tk[1] + "__" + tk[2], "version": None}
        else:
            return {"name": "__".join(tk[0:]), "version": None}

    return {"name": ds, "version": None}


def get_resource_versions(datasources: List[str]):
    """
    return the latest version for all the datasources
    """
    versions = {}
    for x in datasources:
        t = get_name_version(x)
        name = t["name"]
        if t.get("version", None) is not None:
            versions[name] = t["version"]
    return versions


def get_remote_resource_name_without_version(remote_resource_name: str) -> str:
    """
    >>> get_remote_resource_name_without_version("r__datasource")
    'r__datasource'
    >>> get_remote_resource_name_without_version("r__datasource__v0")
    'r__datasource'
    >>> get_remote_resource_name_without_version("datasource")
    'datasource'
    """
    parts = get_name_version(remote_resource_name)
    return parts["name"]


def create_downstream_dependency_graph(dependency_graph: Dict[str, Set[str]], all_resources: Dict[str, Dict[str, Any]]):
    """
    This function reverses the dependency graph obtained from build_graph so you have downstream dependencies for each node in the graph.

    Additionally takes into account target_datasource of materialized views
    """
    downstream_dependency_graph: Dict[str, Set[str]] = {node: set() for node in dependency_graph}

    for node, dependencies in dependency_graph.items():
        for dependency in dependencies:
            if dependency not in downstream_dependency_graph:
                # a shared data source, we can skip it
                continue
            downstream_dependency_graph[dependency].add(node)

    for key in dict(downstream_dependency_graph):
        target_datasource = get_target_materialized_data_source_name(all_resources[key])
        if target_datasource:
            downstream_dependency_graph[key].update({target_datasource})
            try:
                downstream_dependency_graph[target_datasource].remove(key)
            except KeyError:
                pass

    return downstream_dependency_graph


def update_dep_map_recursively(
    dep_map: Dict[str, Set[str]],
    downstream_dep_map: Dict[str, Set[str]],
    all_resources: Dict[str, Dict[str, Any]],
    to_run: Dict[str, Dict[str, Any]],
    dep_map_keys: List[str],
    key: Optional[str] = None,
    visited: Optional[List[str]] = None,
):
    """
    Given a downstream_dep_map obtained from create_downstream_dependency_graph this function updates each node recursively to complete the downstream dependency graph for each node
    """
    if not visited:
        visited = list()
    if not key and len(dep_map_keys) == 0:
        return
    if not key:
        key = dep_map_keys.pop()
    if key not in dep_map:
        dep_map[key] = set()
    else:
        visited.append(key)
        return

    for dep in downstream_dep_map.get(key, {}):
        if dep not in downstream_dep_map:
            continue
        to_run[dep] = all_resources.get(dep, {})
        update_dep_map_recursively(
            dep_map, downstream_dep_map, all_resources, to_run, dep_map_keys, key=dep, visited=visited
        )
        dep_map[key].update(downstream_dep_map[dep])
        dep_map[key].update({dep})
        try:
            dep_map[key].remove(key)
        except KeyError:
            pass

    to_run[key] = all_resources.get(key, {})
    update_dep_map_recursively(
        dep_map, downstream_dep_map, all_resources, to_run, dep_map_keys, key=None, visited=visited
    )


def generate_forkdownstream_graph(
    all_dep_map: Dict[str, Set[str]],
    all_resources: Dict[str, Dict[str, Any]],
    to_run: Dict[str, Dict[str, Any]],
    dep_map_keys: List[str],
) -> Tuple[Dict[str, Set[str]], Dict[str, Dict[str, Any]]]:
    """
    This function for a given graph of dependencies from left to right. It will generate a new graph with the dependencies from right to left, but taking into account that even if some nodes are not inside to_run, they are still dependencies that need to be deployed.

    >>> deps, _ = generate_forkdownstream_graph(
    ...     {
    ...         'a': {'b'},
    ...         'b': {'c'},
    ...         'c': set(),
    ...     },
    ...     {
    ...         'a': {'resource_name': 'a'},
    ...         'b': {'resource_name': 'b', 'nodes': [{'params': {'type': 'materialized', 'datasource': 'c'}}] },
    ...         'c': {'resource_name': 'c'},
    ...     },
    ...     {
    ...         'a': {'resource_name': 'a'},
    ...     },
    ...     ['a', 'b', 'c'],
    ... )
    >>> {k: sorted(v) for k, v in deps.items()}
    {'c': [], 'b': ['a', 'c'], 'a': []}

    >>> deps, _ = generate_forkdownstream_graph(
    ...     {
    ...         'a': {'b'},
    ...         'b': {'c'},
    ...         'c': set(),
    ...     },
    ...     {
    ...         'a': {'resource_name': 'a'},
    ...         'b': {'resource_name': 'b', 'nodes': [{'params': {'type': 'materialized', 'datasource': 'c'}}] },
    ...         'c': {'resource_name': 'c'},
    ...     },
    ...     {
    ...         'b': {'resource_name': 'b'},
    ...     },
    ...     ['a', 'b', 'c'],
    ... )
    >>> {k: sorted(v) for k, v in deps.items()}
    {'c': [], 'b': ['a', 'c'], 'a': []}

    >>> deps, _ = generate_forkdownstream_graph(
    ...     {
    ...         'migrated__a': {'a'},
    ...         'a': {'b'},
    ...         'b': {'c'},
    ...         'c': set(),
    ...     },
    ...     {
    ...         'migrated__a': {'resource_name': 'migrated__a', 'nodes': [{'params': {'type': 'materialized', 'datasource': 'a'}}]},
    ...         'a': {'resource_name': 'a'},
    ...         'b': {'resource_name': 'b', 'nodes': [{'params': {'type': 'materialized', 'datasource': 'c'}}] },
    ...         'c': {'resource_name': 'c'},
    ...     },
    ...     {
    ...         'migrated__a': {'resource_name': 'migrated__a'},
    ...         'a': {'resource_name': 'a'},
    ...     },
    ...     ['migrated_a', 'a', 'b', 'c'],
    ... )
    >>> {k: sorted(v) for k, v in deps.items()}
    {'c': [], 'b': ['a', 'c'], 'a': [], 'migrated_a': []}
    """
    downstream_dep_map = create_downstream_dependency_graph(all_dep_map, all_resources)
    new_dep_map: Dict[str, Set[str]] = {}
    new_to_run = deepcopy(to_run)
    update_dep_map_recursively(new_dep_map, downstream_dep_map, all_resources, new_to_run, dep_map_keys)
    return new_dep_map, new_to_run


@dataclass
class GraphDependencies:
    """
    This class is used to store the dependencies graph and the resources that are going to be deployed
    """

    dep_map: Dict[str, Set[str]]
    to_run: Dict[str, Dict[str, Any]]

    # The same as above but for the whole project, not just the resources affected by the current deployment
    all_dep_map: Dict[str, Set[str]]
    all_resources: Dict[str, Dict[str, Any]]


async def build_graph(
    filenames: Iterable[str],
    tb_client: TinyB,
    dir_path: Optional[str] = None,
    resource_versions=None,
    workspace_map: Optional[Dict] = None,
    process_dependencies: bool = False,
    verbose: bool = False,
    skip_connectors: bool = False,
    workspace_lib_paths: Optional[List[Tuple[str, str]]] = None,
    current_ws: Optional[Dict[str, Any]] = None,
    changed: Optional[Dict[str, Any]] = None,
    only_changes: bool = False,
    fork_downstream: Optional[bool] = False,
    is_internal: Optional[bool] = False,
) -> GraphDependencies:
    """
    This method will generate a dependency graph for the given files. It will also return a map of all the resources that are going to be deployed.
    By default it will generate the graph from left to right, but if fork-downstream, it will generate the graph from right to left.
    """
    to_run: Dict[str, Any] = {}
    deps: List[str] = []
    dep_map: Dict[str, Any] = {}
    embedded_datasources = {}
    if not workspace_map:
        workspace_map = {}

    # These dictionaries are used to store all the resources and there dependencies for the whole project
    # This is used for the downstream dependency graph
    all_dep_map: Dict[str, Set[str]] = {}
    all_resources: Dict[str, Dict[str, Any]] = {}

    if dir_path is None:
        dir_path = os.getcwd()

    # When using fork-downstream or --only-changes, we need to generate all the graph of all the resources and their dependencies
    # This way we can add more resources into the to_run dictionary if needed.
    if process_dependencies and only_changes:
        all_dependencies_graph = await build_graph(
            get_project_filenames(dir_path),
            tb_client,
            dir_path=dir_path,
            process_dependencies=True,
            resource_versions=resource_versions,
            workspace_map=workspace_map,
            skip_connectors=True,
            workspace_lib_paths=workspace_lib_paths,
            current_ws=current_ws,
            changed=None,
            only_changes=False,
            is_internal=is_internal,
        )
        all_dep_map = all_dependencies_graph.dep_map
        all_resources = all_dependencies_graph.to_run

    async def process(
        filename: str,
        deps: List[str],
        dep_map: Dict[str, Any],
        to_run: Dict[str, Any],
        workspace_lib_paths: Optional[List[Tuple[str, str]]],
    ):
        name, kind = filename.rsplit(".", 1)
        warnings = []

        try:
            res = await process_file(
                filename,
                tb_client,
                resource_versions=resource_versions,
                skip_connectors=skip_connectors,
                workspace_map=workspace_map,
                workspace_lib_paths=workspace_lib_paths,
                current_ws=current_ws,
            )
        except click.ClickException as e:
            raise e
        except IncludeFileNotFoundException as e:
            raise click.ClickException(FeedbackManager.error_deleted_include(include_file=str(e), filename=filename))
        except Exception as e:
            raise click.ClickException(str(e))

        for r in res:
            fn = r["resource_name"]
            warnings = r.get("warnings", [])
            if changed and fn in changed and (not changed[fn] or changed[fn] in ["shared", "remote"]):
                continue

            if (
                fork_downstream
                and r.get("resource", "") == "pipes"
                and any(["engine" in x.get("params", {}) for x in r.get("nodes", [])])
            ):
                raise click.ClickException(FeedbackManager.error_forkdownstream_pipes_with_engine(pipe=fn))

            to_run[fn] = r
            file_deps = r.get("deps", [])
            deps += file_deps
            # calculate and look for deps
            dep_list = []
            for x in file_deps:
                if x not in INTERNAL_TABLES or is_internal:
                    f, ds = find_file_by_name(dir_path, x, verbose, workspace_lib_paths=workspace_lib_paths, resource=r)
                    if f:
                        dep_list.append(f.rsplit(".", 1)[0])
                    if ds:
                        ds_fn = ds["resource_name"]
                        prev = to_run.get(ds_fn, {})
                        to_run[ds_fn] = deepcopy(r)
                        try:
                            to_run[ds_fn]["deps"] = list(
                                set(to_run[ds_fn].get("deps", []) + prev.get("deps", []) + [fn])
                            )
                        except ValueError:
                            pass
                        embedded_datasources[x] = to_run[ds_fn]
                    else:
                        e_ds = embedded_datasources.get(x, None)
                        if e_ds:
                            dep_list.append(e_ds["resource_name"])

            # In case the datasource is to be shared and we have mapping, let's replace the name
            if "shared_with" in r and workspace_map:
                mapped_workspaces: List[str] = []
                for shared_with in r["shared_with"]:
                    mapped_workspaces.append(
                        workspace_map.get(shared_with)
                        if workspace_map.get(shared_with, None) is not None
                        else shared_with  # type: ignore
                    )
                r["shared_with"] = mapped_workspaces

            dep_map[fn] = set(dep_list)
        return os.path.basename(name), warnings

    processed = set()

    async def get_processed(filenames: Iterable[str]):
        for filename in filenames:
            # just process changed filenames (tb deploy and --only-changes)
            if changed:
                resource = Path(filename).resolve().stem
                if resource in changed and (not changed[resource] or changed[resource] in ["shared", "remote"]):
                    continue
            if os.path.isdir(filename):
                await get_processed(filenames=get_project_filenames(filename))
            else:
                if verbose:
                    click.echo(FeedbackManager.info_processing_file(filename=filename))

                if ".incl" in filename:
                    click.echo(FeedbackManager.warning_skipping_include_file(file=filename))

                name, warnings = await process(filename, deps, dep_map, to_run, workspace_lib_paths)
                processed.add(name)

                if verbose:
                    if len(warnings) == 1:
                        click.echo(FeedbackManager.warning_pipe_restricted_param(word=warnings[0]))
                    elif len(warnings) > 1:
                        click.echo(
                            FeedbackManager.warning_pipe_restricted_params(
                                words=", ".join(["'{}'".format(param) for param in warnings[:-1]]),
                                last_word=warnings[-1],
                            )
                        )

    await get_processed(filenames=filenames)

    if process_dependencies:
        if only_changes:
            for key in dict(to_run):
                # look for deps that are the target data source of a materialized node
                target_datasource = get_target_materialized_data_source_name(to_run[key])
                if target_datasource:
                    # look in all_dep_map items that have as a dependency the target data source and are an endpoint
                    for _key, _deps in all_dep_map.items():
                        for dep in _deps:
                            if (
                                dep == target_datasource
                                or (dep == key and target_datasource not in all_dep_map.get(key, []))
                            ) and is_endpoint_with_no_dependencies(
                                all_resources.get(_key, {}), all_dep_map, all_resources
                            ):
                                dep_map[_key] = _deps
                                to_run[_key] = all_resources.get(_key)
        else:
            while len(deps) > 0:
                dep = deps.pop()
                if dep not in processed:
                    processed.add(dep)
                    f = full_path_by_name(dir_path, dep, workspace_lib_paths)
                    if f:
                        if verbose:
                            try:
                                processed_filename = f.relative_to(os.getcwd())
                            except ValueError:
                                processed_filename = f
                            # This is to avoid processing shared data sources
                            if "vendor/" in str(processed_filename):
                                click.echo(FeedbackManager.info_skipping_resource(resource=processed_filename))
                                continue
                            click.echo(FeedbackManager.info_processing_file(filename=processed_filename))
                        await process(str(f), deps, dep_map, to_run, workspace_lib_paths)

    return GraphDependencies(dep_map, to_run, all_dep_map, all_resources)


async def get_changes_from_main(
    only_changes: bool,
    client: TinyB,
    config: Optional[CLIConfig] = None,
    current_ws: Optional[Dict[str, Any]] = None,
    filenames: Optional[List[str]] = None,
):
    changed = None
    if not only_changes:
        return changed

    if current_ws and not current_ws.get("is_branch"):
        changed = await diff_command(filenames, True, client, no_color=True, with_print=False)
    elif config and config.get("host"):
        workspace = await get_current_main_workspace(config)

        if workspace:
            ws_client = _get_tb_client(workspace["token"], config["host"])
            changed = await diff_command(filenames, True, ws_client, no_color=True, with_print=False)
    return changed


def get_project_filenames(folder: str, with_vendor=False) -> List[str]:
    folders: List[str] = [
        f"{folder}/*.datasource",
        f"{folder}/datasources/*.datasource",
        f"{folder}/*.pipe",
        f"{folder}/pipes/*.pipe",
        f"{folder}/endpoints/*.pipe",
        f"{folder}/*.token",
        f"{folder}/tokens/*.token",
    ]
    if with_vendor:
        folders.append(f"{folder}/vendor/**/**/*.datasource")
    filenames: List[str] = []
    for x in folders:
        filenames += glob.glob(x)
    return filenames


async def name_matches_existing_resource(resource: str, name: str, tb_client: TinyB):
    if resource == "datasources":
        current_pipes: List[Dict[str, Any]] = await tb_client.pipes()
        if name in [x["name"] for x in current_pipes]:
            return True
    else:
        current_datasources: List[Dict[str, Any]] = await tb_client.datasources()
        if name in [x["name"] for x in current_datasources]:
            return True
    return False


def is_new(
    name: str,
    changed: Dict[str, str],
    normal_dependency: Dict[str, Set[str]],
    fork_downstream_dependency: Dict[str, Set[str]],
) -> bool:
    def is_git_new(name: str):
        return changed and changed.get(name) == "A"

    if not is_git_new(name):
        return False

    # if should not depend on a changed resource
    if back_deps := normal_dependency.get(name):
        for dep in back_deps:
            if dep in fork_downstream_dependency and not is_git_new(dep):
                return False

    return True


async def folder_push(
    tb_client: TinyB,
    filenames: Optional[List[str]] = None,
    dry_run: bool = False,
    check: bool = False,
    push_deps: bool = False,
    only_changes: bool = False,
    git_release: bool = False,
    debug: bool = False,
    force: bool = False,
    override_datasource: bool = False,
    folder: str = ".",
    populate: bool = False,
    populate_subset=None,
    populate_condition: Optional[str] = None,
    unlink_on_populate_error: bool = False,
    upload_fixtures: bool = False,
    wait: bool = False,
    ignore_sql_errors: bool = False,
    skip_confirmation: bool = False,
    only_response_times: bool = False,
    workspace_map=None,
    workspace_lib_paths=None,
    no_versions: bool = False,
    run_tests: bool = False,
    as_standard: bool = False,
    raise_on_exists: bool = False,
    verbose: bool = True,
    tests_to_run: int = 0,
    tests_relative_change: float = 0.01,
    tests_sample_by_params: int = 0,
    tests_filter_by: Optional[List[str]] = None,
    tests_failfast: bool = False,
    tests_ignore_order: bool = False,
    tests_validate_processed_bytes: bool = False,
    tests_check_requests_from_branch: bool = False,
    config: Optional[CLIConfig] = None,
    user_token: Optional[str] = None,
    fork_downstream: Optional[bool] = False,
    fork: Optional[bool] = False,
    is_internal: Optional[bool] = False,
    release_created: Optional[bool] = False,
    auto_promote: Optional[bool] = False,
    check_backfill_required: bool = False,
    use_main: bool = False,
    check_outdated: bool = True,
    hide_folders: bool = False,
):
    workspaces: List[Dict[str, Any]] = (await tb_client.user_workspaces_and_branches()).get("workspaces", [])
    current_ws: Dict[str, Any] = next(
        (workspace for workspace in workspaces if config and workspace.get("id", ".") == config.get("id", "..")), {}
    )
    is_branch = current_ws.get("is_branch", False)
    has_semver: bool = release_created or False

    deployment = Deployment(current_ws, git_release, tb_client, dry_run, only_changes)

    if not workspace_map:
        workspace_map = {}
    if not workspace_lib_paths:
        workspace_lib_paths = []

    workspace_lib_paths = list(workspace_lib_paths)
    # include vendor libs without overriding user ones
    existing_workspaces = set(x[1] for x in workspace_lib_paths)
    vendor_path = Path("vendor")
    if vendor_path.exists():
        for x in vendor_path.iterdir():
            if x.is_dir() and x.name not in existing_workspaces:
                workspace_lib_paths.append((x.name, x))

    datasources: List[Dict[str, Any]] = await tb_client.datasources()
    pipes: List[Dict[str, Any]] = await tb_client.pipes(dependencies=True)

    existing_resources: List[str] = [x["name"] for x in datasources] + [x["name"] for x in pipes]
    # replace workspace mapping names
    for old_ws, new_ws in workspace_map.items():
        existing_resources = [re.sub(f"^{old_ws}\.", f"{new_ws}.", x) for x in existing_resources]

    remote_resource_names = [get_remote_resource_name_without_version(x) for x in existing_resources]

    # replace workspace mapping names
    for old_ws, new_ws in workspace_map.items():
        remote_resource_names = [re.sub(f"^{old_ws}\.", f"{new_ws}.", x) for x in remote_resource_names]

    if not filenames:
        filenames = get_project_filenames(folder)

    # get the list of changes
    changed, deleted = await deployment.detect_changes(
        filenames, only_changes, config, use_main, check_outdated=check_outdated
    )

    deployment.check_changes(changed, pipes, release_created)

    deployment.preparing_release()

    # build graph to get new versions for all the files involved in the query
    # dependencies need to be processed always to get the versions
    dependencies_graph = await build_graph(
        filenames,
        tb_client,
        dir_path=folder,
        process_dependencies=True,
        workspace_map=workspace_map,
        skip_connectors=True,
        workspace_lib_paths=workspace_lib_paths,
        current_ws=current_ws,
        changed=changed,
        only_changes=only_changes or (deployment.is_git_release and has_semver),
        fork_downstream=fork_downstream,
        is_internal=is_internal,
    )

    # update existing versions
    if not no_versions:
        resource_versions = get_resource_versions(existing_resources)
        latest_datasource_versions = resource_versions.copy()

        for dep in dependencies_graph.to_run.values():
            ds = dep["resource_name"]
            if dep["version"] is not None:
                latest_datasource_versions[ds] = dep["version"]
    else:
        resource_versions = {}
        latest_datasource_versions = {}

    # If we have datasources using VERSION, let's try to get the latest version
    dependencies_graph = await build_graph(
        filenames,
        tb_client,
        dir_path=folder,
        resource_versions=latest_datasource_versions,
        workspace_map=workspace_map,
        process_dependencies=push_deps,
        verbose=verbose,
        workspace_lib_paths=workspace_lib_paths,
        current_ws=current_ws,
        changed=changed,
        only_changes=only_changes or (deployment.is_git_release and has_semver),
        fork_downstream=fork_downstream,
        is_internal=is_internal,
    )

    if debug:
        pp.pprint(dependencies_graph.to_run)

    if verbose:
        click.echo(FeedbackManager.info_building_dependencies())

    deployment.preparing_dependencies(changed, dependencies_graph.dep_map, latest_datasource_versions)

    def should_push_file(
        name: str,
        remote_resource_names: List[str],
        latest_datasource_versions: Dict[str, Any],
        force: bool,
        run_tests: bool,
    ) -> bool:
        """
        Function to know if we need to run a file or not
        """
        if name not in remote_resource_names:
            return True
        # When we need to try to push a file when it doesn't exist and the version is different that the existing one
        resource_full_name = (
            f"{name}__v{latest_datasource_versions.get(name)}" if name in latest_datasource_versions else name
        )
        if resource_full_name not in existing_resources:
            return True
        if force or run_tests:
            return True
        return False

    async def push(
        name: str,
        to_run: Dict[str, Dict[str, Any]],
        resource_versions: Dict[str, Any],
        latest_datasource_versions: Dict[str, Any],
        dry_run: bool,
        fork_downstream: Optional[bool] = False,
        fork: Optional[bool] = False,
    ):
        if name in to_run:
            resource = to_run[name]["resource"]
            if not dry_run:
                if should_push_file(name, remote_resource_names, latest_datasource_versions, force, run_tests):
                    if name not in resource_versions:
                        version = ""
                        if name in latest_datasource_versions:
                            version = f"(v{latest_datasource_versions[name]})"
                        click.echo(FeedbackManager.info_processing_new_resource(name=name, version=version))
                    else:
                        click.echo(
                            FeedbackManager.info_processing_resource(
                                name=name,
                                version=latest_datasource_versions[name],
                                latest_version=resource_versions.get(name),
                            )
                        )
                    try:
                        await exec_file(
                            to_run[name],
                            tb_client,
                            force,
                            check,
                            debug and verbose,
                            populate,
                            populate_subset,
                            populate_condition,
                            unlink_on_populate_error,
                            wait,
                            user_token,
                            override_datasource,
                            ignore_sql_errors,
                            skip_confirmation,
                            only_response_times,
                            run_tests,
                            as_standard,
                            tests_to_run,
                            tests_relative_change,
                            tests_sample_by_params,
                            tests_filter_by,
                            tests_failfast,
                            tests_ignore_order,
                            tests_validate_processed_bytes,
                            tests_check_requests_from_branch,
                            current_ws,
                            fork_downstream,
                            fork,
                            git_release,
                        )
                        if not run_tests:
                            click.echo(
                                FeedbackManager.success_create(
                                    name=(
                                        name
                                        if to_run[name]["version"] is None
                                        else f'{name}__v{to_run[name]["version"]}'
                                    )
                                )
                            )
                    except Exception as e:
                        filename = (
                            os.path.basename(to_run[name]["filename"]) if hide_folders else to_run[name]["filename"]
                        )
                        exception = FeedbackManager.error_push_file_exception(
                            filename=filename,
                            error=e,
                        )
                        raise click.ClickException(exception)
                else:
                    if raise_on_exists:
                        raise AlreadyExistsException(
                            FeedbackManager.warning_name_already_exists(
                                name=name if to_run[name]["version"] is None else f'{name}__v{to_run[name]["version"]}'
                            )
                        )
                    else:
                        if await name_matches_existing_resource(resource, name, tb_client):
                            if resource == "pipes":
                                click.echo(FeedbackManager.error_pipe_cannot_be_pushed(name=name))
                            else:
                                click.echo(FeedbackManager.error_datasource_cannot_be_pushed(name=name))
                        else:
                            click.echo(
                                FeedbackManager.warning_name_already_exists(
                                    name=(
                                        name
                                        if to_run[name]["version"] is None
                                        else f'{name}__v{to_run[name]["version"]}'
                                    )
                                )
                            )
            else:
                if should_push_file(name, remote_resource_names, latest_datasource_versions, force, run_tests):
                    if name not in resource_versions:
                        version = ""
                        if name in latest_datasource_versions:
                            version = f"(v{latest_datasource_versions[name]})"
                        click.echo(FeedbackManager.info_dry_processing_new_resource(name=name, version=version))
                    else:
                        click.echo(
                            FeedbackManager.info_dry_processing_resource(
                                name=name,
                                version=latest_datasource_versions[name],
                                latest_version=resource_versions.get(name),
                            )
                        )
                else:
                    if await name_matches_existing_resource(resource, name, tb_client):
                        if resource == "pipes":
                            click.echo(FeedbackManager.warning_pipe_cannot_be_pushed(name=name))
                        else:
                            click.echo(FeedbackManager.warning_datasource_cannot_be_pushed(name=name))
                    else:
                        click.echo(FeedbackManager.warning_dry_name_already_exists(name=name))

    async def push_files(
        dependency_graph: GraphDependencies,
        dry_run: bool = False,
        check_backfill_required: bool = False,
    ):
        endpoints_dep_map = dict()
        processed = set()

        dependencies_graph = dependency_graph.dep_map
        resources_to_run = dependency_graph.to_run

        if not fork_downstream:
            # First, we will deploy the all the resources following the dependency graph except for the endpoints
            groups = [group for group in toposort(dependencies_graph)]
            for group in groups:
                for name in group:
                    if name in processed:
                        continue

                    if is_endpoint_with_no_dependencies(
                        resources_to_run.get(name, {}),
                        dependencies_graph,
                        resources_to_run,
                    ):
                        endpoints_dep_map[name] = dependencies_graph[name]
                        continue

                    await push(
                        name,
                        resources_to_run,
                        resource_versions,
                        latest_datasource_versions,
                        dry_run,
                        fork_downstream,
                        fork,
                    )
                    processed.add(name)

            # Then, we will deploy the endpoints that are on the dependency graph
            groups = [group for group in toposort(endpoints_dep_map)]
            for group in groups:
                for name in group:
                    if name not in processed:
                        await push(
                            name,
                            resources_to_run,
                            resource_versions,
                            latest_datasource_versions,
                            dry_run,
                            fork_downstream,
                            fork,
                        )
                        processed.add(name)
        else:
            # This will generate the graph from right to left and will fill the gaps of the dependencies
            # If we have a graph like this:
            # A -> B -> C
            # If we only modify A, the normal dependencies graph will only contain a node like _{A => B}
            # But we need a graph that contains A, B and C and the dependencies between them to deploy them in the right order
            dependencies_graph_fork_downstream, resources_to_run_fork_downstream = generate_forkdownstream_graph(
                dependency_graph.all_dep_map,
                dependency_graph.all_resources,
                resources_to_run,
                list(dependency_graph.dep_map.keys()),
            )

            # First, we will deploy the datasources that need to be deployed.
            # We need to deploy the datasources from left to right as some datasources might have MV that depend on the column types of previous datasources. Ex: `test_change_column_type_landing_datasource` test
            groups = [group for group in toposort(dependencies_graph_fork_downstream)]
            groups.reverse()
            for group in groups:
                for name in group:
                    if name in processed or not is_datasource(resources_to_run_fork_downstream[name]):
                        continue

                    # If the resource is new, we will use the normal resource information to deploy it
                    # This is mostly used for datasources with connections.
                    # At the moment, `resources_to_run_fork_downstream` is generated by `all_resources` and this is generated using the parameter `skip_connectors=True`
                    # TODO: Should the `resources_to_run_fork_downstream` be generated using the `skip_connectors` parameter?
                    if is_new(name, changed, dependencies_graph_fork_downstream, dependencies_graph_fork_downstream):
                        await push(
                            name,
                            resources_to_run,
                            resource_versions,
                            latest_datasource_versions,
                            dry_run,
                            fork_downstream,
                            fork,
                        )
                    else:
                        # If we are trying to modify a Kafka or CDK datasource, we need to inform the user that the resource needs to be post-released
                        kafka_connection_name = (
                            resources_to_run_fork_downstream[name].get("params", {}).get("kafka_connection_name")
                        )
                        service = resources_to_run_fork_downstream[name].get("params", {}).get("import_service")
                        if release_created and (kafka_connection_name or service):
                            connector = "Kafka" if kafka_connection_name else service
                            error_msg = FeedbackManager.error_connector_require_post_release(connector=connector)
                            raise click.ClickException(error_msg)

                        # If we are pushing a modified datasource, inform about the backfill``
                        if check_backfill_required and auto_promote and release_created:
                            error_msg = FeedbackManager.error_check_backfill_required(resource_name=name)
                            raise click.ClickException(error_msg)

                        await push(
                            name,
                            resources_to_run_fork_downstream,
                            resource_versions,
                            latest_datasource_versions,
                            dry_run,
                            fork_downstream,
                            fork,
                        )
                    processed.add(name)

            # Now, we will create a map of all the endpoints and there dependencies
            # We are using the forkdownstream graph to get the dependencies of the endpoints as the normal dependencies graph only contains the resources that are going to be deployed
            # But does not include the missing gaps
            # If we have ENDPOINT_A ----> MV_PIPE_B -----> DATASOURCE_B ------> ENDPOINT_C
            # Where endpoint A is being used in the MV_PIPE_B, if we only modify the endpoint A
            # The dependencies graph will only contain the endpoint A and the MV_PIPE_B, but not the DATASOURCE_B and the ENDPOINT_C
            groups = [group for group in toposort(dependencies_graph_fork_downstream)]
            for group in groups:
                for name in group:
                    if name in processed or not is_endpoint(resources_to_run_fork_downstream[name]):
                        continue

                    endpoints_dep_map[name] = dependencies_graph_fork_downstream[name]

            # Now that we have the dependencies of the endpoints, we need to check that the resources has not been deployed yet and only care about the endpoints that depend on endpoints
            groups = [group for group in toposort(endpoints_dep_map)]

            # As we have used the forkdownstream graph to get the dependencies of the endpoints, we have all the dependencies of the endpoints
            # But we need to deploy the endpoints and the dependencies of the endpoints from left to right
            # So we need to reverse the groups
            groups.reverse()
            for group in groups:
                for name in group:
                    if name in processed or not is_endpoint(resources_to_run_fork_downstream[name]):
                        continue

                    await push(
                        name,
                        resources_to_run_fork_downstream,
                        resource_versions,
                        latest_datasource_versions,
                        dry_run,
                        fork_downstream,
                        fork,
                    )
                    processed.add(name)

            # Now we should have the endpoints and datasources deployed, we can deploy the rest of the pipes (copy & sinks)
            # We need to rely on the forkdownstream graph as it contains all the modified pipes as well as the dependencies of the pipes
            # In this case, we don't need to generate a new graph as we did for the endpoints as the pipes are not going to be used as dependencies and the datasources are already deployed
            groups = [group for group in toposort(dependencies_graph_fork_downstream)]
            for group in groups:
                for name in group:
                    if name in processed or is_materialized(resources_to_run_fork_downstream.get(name)):
                        continue

                    await push(
                        name,
                        resources_to_run_fork_downstream,
                        resource_versions,
                        latest_datasource_versions,
                        dry_run,
                        fork_downstream,
                        fork,
                    )
                    processed.add(name)

            # Finally, we need to deploy the materialized views from right to left.
            # We need to rely on the forkdownstream graph as it contains all the modified materialized views as well as the dependencies of the materialized views
            # In this case, we don't need to generate a new graph as we did for the endpoints as the pipes are not going to be used as dependencies and the datasources are already deployed
            groups = [group for group in toposort(dependencies_graph_fork_downstream)]
            for group in groups:
                for name in group:
                    if name in processed or not is_materialized(resources_to_run_fork_downstream.get(name)):
                        continue

                    await push(
                        name,
                        resources_to_run_fork_downstream,
                        resource_versions,
                        latest_datasource_versions,
                        dry_run,
                        fork_downstream,
                        fork,
                    )
                    processed.add(name)

    if deployment.is_git_release:
        deployment.deploying_dry_run()
        await push_files(dependencies_graph, dry_run=True, check_backfill_required=check_backfill_required)

        await deployment.delete_resources(deleted, pipes, dry_run=True)
        if not deployment.dry_run:
            deployment.deploying()
            await push_files(dependencies_graph, dry_run)
            await deployment.delete_resources(deleted, pipes)
    else:
        await push_files(dependencies_graph, dry_run)

    if not dry_run and not run_tests:
        if upload_fixtures:
            click.echo(FeedbackManager.info_pushing_fixtures())

            # We need to upload the fixtures even if there is no change
            if is_branch:
                filenames = get_project_filenames(folder, with_vendor=True)
                dependencies_graph = await build_graph(
                    filenames,
                    tb_client,
                    dir_path=folder,
                    resource_versions=latest_datasource_versions,
                    workspace_map=workspace_map,
                    process_dependencies=push_deps,
                    verbose=verbose,
                    workspace_lib_paths=workspace_lib_paths,
                    current_ws=current_ws,
                )

            processed = set()
            for group in toposort(dependencies_graph.dep_map):
                for f in group:
                    name = os.path.basename(f)
                    if name not in processed and name in dependencies_graph.to_run:
                        await check_fixtures_data(
                            tb_client,
                            dependencies_graph.to_run[name],
                            debug,
                            folder,
                            force,
                            mode="append" if is_branch else "replace",
                        )
                        processed.add(name)
            for f in dependencies_graph.to_run:
                if f not in processed:
                    await check_fixtures_data(
                        tb_client,
                        dependencies_graph.to_run[f],
                        debug,
                        folder,
                        force,
                        mode="append" if is_branch else "replace",
                    )
        else:
            if verbose:
                click.echo(FeedbackManager.info_not_pushing_fixtures())

    await deployment.update_release(has_semver=has_semver, release_created=release_created)

    return dependencies_graph.to_run


async def check_fixtures_data(
    client: TinyB, resource: Dict[str, Any], debug: bool, folder: str = "", force: bool = False, mode: str = "replace"
):
    if debug:
        click.echo(FeedbackManager.info_checking_file(file=pp.pformat(resource)))
    if resource["resource"] in ["pipes", "tokens"]:
        pass
    elif resource["resource"] == "datasources":
        datasource_name = resource["params"]["name"]
        name = os.path.basename(resource["filename"]).rsplit(".", 1)[0]
        fixture_path = Path(folder) / "fixtures" / f"{name}.csv"

        if not fixture_path.exists():
            fixture_path = Path(folder) / "datasources" / "fixtures" / f"{name}.csv"
        if not fixture_path.exists():
            fixture_path = Path(folder) / "datasources" / "fixtures" / f"{name}.ndjson"
        if not fixture_path.exists():
            fixture_path = Path(folder) / "datasources" / "fixtures" / f"{name}.parquet"
        if fixture_path.exists():
            # Let's validate only when when we are going to replace the actual data
            result = await client.query(sql=f"SELECT count() as c FROM {datasource_name} FORMAT JSON")
            count = result["data"][0]["c"]

            if count > 0 and not force:
                raise click.ClickException(
                    FeedbackManager.error_push_fixture_will_replace_data(datasource=datasource_name)
                )

            click.echo(
                FeedbackManager.info_checking_file_size(
                    filename=resource["filename"], size=sizeof_fmt(os.stat(fixture_path).st_size)
                )
            )
            sys.stdout.flush()
            try:
                await client.datasource_append_data(
                    datasource_name=resource["params"]["name"],
                    file=fixture_path,
                    mode=mode,
                    format=fixture_path.suffix[1:],
                )
                click.echo(FeedbackManager.success_processing_data())
            except Exception as e:
                raise click.ClickException(FeedbackManager.error_processing_blocks(error=e))

        else:
            click.echo(FeedbackManager.warning_fixture_not_found(datasource_name=name))
    else:
        raise click.ClickException(FeedbackManager.error_unknown_resource(resource=resource["resource"]))


DATAFILE_NEW_LINE = "\n"
DATAFILE_INDENT = " " * 4


def format_schema(file_parts: List[str], node: Dict[str, Any]) -> List[str]:
    if node.get("schema"):
        file_parts.append("SCHEMA >")
        file_parts.append(DATAFILE_NEW_LINE)
        columns = schema_to_sql_columns(node["columns"])
        file_parts.append(f",{DATAFILE_NEW_LINE}".join(map(lambda x: f"    {x}", columns)))
        file_parts.append(DATAFILE_NEW_LINE)
        file_parts.append(DATAFILE_NEW_LINE)

    return file_parts


def format_indices(file_parts: List[str], node: Dict[str, Any]) -> List[str]:
    if node.get("indexes"):
        indexes = node["indexes"]
        file_parts.append("INDEXES >")
        file_parts.append(DATAFILE_NEW_LINE)
        file_parts.append(f"{DATAFILE_NEW_LINE}".join(map(lambda index: f"    {index.to_datafile()}", indexes)))
        file_parts.append(DATAFILE_NEW_LINE)
        file_parts.append(DATAFILE_NEW_LINE)

    return file_parts


def format_data_connector(file_parts: List[str], node: Dict[str, Any]) -> List[str]:
    ll = len(file_parts)
    quotes = "''"
    [file_parts.append(f"{k.upper()} {v or quotes}{DATAFILE_NEW_LINE}") for k, v in node.items() if "kafka" in k]  # type: ignore
    if ll < len(file_parts):
        file_parts.append(DATAFILE_NEW_LINE)
    return file_parts


def format_import_settings(file_parts: List[str], node: Dict[str, Any]) -> List[str]:
    ll = len(file_parts)
    [file_parts.append(f"{k.upper()} {v}{DATAFILE_NEW_LINE}") for k, v in node.items() if "import_" in k]  # type: ignore
    if ll < len(file_parts):
        file_parts.append(DATAFILE_NEW_LINE)
    return file_parts


def format_include(file_parts: List[str], doc: Datafile, unroll_includes: bool = False) -> List[str]:
    if unroll_includes:
        return file_parts

    assert doc.raw is not None

    include = [line for line in doc.raw if "INCLUDE" in line and ".incl" in line]
    if len(include):
        file_parts.append(include[0])
        file_parts.append(DATAFILE_NEW_LINE)
    return file_parts


async def format_datasource(
    filename: str,
    unroll_includes: bool = False,
    for_diff: bool = False,
    client: Optional[TinyB] = None,
    replace_includes: bool = False,
    datafile: Optional[Datafile] = None,
    for_deploy_diff: bool = False,
    skip_eval: bool = False,
    content: Optional[str] = None,
) -> str:
    if datafile:
        doc = datafile
    else:
        doc = parse_datasource(filename, replace_includes=replace_includes, skip_eval=skip_eval, content=content)

    file_parts: List[str] = []
    if for_diff:
        is_kafka = "kafka_connection_name" in doc.nodes[0]
        if is_kafka:
            kafka_metadata_columns = [
                "__value",
                "__headers",
                "__topic",
                "__partition",
                "__offset",
                "__timestamp",
                "__key",
            ]
            columns = [c for c in doc.nodes[0]["columns"] if c["name"] not in kafka_metadata_columns]
            doc.nodes[0].update(
                {
                    "columns": columns,
                }
            )
        format_version(file_parts, doc)
        if for_deploy_diff:
            format_description(file_parts, doc)
        format_tags(file_parts, doc)
        format_schema(file_parts, doc.nodes[0])
        format_indices(file_parts, doc.nodes[0])
        await format_engine(file_parts, doc.nodes[0], only_ttl=bool(not for_deploy_diff), client=client)
        if for_deploy_diff:
            format_import_settings(file_parts, doc.nodes[0])
        format_shared_with(file_parts, doc)
    else:
        format_sources(file_parts, doc)
        format_maintainer(file_parts, doc)
        format_version(file_parts, doc)
        format_description(file_parts, doc)
        format_tokens(file_parts, doc)
        format_tags(file_parts, doc)
        format_schema(file_parts, doc.nodes[0])
        format_indices(file_parts, doc.nodes[0])
        await format_engine(file_parts, doc.nodes[0])
        format_include(file_parts, doc, unroll_includes=unroll_includes)
        format_data_connector(file_parts, doc.nodes[0])
        format_import_settings(file_parts, doc.nodes[0])
        format_shared_with(file_parts, doc)
    result = "".join(file_parts)
    result = result.rstrip("\n") + "\n"
    return result


def format_version(file_parts: List[str], doc: Datafile) -> List[str]:
    version = doc.version if doc.version is not None else ""
    if version != "":
        file_parts.append(f"VERSION {version}")
        file_parts.append(DATAFILE_NEW_LINE)
        file_parts.append(DATAFILE_NEW_LINE)
    return file_parts


def format_maintainer(file_parts: List[str], doc: Datafile) -> List[str]:
    maintainer = doc.maintainer if doc.maintainer is not None else ""
    if maintainer:
        file_parts.append(f"MAINTAINER {maintainer}")
        file_parts.append(DATAFILE_NEW_LINE)
        file_parts.append(DATAFILE_NEW_LINE)
    return file_parts


def format_sources(file_parts: List[str], doc: Datafile) -> List[str]:
    for source in doc.sources:
        file_parts.append(f"SOURCE {source}")
        file_parts.append(DATAFILE_NEW_LINE)
    return file_parts


def format_description(file_parts: List[str], doc: Any) -> List[str]:
    description = doc.description if doc.description is not None else ""
    if description:
        file_parts.append("DESCRIPTION >")
        file_parts.append(DATAFILE_NEW_LINE)
        [
            file_parts.append(f"{DATAFILE_INDENT}{d.strip()}\n")  # type: ignore
            for d in description.split(DATAFILE_NEW_LINE)
            if d.strip()
        ]
        file_parts.append(DATAFILE_NEW_LINE)
    return file_parts


def format_tokens(file_parts: List[str], doc: Datafile) -> List[str]:
    for token in doc.tokens:
        file_parts.append(f'TOKEN "{token["token_name"]}" {token["permissions"]}')
        file_parts.append(DATAFILE_NEW_LINE)
    if len(doc.tokens):
        file_parts.append(DATAFILE_NEW_LINE)
    return file_parts


def format_node_sql(
    file_parts: List[str], node: Dict[str, Any], line_length: Optional[int] = None, lower_keywords: bool = False
) -> List[str]:
    file_parts.append("SQL >")
    file_parts.append(DATAFILE_NEW_LINE)
    file_parts.append(format_sql(node["sql"], DATAFILE_INDENT, line_length=line_length, lower_keywords=lower_keywords))
    file_parts.append(DATAFILE_NEW_LINE)
    file_parts.append(DATAFILE_NEW_LINE)
    return file_parts


def format_shared_with(file_parts: List[str], doc: Datafile) -> List[str]:
    if doc.shared_with:
        file_parts.append("SHARED_WITH >")
        file_parts.append(DATAFILE_NEW_LINE)
        file_parts.append("\n".join([f"{DATAFILE_INDENT}{workspace_name}" for workspace_name in doc.shared_with]))
    return file_parts


def format_tags(file_parts: List[str], doc: Datafile) -> List[str]:
    if doc.filtering_tags:
        file_parts.append(f'TAGS "{", ".join(doc.filtering_tags)}"')
        file_parts.append(DATAFILE_NEW_LINE)
        file_parts.append(DATAFILE_NEW_LINE)
    return file_parts


async def format_engine(
    file_parts: List[str], node: Dict[str, Any], only_ttl: bool = False, client: Optional[TinyB] = None
) -> List[str]:
    if only_ttl:
        if node.get("engine", None):
            for arg in sorted(node["engine"].get("args", [])):
                if arg[0].upper() == "TTL":
                    elem = ", ".join([x.strip() for x in arg[1].split(",")])
                    try:
                        if client:
                            ttl_sql = await client.sql_get_format(f"select {elem}", with_clickhouse_format=True)
                            formatted_ttl = ttl_sql[7:]
                        else:
                            formatted_ttl = elem
                    except Exception:
                        formatted_ttl = elem
                    file_parts.append(f"ENGINE_{arg[0].upper()} {formatted_ttl}")
                    file_parts.append(DATAFILE_NEW_LINE)
            file_parts.append(DATAFILE_NEW_LINE)
        return file_parts
    else:
        if node.get("engine", None):
            empty = '""'
            file_parts.append(f'ENGINE {node["engine"]["type"]}' if node.get("engine", {}).get("type") else empty)
            file_parts.append(DATAFILE_NEW_LINE)
            for arg in sorted(node["engine"].get("args", [])):
                elem = ", ".join([x.strip() for x in arg[1].split(",")])
                file_parts.append(f"ENGINE_{arg[0].upper()} {elem if elem else empty}")
                file_parts.append(DATAFILE_NEW_LINE)
            file_parts.append(DATAFILE_NEW_LINE)
        return file_parts


async def format_node_type(file_parts: List[str], node: Dict[str, Any]) -> List[str]:
    node_type = node.get("type", "").lower()
    node_type_upper = f"TYPE {node_type.upper()}"
    # Materialized pipe
    if node_type == PipeNodeTypes.MATERIALIZED:
        file_parts.append(node_type_upper)
        file_parts.append(DATAFILE_NEW_LINE)
        file_parts.append(f'DATASOURCE {node["datasource"]}')
        file_parts.append(DATAFILE_NEW_LINE)
        await format_engine(file_parts, node)

    # Copy pipe
    if node_type == PipeNodeTypes.COPY:
        file_parts.append(node_type_upper)
        file_parts.append(DATAFILE_NEW_LINE)
        file_parts.append(f'TARGET_DATASOURCE {node["target_datasource"]}')
        if node.get("mode"):
            file_parts.append(DATAFILE_NEW_LINE)
            file_parts.append(f'COPY_MODE {node.get("mode")}')

        if node.get(CopyParameters.COPY_SCHEDULE):
            is_ondemand = node[CopyParameters.COPY_SCHEDULE].lower() == ON_DEMAND
            file_parts.append(DATAFILE_NEW_LINE)
            file_parts.append(
                f"{CopyParameters.COPY_SCHEDULE.upper()} {ON_DEMAND if is_ondemand else node[CopyParameters.COPY_SCHEDULE]}"
            )
        else:
            file_parts.append(DATAFILE_NEW_LINE)
            file_parts.append(f"{CopyParameters.COPY_SCHEDULE.upper()} {ON_DEMAND}")
        file_parts.append(DATAFILE_NEW_LINE)

    # Sink or Stream pipe
    if ExportReplacements.is_export_node(node):
        file_parts.append(node_type_upper)
        export_params = ExportReplacements.get_params_from_datafile(node)
        file_parts.append(DATAFILE_NEW_LINE)
        for param, value in export_params.items():
            if param == "schedule_cron" and not value:
                value = ON_DEMAND
            datafile_key = ExportReplacements.get_datafile_key(param, node)
            if datafile_key and value:
                file_parts.append(f"{datafile_key} {value}")
                file_parts.append(DATAFILE_NEW_LINE)

    return file_parts


def format_pipe_include(file_parts: List[str], node: Dict[str, Any], includes: Dict[str, Any]) -> List[str]:
    if includes:
        for k, v in includes.copy().items():
            if node["name"] in v:
                file_parts.append(f"INCLUDE {k}")
                file_parts.append(DATAFILE_NEW_LINE)
                file_parts.append(DATAFILE_NEW_LINE)
                del includes[k]
    return file_parts


async def format_node(
    file_parts: List[str],
    node: Dict[str, Any],
    includes: Dict[str, Any],
    line_length: Optional[int] = None,
    unroll_includes: bool = False,
    lower_keywords: bool = False,
) -> None:
    if not unroll_includes:
        format_pipe_include(file_parts, node, includes)
    item = [k for k, _ in includes.items() if node["name"].strip() in k]
    if item and not unroll_includes:
        return

    file_parts.append(f'NODE {node["name"].strip()}')
    file_parts.append(DATAFILE_NEW_LINE)

    from collections import namedtuple

    Doc = namedtuple("Doc", ["description"])
    format_description(file_parts, Doc(node.get("description", "")))
    format_node_sql(file_parts, node, line_length=line_length, lower_keywords=lower_keywords)
    await format_node_type(file_parts, node)


async def format_pipe(
    filename: str,
    line_length: Optional[int] = DEFAULT_FMT_LINE_LENGTH,
    unroll_includes: bool = False,
    replace_includes: bool = False,
    datafile: Optional[Datafile] = None,
    for_deploy_diff: bool = False,
    skip_eval: bool = False,
    content: Optional[str] = None,
) -> str:
    if datafile:
        doc = datafile
    else:
        doc = parse_pipe(filename, replace_includes=replace_includes, skip_eval=skip_eval, content=content)

    file_parts: List[str] = []
    format_sources(file_parts, doc)
    format_maintainer(file_parts, doc)
    format_version(file_parts, doc)
    format_description(file_parts, doc)
    format_tokens(file_parts, doc)
    format_tags(file_parts, doc)
    if doc.includes and not unroll_includes:
        for k in doc.includes:
            # We filter only the include files as we currently have 2 items for each include
            # { 'include_file.incl': 'First node of the include" }
            # { 'first node of the pipe after the include': }
            if ".incl" not in k:
                continue

            # We get all the nodes inside the include and remove them from the unrolled pipe as we want things unrolled
            include_parameters = _unquote(k)

            # If they use an include with parameters like `INCLUDE "xxx.incl" "GROUP_COL=path" "MATERIALIZED_VIEW=speed_insights_path_daily_mv"``
            # We just want the file name to take nodes
            include_file = include_parameters.split('"')[0]
            include_file = (
                Path(os.path.dirname(filename)) / eval_var(include_file)
                if "." in include_file
                else eval_var(include_file)
            )
            included_pipe = parse_pipe(include_file, skip_eval=skip_eval)
            pipe_nodes = doc.nodes.copy()
            for included_node in included_pipe.nodes.copy():
                unrolled_included_node = next(
                    (node for node in pipe_nodes if node["name"] == included_node["name"]), None
                )
                if unrolled_included_node:
                    doc.nodes.remove(unrolled_included_node)
    for node in doc.nodes:
        await format_node(
            file_parts,
            node,
            doc.includes,
            line_length=line_length,
            unroll_includes=unroll_includes,
            lower_keywords=bool(for_deploy_diff),
        )

    if not unroll_includes:
        for k, _ in doc.includes.items():
            if ".incl" not in k:
                continue
            file_parts.append(f"INCLUDE {k}")
            file_parts.append(DATAFILE_NEW_LINE)
            file_parts.append(DATAFILE_NEW_LINE)

    result = "".join(file_parts)
    result = result.rstrip("\n") + "\n"
    return result


def format_sql(sql: str, DATAFILE_INDENT: str, line_length: Optional[int] = None, lower_keywords: bool = False) -> str:
    sql = format_sql_template(sql.strip(), line_length=line_length, lower_keywords=lower_keywords)
    return "\n".join([f"{DATAFILE_INDENT}{part}" for part in sql.split("\n") if len(part.strip())])


async def _gather_with_concurrency(n, *tasks):
    semaphore = Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await gather(*(sem_task(task) for task in tasks))


async def folder_pull(
    client: TinyB,
    folder: str,
    auto: bool,
    match: Optional[str],
    force: bool,
    verbose: bool = True,
    progress_bar: bool = False,
    fmt: bool = False,
):
    pattern = re.compile(match) if match else None

    def _get_latest_versions(resources: List[str]):
        versions: Dict[str, Any] = {}

        for x in resources:
            t = get_name_version(x)
            t["original_name"] = x
            if t["version"] is None:
                t["version"] = -1
            name = t["name"]

            if name not in versions or name == x or versions[name]["version"] < t["version"]:
                versions[name] = t
        return versions

    def get_file_folder(extension: str):
        if not auto:
            return None
        if extension == "datasource":
            return "datasources"
        if extension == "pipe":
            return "pipes"
        if extension == "token":
            return "tokens"
        return None

    async def write_files(
        versions: Dict[str, Any],
        resources: List[str],
        extension: str,
        get_resource_function: str,
        progress_bar: bool = False,
        fmt: bool = False,
    ):
        async def write_resource(k: Dict[str, Any]):
            name = f"{k['name']}.{extension}"
            try:
                if pattern and not pattern.search(name):
                    if verbose:
                        click.echo(FeedbackManager.info_skipping_resource(resource=name))
                    return

                resource = await getattr(client, get_resource_function)(k["original_name"])
                resource_to_write = resource

                if fmt:
                    if extension == "datasource":
                        resource_to_write = await format_datasource(name, content=resource)
                    elif extension == "pipe":
                        resource_to_write = await format_pipe(name, content=resource)

                dest_folder = folder
                if "." in k["name"] and extension != "token":
                    dest_folder = Path(folder) / "vendor" / k["name"].split(".", 1)[0]
                    name = f"{k['name'].split('.', 1)[1]}.{extension}"

                file_folder = get_file_folder(extension)
                f = Path(dest_folder) / file_folder if file_folder is not None else Path(dest_folder)

                if not f.exists():
                    f.mkdir(parents=True)

                f = f / name
                resource_names = [x.split(".")[-1] for x in resources]

                if verbose:
                    click.echo(FeedbackManager.info_writing_resource(resource=f))
                if not f.exists() or force:
                    async with aiofiles.open(f, "w") as fd:
                        # versions are a client only thing so
                        # datafiles from the server do not contains information about versions
                        if k["version"] >= 0:
                            if fmt:
                                resource_to_write = "\n" + resource_to_write  # fmt strips the first line

                            resource_to_write = f"VERSION {k['version']}\n" + resource_to_write
                        if resource_to_write:
                            matches = re.findall(r"([^\s\.]*__v\d+)", resource_to_write)
                            for match in set(matches):
                                m = match.split("__v")[0]
                                if m in resources or m in resource_names:
                                    resource_to_write = resource_to_write.replace(match, m)
                            await fd.write(resource_to_write)
                else:
                    if verbose:
                        click.echo(FeedbackManager.info_skip_already_exists())
            except Exception as e:
                raise click.ClickException(FeedbackManager.error_exception(error=e))

        values = versions.values()

        if progress_bar:
            with click.progressbar(values, label=f"Pulling {extension}s") as values:  # type: ignore
                for k in values:
                    await write_resource(k)
        else:
            tasks = [write_resource(k) for k in values]
            await _gather_with_concurrency(5, *tasks)

    try:
        datasources = await client.datasources()
        remote_datasources = sorted([x["name"] for x in datasources])
        datasources_versions = _get_latest_versions(remote_datasources)

        pipes = await client.pipes()
        remote_pipes = sorted([x["name"] for x in pipes])
        pipes_versions = _get_latest_versions(remote_pipes)

        resources = list(datasources_versions.keys()) + list(pipes_versions.keys())

        await write_files(
            datasources_versions, resources, "datasource", "datasource_file", progress_bar=progress_bar, fmt=fmt
        )
        await write_files(pipes_versions, resources, "pipe", "pipe_file", progress_bar=progress_bar, fmt=fmt)

        return

    except AuthNoTokenException:
        raise
    except Exception as e:
        raise click.ClickException(FeedbackManager.error_pull(error=str(e)))


def color_diff(diff: Iterable[str]) -> Generator[str, Any, None]:
    for line in diff:
        if line.startswith("+"):
            yield Fore.GREEN + line + Fore.RESET
        elif line.startswith("-"):
            yield Fore.RED + line + Fore.RESET
        elif line.startswith("^"):
            yield Fore.BLUE + line + Fore.RESET
        else:
            yield line


def peek(iterable):
    try:
        first = next(iterable)
    except Exception:
        return None, None
    return first, itertools.chain([first], iterable)


async def diff_command(
    filenames: Optional[List[str]],
    fmt: bool,
    client: TinyB,
    no_color: Optional[bool] = False,
    with_print: Optional[bool] = True,
    verbose: Optional[bool] = None,
    clean_up: Optional[bool] = False,
    progress_bar: bool = False,
    for_deploy: bool = False,
):
    def is_shared_datasource(name):
        return "." in name

    with_explicit_filenames = filenames
    verbose = True if verbose is None else verbose

    target_dir = getcwd() + os.path.sep + ".diff_tmp"
    Path(target_dir).mkdir(parents=True, exist_ok=True)

    if filenames:
        if len(filenames) == 1:
            filenames = [filenames[0], *get_project_filenames(filenames[0])]
        await folder_pull(client, target_dir, False, None, True, verbose=False)
    else:
        filenames = get_project_filenames(".")
        if verbose:
            click.echo("Saving remote resources in .diff_tmp folder.\n")
        await folder_pull(client, target_dir, False, None, True, verbose=verbose, progress_bar=progress_bar)

    remote_datasources: List[Dict[str, Any]] = await client.datasources()
    remote_pipes: List[Dict[str, Any]] = await client.pipes()

    local_resources = {
        Path(file).resolve().stem: file
        for file in filenames
        if (".datasource" in file or ".pipe" in file) and ".incl" not in file
    }

    changed = {}
    for resource in remote_datasources + remote_pipes:
        properties: Dict[str, Any] = get_name_version(resource["name"])
        name = properties.get("name", None)
        if name:
            (rfilename, file) = next(
                ((rfilename, file) for (rfilename, file) in local_resources.items() if name == rfilename),
                ("", None),
            )
            if not file:
                if not with_explicit_filenames:
                    if with_print:
                        click.echo(f"{resource['name']} only exists remotely\n")
                    if is_shared_datasource(resource["name"]):
                        changed[resource["name"]] = "shared"
                    else:
                        changed[resource["name"]] = "remote"
                continue

            suffix = ".datasource" if ".datasource" in file else ".pipe"
            target = target_dir + os.path.sep + rfilename + suffix

            diff_lines = await diff_files(
                target, file, with_format=fmt, with_color=(not no_color), client=client, for_deploy=for_deploy
            )
            not_empty, diff_lines = peek(diff_lines)
            changed[rfilename] = not_empty
            if not_empty and with_print:
                sys.stdout.writelines(diff_lines)
                click.echo("")

    for rfilename, _ in local_resources.items():
        if rfilename not in changed:
            for resource in remote_datasources + remote_pipes:
                properties = get_name_version(resource["name"])
                name = properties.get("name", None)
                if name and name == rfilename:
                    break

                if with_print and rfilename not in changed:
                    click.echo(f"{rfilename} only exists locally\n")
                changed[rfilename] = "local"
    if clean_up:
        shutil.rmtree(target_dir)

    return changed


async def diff_files(
    from_file: str,
    to_file: str,
    from_file_suffix: str = "[remote]",
    to_file_suffix: str = "[local]",
    with_format: bool = True,
    with_color: bool = False,
    client: Optional[TinyB] = None,
    for_deploy: bool = False,
):
    def file_lines(filename):
        with open(filename) as file:
            return file.readlines()

    async def parse(filename, with_format=True, unroll_includes=False):
        extensions = Path(filename).suffixes
        lines = None
        if is_file_a_datasource(filename):
            lines = (
                await format_datasource(
                    filename,
                    unroll_includes=unroll_includes,
                    for_diff=True,
                    client=client,
                    replace_includes=True,
                    for_deploy_diff=for_deploy,
                )
                if with_format
                else file_lines(filename)
            )
        elif (".pipe" in extensions) or (".incl" in extensions):
            lines = (
                await format_pipe(
                    filename,
                    DEFAULT_FMT_LINE_LENGTH,
                    unroll_includes=unroll_includes,
                    replace_includes=True,
                    for_deploy_diff=for_deploy,
                )
                if with_format
                else file_lines(filename)
            )
        else:
            click.echo(f"Unsupported file type: {filename}")
        if lines:
            return [f"{l}\n" for l in lines.split("\n")] if with_format else lines  # noqa: E741

    try:
        lines1 = await parse(from_file, with_format)
        lines2 = await parse(to_file, with_format, unroll_includes=True)
    except FileNotFoundError as e:
        filename = os.path.basename(str(e)).strip("'")
        raise click.ClickException(FeedbackManager.error_diff_file(filename=filename))

    if not lines1 or not lines2:
        return

    diff = difflib.unified_diff(
        lines1, lines2, fromfile=f"{Path(from_file).name} {from_file_suffix}", tofile=f"{to_file} {to_file_suffix}"
    )

    if with_color:
        diff = color_diff(diff)

    return diff


def is_endpoint(resource: Optional[Dict[str, Any]]) -> bool:
    if resource and len(resource.get("tokens", [])) != 0 and resource.get("resource") == "pipes":
        return True
    return False


def is_materialized(resource: Optional[Dict[str, Any]]) -> bool:
    if not resource:
        return False

    is_materialized = any(
        [node.get("params", {}).get("type", None) == "materialized" for node in resource.get("nodes", []) or []]
    )
    return is_materialized


def is_endpoint_with_no_dependencies(
    resource: Dict[str, Any], dep_map: Dict[str, Set[str]], to_run: Dict[str, Dict[str, Any]]
) -> bool:
    if not resource or resource.get("resource") == "datasources":
        return False

    for node in resource.get("nodes", []):
        # FIXME: https://gitlab.com/tinybird/analytics/-/issues/2391
        if node.get("params", {}).get("type", "").lower() in [
            PipeNodeTypes.MATERIALIZED,
            PipeNodeTypes.COPY,
            PipeNodeTypes.DATA_SINK,
            PipeNodeTypes.STREAM,
        ]:
            return False

    for key, values in dep_map.items():
        if resource["resource_name"] in values:
            r = to_run.get(key, None)
            if not r:
                continue
            return False

    deps = dep_map.get(resource["resource_name"])
    if not deps:
        return True

    for dep in deps:
        r = to_run.get(dep, None)
        if is_endpoint(r) or is_materialized(r):
            return False

    return True


def get_target_materialized_data_source_name(resource: Optional[Dict[str, Any]]) -> Optional[str]:
    if not resource:
        return None

    for node in resource.get("nodes", []):
        # FIXME: https://gitlab.com/tinybird/analytics/-/issues/2391
        if node.get("params", {}).get("type", "").lower() == PipeNodeTypes.MATERIALIZED:
            return node.get("params")["datasource"].split("__v")[0]

    return None


def is_datasource(resource: Optional[Dict[str, Any]]) -> bool:
    if resource and resource.get("resource") == "datasources":
        return True
    return False


async def create_release(
    client: TinyB, config: Union[Dict[str, Any], CLIConfig], semver: str, folder: Optional[str] = None
) -> None:
    if not folder:
        folder = getcwd()
    cli_git_release = None
    try:
        cli_git_release = CLIGitRelease(path=folder)
        commit = cli_git_release.head_commit()
    except CLIGitReleaseException:
        raise CLIGitReleaseException(FeedbackManager.error_no_git_repo_for_init(repo_path=folder))

    await client.release_new(config["id"], semver, commit)
    click.echo(FeedbackManager.success_deployment_release(semver=semver))


def has_internal_datafiles(folder: str) -> bool:
    folder = folder or "."
    filenames = get_project_filenames(folder)
    return any([f for f in filenames if "spans" in str(f) and "vendor" not in str(f)])


def is_file_a_datasource(filename: str) -> bool:
    extensions = Path(filename).suffixes
    if ".datasource" in extensions:  # Accepts '.datasource' and '.datasource.incl'
        return True

    if ".incl" in extensions:
        lines = []
        with open(filename) as file:
            lines = file.readlines()

        for line in lines:
            trimmed_line = line.strip().lower()
            if trimmed_line.startswith("schema") or trimmed_line.startswith("engine"):
                return True

    return False
