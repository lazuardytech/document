from dataclasses import dataclass
from enum import Enum
from typing import Dict, NamedTuple, Optional

from tinybird.tb_cli_modules.cicd import (
    APPEND_FIXTURES_SH,
    DEFAULT_REQUIREMENTS_FILE,
    EXEC_TEST_SH,
    GITHUB_CD_YML,
    GITHUB_CI_YML,
    GITHUB_RELEASES_YML,
    WORKFLOW_VERSION,
)


class GitProviders(Enum):
    GITHUB = "github"


class GitHubResource(NamedTuple):
    resource_id: str
    resource_name: str
    resource_type: str
    path: str
    sha: str
    mode: str = "100644"
    type: str = "blob"
    origin: Optional[str] = ""


class GitHubSettingsStatus(Enum):
    IDLE = "idle"  # Nothing yet
    LINKED = "linked"  # Connection flow has started
    SYNCED = "synced"  # Repository is synced
    SYNCING = "syncing"  # Repository is syncing
    UNLINKED = "unlinked"  # Connection has been removed
    CLI = "cli"  # Connection has been made through the CLI


@dataclass
class GitHubSettings:
    provider: Optional[str] = ""
    owner: Optional[str] = ""
    owner_type: Optional[str] = ""
    remote: Optional[str] = ""
    access_token: Optional[str] = ""
    branch: Optional[str] = ""
    project_path: Optional[str] = ""
    name: Optional[str] = ""
    status: Optional[str] = GitHubSettingsStatus.IDLE.value


AVAILABLE_GIT_PROVIDERS = [GitProviders.GITHUB.value]

DEFAULT_BRANCH = "main"

CI_WORKFLOW_VERSION = WORKFLOW_VERSION

DEFAULT_TINYENV_FILE = """
VERSION=0.0.0



##########
# OPTIONAL env vars

# Don't print CLI version warning message if there's a new available version
# TB_VERSION_WARNING=0

# Skip regression tests
# TB_SKIP_REGRESSION=0

# Use `OBFUSCATE_REGEX_PATTERN` and `OBFUSCATE_PATTERN_SEPARATOR` environment variables to define a regex pattern and a separator (in case of a single string with multiple regex) to obfuscate secrets in the CLI output.
# OBFUSCATE_REGEX_PATTERN="https://(www\.)?[^/]+||^Follow these instructions =>"
# OBFUSCATE_PATTERN_SEPARATOR=||
##########
"""

GITHUB_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

GITHUB_DEFAULT_EMPTY_TREE_HASH = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"

DEFAULT_INIT_FILES = {
    "requirements.txt": DEFAULT_REQUIREMENTS_FILE,
    ".tinyenv": DEFAULT_TINYENV_FILE,
    "scripts/exec_test.sh": EXEC_TEST_SH,
    "scripts/append_fixtures.sh": APPEND_FIXTURES_SH,
    "datasources/.gitkeep": "",
    "datasources/fixtures/.gitkeep": "",
    "pipes/.gitkeep": "",
    "endpoints/.gitkeep": "",
    "deploy/.gitkeep": "",
    "tests/.gitkeep": "",
}

GITHUB_CI_YML_PATH = ".github/workflows/tinybird_ci.yml"
GITHUB_CD_YML_PATH = ".github/workflows/tinybird_cd.yml"
GITHUB_RELEASES_YML_PATH = ".github/workflows/tinybird_release.yml"

DEFAULT_INIT_FILES_DEPLOY = {
    GITHUB_CI_YML_PATH: GITHUB_CI_YML,
    GITHUB_CD_YML_PATH: GITHUB_CD_YML,
    GITHUB_RELEASES_YML_PATH: GITHUB_RELEASES_YML,
}


class SemverVersions(Enum):
    MAJOR: str = "major"
    MINOR: str = "minor"
    PATCH: str = "patch"
    CURRENT: str = "current"


def bump_version(version: str, next_version: Optional[str]) -> str:
    if not next_version:
        return version

    parts = version.split(".")
    is_valid_semver = len(parts) == 3

    if is_valid_semver and next_version in [
        SemverVersions.MAJOR.value,
        SemverVersions.MINOR.value,
        SemverVersions.PATCH.value,
    ]:
        major, minor, patch = map(int, parts)
        if next_version == SemverVersions.MAJOR.value:
            major += 1
        elif next_version == SemverVersions.MINOR.value:
            minor += 1
        elif next_version == SemverVersions.PATCH.value:
            patch += 1

        return f"{major}.{minor}.{patch}"
    # TODO validate next version
    return next_version


def get_default_init_files_deploy(workspace_name: str) -> Dict[str, str]:
    modified_files = {
        path.replace("tinybird_", f"tinybird_{workspace_name.lower()}_"): content
        for path, content in DEFAULT_INIT_FILES_DEPLOY.items()
    }

    return modified_files
