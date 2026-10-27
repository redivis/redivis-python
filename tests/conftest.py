"""Configuration shared by the whole test suite: which Redivis environment the
integration tests run against, and the command-line options that control them.

The target environment is applied to the process environment in pytest_configure(),
before any test module imports redivis, since the client reads it at import time.
"""

import importlib
import os
import re
import sys
from pathlib import Path

import pytest

# Named environments that --redivis-env accepts, in addition to an API URL, with
# defaults for the options below. The dev environment's user accounts are limited to
# 50 datasets, so tests create theirs in an organization.
ENVIRONMENTS = {
    "dev": {
        "api_endpoint": "https://local.host:8443/api/v1",
        "owner": "demo",
        "organization": "demo",
    },
    "prod": {
        "api_endpoint": "https://redivis.com/api/v1",
    },
}
DEFAULT_ENVIRONMENT = "dev"

# Variables that change how references resolve or how the client authenticates. A
# developer's shell (or a Redivis notebook) may well set them, but the tests have to
# run the same regardless.
AMBIENT_VARIABLES = [
    "REDIVIS_DEFAULT_USER",
    "REDIVIS_DEFAULT_ORGANIZATION",
    "REDIVIS_DEFAULT_WORKFLOW",
    "REDIVIS_DEFAULT_DATASET",
    "REDIVIS_DEFAULT_NOTEBOOK",
]

IMPORT_TIME_CONFIGURED_MODULES = [
    "redivis.common.auth",
    "redivis.common.retryable_upload",
]


def pytest_addoption(parser):
    group = parser.getgroup("redivis", "Redivis integration tests")
    group.addoption(
        "--redivis-env",
        default=os.getenv("REDIVIS_TEST_ENV", DEFAULT_ENVIRONMENT),
        help=(
            f"The environment to run integration tests against: one of "
            f"{', '.join(ENVIRONMENTS)}, or the URL of an API, e.g. "
            f"https://example.com/api/v1. Defaults to $REDIVIS_TEST_ENV, or "
            f"'{DEFAULT_ENVIRONMENT}'."
        ),
    )
    group.addoption(
        "--redivis-owner",
        default=os.getenv("REDIVIS_TEST_OWNER"),
        help=(
            "The user or organization that owns the datasets the integration tests "
            "create. Defaults to $REDIVIS_TEST_OWNER, then the environment's default "
            "(see ENVIRONMENTS in tests/conftest.py), then the authenticated user."
        ),
    )
    group.addoption(
        "--redivis-workflow",
        default=os.getenv("REDIVIS_TEST_WORKFLOW"),
        help=(
            "A scratch workflow (owner.workflow_name) for the workflow tests to "
            "create transforms, parameters, and datasources in. Defaults to "
            "$REDIVIS_TEST_WORKFLOW, or else a workflow created for (and deleted "
            "after) the run."
        ),
    )
    group.addoption(
        "--redivis-organization",
        default=os.getenv("REDIVIS_TEST_ORGANIZATION"),
        help=(
            "An organization the test user administers, for the organization tests. "
            "Defaults to $REDIVIS_TEST_ORGANIZATION, then the environment's default; "
            "the tests are skipped without one."
        ),
    )
    group.addoption(
        "--keep-resources",
        action="store_true",
        help="Don't delete the datasets created by the integration tests, e.g. to inspect them.",
    )
    group.addoption(
        "--run-slow",
        action="store_true",
        help="Also run tests marked slow.",
    )


def resolve_environment(environment):
    """The settings for --redivis-env, which is either a name or an API URL."""
    if environment in ENVIRONMENTS:
        return ENVIRONMENTS[environment]
    if re.match(r"https?://", environment):
        api_endpoint = environment.rstrip("/")
        # A URL for a named environment gets its defaults too
        for settings in ENVIRONMENTS.values():
            if settings["api_endpoint"] == api_endpoint:
                return settings
        return {"api_endpoint": api_endpoint}
    raise pytest.UsageError(
        f"Unknown --redivis-env {environment!r}: expected one of "
        f"{', '.join(ENVIRONMENTS)}, or an API URL"
    )


def credentials_dir_for(api_endpoint):
    """Where the integration tests keep credentials for an environment.

    The client keeps a single set of credentials in ~/.redivis, whichever environment
    they're for. Pointing it at another environment means its refresh token gets
    rejected there, at which point the client deletes the credentials. So each
    environment's are kept apart, and never touch the developer's own.
    """
    if os.getenv("REDIVIS_TEST_CONFIG_DIR"):
        return Path(os.environ["REDIVIS_TEST_CONFIG_DIR"]).expanduser()
    host = re.match(r"https?://([^/]+)", api_endpoint).group(1)
    return Path("~/.redivis/environments").expanduser() / re.sub(r"[^\w.-]", "_", host)


def pytest_configure(config):
    settings = resolve_environment(config.getoption("--redivis-env"))
    api_endpoint = settings["api_endpoint"]
    config.redivis_api_endpoint = api_endpoint
    config.redivis_owner = config.getoption("--redivis-owner") or settings.get("owner")
    config.redivis_organization = config.getoption(
        "--redivis-organization"
    ) or settings.get("organization")

    os.environ["REDIVIS_API_ENDPOINT"] = api_endpoint
    os.environ["REDIVIS_CONFIG_DIR"] = str(credentials_dir_for(api_endpoint))
    for variable in AMBIENT_VARIABLES:
        os.environ.pop(variable, None)

    # These modules read their configuration from the environment when they're
    # imported, and conftest.py files that import redivis are loaded before this hook
    # runs. Reloading re-executes them in place, so code that already imported their
    # functions sees the new configuration too.
    for module in IMPORT_TIME_CONFIGURED_MODULES:
        if module in sys.modules:
            importlib.reload(sys.modules[module])


def pytest_report_header(config):
    return [
        f"redivis API: {config.redivis_api_endpoint}",
        f"redivis credentials: {os.environ['REDIVIS_CONFIG_DIR']}",
        f"redivis test owner: {config.redivis_owner or '(the authenticated user)'}",
    ]


def pytest_collection_modifyitems(config, items):
    integration_dir = Path(__file__).parent / "integration"
    skip_slow = pytest.mark.skip(reason="slow; run with --run-slow")

    for item in items:
        if integration_dir in Path(item.fspath).parents:
            item.add_marker(pytest.mark.integration)
        if "slow" in item.keywords and not config.getoption("--run-slow"):
            item.add_marker(skip_slow)
