"""Fixtures for tests that run against a live Redivis API.

The environment comes from --redivis-env (see tests/conftest.py). Since a fresh
environment has no data in it, the tests upload their own: everything they create lives
in datasets and workflows named with a per-run prefix, owned by --redivis-owner, which
are deleted when the tests that made them finish. Any left behind by an interrupted run
are deleted by a later one.
"""

import datetime
import os
import re
import secrets
from dataclasses import dataclass, field
from typing import Any, List, Optional

import pytest
import niquests

import redivis
from redivis.common import auth, exceptions

from . import helpers

RESOURCE_PREFIX = "pytest_"
# Datasets from an earlier run that are older than this are assumed to be abandoned
STALE_AFTER = datetime.timedelta(hours=12)


@dataclass
class LiveEnvironment:
    api_endpoint: str
    user: Any
    owner: Any
    organization_name: Optional[str]
    workflow_reference: Optional[str]
    run_id: str
    keep_resources: bool
    created_datasets: List[Any] = field(default_factory=list)

    def unique_name(self, label=""):
        """A name that's unique to this run, and marks a resource as the tests'."""
        return f"{self.run_id}_{label + '_' if label else ''}{secrets.token_hex(3)}"

    def create_dataset(self, label="", **kwargs):
        dataset = self.owner.dataset(self.unique_name(label)).create(**kwargs)
        self.created_datasets.append(dataset)
        return dataset

    def delete_dataset(self, dataset):
        if self.keep_resources:
            return
        # The dataset as a whole, rather than whichever version it references
        reference = re.sub(r":(v\d+_\d+|next|current)$", "", dataset.qualified_reference)
        helpers.delete_dataset(redivis.dataset(reference))


@pytest.fixture(scope="session")
def live(pytestconfig, worker_id):
    """The live environment the integration tests run against, once it's verified to
    be reachable and the client has credentials for it."""
    api_endpoint = pytestconfig.redivis_api_endpoint
    try:
        niquests.get(api_endpoint, timeout=10, verify=auth.get_verify_ssl())
    except niquests.ConnectionError as error:
        pytest.skip(f"The Redivis API at {api_endpoint} is unreachable ({error})")

    user = authenticate(pytestconfig)
    owner_name = pytestconfig.redivis_owner
    if owner_name is None or owner_name == user.name:
        owner = user
    else:
        owner = redivis.organization(owner_name)
        if not owner.exists():
            owner = redivis.user(owner_name)

    environment = LiveEnvironment(
        api_endpoint=api_endpoint,
        user=user,
        owner=owner,
        organization_name=pytestconfig.redivis_organization,
        workflow_reference=pytestconfig.getoption("--redivis-workflow"),
        run_id=f"{RESOURCE_PREFIX}{datetime.datetime.now():%Y%m%d_%H%M%S}_{worker_id}",
        keep_resources=pytestconfig.getoption("--keep-resources"),
    )
    delete_stale_resources(owner)

    yield environment

    for dataset in environment.created_datasets:
        try:
            environment.delete_dataset(dataset)
        except exceptions.RedivisError as error:
            print(f"Couldn't delete {dataset}: {error}")


@pytest.fixture(scope="session")
def worker_id(request):
    """Distinguishes the workers of a parallel (pytest-xdist) run."""
    return getattr(request.config, "workerinput", {}).get("workerid", "main")


def authenticate(pytestconfig):
    """Make sure the client has credentials, logging in interactively if need be."""
    # A login must never be sent to (or credentials saved for) any other environment.
    # See pytest_configure() in tests/conftest.py.
    expected_base_url = re.match(
        r"https?://[^/]+", pytestconfig.redivis_api_endpoint
    ).group(0)
    assert auth.get_base_url() == expected_base_url, auth.get_base_url()
    assert (
        str(auth.get_redivis_dir()) == os.environ["REDIVIS_CONFIG_DIR"]
    ), auth.get_redivis_dir()

    if not auth.has_credentials() and os.getenv("CI"):
        pytest.fail(
            "No Redivis credentials are available. Set REDIVIS_API_TOKEN to run the "
            "integration tests non-interactively.",
            pytrace=False,
        )

    # A login prints a URL to visit, which shouldn't be swallowed by output capturing
    capture_manager = pytestconfig.pluginmanager.getplugin("capturemanager")
    with capture_manager.global_and_fixture_disabled():
        return redivis.current_user()


def delete_stale_resources(owner):
    """Delete the datasets and workflows left behind by earlier, interrupted runs."""
    now = datetime.datetime.now(datetime.timezone.utc)

    def is_stale(resource):
        created_at = resource.properties["createdAt"]
        if isinstance(created_at, str):
            # Workflows' are ISO 8601 strings, rather than milliseconds
            created_at = datetime.datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        else:
            created_at = datetime.datetime.fromtimestamp(
                created_at / 1000, datetime.timezone.utc
            )
        return resource.name.startswith(RESOURCE_PREFIX) and now - created_at > STALE_AFTER

    for dataset in filter(is_stale, owner.list_datasets()):
        try:
            helpers.delete_dataset(dataset)
        except exceptions.RedivisError as error:
            print(f"Couldn't delete stale dataset {dataset}: {error}")

    for workflow in filter(is_stale, owner.list_workflows()):
        try:
            workflow.delete()
        except exceptions.RedivisError as error:
            print(f"Couldn't delete stale workflow {workflow}: {error}")


@pytest.fixture
def new_dataset(live):
    """Creates datasets that are deleted once the test finishes."""
    created = []

    def create(label="", **kwargs):
        dataset = live.owner.dataset(live.unique_name(label)).create(**kwargs)
        created.append(dataset)
        return dataset

    yield create

    for dataset in created:
        live.delete_dataset(dataset)


@pytest.fixture(scope="session")
def scratch_dataset(live):
    """An unreleased dataset, for tests to create tables in."""
    return live.create_dataset("scratch")


@pytest.fixture
def new_table(live, scratch_dataset):
    """Creates tables in the scratch dataset, which are deleted once the test finishes."""
    created = []

    def create(label="table", **kwargs):
        table = scratch_dataset.table(live.unique_name(label)).create(**kwargs)
        created.append(table)
        return table

    yield create

    if not live.keep_resources:
        for table in created:
            if table.exists():
                table.delete()


@pytest.fixture(scope="session")
def sample_dataset(live, tmp_path_factory):
    """A dataset holding tables of known contents, for tests to read.

    Each table's upload is started here, and waited for by the fixture for that table,
    so that they're processed concurrently.
    """
    dataset = live.create_dataset("sample", description="Sample data for tests")

    tiny = dataset.table("tiny").create(description="tiny.csv")
    tiny.upload("tiny.csv").create(helpers.TINY_CSV, wait_for_finish=False, progress=False)

    typed = dataset.table("typed").create()
    typed.upload("typed").create(
        helpers.typed_arrow_table(), wait_for_finish=False, progress=False
    )

    geo = dataset.table("geo").create()
    geo.upload("features.geojson").create(
        helpers.geojson_bytes(), type="geojson", wait_for_finish=False, progress=False
    )

    files = dataset.table("files").create(is_file_index=True)
    files.add_files(
        directory=str(
            helpers.write_files(tmp_path_factory.mktemp("files"), helpers.raw_files())
        ),
        progress=False,
    )

    return dataset


def finished_table(dataset, name, upload_name):
    table = dataset.table(name)
    helpers.wait_for_upload(table.upload(upload_name))
    return table.get()


@pytest.fixture(scope="session")
def tiny_table(sample_dataset):
    """tests/data/tiny.csv, uploaded as a CSV."""
    return finished_table(sample_dataset, "tiny", "tiny.csv")


@pytest.fixture(scope="session")
def typed_table(sample_dataset):
    """helpers.typed_arrow_table(), uploaded as an arrow table (i.e., parquet)."""
    return finished_table(sample_dataset, "typed", "typed")


@pytest.fixture(scope="session")
def geo_table(sample_dataset):
    """helpers.GEO_FEATURES, uploaded as GeoJSON."""
    return finished_table(sample_dataset, "geo", "features.geojson")


@pytest.fixture(scope="session")
def files_table(sample_dataset):
    """A file index table holding helpers.raw_files()."""
    return sample_dataset.table("files").get()


@pytest.fixture(scope="session")
def created_workflow(live):
    """A workflow created by this run, which is deleted when the run finishes."""
    workflow = live.owner.workflow(live.unique_name("workflow")).create(
        description="Created by the redivis-python tests"
    )
    yield workflow
    if not live.keep_resources:
        workflow.delete()


@pytest.fixture(scope="session")
def scratch_workflow(live, request):
    """A workflow for tests to create transforms, parameters, and datasources in:
    --redivis-workflow, or else one created for this run."""
    if live.workflow_reference:
        return redivis.workflow(live.workflow_reference).get()
    return request.getfixturevalue("created_workflow")


@pytest.fixture
def organization(live):
    if not live.organization_name:
        pytest.skip("needs an organization; pass --redivis-organization")
    return redivis.organization(live.organization_name)
