"""Tables referenced through a workflow, including those inherited through the
workflow's datasources."""

import pytest
import redivis
from redivis.classes import Notebook as notebook_module
from redivis.classes import Table as table_module
from redivis.classes import Transform as transform_module
from redivis.classes import Variable as variable_module
from redivis.classes import Workflow as workflow_module

CANONICAL_URI = "/tables/demo.omop%3Aabcd%3Av1_0.person%3Aefgh"
WORKFLOW_URI = "/workflows/owner.workflow%3Aabcd/tables/person"


class RequestLog(list):
    def __init__(self):
        super().__init__()
        self.payloads = []


@pytest.fixture
def requests(monkeypatch):
    """Stub out the API, recording each request as "METHOD path". Fetching a
    table through its workflow returns a table inherited from a datasource."""
    log = RequestLog()

    def make_request(*, method="GET", path="", payload=None, **kwargs):
        log.append(f"{method.upper()} {path}")
        log.payloads.append(payload)
        if path in (WORKFLOW_URI, CANONICAL_URI):
            return {
                "kind": "table",
                "name": "Person",
                "scopedReference": "person:efgh",
                "qualifiedReference": "demo.omop:abcd:v1_0.person:efgh",
                "uri": CANONICAL_URI,
                "container": {"kind": "dataset"},
            }
        return {"uri": path}

    monkeypatch.setattr(table_module, "make_request", make_request)
    monkeypatch.setattr(variable_module, "make_request", make_request)
    monkeypatch.setattr(transform_module, "make_request", make_request)
    monkeypatch.setattr(notebook_module, "make_request", make_request)
    return log


@pytest.fixture
def workflow():
    return redivis.workflow("owner.workflow:abcd")


def test_workflow_tables_are_resolved_within_their_workflow(workflow):
    assert workflow.table("person").uri == WORKFLOW_URI
    assert (
        workflow.table("Condition occurrence").uri
        == "/workflows/owner.workflow%3Aabcd/tables/Condition%20occurrence"
    )
    assert (
        redivis.dataset("owner.dataset:abcd").table("person").uri
        == "/tables/owner.dataset%3Aabcd.person"
    )


def test_reads_use_the_workflow_scoped_uri(requests, workflow):
    table = workflow.table("person")
    table.exists()
    table.variable("person_id").get()
    assert requests == [
        f"HEAD {WORKFLOW_URI}",
        f"GET {WORKFLOW_URI}/variables/person_id",
    ]


def test_table_update_fetches_the_canonical_uri_first(requests, workflow):
    workflow.table("person").update(description="x")
    assert requests == [f"GET {WORKFLOW_URI}", f"PATCH {CANONICAL_URI}"]


def test_table_update_variables_fetches_the_canonical_uri_first(requests, workflow):
    workflow.table("person").update_variables([])
    assert requests == [f"GET {WORKFLOW_URI}", f"PATCH {CANONICAL_URI}/variables"]


def test_variable_update_fetches_the_canonical_uri_first(requests, workflow):
    workflow.table("person").variable("person_id").update(label="x")
    assert requests == [
        f"GET {WORKFLOW_URI}",
        f"PATCH {CANONICAL_URI}/variables/person_id",
    ]


def test_fetched_tables_are_not_fetched_again(requests, workflow):
    table = workflow.table("person")
    table.get()
    table.update(description="x")
    table.update_variables([])
    assert requests == [
        f"GET {WORKFLOW_URI}",
        f"PATCH {CANONICAL_URI}",
        f"PATCH {CANONICAL_URI}/variables",
    ]


def test_list_tables_can_include_datasources(monkeypatch, workflow):
    calls = []

    def make_paginated_request(*, path, query={}, **kwargs):
        calls.append((path, query))
        return []

    monkeypatch.setattr(
        workflow_module, "make_paginated_request", make_paginated_request
    )
    workflow.list_tables()
    workflow.list_tables(include_datasources=True)
    assert calls == [
        (f"{workflow.uri}/tables", {}),
        (f"{workflow.uri}/tables", {"includeDataSources": "true"}),
    ]


@pytest.mark.parametrize("kind", ["transform", "notebook"])
def test_source_tables_fetch_the_canonical_reference_first(requests, workflow, kind):
    node = getattr(workflow, kind)("n")
    node_uri = node.uri
    node.update(source_table=workflow.table("person"))
    assert requests == [f"GET {WORKFLOW_URI}", f"PATCH {node_uri}"]
    assert requests.payloads[1]["sourceTable"] == "demo.omop:abcd:v1_0.person:efgh"
