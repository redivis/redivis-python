"""Resource classes: references, versions, datasources, transforms, and their paths."""

import pytest

import redivis
from redivis.common import exceptions


def script(api, *responses):
    """Respond to the next requests with these bodies, in order."""
    api["response_script"].extend((200, body) for body in responses)


def json_bodies(api, method):
    return [body for m, _, body in api["json_bodies"] if m == method]


def test_references_with_dotted_version_tags(api):
    assert redivis.dataset("owner.ds:v1.0").qualified_reference == "owner.ds:v1_0"
    table = redivis.table("owner.ds:abc1:v1.0.my_table")
    assert table.qualified_reference == "owner.ds:abc1:v1_0.my_table"
    assert table.dataset.qualified_reference == "owner.ds:abc1:v1_0"


def test_default_workflow_and_dataset_may_be_owned_by_an_organization(api, monkeypatch):
    monkeypatch.setenv("REDIVIS_DEFAULT_WORKFLOW", "org.wf")
    table = redivis.table("t")
    assert table.uri == "/workflows/org.wf/tables/t"
    assert table.workflow.organization.name == "org"

    monkeypatch.delenv("REDIVIS_DEFAULT_WORKFLOW")
    monkeypatch.setenv("REDIVIS_DEFAULT_DATASET", "org.ds:v1.0")
    assert redivis.table("t").qualified_reference == "org.ds:v1_0.t"

    monkeypatch.setenv("REDIVIS_DEFAULT_DATASET", "not_qualified")
    with pytest.raises(exceptions.ValueError):
        redivis.table("t")


def test_workflow_organization_by_name(api):
    workflow = redivis.workflow("wf", organization="org")
    assert workflow.organization.name == "org"
    assert workflow.qualified_reference == "org.wf"


def test_names_are_encoded_in_paths(api):
    assert redivis.user("u").secret("my secret").uri == "/users/u/secrets/my%20secret"
    assert (
        redivis.organization("o").member("a/b").uri == "/organizations/o/members/a%2Fb"
    )
    assert (
        redivis.workflow("u.wf").parameter("p q").uri
        == "/workflows/u.wf/parameters/p%20q"
    )
    assert redivis.table("a.b.c").variable("v w").uri == "/tables/a.b.c/variables/v%20w"


def test_dataset_version_navigation(api):
    dataset_properties = {
        "name": "ds",
        "uri": "/datasets/owner.ds:abc1:v1_1",
        "qualifiedReference": "owner.ds:abc1:v1_1",
        "scopedReference": "ds:abc1:v1_1",
        "owner": {"kind": "organization"},
        "version": {"tag": "v1.1"},
    }
    script(
        api,
        dataset_properties,
        {
            "tag": "v1.1",
            "uri": "/v/v1_1",
            "previousVersion": {"tag": "v1.0"},
            "nextVersion": None,
        },
    )
    dataset = redivis.dataset("owner.ds:abc1")
    previous = dataset.previous_version()
    assert previous.qualified_reference == "owner.ds:abc1:v1.0"
    assert dataset.version_tag == "v1_1"

    script(
        api,
        {
            "tag": "v1.1",
            "uri": "/v/v1_1",
            "previousVersion": {"tag": "v1.0"},
            "nextVersion": None,
        },
    )
    assert dataset.next_version() is None

    version = redivis.dataset("owner.ds:abc1").version("v2.0")
    assert version.dataset().qualified_reference == "owner.ds:abc1:v2.0"


def test_version_delete_and_undelete_return_the_version(api):
    version = redivis.dataset("owner.ds").version("v1_0")
    script(api, {"uri": "/v/1", "isDeleted": True}, {"uri": "/v/1", "isDeleted": False})
    assert version.delete() is version
    assert version.undelete() is version


def test_datasource_updates_keep_the_sample_unless_changed(api):
    datasource = redivis.workflow("u.wf").datasource("owner.ds:sample")
    script(
        api,
        {
            "uri": "/ds/1",
            "sourceDataset": {"qualifiedReference": "owner.ds:abc1:v1_0:sample"},
        },
    )
    script(api, {"uri": "/ds/1"})
    datasource.update(version="v2_0")

    script(api, {"uri": "/ds/1"})
    datasource.update(source_dataset="owner.ds:abc1:sample", sample=False)

    # Updates that don't involve a dataset only send what changed
    script(api, {"uri": "/ds/1"})
    datasource.update(mapped_tables=[("old", "new")])

    assert json_bodies(api, "PATCH") == [
        {"sourceDataset": "owner.ds:abc1:v2_0:sample"},
        {"sourceDataset": "owner.ds:abc1"},
        {"mappedTables": {"old": "new"}},
    ]


def test_transform_runs_wait_for_the_job_they_started(api):
    transform = redivis.transform("u.wf.t")
    script(
        api,
        {"uri": "/transforms/t", "currentJob": {"id": "j2", "status": "queued"}},
        # The previous run's job finished, but this one hasn't
        {
            "uri": "/transforms/t",
            "currentJob": {"id": "j2", "status": "running"},
            "lastRunJob": {"id": "j1", "status": "completed"},
        },
        {
            "uri": "/transforms/t",
            "currentJob": None,
            "lastRunJob": {"id": "j2", "status": "completed"},
        },
    )
    assert transform.run() is transform
    assert (
        len(
            [
                path
                for _, path, _, _ in api["requests"]
                if path == "/api/v1/transforms/t"
            ]
        )
        == 2
    )


def test_cancelled_runs_raise(api):
    transform = redivis.transform("u.wf.t")
    script(
        api,
        {"uri": "/transforms/t", "currentJob": {"id": "j2", "status": "queued"}},
        {
            "uri": "/transforms/t",
            "currentJob": None,
            "lastRunJob": {"id": "j2", "status": "cancelled"},
        },
    )
    with pytest.raises(exceptions.JobError):
        transform.run()

    script(api, {"uri": "/transforms/t"})
    assert transform.cancel() is transform


def test_unnamed_transforms_are_named_on_creation(api):
    transform = redivis.workflow("u.wf").transform()
    assert transform.qualified_reference is None
    script(
        api,
        {
            "name": "transform_1",
            "uri": "/transforms/x",
            "qualifiedReference": "u.wf.transform_1",
            "scopedReference": "transform_1",
        },
    )
    transform.create(source_table="owner.ds.t", query="SELECT 1")
    assert transform.qualified_reference == "u.wf.transform_1"
    assert json_bodies(api, "POST")[-1] == {
        "sourceTable": "owner.ds.t",
        "query": "SELECT 1",
    }


def workflow_properties(owner_kind):
    return {
        "name": "wf",
        "uri": "/workflows/owner.wf:abc1",
        "qualifiedReference": "owner.wf:abc1",
        "scopedReference": "wf:abc1",
        "owner": {"kind": owner_kind},
    }


def test_workflow_create(api):
    script(api, workflow_properties("user"))

    workflow = redivis.user("owner").workflow("wf").create(description="About")

    assert ("POST", "/api/v1/users/owner/workflows") in [
        (method, path) for method, path, _, _ in api["requests"]
    ]
    assert json_bodies(api, "POST") == [
        {"name": "wf", "description": "About", "isPublic": False}
    ]
    assert workflow.qualified_reference == "owner.wf:abc1"
    assert workflow.uri == "/workflows/owner.wf:abc1"
    assert workflow.organization is None


def test_workflow_create_resolves_an_ambiguous_owner(api):
    # The owner of "owner.wf" is an organization if one by that name exists
    script(api, {"results": [], "nextPageToken": None}, workflow_properties("organization"))

    workflow = redivis.workflow("owner.wf").create(is_public=True)

    assert [path for _, path, _, _ in api["requests"]] == [
        "/api/v1/organizations/owner/datasets",
        "/api/v1/organizations/owner/workflows",
    ]
    assert json_bodies(api, "POST")[-1]["isPublic"] is True
    assert workflow.user is None
    assert workflow.organization.name == "owner"


@pytest.mark.parametrize(
    "resource, path",
    [
        (lambda: redivis.workflow("owner.wf"), "/api/v1/workflows/owner.wf"),
        (lambda: redivis.transform("owner.wf.t"), "/api/v1/transforms/owner.wf.t"),
        (
            lambda: redivis.workflow("owner.wf").datasource("owner.ds"),
            "/api/v1/workflows/owner.wf/dataSources/owner.ds",
        ),
    ],
)
def test_delete(api, resource, path):
    # As the API responds when there's nothing to return
    api["response_script"].append((200, b"OK"))

    assert resource().delete() is None

    assert [(m, p) for m, p, _, _ in api["requests"]] == [("DELETE", path)]
