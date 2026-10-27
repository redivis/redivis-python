"""Workflows, and the datasources, transforms, and parameters in them.

The tests that modify a workflow do so in a scratch workflow: --redivis-workflow if it's
given, or else one created for the run. Everything they create in it is deleted
afterwards.
"""

import pandas
import pytest

import redivis

from . import helpers


@pytest.fixture
def any_workflow(live):
    workflows = live.user.list_workflows(max_results=1)
    if not workflows:
        pytest.skip(f"{live.user.name} has no workflows to read")
    return workflows[0]


@pytest.fixture
def sample_datasource(scratch_workflow, sample_dataset):
    datasource = scratch_workflow.datasource(sample_dataset).create()
    yield datasource
    if datasource.exists():
        datasource.delete()


def test_get(any_workflow):
    workflow = redivis.workflow(any_workflow.qualified_reference).get()

    assert workflow.exists()
    assert workflow.properties["kind"] == "workflow"
    assert workflow.name == any_workflow.name


def test_list_contents(any_workflow):
    tables = any_workflow.list_tables()
    assert all(isinstance(t, redivis.table) for t in tables)
    assert all(t.workflow is any_workflow for t in tables)

    for listing in (
        any_workflow.list_datasources,
        any_workflow.list_transforms,
        any_workflow.list_notebooks,
        any_workflow.list_parameters,
    ):
        assert isinstance(listing(), list)


def test_create(created_workflow, live):
    assert created_workflow.exists()
    assert (
        created_workflow.properties["description"]
        == "Created by the redivis-python tests"
    )
    assert created_workflow.properties["publicAccessLevel"] == "none"
    assert created_workflow.properties["owner"]["kind"] == (
        "user" if created_workflow.user else "organization"
    )
    assert (
        created_workflow.organization or created_workflow.user
    ).name == live.owner.name
    assert created_workflow.qualified_reference.startswith(f"{live.owner.name}.")

    fetched = redivis.workflow(created_workflow.qualified_reference).get()
    assert fetched.properties["id"] == created_workflow.properties["id"]
    assert fetched.list_tables() == []


def test_delete(live):
    workflow = live.owner.workflow(live.unique_name("delete")).create()
    assert workflow.exists()

    workflow.delete()

    assert not workflow.exists()
    assert workflow.uri not in [w.uri for w in live.owner.list_workflows()]


def test_create_requires_a_name(live):
    with pytest.raises(redivis.exceptions.APIError) as excinfo:
        live.owner.workflow("").create()

    assert excinfo.value.status_code == 400


def test_missing_workflow(live):
    assert not live.user.workflow(live.unique_name("missing")).exists()


def test_query_scoped_to_a_workflow(scratch_workflow):
    df = scratch_workflow.query("SELECT 1 AS x").to_pandas_dataframe(progress=False)

    assert df["x"].tolist() == [1]


def test_datasource(scratch_workflow, sample_dataset, sample_datasource):
    assert sample_datasource.exists()
    assert (
        sample_datasource.source_dataset().properties["id"]
        == sample_dataset.properties["id"]
    )
    listed = [d.uri for d in scratch_workflow.list_datasources()]
    assert sample_datasource.uri in listed

    sample_datasource.delete()

    assert not sample_datasource.exists()
    assert sample_datasource.uri not in [
        d.uri for d in scratch_workflow.list_datasources()
    ]


def test_deleting_a_datasource_deletes_its_transforms(
    scratch_workflow, sample_datasource, tiny_table, live
):
    transform = scratch_workflow.transform(live.unique_name("transform")).create(
        source_table=tiny_table, query="SELECT id FROM _source_"
    )

    sample_datasource.delete()

    assert not transform.exists()


def test_transform(scratch_workflow, sample_datasource, tiny_table, live):
    transform = scratch_workflow.transform(live.unique_name("transform")).create(
        source_table=tiny_table,
        query="SELECT id, Key FROM _source_ WHERE Boolean",
    )
    assert transform.exists()
    assert transform.source_table().properties["id"] == tiny_table.properties["id"]
    assert transform.uri in [t.uri for t in scratch_workflow.list_transforms()]

    try:
        transform.run()

        expected = helpers.tiny_dataframe()
        expected = expected[expected["Boolean"]][["id", "Key"]].reset_index(drop=True)
        actual = transform.output_table().to_pandas_dataframe(progress=False)
        pandas.testing.assert_frame_equal(
            helpers.sorted_by(actual), expected, check_dtype=False
        )
        assert transform.properties["lastRunJob"]["status"] == "completed"
    finally:
        transform.delete()

    assert not transform.exists()
    assert transform.uri not in [t.uri for t in scratch_workflow.list_transforms()]


def test_parameter(scratch_workflow, live):
    name = live.unique_name("param")
    parameter = scratch_workflow.parameter(name).create(
        type="string", values=["a", "b"]
    )

    try:
        assert parameter.exists()
        assert parameter.get_values() == ["a", "b"]
        assert name in [p.name for p in scratch_workflow.list_parameters()]

        parameter.update(values=["c"])
        assert parameter.get_values() == ["c"]
    finally:
        parameter.delete()

    assert not parameter.exists()
