"""Table and variable metadata."""

import pytest

import redivis

from . import helpers


def test_create(new_table):
    table = new_table(description="A table")

    assert table.exists()
    assert table.properties["description"] == "A table"
    assert table.properties["uploadMergeStrategy"] == "append"
    assert table.properties["isFileIndex"] is False
    assert table.properties["numRows"] == 0


def test_create_with_merge_strategy(new_table):
    table = new_table(upload_merge_strategy="replace")

    assert table.properties["uploadMergeStrategy"] == "replace"
    assert table.get().properties["uploadMergeStrategy"] == "replace"


def test_update(new_table):
    table = new_table()
    new_name = f"{table.name}_renamed"

    table.update(name=new_name, description="Updated", upload_merge_strategy="replace")

    assert table.name == new_name
    fetched = table.get()
    assert fetched.properties["description"] == "Updated"
    assert fetched.properties["uploadMergeStrategy"] == "replace"


def test_delete(scratch_dataset, live):
    table = scratch_dataset.table(live.unique_name("delete")).create()

    table.delete()

    assert not table.exists()


def test_get_by_qualified_reference(tiny_table):
    table = redivis.table(tiny_table.qualified_reference).get()

    assert table.properties["id"] == tiny_table.properties["id"]
    assert table.properties["numRows"] == len(helpers.tiny_dataframe())
    assert table.properties["container"]["kind"] == "dataset"


def test_list_uploads(tiny_table):
    uploads = tiny_table.list_uploads()

    assert [upload.name for upload in uploads] == ["tiny.csv"]
    assert uploads[0].properties["status"] == "completed"


def test_list_variables(tiny_table):
    variables = tiny_table.list_variables()

    assert [v.name for v in variables] == list(helpers.tiny_dataframe().columns)
    assert {v.name: v["type"] for v in variables} == {
        "id": "integer",
        "Numeric": "integer",
        "Key": "string",
        "Timestamp": "string",
        "Year": "integer",
        "Boolean": "boolean",
        "Text": "string",
    }


def test_list_variables_max_results(tiny_table):
    assert len(tiny_table.list_variables(max_results=2)) == 2


def test_typed_variables(typed_table):
    variables = typed_table.list_variables()

    assert {v.name: v["type"] for v in variables} == helpers.TYPED_VARIABLE_TYPES


def test_variable_statistics(tiny_table):
    expected = helpers.tiny_dataframe()["Key"]

    statistics = tiny_table.variable("Key").get_statistics()

    assert statistics["status"] == "completed"
    assert statistics["count"] == len(expected)
    assert statistics["numDistinct"] == expected.nunique()


def test_update_variable(new_table):
    table = new_table()
    table.upload("data.csv").create(b"code,value\na,1\nb,2\n", progress=False)

    variable = table.variable("code").update(
        label="A code",
        description="Identifies the row",
        value_labels=[{"value": "a", "label": "The letter a"}],
    )

    assert variable.properties["label"] == "A code"
    fetched = table.variable("code").get()
    assert fetched.properties["description"] == "Identifies the row"
    assert fetched.properties["valueLabels"] == [
        {"value": "a", "label": "The letter a"}
    ]


def test_update_variables_in_bulk(new_table):
    table = new_table()
    table.upload("data.csv").create(b"code,value\na,1\nb,2\n", progress=False)

    table.update_variables(
        [
            {"name": "code", "label": "Code label"},
            {"name": "value", "description": "Value description"},
        ]
    )

    assert table.variable("code").get().properties["label"] == "Code label"
    assert (
        table.variable("value").get().properties["description"] == "Value description"
    )


@pytest.mark.parametrize("name", ["Key", "key", "KEY"])
def test_variables_are_looked_up_case_insensitively(tiny_table, name):
    assert tiny_table.variable(name).get().properties["name"] == "Key"
