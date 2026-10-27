"""Reading rows and listing files: read streams, sizing, columns, and caching."""

import pytest

from redivis.common import exceptions


def test_tables_of_unknown_size_can_be_read(api, table):
    api["num_bytes"] = None
    assert table.to_arrow_table().num_rows == api["num_rows"]


def test_read_streams_reject_options_they_would_ignore(api, table):
    stream = table.to_read_streams(2)[0]
    with pytest.raises(exceptions.ValueError, match="to_read_streams"):
        stream.to_arrow_table(variables=["id"])
    with pytest.raises(exceptions.ValueError, match="max_results"):
        stream.to_arrow_table(max_results=10)
    assert stream.to_arrow_table().num_rows > 0


def test_streamed_columns_match_variables_case_insensitively(api, table):
    # The table is in a dataset, so its columns are coerced to the variables' types
    api["container_kind"] = "dataset"
    api["variables"] = [
        {"name": "ID", "type": "integer"},
        {"name": "Name", "type": "string"},
        {"name": "VAL", "type": "float"},
    ]
    api["num_bytes"] = 1e9  # Read through a read session, rather than listRows
    batches = list(table.to_arrow_batch_iterator())
    assert sum(batch.num_rows for batch in batches) == api["num_rows"]
    assert batches[0].schema.names == ["ID", "Name", "VAL"]


@pytest.mark.parametrize("num_bytes", [1000, 1e9], ids=["list_rows", "read_session"])
def test_columns_are_coerced_to_string_variables(api, table, num_bytes):
    # E.g., a column of integers in one upload, in a table where another upload's
    # values made it a string
    api["container_kind"] = "dataset"
    api["num_bytes"] = num_bytes
    api["variables"] = [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "val", "type": "string"},
    ]

    result = table.to_arrow_table(progress=False).sort_by("name")

    assert [str(t) for t in result.schema.types] == ["string"] * 3
    assert result.column("id").to_pylist()[:2] == ["0", "1"]
    assert result.column("val").to_pylist()[:2] == ["0", "0.5"]


def test_directory_listings_are_cached_by_their_variables(api, table):
    api["file_index"] = [
        {"file_id": "f1", "file_name": "a.png", "alt_name": "alt_a.png"},
        {"file_id": "f2", "file_name": "b.png", "alt_name": "alt_b.png"},
    ]
    names = sorted(f.name for f in table.to_directory().list(mode="files"))
    assert names == ["a.png", "b.png"]

    alt_names = sorted(
        f.name
        for f in table.to_directory(file_name_variable="alt_name").list(mode="files")
    )
    assert alt_names == ["alt_a.png", "alt_b.png"]

    # The same listing again is revalidated, and reused
    assert table.to_directory().get("a.png").id == "f1"
    # file() looks files up with the variables of the last listing
    assert table.file("a.png").id == "f1"
