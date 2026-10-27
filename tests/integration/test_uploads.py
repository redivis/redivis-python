"""Uploading data to tables, in each of the forms that upload.create() accepts."""

import datetime
import io

import pandas
import polars
import pyarrow
import pytest

from redivis import exceptions

from . import helpers

SMALL_CSV = b"id,name,value\n1,a,1.5\n2,b,2.5\n3,c,\n"


def read_sorted(reader, column="id"):
    return helpers.sorted_by(
        reader.to_pandas_dataframe(progress=False, dtype_backend="numpy_nullable"),
        column,
    )


def test_upload_file_path_names_the_upload_after_the_file(new_table):
    table = new_table()

    upload = table.upload().create(helpers.TINY_CSV, progress=False)

    assert upload.name == "tiny.csv"
    assert upload.properties["status"] == "completed"
    assert upload.properties["numRows"] == len(helpers.tiny_dataframe())
    assert table.get().properties["numRows"] == len(helpers.tiny_dataframe())


def test_upload_file_object(new_table):
    table = new_table()

    with open(helpers.TINY_CSV, "rb") as f:
        upload = table.upload("tiny.csv").create(f, type="delimited", progress=False)

    actual = helpers.sorted_by(
        upload.to_pandas_dataframe(progress=False, dtype_backend="numpy")
    )
    pandas.testing.assert_frame_equal(
        actual, helpers.tiny_dataframe(), check_dtype=False
    )


def test_upload_bytes(new_table):
    table = new_table()

    table.upload("small.csv").create(SMALL_CSV, progress=False)

    df = read_sorted(table)
    assert df["id"].tolist() == [1, 2, 3]
    assert df["name"].tolist() == ["a", "b", "c"]
    assert df["value"].tolist()[:2] == [1.5, 2.5]
    assert df["value"].isna().tolist() == [False, False, True]


def test_quoted_newlines(new_table):
    table = new_table()
    content = b'id,text\n1,"first\nsecond"\n2,"a ""quoted"" word"\n3,plain\n'

    table.upload("newlines.csv").create(
        content, has_quoted_newlines=True, progress=False
    )

    assert read_sorted(table)["text"].tolist() == [
        "first\nsecond",
        'a "quoted" word',
        "plain",
    ]


def test_delimited_options(new_table):
    table = new_table()
    content = b"1|a|NA\n2|NA|2.5\n"

    table.upload("data.psv").create(
        content,
        type="delimited",
        delimiter="|",
        has_header_row=False,
        null_markers=["NA"],
        progress=False,
    )

    df = helpers.sorted_by(
        table.to_pandas_dataframe(progress=False, dtype_backend="numpy_nullable"),
        column=table.list_variables()[0].name,
    )
    assert len(table.list_variables()) == 3
    assert df.iloc[:, 1].isna().tolist() == [False, True]
    assert df.iloc[:, 2].isna().tolist() == [True, False]


def dataframe_contents():
    return {
        "id": [1, 2, 3],
        "name": ["a", "b", None],
        "value": [1.5, None, -3.0],
        "flag": [True, False, True],
        "day": [datetime.date(2020, 1, 1), datetime.date(2021, 6, 15), None],
    }


def dask_dataframe(contents):
    import dask
    import dask.dataframe

    # By default, dask converts object columns (like this one of python dates) to
    # strings when the DataFrame is created. Several partitions are uploaded as one.
    with dask.config.set({"dataframe.convert-string": False}):
        return dask.dataframe.from_pandas(pandas.DataFrame(contents), npartitions=2)


DATAFRAME_FACTORIES = {
    "pandas": lambda: pandas.DataFrame(dataframe_contents()),
    "polars": lambda: polars.DataFrame(dataframe_contents()),
    "polars_lazy": lambda: polars.LazyFrame(dataframe_contents()),
    "arrow_table": lambda: pyarrow.table(dataframe_contents()),
    "arrow_record_batch": lambda: pyarrow.RecordBatch.from_pydict(dataframe_contents()),
    "dask": lambda: dask_dataframe(dataframe_contents()),
}


@pytest.mark.parametrize("kind", DATAFRAME_FACTORIES)
def test_upload_dataframe(new_table, kind):
    table = new_table(kind)

    upload = table.upload(kind).create(DATAFRAME_FACTORIES[kind](), progress=False)

    assert upload.properties["type"] == "parquet"
    actual = table.to_arrow_table(progress=False).sort_by("id").to_pydict()
    expected = dataframe_contents()
    assert actual["id"] == expected["id"]
    assert actual["name"] == expected["name"]
    assert actual["value"] == expected["value"]
    assert actual["flag"] == expected["flag"]
    assert actual["day"] == expected["day"]


def test_upload_geodataframe(new_table):
    geopandas = pytest.importorskip("geopandas")
    from shapely.geometry import Point

    table = new_table()
    gdf = geopandas.GeoDataFrame(
        {"id": [1, 2], "geometry": [Point(-122.0, 37.0), Point(0.5, 51.5)]},
        crs="EPSG:4326",
    )

    table.upload("points").create(gdf, progress=False)

    assert {v.name: v["type"] for v in table.list_variables()}[
        "geometry"
    ] == "geography"
    actual = helpers.sorted_by(table.to_geopandas_dataframe(progress=False))
    assert [(p.x, p.y) for p in actual.geometry] == [(-122.0, 37.0), (0.5, 51.5)]


def test_upload_metadata_sets_variable_metadata(new_table):
    table = new_table()

    table.upload("small.csv").create(
        SMALL_CSV,
        metadata={
            "name": {
                "label": "Name label",
                "description": "Name description",
                "valueLabels": {"a": "The letter a"},
            }
        },
        progress=False,
    )

    variable = table.variable("name").get()
    assert variable.properties["label"] == "Name label"
    assert variable.properties["description"] == "Name description"
    assert variable.properties["valueLabels"] == [
        {"value": "a", "label": "The letter a"}
    ]


def test_append_merge_strategy(new_table):
    table = new_table(upload_merge_strategy="append")

    table.upload("first.csv").create(SMALL_CSV, progress=False)
    table.upload("second.csv").create(SMALL_CSV, progress=False)

    assert table.get().properties["numRows"] == 6
    assert sorted(u.name for u in table.list_uploads()) == ["first.csv", "second.csv"]


def test_replace_merge_strategy(new_dataset):
    # The uploads to a new version replace the previous version's data, rather than
    # adding to it
    dataset = new_dataset()
    table = dataset.table("data").create(upload_merge_strategy="replace")
    table.upload("first.csv").create(SMALL_CSV, progress=False)
    dataset.release(wait_for_finalization=True)

    next_table = dataset.create_next_version().table("data")
    next_table.upload("second.csv").create(b"id,name,value\n9,z,9.5\n", progress=False)

    assert read_sorted(next_table)["id"].tolist() == [9]
    assert [u.name for u in next_table.list_uploads()] == ["second.csv"]


def test_append_merge_strategy_across_versions(new_dataset):
    dataset = new_dataset()
    table = dataset.table("data").create()
    table.upload("first.csv").create(SMALL_CSV, progress=False)
    dataset.release(wait_for_finalization=True)

    next_table = dataset.create_next_version().table("data")
    next_table.upload("second.csv").create(b"id,name,value\n9,z,9.5\n", progress=False)

    assert next_table.get().properties["numRows"] == 4


def test_read_table_with_several_unreleased_uploads(new_table):
    # Uploads with the same schema are read as one response from listRows
    table = new_table()
    table.upload("first.csv").create(SMALL_CSV, progress=False)
    table.upload("second.csv").create(b"id,name,value\n9,z,9.5\n", progress=False)

    assert read_sorted(table)["id"].tolist() == [1, 2, 3, 9]


def test_read_table_with_unreleased_uploads_of_different_schemas(new_table):
    # listRows can't combine these into one response, so they're read through a read
    # session, with each upload's rows conformed to the table's variables
    table = new_table()
    table.upload("first.csv").create(b"id,code,extra\n1,10,x\n2,20,y\n", progress=False)
    table.upload("second.csv").create(b"id,code\n3,abc\n", progress=False)

    assert {v.name: v["type"] for v in table.list_variables()} == {
        "id": "integer",
        "code": "string",
        "extra": "string",
    }
    for read in (
        lambda: table.to_arrow_table(progress=False),
        lambda: pyarrow.Table.from_batches(
            list(table.to_arrow_batch_iterator(progress=False))
        ),
    ):
        rows = read().sort_by("id").to_pylist()
        assert rows == [
            {"id": 1, "code": "10", "extra": "x"},
            {"id": 2, "code": "20", "extra": "y"},
            {"id": 3, "code": "abc", "extra": None},
        ]


def test_upload_name_conflicts(new_table):
    table = new_table()
    original = table.upload("data.csv").create(SMALL_CSV, progress=False)

    with pytest.raises(exceptions.ValueError, match="already exists"):
        table.upload("data.csv").create(SMALL_CSV, progress=False)

    existing = table.upload("data.csv").create(
        SMALL_CSV, if_not_exists=True, progress=False
    )
    assert existing.uri == original.uri
    assert len(table.list_uploads()) == 1

    renamed = table.upload("data.csv").create(
        SMALL_CSV, rename_on_conflict=True, progress=False
    )
    assert renamed.name != "data.csv"
    assert len(table.list_uploads()) == 2

    table.upload("data.csv").create(
        b"id,name,value\n9,z,9.5\n", replace_on_conflict=True, progress=False
    )
    assert len(table.list_uploads()) == 2
    assert table.get().properties["numRows"] == 4

    with pytest.raises(exceptions.ValueError):
        table.upload("data.csv").create(
            SMALL_CSV, rename_on_conflict=True, replace_on_conflict=True
        )


JAGGED_CSV = b"a,b\n1,2\n3\n4,5\n"


def test_failed_upload_raises(new_table):
    table = new_table()

    with pytest.raises(exceptions.JobError) as excinfo:
        table.upload("jagged.csv").create(JAGGED_CSV, progress=False)

    assert excinfo.value.status == "failed"
    assert table.upload("jagged.csv").exists()


def test_failed_upload_can_be_removed_without_raising(new_table):
    table = new_table()

    upload = table.upload("jagged.csv").create(
        JAGGED_CSV, remove_on_fail=True, raise_on_fail=False, progress=False
    )

    assert upload.properties["status"] == "failed"
    assert not upload.exists()


def test_allow_jagged_rows(new_table):
    table = new_table()

    table.upload("jagged.csv").create(
        JAGGED_CSV, allow_jagged_rows=True, progress=False
    )

    df = read_sorted(table, "a")
    assert df["a"].tolist() == [1, 3, 4]
    assert df["b"].isna().tolist() == [False, True, False]


def test_skip_bad_records(new_table):
    table = new_table()

    table.upload("jagged.csv").create(JAGGED_CSV, skip_bad_records=True, progress=False)

    assert read_sorted(table, "a")["a"].tolist() == [1, 4]


def large_csv(num_rows):
    lines = ["id,text"] + [
        f"{i},row number {i} of the large upload" for i in range(num_rows)
    ]
    return ("\n".join(lines) + "\n").encode()


def test_large_upload_goes_through_temp_storage(new_table):
    # Content over 1MB is sent to temp storage first, rather than with the request
    table = new_table()
    content = large_csv(40_000)
    assert len(content) > 2**20

    upload = table.upload("large.csv").create(io.BytesIO(content), progress=False)

    assert upload.properties["numRows"] == 40_000
    assert table.get().properties["numRows"] == 40_000


@pytest.mark.slow
def test_resumable_upload(new_table, tmp_path):
    # Files of 32MB or more are uploaded resumably
    table = new_table()
    path = tmp_path / "huge.csv"
    path.write_bytes(large_csv(1_000_000))
    assert path.stat().st_size >= 2**25

    upload = table.upload().create(path, progress=False)

    assert upload.properties["numRows"] == 1_000_000


def test_transfer_from_a_redivis_table(new_table, tiny_table):
    table = new_table()

    table.upload("transferred").create(
        transfer_specification={
            "sourceType": "redivis",
            "sourcePath": tiny_table.qualified_reference,
        },
        progress=False,
    )

    actual = helpers.sorted_by(
        table.to_pandas_dataframe(progress=False, dtype_backend="numpy")
    )
    pandas.testing.assert_frame_equal(
        actual, helpers.tiny_dataframe(), check_dtype=False
    )


def test_stream_upload(new_table):
    table = new_table()
    upload = table.upload("stream").create(
        type="stream",
        schema=[
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "string"},
            {"name": "when", "type": "dateTime"},
        ],
        progress=False,
    )

    upload.insert_rows(
        [
            {"id": 1, "name": "a", "when": "2020-01-01T00:00:00"},
            {"id": 2, "name": None, "when": None},
        ]
    )
    upload.insert_rows([{"id": 3, "name": "c", "when": "2021-06-15T12:30:00"}])

    assert {v.name: v["type"] for v in upload.list_variables()} == {
        "id": "integer",
        "name": "string",
        "when": "dateTime",
    }
    df = helpers.sorted_by(
        upload.to_pandas_dataframe(progress=False, dtype_backend="numpy_nullable")
    )
    assert df["id"].tolist() == [1, 2, 3]
    assert df["name"].isna().tolist() == [False, True, False]


def test_upload_lifecycle(new_table):
    table = new_table()
    upload = table.upload("small.csv")
    assert not upload.exists()

    upload.create(SMALL_CSV, progress=False)
    assert upload.exists()
    assert upload.get().properties["numRows"] == 3
    assert [v.name for v in upload.list_variables()] == ["id", "name", "value"]
    assert upload.variable("name").get().properties["type"] == "string"

    upload.delete()
    assert not upload.exists()
    assert table.get().properties["numRows"] == 0
