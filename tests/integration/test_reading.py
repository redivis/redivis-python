"""Reading tables and uploads into each of the supported formats, verified against the
data that was uploaded."""

import pandas
import pyarrow
import pytest

import redivis
from redivis import exceptions

from . import helpers


@pytest.fixture(scope="module")
def expected():
    return helpers.tiny_dataframe()


def assert_matches_tiny(df, expected):
    pandas.testing.assert_frame_equal(
        helpers.sorted_by(df), expected, check_dtype=False
    )


@pytest.mark.parametrize("dtype_backend", ["pyarrow", "numpy", "numpy_nullable"])
def test_to_pandas_dataframe(tiny_table, expected, dtype_backend):
    df = tiny_table.to_pandas_dataframe(progress=False, dtype_backend=dtype_backend)

    assert_matches_tiny(df.astype(object), expected.astype(object))
    if dtype_backend == "pyarrow":
        assert all(isinstance(dtype, pandas.ArrowDtype) for dtype in df.dtypes)
    elif dtype_backend == "numpy_nullable":
        assert df["id"].dtype == pandas.Int64Dtype()
    else:
        assert df["id"].dtype == "int64"


def test_invalid_dtype_backend(tiny_table):
    with pytest.raises(exceptions.ValueError):
        tiny_table.to_pandas_dataframe(progress=False, dtype_backend="nope")


def test_to_arrow_table(tiny_table, expected):
    table = tiny_table.to_arrow_table(progress=False)

    assert isinstance(table, pyarrow.Table)
    assert table.schema.names == list(expected.columns)
    assert table.schema.field("id").type == pyarrow.int64()
    assert table.schema.field("Boolean").type == pyarrow.bool_()
    assert_matches_tiny(table.to_pandas(), expected)


def test_to_arrow_dataset(tiny_table, expected):
    dataset = tiny_table.to_arrow_dataset(progress=False)

    assert dataset.count_rows() == len(expected)
    assert_matches_tiny(dataset.to_table().to_pandas(), expected)


def test_to_arrow_batch_iterator(tiny_table, expected):
    batches = list(tiny_table.to_arrow_batch_iterator(progress=False))

    assert all(isinstance(batch, pyarrow.RecordBatch) for batch in batches)
    assert_matches_tiny(pyarrow.Table.from_batches(batches).to_pandas(), expected)


def test_to_polars_lazyframe(tiny_table, expected):
    import polars

    lazyframe = tiny_table.to_polars_lazyframe(progress=False)

    assert isinstance(lazyframe, polars.LazyFrame)
    assert_matches_tiny(lazyframe.collect().to_pandas(), expected)


def test_to_dask_dataframe(tiny_table, expected):
    df = tiny_table.to_dask_dataframe(progress=False)

    assert_matches_tiny(df.compute().reset_index(drop=True), expected)


def test_to_dataframe_is_deprecated(tiny_table, expected):
    with pytest.warns(FutureWarning, match="to_pandas_dataframe"):
        df = tiny_table.to_dataframe(progress=False)

    assert_matches_tiny(df, expected)


def test_list_rows_is_removed(tiny_table):
    with pytest.raises(exceptions.DeprecationError):
        tiny_table.list_rows()


def test_max_results(tiny_table):
    df = tiny_table.to_pandas_dataframe(10, progress=False)

    assert len(df) == 10


def test_zero_max_results(typed_table):
    table = typed_table.to_arrow_table(max_results=0, progress=False)

    assert table.num_rows == 0
    assert table.schema == helpers.typed_arrow_table().schema


def test_table_without_rows(new_table):
    table = new_table()
    table.upload("header_only.csv").create(b"id,name\n", progress=False)

    df = table.to_pandas_dataframe(progress=False)

    assert len(df) == 0
    assert list(df.columns) == ["id", "name"]


def test_selected_variables_are_returned_in_the_order_given(tiny_table, expected):
    # Variable names are matched case-insensitively
    df = tiny_table.to_pandas_dataframe(variables=["text", "ID"], progress=False)

    assert list(df.columns) == ["Text", "id"]
    assert_matches_tiny(df, expected[["Text", "id"]].sort_values("id"))


def test_read_by_qualified_reference(tiny_table, expected):
    table = redivis.table(tiny_table.qualified_reference)

    assert_matches_tiny(table.to_pandas_dataframe(progress=False), expected)


def test_read_upload(tiny_table, expected):
    upload = tiny_table.upload("tiny.csv")

    assert_matches_tiny(upload.to_pandas_dataframe(progress=False), expected)


@pytest.mark.parametrize("target_count", [1, 3])
def test_read_streams(tiny_table, expected, target_count):
    streams = tiny_table.to_read_streams(target_count)

    assert 1 <= len(streams) <= target_count
    combined = pandas.concat(
        [stream.to_pandas_dataframe(progress=False) for stream in streams]
    )
    assert_matches_tiny(combined.reset_index(drop=True), expected)


def test_read_streams_with_selected_variables(tiny_table, expected):
    streams = tiny_table.to_read_streams(2, variables=["Key", "id"])

    combined = pyarrow.concat_tables(
        [stream.to_arrow_table(progress=False) for stream in streams]
    )
    assert combined.schema.names == ["Key", "id"]
    assert_matches_tiny(combined.to_pandas(), expected[["Key", "id"]])


def test_read_streams_reject_an_invalid_count(tiny_table):
    with pytest.raises(exceptions.ValueError):
        tiny_table.to_read_streams(0)


def test_typed_values_round_trip(typed_table):
    expected = helpers.typed_arrow_table()

    actual = typed_table.to_arrow_table(progress=False).sort_by("id")

    assert actual.schema == expected.schema
    assert actual.equals(expected)


def test_typed_values_to_pandas(typed_table):
    expected = helpers.typed_arrow_table().to_pandas()

    actual = helpers.sorted_by(
        typed_table.to_pandas_dataframe(
            progress=False, dtype_backend="numpy", date_as_object=True
        )
    )

    pandas.testing.assert_frame_equal(actual, expected)


@pytest.mark.parametrize("format", ["csv", "parquet"])
def test_download(tiny_table, expected, tmp_path, format):
    paths = tiny_table.download(tmp_path, format=format, progress=False)

    assert [p.rsplit("/", 1)[1] for p in paths] == [f"tiny.{format}"]
    if format == "csv":
        downloaded = pandas.read_csv(paths[0], keep_default_na=False)
    else:
        downloaded = pandas.read_parquet(paths[0])
    assert_matches_tiny(downloaded, expected)


def test_download_to_a_file_path(tiny_table, tmp_path):
    path = tmp_path / "out" / "my_table.csv"

    assert tiny_table.download(str(path), progress=False) == [str(path)]
    assert len(pandas.read_csv(path)) == len(helpers.tiny_dataframe())

    with pytest.raises(exceptions.ValueError, match="already exists"):
        tiny_table.download(str(path), progress=False)
    tiny_table.download(str(path), overwrite=True, progress=False)


def test_download_upload(tiny_table, expected, tmp_path):
    paths = tiny_table.upload("tiny.csv").download(tmp_path, progress=False)

    assert_matches_tiny(pandas.read_csv(paths[0], keep_default_na=False), expected)
