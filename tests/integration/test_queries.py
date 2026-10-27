"""SQL queries, run against the uploaded sample data."""

import datetime

import pandas
import pyarrow
import pytest

import redivis
from redivis import exceptions

from . import helpers


@pytest.fixture(scope="module")
def expected():
    return helpers.tiny_dataframe()


def data_columns(df, expected):
    """Queries of an unreleased version's tables also return the columns that
    identify each row's upload (_IMPORT_ID and _IMPORT_NAME)."""
    return df[expected.columns]


def test_global_query(tiny_table, expected):
    query = redivis.query(
        f"""
        SELECT Key, COUNT(*) AS n, SUM(Numeric) AS total
        FROM `{tiny_table.qualified_reference}`
        GROUP BY Key
        """
    )

    actual = helpers.sorted_by(query.to_pandas_dataframe(progress=False), "Key")

    grouped = expected.groupby("Key").agg(n=("id", "size"), total=("Numeric", "sum"))
    pandas.testing.assert_frame_equal(
        actual, grouped.reset_index(), check_dtype=False
    )


def test_query_scoped_to_a_dataset(sample_dataset, tiny_table, expected):
    query = sample_dataset.query("SELECT * FROM tiny WHERE Boolean ORDER BY id")

    actual = query.to_pandas_dataframe(progress=False)

    pandas.testing.assert_frame_equal(
        helpers.sorted_by(data_columns(actual, expected)),
        expected[expected["Boolean"]].reset_index(drop=True),
        check_dtype=False,
    )


def test_query_properties_and_variables(tiny_table):
    query = redivis.query(
        f"SELECT id, Key FROM `{tiny_table.qualified_reference}` WHERE id <= 10"
    )

    variables = query.list_variables()

    assert [(v.name, v["type"]) for v in variables] == [
        ("id", "integer"),
        ("Key", "string"),
    ]
    assert query.properties["status"] == "completed"
    assert query.get().properties["outputNumRows"] == 10
    assert query.variable("Key").get().properties["type"] == "string"


def test_literal_types():
    query = redivis.query(
        """
        SELECT
            1 AS int_col,
            1.5 AS float_col,
            'text' AS string_col,
            TRUE AS bool_col,
            DATE '2020-01-02' AS date_col,
            DATETIME '2020-01-02 03:04:05' AS datetime_col,
            TIME '03:04:05' AS time_col
        UNION ALL
        SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL
        """
    )

    table = query.to_arrow_table(progress=False)

    assert table.schema.field("int_col").type == pyarrow.int64()
    assert table.schema.field("float_col").type == pyarrow.float64()
    assert table.schema.field("string_col").type == pyarrow.string()
    assert table.schema.field("bool_col").type == pyarrow.bool_()
    assert table.schema.field("date_col").type == pyarrow.date32()
    rows = sorted(table.to_pylist(), key=lambda row: row["int_col"] is None)
    assert rows[0] == {
        "int_col": 1,
        "float_col": 1.5,
        "string_col": "text",
        "bool_col": True,
        "date_col": datetime.date(2020, 1, 2),
        "datetime_col": datetime.datetime(2020, 1, 2, 3, 4, 5),
        "time_col": datetime.time(3, 4, 5),
    }
    assert set(rows[1].values()) == {None}


def test_empty_result(tiny_table):
    query = redivis.query(
        f"SELECT id, Key FROM `{tiny_table.qualified_reference}` WHERE FALSE"
    )

    df = query.to_pandas_dataframe(progress=False)

    assert len(df) == 0
    assert list(df.columns) == ["id", "Key"]


def test_invalid_query_raises():
    query = redivis.query("SELECT * FROM a_table_that_does_not_exist")

    with pytest.raises((exceptions.JobError, exceptions.APIError)):
        query.to_pandas_dataframe(progress=False)


def test_query_read_streams(tiny_table, expected):
    query = redivis.query(f"SELECT * FROM `{tiny_table.qualified_reference}`")

    streams = query.to_read_streams(2)

    combined = pandas.concat(
        [stream.to_pandas_dataframe(progress=False) for stream in streams]
    )
    pandas.testing.assert_frame_equal(
        helpers.sorted_by(data_columns(combined, expected)), expected, check_dtype=False
    )


def test_query_download(tiny_table, expected, tmp_path):
    query = redivis.query(f"SELECT * FROM `{tiny_table.qualified_reference}`")

    paths = query.download(tmp_path, progress=False)

    downloaded = pandas.read_csv(paths[0], keep_default_na=False)
    pandas.testing.assert_frame_equal(
        helpers.sorted_by(data_columns(downloaded, expected)),
        expected,
        check_dtype=False,
    )
