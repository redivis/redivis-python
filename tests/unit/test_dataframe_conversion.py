"""Converting the dataframes that can be uploaded to the parquet file that's sent."""

import datetime
import os

import dask
import dask.dataframe as dd
import pandas
import pyarrow
import pyarrow.parquet
import pytest

from redivis.common.util import convert_data_to_parquet

DAYS = [datetime.date(2020, 1, 1), datetime.date(2021, 6, 15), None]


def read(path):
    assert os.path.isfile(path)
    return pyarrow.parquet.read_table(path).sort_by("id")


@pytest.mark.parametrize("npartitions", [1, 3])
def test_dask_dataframes_are_converted_whole(npartitions):
    df = pandas.DataFrame({"id": range(100), "value": [i / 2 for i in range(100)]})

    table = read(convert_data_to_parquet(dd.from_pandas(df, npartitions=npartitions)))

    assert table.column("id").to_pylist() == list(range(100))
    assert table.column("value").to_pylist() == [i / 2 for i in range(100)]


def test_dask_dataframes_keep_their_types():
    df = pandas.DataFrame(
        {
            "id": [1, 2, 3],
            "day": pandas.array(DAYS, dtype=pandas.ArrowDtype(pyarrow.date32())),
            "time": pandas.to_datetime(["2020-01-01 12:30", None, "1999-12-31 00:00"]),
        }
    )

    table = read(convert_data_to_parquet(dd.from_pandas(df, npartitions=2)))

    assert table.schema.field("day").type == pyarrow.date32()
    assert table.column("day").to_pylist() == DAYS
    assert table.schema.field("time").type == pyarrow.timestamp("us")


def test_dask_object_columns_of_dates_stay_dates():
    # By default, dask converts object columns to strings when the DataFrame is
    # created, so such a column only holds dates with that turned off. It's still on
    # when the DataFrame is uploaded.
    with dask.config.set({"dataframe.convert-string": False}):
        df = dd.from_pandas(
            pandas.DataFrame(
                {"id": [1, 2, 3], "day": DAYS, "text": ["a", None, "c"], "empty": None}
            ),
            npartitions=2,
        )

    table = read(convert_data_to_parquet(df))

    assert table.schema.field("day").type == pyarrow.date32()
    assert table.column("day").to_pylist() == DAYS
    assert table.column("text").to_pylist() == ["a", None, "c"]
    assert table.column("empty").null_count == 3


def test_dask_partitions_are_cleaned_up(tmp_path, monkeypatch):
    monkeypatch.setenv("REDIVIS_TMPDIR", str(tmp_path))
    df = dd.from_pandas(pandas.DataFrame({"id": [1, 2, 3]}), npartitions=2)

    path = convert_data_to_parquet(df)

    assert os.listdir(os.path.dirname(path)) == [os.path.basename(path)]
