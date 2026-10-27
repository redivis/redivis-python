"""Test data, and helpers for working with a live Redivis API."""

import datetime
import json
import random
import time
from pathlib import Path

import pandas
import pyarrow

from redivis.common import exceptions

DATA_DIR = Path(__file__).parents[1] / "data"
TINY_CSV = DATA_DIR / "tiny.csv"

# Uploads and other jobs normally finish in well under a minute
JOB_TIMEOUT_SECONDS = 15 * 60


def tiny_dataframe():
    """The contents of tiny.csv, as Redivis should read them."""
    return pandas.read_csv(TINY_CSV, keep_default_na=False)


def typed_arrow_table():
    """A table with a column for each of the scalar types that Redivis supports,
    including nulls and the sorts of strings that trip up parsers."""
    return pyarrow.table(
        {
            "id": pyarrow.array([1, 2, 3, 4, 5], pyarrow.int64()),
            "float_col": pyarrow.array([1.5, -2.25, None, 0.0, 1e10], pyarrow.float64()),
            "string_col": pyarrow.array(
                ["plain", "with, comma", 'with "quotes"', "multi\nline", None],
                pyarrow.string(),
            ),
            "unicode_col": pyarrow.array(
                ["héllo", "日本語", "emoji 🎉", "tab\there", None], pyarrow.string()
            ),
            "bool_col": pyarrow.array([True, False, None, True, False], pyarrow.bool_()),
            "date_col": pyarrow.array(
                [
                    datetime.date(2020, 1, 1),
                    datetime.date(1999, 12, 31),
                    None,
                    datetime.date(2024, 2, 29),
                    datetime.date(1970, 1, 1),
                ],
                pyarrow.date32(),
            ),
            "datetime_col": pyarrow.array(
                [
                    datetime.datetime(2020, 1, 1, 12, 30, 15, 123456),
                    datetime.datetime(1999, 12, 31, 23, 59, 59),
                    datetime.datetime(2024, 2, 29, 0, 0, 0),
                    None,
                    datetime.datetime(1970, 1, 1, 0, 0, 0),
                ],
                pyarrow.timestamp("us"),
            ),
        }
    )


# The Redivis type of each column of typed_arrow_table()
TYPED_VARIABLE_TYPES = {
    "id": "integer",
    "float_col": "float",
    "string_col": "string",
    "unicode_col": "string",
    "bool_col": "boolean",
    "date_col": "date",
    "datetime_col": "dateTime",
}


GEO_FEATURES = [
    {"name": "point", "value": 1, "geometry": {"type": "Point", "coordinates": [-122.17, 37.43]}},
    {
        "name": "line",
        "value": 2,
        "geometry": {"type": "LineString", "coordinates": [[-122.0, 37.0], [-121.0, 38.0]]},
    },
    {
        "name": "polygon",
        "value": 3,
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-122.0, 37.0], [-121.0, 37.0], [-121.0, 38.0], [-122.0, 37.0]]],
        },
    },
]


def geojson_bytes():
    return json.dumps(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": feature["geometry"],
                    "properties": {"name": feature["name"], "value": feature["value"]},
                }
                for feature in GEO_FEATURES
            ],
        }
    ).encode()


def raw_files():
    """The files uploaded to the file index table, by path."""
    return {
        "readme.txt": "A file for testing.\nIt has a few lines,\nincluding ünïcödé.\n".encode(),
        "data/numbers.csv": "".join(f"{i},{i * i}\n" for i in range(1000)).encode(),
        # Large enough to need several reads when streamed
        "data/nested/random.bin": random.Random(0).getrandbits(8 * 3 * 2**20).to_bytes(
            3 * 2**20, "little"
        ),
        "with space & symbols.txt": b"named awkwardly",
    }


def write_files(directory, files):
    for path, content in files.items():
        file_path = Path(directory) / path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(content)
    return Path(directory)


def add_data(dataset, table_name="data"):
    """Give a dataset some data, which it needs before it can be released."""
    table = dataset.table(table_name).create()
    table.upload("data.csv").create(b"a\n1\n", progress=False)
    return table


def wait_for_upload(upload, timeout=JOB_TIMEOUT_SECONDS):
    """Wait for an upload started with wait_for_finish=False, raising if it fails."""
    deadline = time.monotonic() + timeout
    while True:
        status = upload.get().properties["status"]
        if status == "completed":
            return upload
        if status == "failed":
            raise exceptions.JobError(
                message=upload.properties.get("errorMessage"), status=status
            )
        if time.monotonic() > deadline:
            raise TimeoutError(f"{upload} didn't finish within {timeout}s ({status})")
        time.sleep(2)


def delete_dataset(dataset, timeout=JOB_TIMEOUT_SECONDS):
    """Delete a dataset, waiting out a release that's still in progress."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            dataset.delete()
            return
        except exceptions.NotFoundError:
            return
        except exceptions.APIError as error:
            if "process of being released" not in str(error) or (
                time.monotonic() > deadline
            ):
                raise
            time.sleep(2)


def sorted_by(df, column="id"):
    """Rows come back in no particular order, so compare them sorted."""
    return df.sort_values(column, ignore_index=True)
