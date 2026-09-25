"""Reads and downloads that go through the export API."""

import os
import re

import pytest
import redivis
from redivis.classes.Upload import Upload
from redivis.common import exceptions

from mock_redivis_api import DEFAULT_NUM_ROWS as N, paths


def test_large_query_reads_via_export(api):
    api["query_bytes"] = int(2e9)

    df = redivis.query("SELECT 1").to_pandas_dataframe(progress=False)

    assert [p for p in paths() if p.endswith("/exports")] == [
        "/api/v1/queries/q1/exports"
    ]
    assert not paths("/readSessions")
    assert sorted(df["id"].tolist()) == list(range(N))


def test_query_download_is_named_by_finish_time(api, tmp_path):
    [path] = redivis.query("SELECT 1").download(
        f"{tmp_path}/", format="parquet", progress=False
    )

    assert re.fullmatch(
        r"query_\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}_\d+\.parquet",
        os.path.basename(path),
    )


def test_table_download_is_named_by_table(api, table, tmp_path):
    [path] = table.download(f"{tmp_path}/", format="parquet", progress=False)

    assert os.path.basename(path) == "mock.parquet"
    assert os.path.getsize(path) > 0


def test_upload_download_fetches_its_own_properties(api, table, tmp_path):
    upload = Upload("u1", table=table)  # properties=None until fetched

    [path] = upload.download(f"{tmp_path}/", format="parquet", progress=False)

    assert os.path.basename(path) == "my_upload.parquet"


def test_incomplete_upload_cannot_be_read(api, table):
    api["upload_status"] = "running"

    with pytest.raises(exceptions.ValueError, match="status: running"):
        Upload("u1", table=table).to_arrow_table(progress=False)
