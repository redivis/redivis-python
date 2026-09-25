"""How table reads are routed (listRows vs. read sessions) and recover from interruptions."""

import pytest
import redivis
from redivis.common import exceptions
from redivis.common.fetch_rows import LIST_ROWS_MAX_BYTES

from mock_redivis_api import DEFAULT_NUM_ROWS as N, TABLE_PATH, paths

ROWS = f"{TABLE_PATH}/rows"
READ_SESSIONS = f"{TABLE_PATH}/readSessions"


def data_requests():
    return paths("/rows", "/readSessions", "/readStreams/")


def list_rows_queries(api):
    return [q for _, path, q, _ in api["requests"] if path == ROWS]


# --- routing -----------------------------------------------------------------


def test_small_table_reads_in_a_single_list_rows_request(api, table):
    df = table.to_pandas_dataframe(progress=False)

    assert data_requests() == [ROWS]
    assert list_rows_queries(api) == [{"format": "arrow"}]
    assert len(df) == N
    assert list(df.columns) == ["id", "name", "val"]
    assert df["id"].tolist() == list(range(N))


def test_large_table_reads_via_read_session(api, table):
    api["num_bytes"] = int(5e8)

    df = table.to_pandas_dataframe(progress=False)

    assert data_requests()[0] == READ_SESSIONS
    assert sorted(data_requests()[1:]) == [
        "/api/v1/readStreams/s0",
        "/api/v1/readStreams/s1",
    ]
    assert sorted(df["id"].tolist()) == list(range(N))


@pytest.mark.parametrize(
    "num_bytes, expected",
    [
        (int(LIST_ROWS_MAX_BYTES) - 1, [ROWS]),
        (int(LIST_ROWS_MAX_BYTES), [READ_SESSIONS]),
    ],
)
def test_size_threshold(api, table, num_bytes, expected):
    api["num_bytes"] = num_bytes

    table.to_arrow_table(progress=False)

    assert data_requests()[:1] == expected


@pytest.mark.parametrize(
    "max_results, expected",
    [
        (50, ROWS),  # ~5e7 bytes of a 5e8 byte table
        (400, READ_SESSIONS),  # ~4e8 bytes
        (N * 10, READ_SESSIONS),  # capped at the table's size
    ],
)
def test_max_results_scales_the_size_estimate(api, table, max_results, expected):
    api["num_bytes"] = int(5e8)

    df = table.to_pandas_dataframe(max_results=max_results, progress=False)

    assert data_requests()[0] == expected
    assert len(df) == min(max_results, N)


def test_max_results_and_variables_are_forwarded(api, table):
    df = table.to_pandas_dataframe(
        max_results=7, variables=["val", "id"], progress=False
    )

    assert list_rows_queries(api) == [
        {"format": "arrow", "maxResults": "7", "selectedVariables": "val,id"}
    ]
    assert len(df) == 7
    assert list(df.columns) == ["val", "id"]


@pytest.mark.parametrize(
    "read",
    [
        lambda t: t.to_arrow_table(progress=False).num_rows,
        lambda t: t.to_arrow_dataset(progress=False).count_rows(),
        lambda t: t.to_polars_lazyframe(progress=False).collect().height,
        lambda t: sum(b.num_rows for b in t.to_arrow_batch_iterator(progress=False)),
    ],
    ids=["arrow_table", "arrow_dataset", "polars_lazyframe", "arrow_batch_iterator"],
)
def test_every_output_type_reads_via_list_rows(api, table, read):
    assert read(table) == N
    assert data_requests() == [ROWS]


def test_dataset_tables_read_via_list_rows(api, table):
    api["container_kind"] = "dataset"  # schema coercion applies

    df = table.to_pandas_dataframe(progress=False)

    assert data_requests() == [ROWS]
    assert len(df) == N


def test_queries_never_use_list_rows(api):
    df = redivis.query("SELECT 1").to_pandas_dataframe(progress=False)

    assert len(df) == N
    assert not paths("/rows")


# --- table metadata freshness ------------------------------------------------


def test_new_table_object_fetches_metadata_once(api, table):
    table.to_pandas_dataframe(progress=False)

    assert paths().count(TABLE_PATH) == 1


def test_reused_table_object_refetches_metadata_before_reading(api, table):
    table.to_pandas_dataframe(progress=False)
    api["num_rows"] = 300  # the table shrinks, e.g. after a replace-merge upload
    api["requests"].clear()

    df = table.to_pandas_dataframe(progress=False)

    assert TABLE_PATH in paths()
    assert len(df) == 300
    # A stale row count would make the complete read look truncated
    assert data_requests() == [ROWS]


# --- interrupted listRows reads ----------------------------------------------


def test_truncated_list_rows_is_reread_from_the_start(api, table):
    api["truncate_rows_once"] = 120

    df = table.to_pandas_dataframe(progress=False)

    assert data_requests() == [ROWS, ROWS]
    assert df["id"].tolist() == list(range(N))


def test_truncated_list_rows_on_disk_leaves_no_duplicates(api, table):
    api["truncate_rows_once"] = 120

    result = table.to_arrow_dataset(progress=False).to_table()

    assert sorted(result.column("id").to_pylist()) == list(range(N))


def test_truncated_iterator_raises_rather_than_duplicating_rows(api, table):
    # The iterator has already handed out batches, and a re-issued listRows
    # request isn't guaranteed to return rows in the same order.
    api["truncate_rows_once"] = 120

    with pytest.raises(exceptions.NetworkError, match="cannot be resumed"):
        for _ in table.to_arrow_batch_iterator(progress=False):
            pass


def test_iterator_retries_a_failure_before_any_batch(api, table):
    api["fail_rows_once"] = True

    total = sum(b.num_rows for b in table.to_arrow_batch_iterator(progress=False))

    assert total == N
    assert data_requests() == [ROWS, ROWS]


# --- read streams ------------------------------------------------------------


def test_read_stream_outputs(api, table):
    streams = table.to_read_streams(target_count=2)

    assert len(streams[0].to_pandas_dataframe(progress=False)) == N // 2
    assert streams[1].to_arrow_table(progress=False).num_rows == N - N // 2
    assert not paths("/exports")


def test_read_streams_fetch_their_parent_once(api, table):
    streams = table.to_read_streams(target_count=2)
    for stream in streams:
        stream.to_arrow_table(progress=False)
    streams[0].to_arrow_table(progress=False)

    assert paths().count(TABLE_PATH) == 1


# --- anonymous access --------------------------------------------------------


def test_public_table_reads_anonymously(anonymous, table):
    df = table.to_pandas_dataframe(progress=False)

    assert len(df) == N
    assert not any(had_auth for *_, had_auth in anonymous["requests"])
