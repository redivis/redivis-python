"""How table reads are routed (listRows vs. read sessions) and recover from interruptions."""

import os
import threading

import pytest
import redivis
from redivis.common import exceptions
from redivis.common.fetch_rows import LIST_ROWS_MAX_BYTES

from .mock_redivis_api import DEFAULT_NUM_ROWS as N, TABLE_PATH, paths

ROWS = f"{TABLE_PATH}/rows"
READ_SESSIONS = f"{TABLE_PATH}/readSessions"
QUERIES = "/api/v1/queries"
QUERY = "/api/v1/queries/q1"


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


@pytest.mark.parametrize("output_type", ["arrow_table", "arrow_iterator"])
def test_rows_listrows_cant_serve_are_read_through_a_read_session(
    api, table, output_type
):
    api["rows_read_session_required"] = True

    if output_type == "arrow_iterator":
        num_rows = sum(
            batch.num_rows for batch in table.to_arrow_batch_iterator(progress=False)
        )
    else:
        num_rows = table.to_arrow_table(progress=False).num_rows

    assert num_rows == N
    assert data_requests()[:2] == [ROWS, READ_SESSIONS]


def test_other_list_rows_errors_are_raised(api, table):
    api["response_script"] = [
        (None, None),  # The table
        (None, None),  # Its variables
        (400, {"status": 400, "error": "invalid_request", "error_description": "No"}),
    ]

    with pytest.raises(exceptions.APIError, match="No"):
        table.to_arrow_table(progress=False)
    assert not paths("/readSessions")


# --- queries -----------------------------------------------------------------


def list_rows_options(api):
    """The listRows options sent with each POST /queries."""
    return [
        body.get("listRows")
        for method, path, body in api["json_bodies"]
        if (method, path) == ("POST", QUERIES)
    ]


def test_quick_query_is_read_from_the_response_that_runs_it(api):
    query = redivis.query("SELECT 1")
    df = query.to_pandas_dataframe(progress=False)

    assert list_rows_options(api) == [
        {"maxResults": None, "format": "arrow", "eosSentinel": True}
    ]
    assert paths() == [QUERIES, f"{QUERY}/variables"]
    assert df["id"].tolist() == list(range(N))
    assert list(df.columns) == ["id", "name", "val"]
    assert query.uri == "/queries/q1"
    assert query.properties["status"] == "completed"


@pytest.mark.parametrize(
    "read",
    [
        lambda q: q.to_arrow_table(progress=False).num_rows,
        lambda q: q.to_arrow_dataset(progress=False).count_rows(),
        lambda q: q.to_polars_lazyframe(progress=False).collect().height,
        lambda q: sum(b.num_rows for b in q.to_arrow_batch_iterator(progress=False)),
    ],
    ids=["arrow_table", "arrow_dataset", "polars_lazyframe", "arrow_batch_iterator"],
)
def test_every_output_type_reads_a_quick_query_inline(api, read):
    assert read(redivis.query("SELECT 1")) == N
    assert paths() == [QUERIES, f"{QUERY}/variables"]


def test_inline_query_reads_respect_max_results_and_variables(api):
    df = redivis.query("SELECT 1").to_pandas_dataframe(
        max_results=7, variables=["VAL", "id"], progress=False
    )

    assert list_rows_options(api) == [
        {
            "maxResults": 7,
            "format": "arrow",
            "eosSentinel": True,
            "selectedVariables": ["VAL", "id"],
        }
    ]
    assert paths() == [QUERIES, f"{QUERY}/variables"]
    assert list(df.columns) == ["val", "id"]
    assert df["id"].tolist() == list(range(7))


def test_inline_query_reads_accept_any_iterable_of_variables(api):
    df = redivis.query("SELECT 1").to_pandas_dataframe(
        variables=(name for name in ["val", "id"]), progress=False
    )

    assert list_rows_options(api)[0]["selectedVariables"] == ["val", "id"]
    assert list(df.columns) == ["val", "id"]


def test_invalid_variables_are_reported_by_the_client_not_sent(api):
    with pytest.raises(Exception):
        redivis.query("SELECT 1").to_pandas_dataframe(variables=[1], progress=False)

    assert "selectedVariables" not in list_rows_options(api)[0]


def test_empty_query_result_is_read_inline(api):
    api["num_rows"] = 0

    table = redivis.query("SELECT 1").to_arrow_table(progress=False)

    assert table.num_rows == 0
    assert table.column_names == ["id", "name", "val"]
    assert paths() == [QUERIES, f"{QUERY}/variables"]


def test_truncated_inline_rows_are_requested_again(api):
    # Without the end-of-stream sentinel, the response was cut off
    api["truncate_rows_once"] = 120

    df = redivis.query("SELECT 1").to_pandas_dataframe(max_results=300, progress=False)

    assert paths() == [QUERIES, f"{QUERY}/variables", f"{QUERY}/rows"]
    assert [q for _, path, q, _ in api["requests"] if path == f"{QUERY}/rows"] == [
        {"format": "arrow", "maxResults": "300", "eosSentinel": "true"}
    ]
    assert df["id"].tolist() == list(range(300))


@pytest.mark.parametrize(
    "configure",
    [
        # An older API, which ignores listRows
        lambda api: api.update(query_responds_with_rows=False),
        # Rows too large to list
        lambda api: api.update(query_bytes=int(5e8)),
    ],
    ids=["old_api", "large_result"],
)
def test_queries_that_respond_with_themselves_are_read_as_usual(api, configure):
    configure(api)

    df = redivis.query("SELECT 1").to_pandas_dataframe(progress=False)

    assert list_rows_options(api)[0] is not None
    assert paths()[:3] == [QUERIES, f"{QUERY}/variables", f"{QUERY}/readSessions"]
    assert not paths("/rows")
    assert sorted(df["id"].tolist()) == list(range(N))


@pytest.mark.parametrize(
    "operation",
    [
        lambda q: q.get(),
        lambda q: q.list_variables(),
        lambda q: q.to_read_streams(target_count=2),
        lambda q: q.list_files(),
        lambda q, tmp_path: q.download(f"{tmp_path}/", progress=False),
    ],
    ids=["get", "list_variables", "to_read_streams", "list_files", "download"],
)
def test_operations_that_dont_read_rows_dont_request_them(api, tmp_path, operation):
    query = redivis.query("SELECT 1")
    if operation.__code__.co_argcount == 2:
        operation(query, tmp_path)
    else:
        operation(query)

    assert list_rows_options(api) == [None]


def test_query_run_before_reading_is_read_as_usual(api):
    query = redivis.query("SELECT 1").get()
    df = query.to_pandas_dataframe(progress=False)

    assert list_rows_options(api) == [None]
    assert f"{QUERY}/readSessions" in paths()
    assert sorted(df["id"].tolist()) == list(range(N))


def test_query_read_inline_can_be_read_again(api, tmp_path):
    query = redivis.query("SELECT 1")
    query.to_arrow_table(progress=False)
    api["requests"].clear()

    # The query's size wasn't known, so it's fetched to size the read
    df = query.to_pandas_dataframe(progress=False)

    assert paths()[:3] == [f"{QUERY}/variables", QUERY, f"{QUERY}/readSessions"]
    assert sorted(df["id"].tolist()) == list(range(N))
    assert query.properties["outputNumRows"] == N

    # As well as its other properties, e.g. to name a download
    [path] = query.download(f"{tmp_path}/", format="parquet", progress=False)
    assert not os.path.basename(path).startswith("query_1970")


def test_concurrent_reads_run_the_query_once(api):
    query = redivis.query("SELECT 1")
    results = []

    def read():
        results.append(query.to_arrow_table(progress=False).num_rows)

    threads = [threading.Thread(target=read) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert results == [N] * 4
    assert paths().count(QUERIES) == 1
    # Only the read that ran the query was given its rows
    assert paths().count(f"{QUERY}/readSessions") == 3


def test_inline_rows_are_closed_if_the_read_fails_first(api, monkeypatch):
    from redivis.classes.Query import Query

    responses = []
    initiate = Query._initiate

    def capture(self, list_rows=None):
        response = initiate(self, list_rows)
        if response is not None:
            responses.append(response)
        return response

    monkeypatch.setattr(Query, "_initiate", capture)
    api["response_script"] = [
        (None, None),  # POST /queries
        (500, {"status": 500, "error": "internal", "error_description": "Oops"}),
    ]

    with pytest.raises(exceptions.APIError, match="Oops"):
        redivis.query("SELECT 1").to_arrow_table(progress=False)

    [response] = responses
    assert response.raw.closed


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


def test_query_read_streams_start_the_query(api):
    streams = redivis.query("SELECT 1").to_read_streams(target_count=2)

    assert paths("/queries", "/readSessions") == [
        "/api/v1/queries",
        "/api/v1/queries/q1/readSessions",
    ]
    assert sum(s.to_arrow_table(progress=False).num_rows for s in streams) == N


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
