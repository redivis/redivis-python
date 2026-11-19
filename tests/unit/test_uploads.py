"""Uploads: the temp upload protocols (still used by notebook output tables), the
imports that Upload.create() and Table.add_files() upload through, and their conflicts
and failures."""

import pathlib

import pytest
import niquests

from redivis.common import exceptions, imports, retryable_upload


class FakeResponse:
    def __init__(self, status_code=200, headers=None):
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise niquests.HTTPError(f"{self.status_code} error", response=self)


class FakeStorage:
    """Stands in for niquests.put / niquests.post against temp upload URLs.

    Plays out `script`: responses (or exceptions to raise) for successive requests.
    """

    def __init__(self, monkeypatch, script):
        self.script = list(script)
        self.requests = []
        monkeypatch.setattr(retryable_upload.niquests, "put", self.put)
        monkeypatch.setattr(retryable_upload.niquests, "post", self.post)

    def _respond(self, method, url, headers, data):
        body = data.read() if hasattr(data, "read") else data
        self.requests.append((method, url, dict(headers or {}), body))
        result = self.script.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def put(self, url, headers=None, data=None, verify=None):
        return self._respond("PUT", url, headers, data)

    def post(self, url, headers=None, data=None, verify=None):
        return self._respond("POST", url, headers, data)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(retryable_upload.time, "sleep", lambda seconds: None)


def test_standard_upload_resends_the_whole_body_after_a_failure(monkeypatch):
    import io

    storage = FakeStorage(
        monkeypatch, [niquests.ConnectionError("reset"), FakeResponse(200)]
    )
    retryable_upload.perform_standard_upload(
        data=io.BytesIO(b"hello world"), temp_upload_url="https://proxy/temp"
    )
    assert [body for _, _, _, body in storage.requests] == [b"hello world"] * 2


def test_standard_upload_accepts_bytes_with_a_progress_bar(monkeypatch):
    import io
    from tqdm.auto import tqdm

    storage = FakeStorage(monkeypatch, [FakeResponse(200)])
    progressbar = tqdm(total=5, file=io.StringIO())
    retryable_upload.perform_standard_upload(
        data=b"bytes", temp_upload_url="https://proxy/temp", progressbar=progressbar
    )
    assert storage.requests[0][3] == b"bytes"
    assert progressbar.n == 5


def test_resumable_upload_restarts_an_expired_session(monkeypatch):
    storage = FakeStorage(
        monkeypatch,
        [
            # Starting the session fails to connect, then succeeds
            niquests.ConnectionError("no route"),
            FakeResponse(201, {"location": "https://proxy/session/1"}),
            # The upload is interrupted, and the session turns out to have expired
            niquests.ConnectionError("reset"),
            FakeResponse(404),
            FakeResponse(201, {"location": "https://proxy/session/2"}),
            FakeResponse(200),
        ],
    )
    retryable_upload.perform_resumable_upload(
        data=b"0123456789", size=10, temp_upload_url="https://proxy/temp"
    )

    assert [(method, url) for method, url, _, _ in storage.requests] == [
        ("POST", "https://proxy/temp"),
        ("POST", "https://proxy/temp"),
        ("PUT", "https://proxy/session/1"),
        ("PUT", "https://proxy/session/1"),
        ("POST", "https://proxy/temp"),
        ("PUT", "https://proxy/session/2"),
    ]
    assert storage.requests[-1][2]["Content-Range"] == "bytes 0-9/10"
    assert storage.requests[-1][3] == b"0123456789"


def test_resumable_upload_resumes_from_what_was_received(monkeypatch):
    storage = FakeStorage(
        monkeypatch,
        [
            FakeResponse(201, {"location": "https://proxy/session/1"}),
            niquests.ConnectionError("reset"),
            FakeResponse(308, {"Range": "bytes=0-3"}),
            FakeResponse(200),
        ],
    )
    retryable_upload.perform_resumable_upload(
        data=b"0123456789", size=10, temp_upload_url="https://proxy/temp"
    )
    assert storage.requests[-1][2]["Content-Range"] == "bytes 4-9/10"
    assert storage.requests[-1][3] == b"456789"


def test_temp_uploads_never_send_credentials(monkeypatch):
    import io

    storage = FakeStorage(
        monkeypatch,
        [FakeResponse(201, {"location": "https://proxy/session"}), FakeResponse(200)],
    )
    retryable_upload.perform_resumable_upload(
        data=b"data", size=4, temp_upload_url="https://proxy/temp"
    )
    FakeStorage(monkeypatch, [FakeResponse(200)])
    retryable_upload.perform_standard_upload(
        data=io.BytesIO(b"data"), temp_upload_url="https://proxy/temp"
    )
    assert all("Authorization" not in headers for _, _, headers, _ in storage.requests)


# --- Imports ------------------------------------------------------------------

KIB = 2**10


@pytest.fixture
def small_sessions(monkeypatch):
    """Lower the size of files uploaded through upload sessions, and of their chunks."""
    monkeypatch.setattr(imports, "DIRECT_UPLOAD_MAX_SIZE", 512 * KIB)
    monkeypatch.setattr(imports, "INITIAL_CHUNK_SIZE", 256 * KIB)
    monkeypatch.setattr(imports, "MIN_CHUNK_SIZE", 256 * KIB)
    monkeypatch.setattr(imports, "MAX_CHUNK_SIZE", 256 * KIB)


def large_content():
    # Over DIRECT_UPLOAD_MAX_SIZE (as lowered by small_sessions), and not a multiple of
    # the chunk size
    return bytes(range(256)) * (4 * KIB + 7)  # Four 256KiB chunks, and a bit


def requests_to(api, fragment, method=None):
    return [
        (request_method, path, query, had_auth)
        for request_method, path, query, had_auth in api["requests"]
        if fragment in path and (method is None or request_method == method)
    ]


def create_bodies(api):
    return [
        body
        for method, path, body in api["json_bodies"]
        if method == "POST" and path.endswith("/imports")
    ]


def session_puts(api):
    return requests_to(api, "/uploadSession", "PUT")


def test_file_names_are_encoded_as_unpadded_base64url():
    # As the API decodes them, with Buffer.from(name, 'base64url')
    assert (
        imports.encode_import_file_name("données/é ü?.csv")
        == "ZG9ubsOpZXMvw6kgw7w_LmNzdg"
    )


def test_md5_is_computed_from_the_current_position(tmp_path):
    import base64
    import hashlib
    import io

    path = tmp_path / "data.bin"
    path.write_bytes(b"0123456789")
    expected = base64.b64encode(hashlib.md5(b"0123456789").digest()).decode()

    assert imports.compute_md5_base64(path) == expected
    assert imports.compute_md5_base64(b"0123456789") == expected
    content = io.BytesIO(b"skip0123456789")
    content.seek(4)
    assert imports.compute_md5_base64(content) == expected
    assert content.tell() == 4


def test_upload_creates_uploads_and_finalizes_an_import(api, table):
    upload = table.upload("data.csv").create(
        b"a;b\n1;2\n", rename_on_conflict=True, delimiter=";", progress=False
    )

    (body,) = create_bodies(api)
    assert body.pop("idempotencyKey")
    assert body == {
        "name": "data.csv",
        "delimiter": ";",
        "hasHeaderRow": True,
        "skipBadRecords": False,
        "allowJaggedRows": False,
        "totalFileCount": 1,
        "totalSize": 8,
    }
    assert api["import_files"] == {("i1", "data.csv"): b"a;b\n1;2\n"}
    assert [(method, path) for method, path, _, _ in api["requests"]] == [
        ("HEAD", "/api/v1/tables/a.b.mock/uploads/data_csv"),
        ("POST", "/api/v1/tables/a.b.mock/imports"),
        (
            "POST",
            f"/api/v1/imports/i1/files/{imports.encode_import_file_name('data.csv')}",
        ),
        ("POST", "/api/v1/imports/i1/finalize"),
        ("GET", "/api/v1/imports/i1"),
        ("GET", "/api/v1/tables/a.b.mock/uploads/data_csv"),
    ]
    assert upload.properties["status"] == "completed"


def test_upload_without_waiting_returns_the_import(api, table):
    upload = table.upload("data.csv").create(
        b"a,b\n1,2\n", rename_on_conflict=True, wait_for_finish=False, progress=False
    )

    assert upload.properties["status"] == "running"
    assert requests_to(api, "/imports/i1", "GET") == []
    assert upload.uri == "/tables/a.b.mock/uploads/data_csv"


def test_dataframes_are_uploaded_as_parquet(api, table):
    import pyarrow

    upload = table.upload("frame").create(
        pyarrow.table({"a": [1, 2]}), rename_on_conflict=True, progress=False
    )

    (body,) = create_bodies(api)
    assert body["type"] == "parquet" and body["name"] == "frame"
    # Named after the import, since the content has no file name
    assert list(api["import_files"]) == [("i1", "frame")]
    assert upload.name == "frame"


def test_file_paths_name_the_upload_and_the_file(api, table, tmp_path):
    path = tmp_path / "local.csv"
    path.write_bytes(b"a,b\n1,2\n")
    api["import_name"] = "local (1).csv"

    upload = table.upload().create(path, rename_on_conflict=True, progress=False)

    assert create_bodies(api)[0]["name"] == "local.csv"
    assert list(api["import_files"]) == [("i1", "local.csv")]
    assert upload.name == "local (1).csv"


def test_retried_create_reuses_its_idempotency_key(api, table):
    # The first attempt's connection drops, so the client can't know whether the API
    # created the import
    api["path_failures"] = [
        {"method": "POST", "fragment": "/imports", "status": None},
        {"method": "POST", "fragment": "/imports", "status": 502},
    ]

    table.upload("data.csv").create(
        b"a,b\n1,2\n", rename_on_conflict=True, progress=False
    )

    keys = [body["idempotencyKey"] for body in create_bodies(api)]
    assert len(keys) == 3 and len(set(keys)) == 1
    assert list(api["imports"]) == ["i1"]


def test_direct_upload_resends_the_whole_file_after_a_failure(api, table):
    content = b"a,b\n" + b"1,2\n" * 1000
    api["path_failures"] = [
        {"method": "POST", "fragment": "/files/", "status": None},
        {
            "method": "POST",
            "fragment": "/files/",
            "status": 429,
            "headers": {"Retry-After": "1"},
        },
        {"method": "POST", "fragment": "/files/", "status": 500},
    ]

    table.upload("data.csv").create(content, rename_on_conflict=True, progress=False)

    assert len(requests_to(api, "/files/", "POST")) == 4
    assert api["import_files"] == {("i1", "data.csv"): content}


def test_large_files_are_uploaded_in_chunks_without_credentials(
    api, table, small_sessions
):
    content = large_content()

    upload = table.upload("big.csv").create(
        content, rename_on_conflict=True, progress=False
    )

    assert api["import_files"] == {("i1", "big.csv"): content}
    (session,) = api["upload_sessions"].values()
    assert len(session["chunks"]) == 5
    puts = session_puts(api)
    # A status query, then the chunks
    assert len(puts) == 6
    assert not any(had_auth for *_, had_auth in puts)
    assert upload.properties["status"] == "completed"


def test_session_progress_counts_what_was_committed(api, table, small_sessions):
    content = large_content()
    api["commit_limit_once"] = 1000
    progress = []

    api["imports"]["i1"] = {
        "body": {},
        "import": {
            "kind": "import",
            "id": "i1",
            "uri": "/imports/i1",
            "status": "queued",
        },
    }
    imports.upload_import_file(
        {"uri": "/imports/i1"},
        "big.csv",
        content,
        len(content),
        imports.compute_md5_base64(content),
        progress=progress.append,
    )

    assert sum(progress) == len(content)
    # The bar went back to what the session had, after it kept only part of a chunk
    running = 0
    totals = []
    for delta in progress:
        running += delta
        totals.append(running)
    assert 1000 in totals
    assert min(totals) >= 0
    assert api["import_files"] == {("i1", "big.csv"): content}


def test_session_resumes_from_what_was_committed(api, table, small_sessions):
    content = large_content()
    # The session keeps only part of the first chunk, answering 308 with its Range
    api["commit_limit_once"] = 1000

    table.upload("big.csv").create(content, rename_on_conflict=True, progress=False)

    (session,) = api["upload_sessions"].values()
    assert session["chunks"][0] == (0, 256 * KIB - 1)
    assert session["chunks"][1][0] == 1000
    assert api["import_files"] == {("i1", "big.csv"): content}


def test_session_retries_failed_chunks(api, table, small_sessions):
    content = large_content()
    api["path_failures"] = [
        {"method": "PUT", "fragment": "/uploadSession", "status": None},
        {"method": "PUT", "fragment": "/uploadSession", "status": 503},
        {"method": "PUT", "fragment": "/uploadSession", "status": 429},
    ]

    table.upload("big.csv").create(content, rename_on_conflict=True, progress=False)

    assert api["import_files"] == {("i1", "big.csv"): content}
    assert len(api["upload_sessions"]) == 1


def test_session_gives_up_after_repeated_failures(api, table, small_sessions):
    api["path_failures"] = [
        {"method": "PUT", "fragment": "/uploadSession", "status": 503}
    ] * (imports.MAX_CONSECUTIVE_FAILURES + 1)

    with pytest.raises(exceptions.NetworkError):
        table.upload("big.csv").create(
            large_content(), rename_on_conflict=True, progress=False
        )
    assert requests_to(api, "/finalize") == []


def test_session_gives_up_when_it_keeps_none_of_its_chunks(api, table, small_sessions):
    # Answering every chunk with a 308 that shows no progress must count as failing,
    # rather than resending the same chunk forever
    api["commit_nothing"] = True

    with pytest.raises(exceptions.NetworkError):
        table.upload("big.csv").create(
            large_content(), rename_on_conflict=True, progress=False
        )
    (session,) = api["upload_sessions"].values()
    assert len(session["chunks"]) == imports.MAX_CONSECUTIVE_FAILURES + 1
    assert requests_to(api, "/finalize") == []


def test_session_reopens_after_it_dies(api, table, small_sessions):
    content = large_content()
    api["kill_session_after_chunks"] = 2

    table.upload("big.csv").create(content, rename_on_conflict=True, progress=False)

    assert api["import_files"] == {("i1", "big.csv"): content}
    first, second = api["upload_sessions"].values()
    assert first["dead"] and not second["dead"]
    assert len(requests_to(api, "/uploadSession", "POST")) == 2


def test_session_raises_when_reopening_returns_the_same_dead_session(
    api, table, small_sessions
):
    api["kill_session_after_chunks"] = 1
    api["dead_sessions_stay_dead"] = True

    with pytest.raises(exceptions.RedivisError, match="no longer accepting files"):
        table.upload("big.csv").create(
            large_content(), rename_on_conflict=True, progress=False
        )
    assert len(requests_to(api, "/uploadSession", "POST")) == 2
    assert requests_to(api, "/finalize") == []


def test_conflicts_are_checked_before_uploading(api, table):
    content = b"a,b\n1,2\n"

    upload = table.upload("data.csv").create(content, if_not_exists=True)
    assert upload.name == "data.csv"
    assert requests_to(api, "/imports") == []

    with pytest.raises(exceptions.ValueError, match="already exists"):
        table.upload("data.csv").create(content)
    assert requests_to(api, "/imports") == []

    table.upload("data.csv").create(content, replace_on_conflict=True, progress=False)
    methods = [method for method, path, _, _ in api["requests"]]
    assert methods.index("DELETE") < [
        (method, path) for method, path, _, _ in api["requests"]
    ].index(("POST", "/api/v1/tables/a.b.mock/imports"))


def test_failed_uploads_are_removed_even_without_raising(api, table):
    api["import_statuses"] = ["running", "failed"]
    api["import_error_message"] = "Bad CSV"
    api["upload_status"] = "failed"

    upload = table.upload("data.csv").create(
        b"a,b\n1,2\n",
        rename_on_conflict=True,
        remove_on_fail=True,
        raise_on_fail=False,
        progress=False,
    )
    assert upload.properties["status"] == "failed"
    assert upload.properties["errorMessage"] == "Bad CSV"
    assert ("DELETE", "/api/v1/uploads/u1") in [
        (method, path) for method, path, _, _ in api["requests"]
    ]

    api["import_statuses"] = ["failed"]
    with pytest.raises(exceptions.JobError, match="Bad CSV") as excinfo:
        table.upload("data.csv").create(
            b"a,b\n1,2\n", rename_on_conflict=True, progress=False
        )
    assert excinfo.value.status == "failed"


def test_cancelled_imports_raise(api):
    api["imports"]["i1"] = {
        "body": {},
        "import": {
            "kind": "import",
            "id": "i1",
            "uri": "/imports/i1",
            "status": "running",
        },
    }
    api["import_statuses"] = ["running", "running", "cancelled"]

    with pytest.raises(exceptions.JobError) as excinfo:
        imports.wait_for_import({"uri": "/imports/i1", "status": "running"})
    assert excinfo.value.status == "cancelled"
    assert len(requests_to(api, "/imports/i1", "GET")) == 3

    final = imports.wait_for_import(
        {"uri": "/imports/i1", "status": "running"}, raise_on_fail=False
    )
    assert final["status"] == "cancelled"


def test_large_in_memory_file_is_uploaded_from_its_position(api, table):
    import io

    content = io.BytesIO(b"skipped" + b"x" * (2**20 + 1))
    content.seek(len(b"skipped"))

    table.upload("data.csv").create(
        content, rename_on_conflict=True, wait_for_finish=False, progress=False
    )

    assert create_bodies(api)[0]["totalSize"] == 2**20 + 1
    assert list(api["import_files"].values()) == [b"x" * (2**20 + 1)]


def test_unseekable_content_is_uploaded(api, table):
    class Unseekable:
        def __init__(self, data):
            self.data = data

        def read(self, size=-1):
            if size is None or size < 0:
                size = len(self.data)
            chunk, self.data = self.data[:size], self.data[size:]
            return chunk

    table.upload("data.csv").create(
        Unseekable(b"a,b\n1,2\n"), rename_on_conflict=True, progress=False
    )

    assert api["import_files"] == {("i1", "data.csv"): b"a,b\n1,2\n"}


def test_upload_takes_the_name_the_api_gives_it(api, table):
    # As happens with rename_on_conflict
    api["import_name"] = "data (1).csv"
    upload = table.upload("data.csv").create(
        b"a,b\n1,2\n", rename_on_conflict=True, wait_for_finish=False, progress=False
    )

    assert upload.name == "data (1).csv"
    # Found by its name, as any upload is
    assert upload.uri == "/tables/a.b.mock/uploads/data%20%281%29_csv"


def test_transfers_create_an_import_without_uploading(api, table):
    upload = table.upload("transferred").create(
        transfer_specification={
            "sourceType": "gcs",
            "sourcePath": "bucket/data.csv",
            "identity": "me@example.com",
        },
        rename_on_conflict=True,
        progress=False,
    )

    (body,) = create_bodies(api)
    assert body["sourceType"] == "gcs"
    assert body["sourcePaths"] == ["bucket/data.csv"]
    assert body["identity"] == "me@example.com"
    assert "totalFileCount" not in body
    assert requests_to(api, "/files/") == [] and requests_to(api, "/finalize") == []
    assert upload.properties["status"] == "completed"


def test_transfers_omit_a_missing_identity(api, table):
    table.upload("transferred").create(
        transfer_specification={"sourceType": "redivis", "sourcePath": "a.b.c"},
        rename_on_conflict=True,
        wait_for_finish=False,
    )

    assert "identity" not in create_bodies(api)[0]


def test_stream_uploads_are_unchanged(api, table):
    table.upload("stream").create(
        type="stream",
        schema=[{"name": "a", "type": "integer"}],
        rename_on_conflict=True,
    )

    assert requests_to(api, "/imports") == []
    assert [
        body["schema"]
        for method, path, body in api["json_bodies"]
        if path.endswith("/uploads")
    ] == [[{"name": "a", "type": "integer"}]]


def test_workflow_tables_are_written_through_their_own_uri(api):
    import redivis

    table = redivis.workflow("a.wf").table("mock")
    table.upload("data.csv").create(
        b"a,b\n1,2\n", rename_on_conflict=True, wait_for_finish=False, progress=False
    )

    assert requests_to(api, "/imports", "POST")[0][1] == (
        "/api/v1/tables/a.b.mock/imports"
    )


def test_add_files_uses_one_import(api, table, tmp_path, small_sessions):
    path = tmp_path / "local.txt"
    path.write_bytes(b"from disk")
    large = large_content()

    result = table.add_files(
        files=[
            {"name": "a.txt", "data": "hello"},
            {"name": "dir/b.bin", "data": b"\x00\x01"},
            {"name": "large.bin", "data": large},
            str(path),
        ]
    )

    assert result is None
    (body,) = create_bodies(api)
    del body["idempotencyKey"]
    assert body == {"totalFileCount": 4, "totalSize": 5 + 2 + len(large) + 9}
    assert api["import_files"] == {
        ("i1", "a.txt"): b"hello",
        ("i1", "dir/b.bin"): b"\x00\x01",
        ("i1", "large.bin"): large,
        ("i1", "local.txt"): b"from disk",
    }
    assert len(requests_to(api, "/finalize")) == 1
    assert len(requests_to(api, "/files/", "POST")) == 3 + 1  # and a session
    assert requests_to(api, "tempUploads") == []
    assert requests_to(api, "/rawFiles") == []


def test_add_files_from_a_directory(api, table, tmp_path):
    (tmp_path / "nested").mkdir()
    (tmp_path / "top.txt").write_bytes(b"top")
    (tmp_path / "nested" / "inner.txt").write_bytes(b"inner")

    table.add_files(directory=str(tmp_path), progress=False)

    assert {name for _, name in api["import_files"]} == {
        "top.txt",
        str(pathlib.Path("nested") / "inner.txt"),
    }


def test_add_files_raises_when_the_import_fails(api, table):
    api["import_statuses"] = ["failed"]
    api["import_error_message"] = "Something went wrong"

    with pytest.raises(exceptions.JobError, match="Something went wrong"):
        table.add_files(files=[{"name": "a.txt", "data": "hello"}], progress=False)


def test_add_files_of_nothing_does_nothing(api, table, tmp_path):
    table.add_files(directory=str(tmp_path), progress=False)

    assert api["requests"] == []
