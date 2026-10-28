"""Uploads: the temp upload protocols, and Upload.create()'s conflicts and failures."""

import pytest
import niquests

from . import mock_redivis_api
from redivis.common import exceptions, retryable_upload


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


def test_conflicts_are_checked_before_uploading(api, table):
    content = b"x" * (2**20 + 1)  # Large enough to need a temp upload

    upload = table.upload("data.csv").create(content, if_not_exists=True)
    assert upload.name == "data.csv"
    assert mock_redivis_api.paths("tempUploads") == []

    with pytest.raises(exceptions.ValueError, match="already exists"):
        table.upload("data.csv").create(content)
    assert mock_redivis_api.paths("tempUploads") == []


def test_failed_uploads_are_removed_even_without_raising(api, table):
    api["upload_status"] = "failed"
    upload = table.upload("data.csv").create(
        b"a,b\n1,2\n", rename_on_conflict=True, remove_on_fail=True, raise_on_fail=False
    )
    assert upload.properties["status"] == "failed"
    assert ("DELETE", "/api/v1/uploads/u1") in [
        (method, path) for method, path, _, _ in api["requests"]
    ]

    with pytest.raises(exceptions.JobError):
        table.upload("data.csv").create(b"a,b\n1,2\n", rename_on_conflict=True)


def test_large_in_memory_file_is_uploaded_from_its_position(api, table):
    import io

    content = io.BytesIO(b"skipped" + b"x" * (2**20 + 1))
    content.seek(len(b"skipped"))

    table.upload("data.csv").create(
        content, rename_on_conflict=True, wait_for_finish=False
    )

    assert [body for _, _, body in api["json_bodies"] if "tempUploads" in body] == [
        {"tempUploads": [{"size": 2**20 + 1, "name": "data.csv", "resumable": False}]}
    ]
    assert list(api["temp_storage"].values()) == [b"x" * (2**20 + 1)]


def test_upload_takes_the_name_the_api_gives_it(api, table):
    # As happens with rename_on_conflict
    upload = table.upload("data.csv").create(
        b"a,b\n1,2\n", rename_on_conflict=True, wait_for_finish=False
    )

    assert upload.name == "My Upload"


def test_upload_get_returns_the_upload(api, table):
    upload = table.upload("u1")
    assert upload.get() is upload


def test_add_files_uploads_in_memory_data_with_progress(api, table):
    table.add_files(
        files=[
            {"name": "a.txt", "data": "hello"},
            {"name": "b.bin", "data": b"\x00\x01"},
        ]
    )
    assert sorted(api["temp_storage"].values()) == [b"\x00\x01", b"hello"]
    assert len(mock_redivis_api.paths("/rawFiles")) == 1
