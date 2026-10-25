"""File downloads and reads: resuming, verification, partial files, and credentials."""

import base64
import hashlib

import pytest

import mock_redivis_api
from redivis.common import exceptions
from redivis.common.retryable_download import (
    perform_parallel_download,
    perform_retryable_download,
)

CONTENT = bytes(range(256)) * 40


def md5(data):
    return base64.b64encode(hashlib.md5(data).digest()).decode()


def test_download_resumes_into_a_partial_file(api, tmp_path):
    api["raw_files"]["f1"] = CONTENT
    api["drop_raw_file_after"] = 1000
    path = tmp_path / "out.bin"

    perform_retryable_download(
        "/rawFiles/f1",
        filename=str(path),
        size=len(CONTENT),
        md5_hash=md5(CONTENT),
        progress=False,
    )
    assert path.read_bytes() == CONTENT
    assert len(api["raw_file_ranges"]) == 2
    assert sorted(p.name for p in tmp_path.iterdir()) == ["out.bin"]


def test_download_restarts_when_the_server_ignores_the_range(api, tmp_path):
    api["raw_files"]["f1"] = CONTENT
    api["drop_raw_file_after"] = 1000
    api["ignore_raw_file_range"] = True
    path = tmp_path / "out.bin"

    perform_retryable_download("/rawFiles/f1", filename=str(path), progress=False)
    assert path.read_bytes() == CONTENT


def test_download_is_verified_against_its_hash(api, tmp_path):
    api["raw_files"]["f1"] = CONTENT
    path = tmp_path / "out.bin"

    with pytest.raises(exceptions.RedivisError, match="MD5 hash"):
        perform_retryable_download(
            "/rawFiles/f1",
            filename=str(path),
            size=len(CONTENT),
            md5_hash=md5(b"other"),
            progress=False,
        )
    # Tried twice, and left nothing behind
    assert len(api["raw_file_ranges"]) == 2
    assert list(tmp_path.iterdir()) == []


def test_failed_downloads_leave_no_partial_file(api, tmp_path):
    api["response_script"] = [(500, {"error": "internal"})]
    path = tmp_path / "out.bin"
    with pytest.raises(exceptions.APIError):
        perform_retryable_download(
            "/rawFiles/missing", filename=str(path), progress=False
        )
    assert list(tmp_path.iterdir()) == []


def test_parallel_downloads_of_public_files_need_no_credentials(anonymous, tmp_path):
    anonymous["raw_files"]["f1"] = CONTENT
    anonymous["raw_files"]["f2"] = CONTENT[:10]
    perform_parallel_download(
        ["/rawFiles/f1", "/rawFiles/f2"],
        [str(tmp_path / "a.bin"), str(tmp_path / "b.bin")],
        sizes=[len(CONTENT), 10],
        md5_hashes=[md5(CONTENT), md5(CONTENT[:10])],
        progress=False,
    )
    assert (tmp_path / "a.bin").read_bytes() == CONTENT
    assert (tmp_path / "b.bin").read_bytes() == CONTENT[:10]
    assert not any(had_auth for _, _, _, had_auth in anonymous["requests"])


def test_parallel_downloads_authenticate_when_rejected(
    anonymous, monkeypatch, tmp_path
):
    from redivis.common import auth

    logins = []

    def fake_login(scope, amr_values=None, upgrade_credentials=False):
        logins.append(scope)
        auth.cached_credentials = dict(mock_redivis_api.FAKE_CREDENTIALS)

    monkeypatch.setattr(auth, "perform_oauth_login", fake_login)
    anonymous["require_auth"] = True
    for i in range(5):
        anonymous["raw_files"][f"f{i}"] = CONTENT
    perform_parallel_download(
        [f"/rawFiles/f{i}" for i in range(5)],
        [str(tmp_path / f"{i}.bin") for i in range(5)],
        progress=False,
    )
    assert all((tmp_path / f"{i}.bin").read_bytes() == CONTENT for i in range(5))
    # However many downloads were rejected at once, the user logs in once
    assert len(logins) == 1


def test_parallel_downloads_are_verified_against_their_hash(api, tmp_path):
    api["raw_files"]["f1"] = CONTENT
    with pytest.raises(exceptions.RedivisError, match="MD5 hash"):
        perform_parallel_download(
            ["/rawFiles/f1"],
            [str(tmp_path / "a.bin")],
            sizes=[len(CONTENT)],
            md5_hashes=[md5(b"other")],
            progress=False,
        )
    assert list(tmp_path.iterdir()) == []


def file(table, content):
    from redivis.classes.File import File

    table._properties = None
    return File(
        "f1", "f1.bin", table=table, directory=None, properties={"size": len(content)}
    )


def test_read_ranges(api, table):
    api["raw_files"]["f1"] = CONTENT
    f = file(table, CONTENT)
    assert f.read(start_byte=0, end_byte=0) == CONTENT[:1]
    assert f.read(start_byte=10, end_byte=19) == CONTENT[10:20]

    api["ignore_raw_file_range"] = True
    assert f.read(start_byte=10, end_byte=19) == CONTENT[10:20]
    assert f.read(start_byte=10) == CONTENT[10:]


def test_stream_returns_bytes(api, table):
    api["raw_files"]["f1"] = CONTENT
    with pytest.warns(FutureWarning):
        stream = file(table, CONTENT).stream()
    assert stream.read(4) == CONTENT[:4]
