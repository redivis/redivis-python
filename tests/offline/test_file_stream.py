"""File.Stream's io.BufferedIOBase contract, over a fake urllib3-future response."""

import io
import os
from types import SimpleNamespace

import pytest

from redivis.classes import File as file_module
from redivis.classes.File import Stream

DATA = b"aa\nbb\ncc\n"


class FutureLikeRaw:
    """Mimics urllib3-future, where read(-1) returns one chunk rather than the rest of the body"""

    def __init__(self, data, chunk_size=2):
        self._buffer = io.BytesIO(data)
        self._chunk_size = chunk_size

    def read(self, amt=None):
        if amt is not None and amt < 0:
            amt = self._chunk_size
        return self._buffer.read(amt)

    def readinto(self, buffer):
        return self._buffer.readinto(buffer)

    def readline(self, size=-1):
        return self._buffer.readline(size)

    def close(self):
        pass


@pytest.fixture
def stream(monkeypatch):
    def fake_make_request(**kwargs):
        # Always the whole body, so ranged reads exercise Stream's skip-ahead
        return SimpleNamespace(status_code=200, raw=FutureLikeRaw(DATA))

    monkeypatch.setattr(file_module, "make_request", fake_make_request)
    return Stream("/files/test", file=SimpleNamespace(size=len(DATA)))


@pytest.mark.parametrize("args", [(), (-1,), (None,)])
def test_read_returns_whole_file(stream, args):
    assert stream.read(*args) == DATA
    assert stream.tell() == len(DATA)


def test_read1_returns_bounded_chunks(stream):
    stream._iter_chunk_size = 4
    chunks = []
    while chunk := stream.read1():
        assert len(chunk) <= 4
        chunks.append(chunk)
    assert b"".join(chunks) == DATA


def test_readlines_returns_every_line(stream):
    assert stream.readlines() == [b"aa\n", b"bb\n", b"cc\n"]


@pytest.mark.parametrize(
    "hint, expected",
    [(0, [b"aa\n", b"bb\n", b"cc\n"]), (3, [b"aa\n"]), (4, [b"aa\n", b"bb\n"])],
)
def test_readlines_hint(stream, hint, expected):
    assert stream.readlines(hint) == expected


def test_readlines_hint_counts_from_current_position(stream):
    stream.seek(3)
    assert stream.readlines(3) == [b"bb\n"]


def test_readline_at_end_of_file(stream):
    stream.seek(0, os.SEEK_END)
    assert stream.readline() == b""
    assert stream.readlines() == []
