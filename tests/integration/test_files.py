"""Raw files in file index tables: uploading, listing, reading, and downloading them."""

import hashlib
import io
import os
from pathlib import Path

import pytest

from redivis import exceptions

from . import helpers

FILES = helpers.raw_files()


def test_list_files(files_table):
    files = files_table.list_files()

    assert {str(f.path): f.size for f in files} == {
        path: len(content) for path, content in FILES.items()
    }
    for f in files:
        assert f.hash == hashlib.md5(FILES[str(f.path)]).digest()


def test_list_files_max_results(files_table):
    assert len(files_table.list_files(2)) == 2


def test_directory_structure(files_table):
    root = files_table.to_directory()

    assert sorted(child.name for child in root.list()) == sorted(
        ["data", "readme.txt", "with space & symbols.txt"]
    )
    assert [d.name for d in root.list(mode="directories", recursive=True)] == [
        "data",
        "nested",
    ]
    nested = root.get("data/nested")
    assert [f.name for f in nested.list()] == ["random.bin"]
    assert nested.get("../numbers.csv").name == "numbers.csv"
    assert root.get("data/missing.txt") is None


@pytest.mark.parametrize("path", sorted(FILES))
def test_read(files_table, path):
    assert files_table.file(path).read() == FILES[path]


def test_read_as_text(files_table):
    assert files_table.file("readme.txt").read(as_text=True) == FILES["readme.txt"].decode()


def test_read_byte_range(files_table):
    content = FILES["data/nested/random.bin"]
    file = files_table.file("data/nested/random.bin")

    assert file.read(start_byte=100, end_byte=199) == content[100:200]
    assert file.read(start_byte=len(content) - 10) == content[-10:]


def test_missing_file(files_table):
    with pytest.raises(exceptions.NotFoundError):
        files_table.file("missing.txt")

    with pytest.raises(exceptions.ValueError, match="is a directory"):
        files_table.file("data")


def test_open_text(files_table):
    with files_table.file("data/numbers.csv").open() as f:
        lines = f.readlines()

    assert lines == FILES["data/numbers.csv"].decode().splitlines(keepends=True)


def test_open_binary_with_seeks(files_table):
    content = FILES["data/nested/random.bin"]

    with files_table.file("data/nested/random.bin").open("rb") as f:
        assert f.read(10) == content[:10]
        f.seek(2**20)
        assert f.tell() == 2**20
        assert f.read(10) == content[2**20 : 2**20 + 10]
        f.seek(-5, io.SEEK_END)
        assert f.read() == content[-5:]


def test_open_in_chunks(files_table):
    content = FILES["data/nested/random.bin"]

    with files_table.file("data/nested/random.bin").open("rb") as f:
        assert b"".join(iter(lambda: f.read(100_000), b"")) == content


def test_download_file(files_table, tmp_path):
    file = files_table.file("data/nested/random.bin")

    file.download(str(tmp_path) + os.sep, progress=False)
    assert (tmp_path / "random.bin").read_bytes() == FILES["data/nested/random.bin"]

    target = tmp_path / "renamed.bin"
    file.download(str(target), progress=False)
    assert target.read_bytes() == FILES["data/nested/random.bin"]


def test_download_directory(files_table, tmp_path):
    files_table.to_directory().download(tmp_path, progress=False)

    downloaded = {
        str(path.relative_to(tmp_path)): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    assert downloaded == FILES


def test_download_subdirectory(files_table, tmp_path):
    files_table.to_directory().get("data").download(tmp_path, progress=False)

    assert sorted(
        str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*") if p.is_file()
    ) == ["nested/random.bin", "numbers.csv"]


def test_fsspec(files_table):
    fsspec = pytest.importorskip("fsspec")
    root = f"redivis://{files_table.qualified_reference}"

    with fsspec.open(f"{root}/readme.txt", "rb") as f:
        assert f.read() == FILES["readme.txt"]

    fs = fsspec.filesystem("redivis")
    assert sorted(Path(name).name for name in fs.ls(f"{root}/data")) == [
        "nested",
        "numbers.csv",
    ]


def test_add_files_from_memory(new_table):
    table = new_table(is_file_index=True)

    table.add_files(
        files=[
            {"name": "a.txt", "data": "text content"},
            {"name": "dir/b.bin", "data": b"\x00\x01\x02"},
        ],
        progress=False,
    )

    assert table.file("a.txt").read() == b"text content"
    assert table.file("dir/b.bin").read() == b"\x00\x01\x02"


def test_add_files_from_paths(new_table, tmp_path):
    table = new_table(is_file_index=True)
    path = tmp_path / "local.txt"
    path.write_bytes(b"from disk")

    table.add_files(files=[str(path), {"path": str(path), "name": "copy.txt"}], progress=False)

    assert sorted(str(f.path) for f in table.list_files()) == ["copy.txt", "local.txt"]
    assert table.get().properties["numRows"] == 2


def test_add_files_requires_files_or_a_directory(new_table):
    table = new_table(is_file_index=True)

    with pytest.raises(exceptions.ValueError):
        table.add_files()
