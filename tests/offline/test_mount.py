"""Reads through a mounted directory: cached locally in blocks, with sequential reads streamed in one request.

These call the FUSE operations directly, so they need no FUSE installation.
"""

import base64
import hashlib
import os
import threading
from pathlib import Path

import pytest
from redivis.classes.Directory import Directory
from redivis.classes.File import File
from redivis.common import mount_directory
from redivis.common.mount_directory import RedivisFS

BLOCK = 1024
# Kernel reads are much smaller than a block
READ = 256
DATA = os.urandom(10 * BLOCK + 123)


@pytest.fixture(autouse=True)
def small_blocks(monkeypatch):
    monkeypatch.setattr(mount_directory, "BLOCK_SIZE", BLOCK)


@pytest.fixture
def fs(api, table, tmp_path):
    return make_fs(api, table, tmp_path / "cache")


def make_fs(api, table, cache_dir, files=None, max_cache_size=None):
    """A filesystem over a directory of files, given as {name: contents} (by default, just data.bin)"""
    if files is None:
        files = {"data.bin": DATA}
    root = Directory(path="/", table=table)
    for name, data in files.items():
        file_id = f"id-{name}"
        api["raw_files"][file_id] = data
        root.children[name] = File(
            file_id,
            name,
            directory=root,
            table=table,
            properties={
                "size": len(data),
                "md5_hash": base64.b64encode(hashlib.md5(data).digest()).decode(),
            },
        )
    os.makedirs(cache_dir, exist_ok=True)
    return RedivisFS(root, cache_dir, max_cache_size)


def read(fs, offset, length, fh=None, path="/data.bin"):
    own_handle = fh is None
    if own_handle:
        fh = fs.open(path, os.O_RDONLY)
    try:
        return fs.read(path, length, offset, fh)
    finally:
        if own_handle:
            fs.release(path, fh)


def read_all(fs, start=0, path="/data.bin"):
    fh = fs.open(path, os.O_RDONLY)
    chunks = []
    offset = start
    while True:
        chunk = fs.read(path, READ, offset, fh)
        if not chunk:
            break
        chunks.append(chunk)
        offset += len(chunk)
    fs.release(path, fh)
    return b"".join(chunks)


def cached_names(cache_dir):
    return sorted(p.name for p in Path(cache_dir).iterdir())


def key(data):
    return hashlib.md5(data).hexdigest()


def test_sequential_read_is_a_single_request(api, fs):
    assert read_all(fs) == DATA
    assert api["raw_file_ranges"] == [None]


def test_sequential_read_resumes_after_a_dropped_connection(api, fs):
    api["drop_raw_file_after"] = 3 * BLOCK + 10

    assert read_all(fs) == DATA
    # Resumed from the end of the last complete read from the stream
    assert api["raw_file_ranges"] == [None, f"bytes={3 * BLOCK}-"]


def test_reread_is_served_from_the_cache(api, fs):
    read_all(fs)
    assert read(fs, 5 * BLOCK + 7, 300) == DATA[5 * BLOCK + 7 : 5 * BLOCK + 307]
    assert read_all(fs) == DATA
    assert len(api["raw_file_ranges"]) == 1


def test_cache_dir_is_reused_by_a_later_mount(api, table, tmp_path):
    first = make_fs(api, table, tmp_path / "cache")
    read(first, 0, READ)
    read(first, 7 * BLOCK, READ)
    api["raw_file_ranges"].clear()

    second = make_fs(api, table, tmp_path / "cache")
    assert read(second, 0, READ) == DATA[:READ]
    assert read(second, 7 * BLOCK, READ) == DATA[7 * BLOCK : 7 * BLOCK + READ]
    assert api["raw_file_ranges"] == []


def test_changed_file_is_not_served_from_a_stale_cache(api, table, tmp_path):
    read_all(make_fs(api, table, tmp_path / "cache"))

    changed = os.urandom(len(DATA))
    fs = make_fs(api, table, tmp_path / "cache", files={"data.bin": changed})
    assert read_all(fs) == changed


def test_inconsistent_block_map_is_discarded(api, fs, tmp_path):
    read_all(fs)
    with open(tmp_path / "cache" / f"{key(DATA)}.data", "r+b") as f:
        f.truncate(BLOCK)
    api["raw_file_ranges"].clear()

    fresh = RedivisFS(fs.directory, tmp_path / "cache")
    assert read_all(fresh) == DATA
    assert api["raw_file_ranges"] == [None]


def test_random_reads_fetch_only_the_blocks_they_need(api, fs):
    # e.g. a parquet reader, starting with the footer
    footer = len(DATA) - 50
    assert read(fs, footer, 50) == DATA[footer:]
    # A read straddling two blocks
    assert read(fs, 4 * BLOCK - 10, 20) == DATA[4 * BLOCK - 10 : 4 * BLOCK + 10]

    assert api["raw_file_ranges"] == [
        f"bytes={10 * BLOCK}-{len(DATA) - 1}",
        f"bytes={3 * BLOCK}-{5 * BLOCK - 1}",
    ]


def test_server_ignoring_the_range_is_read_from_the_right_place(api, fs):
    api["ignore_raw_file_range"] = True

    assert read(fs, 4 * BLOCK - 10, 20) == DATA[4 * BLOCK - 10 : 4 * BLOCK + 10]
    assert read_all(fs) == DATA


def test_stream_reads_ahead_of_the_reader_then_pauses(api, fs):
    fh = fs.open("/data.bin", os.O_RDONLY)
    read(fs, 0, READ, fh)
    cached_file = fs._file_handles[fh]
    with cached_file.changed:
        cached_file.changed.wait_for(lambda: cached_file.has_block(2), timeout=5)

    assert [cached_file.has_block(b) for b in range(5)] == [1, 1, 1, 0, 0]
    fs.release("/data.bin", fh)


def test_read_continuing_from_cached_data_streams_the_rest(api, fs):
    read(fs, 2 * BLOCK, READ)
    api["raw_file_ranges"].clear()

    assert read_all(fs, start=2 * BLOCK) == DATA[2 * BLOCK :]
    assert api["raw_file_ranges"] == [f"bytes={3 * BLOCK}-"]


def test_readahead_slightly_out_of_order_stays_on_one_stream(api, fs):
    fh = fs.open("/data.bin", os.O_RDONLY)
    for offset in [0, 2 * BLOCK, BLOCK, 3 * BLOCK]:
        assert read(fs, offset, READ, fh) == DATA[offset : offset + READ]
    fs.release("/data.bin", fh)

    assert api["raw_file_ranges"] == [None]


def test_concurrent_reads(api, fs):
    fh = fs.open("/data.bin", os.O_RDONLY)
    offsets = list(range(0, len(DATA), READ))
    results = {}

    def worker(chunk):
        for offset in chunk:
            results[offset] = fs.read("/data.bin", READ, offset, fh)

    threads = [threading.Thread(target=worker, args=(offsets[i::4],)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    fs.release("/data.bin", fh)

    assert b"".join(results[o] for o in offsets) == DATA


def test_empty_file(api, table, tmp_path):
    fs = make_fs(api, table, tmp_path / "cache", files={"data.bin": b""})
    assert read(fs, 0, READ) == b""
    assert api["raw_file_ranges"] == []


@pytest.fixture
def temp_dir(tmp_path, monkeypatch):
    """Where temporary caches are made"""
    path = tmp_path / "tmp"
    path.mkdir()
    monkeypatch.setattr(mount_directory.tempfile, "tempdir", str(path))
    return path


def test_temporary_cache_dir_is_removed_on_unmount(
    api, table, tmp_path, temp_dir, monkeypatch
):
    monkeypatch.setattr(
        mount_directory, "FUSE", lambda fs, *args, **kwargs: read_all(fs)
    )
    temporary_cache_dir = mount_directory._TemporaryCacheDir()
    fs = make_fs(api, table, temporary_cache_dir.path)
    mount_path = tmp_path / "mnt"
    mount_path.mkdir()

    mount_directory._run_fuse_and_cleanup(
        fs, mount_path, remove_mount_dir=True, temporary_cache_dir=temporary_cache_dir
    )

    assert list(temp_dir.iterdir()) == []
    assert not mount_path.exists()


def make_orphaned_cache_dir(temp_dir, name, lock_contents="12345\n"):
    """A temporary cache as left by a process killed while mounted"""
    path = temp_dir / f"{mount_directory.TEMPORARY_CACHE_PREFIX}{name}"
    path.mkdir()
    (path / f"{key(DATA)}.data").write_bytes(DATA)
    if lock_contents is not None:
        (path / mount_directory.TEMPORARY_CACHE_LOCK).write_text(lock_contents)
    return path


def test_temporary_cache_dirs_of_exited_processes_are_removed(temp_dir):
    live = mount_directory._TemporaryCacheDir()
    orphaned = make_orphaned_cache_dir(temp_dir, "orphaned")
    # Not yet locked by the process making it
    being_made = make_orphaned_cache_dir(temp_dir, "being_made", lock_contents="")
    unlocked = make_orphaned_cache_dir(temp_dir, "unlocked", lock_contents=None)
    (temp_dir / "notes.txt").write_text("not a cache")

    new = mount_directory._TemporaryCacheDir()

    assert sorted(p.name for p in temp_dir.iterdir()) == sorted(
        [Path(live.path).name, being_made.name, unlocked.name, Path(new.path).name, "notes.txt"]
    )
    assert not orphaned.exists()
    live.remove()
    new.remove()


def test_temporary_cache_dir_is_removed_if_mounting_fails(table, tmp_path, temp_dir):
    mount_path = tmp_path / "mnt"
    mount_path.touch()
    with pytest.raises(Exception, match="not a directory"):
        mount_directory.mount_directory(
            Directory(path="/", table=table), mount_path, foreground=True
        )
    assert list(temp_dir.iterdir()) == []


# --- mount path ----------------------------------------------------------------


def mount_in_foreground(table, mount_path, monkeypatch, tmp_path):
    """Mounts an empty directory with FUSE stubbed out, returning whether the mount path was a
    directory while "mounted"."""
    mounted = []
    monkeypatch.setattr(
        mount_directory,
        "FUSE",
        lambda fs, path, **kwargs: mounted.append(os.path.isdir(path)),
    )
    mount_directory.mount_directory(
        Directory(path="/", table=table),
        mount_path,
        foreground=True,
        cache_dir=tmp_path / "cache",
    )
    return mounted == [True]


def test_mount_creates_a_new_mount_dir_and_removes_it_on_unmount(
    table, tmp_path, monkeypatch
):
    mount_path = tmp_path / "mnt"
    assert mount_in_foreground(table, mount_path, monkeypatch, tmp_path)
    assert not mount_path.exists()


def test_mount_accepts_an_existing_empty_dir_and_leaves_it_on_unmount(
    table, tmp_path, monkeypatch
):
    mount_path = tmp_path / "mnt"
    mount_path.mkdir()
    assert mount_in_foreground(table, mount_path, monkeypatch, tmp_path)
    assert mount_path.is_dir()


@pytest.mark.parametrize(
    "make_path, message",
    [
        (lambda path: (path.mkdir(), (path / "x").touch()), "not empty"),
        (lambda path: path.touch(), "not a directory"),
    ],
)
def test_mount_rejects_an_existing_path_that_is_not_an_empty_dir(
    table, tmp_path, monkeypatch, make_path, message
):
    mount_path = tmp_path / "mnt"
    make_path(mount_path)
    with pytest.raises(Exception, match=message):
        mount_in_foreground(table, mount_path, monkeypatch, tmp_path)


# --- shared contents -----------------------------------------------------------


def test_files_with_the_same_contents_share_a_cached_copy(api, table, tmp_path):
    fs = make_fs(api, table, tmp_path / "cache", files={"a.bin": DATA, "b.bin": DATA})

    assert read_all(fs, path="/a.bin") == DATA
    assert read_all(fs, path="/b.bin") == DATA
    assert api["raw_file_ranges"] == [None]
    assert cached_names(tmp_path / "cache") == [
        f"{key(DATA)}.blocks",
        f"{key(DATA)}.data",
    ]


# --- max cache size ------------------------------------------------------------

A, B, C = (os.urandom(len(DATA)) for _ in range(3))
ABC = {"a.bin": A, "b.bin": B, "c.bin": C}


def test_least_recently_used_files_are_evicted_past_the_max_size(api, table, tmp_path):
    fs = make_fs(api, table, tmp_path / "cache", ABC, max_cache_size=2.5 * len(DATA))
    for name in ABC:
        read_all(fs, path=f"/{name}")

    assert cached_names(tmp_path / "cache") == sorted(
        f"{key(d)}.{ext}" for d in (B, C) for ext in ("blocks", "data")
    )
    api["raw_file_ranges"].clear()
    assert read_all(fs, path="/a.bin") == A
    assert api["raw_file_ranges"] == [None]


def test_open_files_are_not_evicted(api, table, tmp_path):
    fs = make_fs(api, table, tmp_path / "cache", ABC, max_cache_size=2.5 * len(DATA))
    fh = fs.open("/a.bin", os.O_RDONLY)
    read(fs, 0, len(A), fh, path="/a.bin")
    read_all(fs, path="/b.bin")
    read_all(fs, path="/c.bin")

    assert f"{key(A)}.data" in cached_names(tmp_path / "cache")
    assert f"{key(B)}.data" not in cached_names(tmp_path / "cache")
    api["raw_file_ranges"].clear()
    assert read(fs, 0, len(A), fh, path="/a.bin") == A
    assert api["raw_file_ranges"] == []
    fs.release("/a.bin", fh)


def test_file_closed_while_the_cache_is_over_its_max_size_is_evicted(
    api, table, tmp_path
):
    files = {"a.bin": A, "b.bin": B}
    fs = make_fs(api, table, tmp_path / "cache", files, max_cache_size=1.5 * len(DATA))
    handles = {name: fs.open(f"/{name}", os.O_RDONLY) for name in files}
    for name, data in files.items():
        assert read(fs, 0, len(data), handles[name], path=f"/{name}") == data

    fs.release("/a.bin", handles["a.bin"])

    assert cached_names(tmp_path / "cache") == [f"{key(B)}.blocks", f"{key(B)}.data"]
    fs.release("/b.bin", handles["b.bin"])


def test_cache_left_by_an_earlier_mount_is_trimmed_to_the_max_size(
    api, table, tmp_path
):
    first = make_fs(api, table, tmp_path / "cache", ABC)
    for i, (name, data) in enumerate(ABC.items()):
        read_all(first, path=f"/{name}")
        # Last used in the order read, whatever the filesystem's timestamp resolution
        os.utime(tmp_path / "cache" / f"{key(data)}.data", (1000 + i, 1000 + i))

    make_fs(api, table, tmp_path / "cache", ABC, max_cache_size=1.5 * len(DATA))

    assert cached_names(tmp_path / "cache") == [f"{key(C)}.blocks", f"{key(C)}.data"]


def test_file_larger_than_the_cache_is_streamed_without_caching(api, table, tmp_path):
    fs = make_fs(api, table, tmp_path / "cache", max_cache_size=len(DATA) - 1)

    assert read_all(fs) == DATA
    assert api["raw_file_ranges"] == [None]
    footer = len(DATA) - 50
    assert read(fs, footer, 50) == DATA[footer:]
    assert read(fs, 4 * BLOCK - 10, 20) == DATA[4 * BLOCK - 10 : 4 * BLOCK + 10]
    assert cached_names(tmp_path / "cache") == []


def test_other_files_in_the_cache_dir_are_never_evicted(api, table, tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    for name in ["notes.txt", "results.data", "results.blocks.bak"]:
        (cache_dir / name).write_bytes(DATA)

    fs = make_fs(api, table, cache_dir, max_cache_size=0)
    read_all(fs)

    assert cached_names(cache_dir) == [
        "notes.txt",
        "results.blocks.bak",
        "results.data",
    ]
