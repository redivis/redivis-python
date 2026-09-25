import atexit
import os
import re
import shutil
import stat
import errno
import tempfile
import threading
import time
from collections import OrderedDict

from ..common import exceptions
from mfusepy import FUSE, FuseOSError, Operations

# Files are downloaded into the local cache in blocks of this size, and a read is served
# once every block it touches has been cached. Keep in step with redivis-r's fuse_mount.c.
BLOCK_SIZE = 4 * 1024 * 1024

# A read up to this many blocks past where an open sequential stream has reached is served by
# reading the stream forward to it, rather than abandoning the stream for a new request. This
# absorbs the kernel's readahead requests arriving slightly out of order.
MAX_STREAM_SKIP_BLOCKS = 4

# A stream downloads up to this many blocks past the furthest one a reader has asked for, then
# pauses until reads catch up, so that a file that's only partly read isn't downloaded in full.
STREAM_READAHEAD_BLOCKS = 2

# Blocks are read from a stream in chunks of this size. If the connection drops, the stream resumes
# from the end of the last complete chunk, so this bounds how much is downloaded twice.
STREAM_CHUNK_SIZE = 1024 * 1024

# Open sequential streams kept per file, so that a few interleaved sequential readers (e.g.
# parallel column chunk reads of a parquet file) each keep their own request.
MAX_STREAMS_PER_FILE = 4

# Files too large for the cache are read through a buffer of this many recent blocks instead.
# It holds the blocks a stream reads ahead, as well as the one being read.
MEMORY_BUFFER_BLOCKS = MAX_STREAM_SKIP_BLOCKS + STREAM_READAHEAD_BLOCKS + 2

# Once the cache exceeds its maximum size, files are evicted until it is back under this fraction
# of it, so that eviction runs occasionally rather than on every block downloaded.
EVICTION_TARGET = 0.9


class _BlockStream:
    """A single open-ended request for a file, read on a thread of its own that caches each block as
    it arrives. It stays up to STREAM_READAHEAD_BLOCKS ahead of the furthest block a reader has asked
    of it, then pauses until reads catch up.

    The underlying File stream transparently resumes, via a Range request from where it left off,
    if the connection drops or the server times out while it's paused.
    """

    def __init__(self, cached_file, block):
        # The block being received, and the furthest block a reader has asked for
        self.next_block = block
        self.want_block = block
        # Set once the thread has finished, with the error that stopped it, if any
        self.done = False
        self.error = None
        # Set (under the file's lock) to stop the stream; it then stores nothing more
        self.abandoned = False
        self._cached_file = cached_file
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        f = self._cached_file
        stream = None
        try:
            stream = f.node.open(mode="rb", start_byte=self.next_block * BLOCK_SIZE)
            while True:
                with f.changed:
                    while (
                        not self.abandoned
                        and self.next_block < f.n_blocks
                        and self.next_block > self.want_block + STREAM_READAHEAD_BLOCKS
                    ):
                        f.changed.wait()
                    if self.abandoned or self.next_block >= f.n_blocks:
                        return
                    block = self.next_block
                    length = f.block_length(block)

                data = _read_exactly(stream, length, lambda: self.abandoned)

                with f.changed:
                    if self.abandoned:
                        return
                    if not f.has_block(block):
                        f.store_block(block, data)
                    self.next_block += 1
                    f.changed.notify_all()
        except Exception as e:
            self.error = e
        finally:
            with f.changed:
                self.done = True
                f.changed.notify_all()
            if stream is not None:
                try:
                    stream.close()
                except Exception:
                    pass


class _Cache:
    """The files cached in cache_dir, evicting the least recently used once they exceed max_size.

    Only closed files are evicted, so the cache can run over max_size while large files are open.
    """

    def __init__(self, cache_dir, max_size):
        self.cache_dir = cache_dir
        self.size = 0
        self._files = {}
        self._lock = threading.Lock()
        self._evicting = False

        # Account for anything left by an earlier mount of the same cache_dir. Only files with both
        # parts of a cached copy are considered, so nothing else in cache_dir is ever evicted.
        with os.scandir(cache_dir) as entries:
            names = {entry.name: entry for entry in entries if entry.is_file()}
        for name, entry in names.items():
            key = name[: -len(".data")]
            if not name.endswith(".data") or f"{key}.blocks" not in names:
                continue
            cached_file = _CachedFile(self, key)
            cached_file.size_on_disk = _size_on_disk(entry.stat())
            cached_file.last_used = entry.stat().st_mtime
            self._files[key] = cached_file
            self.size += cached_file.size_on_disk

        if max_size is None:
            # Default to half the space available to the cache, leaving room for everything else
            max_size = (shutil.disk_usage(cache_dir).free + self.size) // 2
        self.max_size = max_size
        self.add_size(0)

    def get(self, key):
        with self._lock:
            cached_file = self._files.get(key)
            if cached_file is None:
                cached_file = _CachedFile(self, key)
                self._files[key] = cached_file
            return cached_file

    def add_size(self, size):
        with self._lock:
            self.size += size
            if self.size <= self.max_size or self._evicting:
                return
            # Only one thread evicts at a time; others carry on without waiting for it
            self._evicting = True
            files = sorted(self._files.values(), key=lambda f: f.last_used)

        try:
            for cached_file in files:
                with self._lock:
                    if self.size <= self.max_size * EVICTION_TARGET:
                        break
                freed = cached_file.evict()
                with self._lock:
                    self.size -= freed
        finally:
            with self._lock:
                self._evicting = False


class _CachedFile:
    """The local cache for one file's contents: a sparse copy of it, filled in block by block as it's read.

    Its contents are in "<key>.data", and which blocks have been downloaded is tracked alongside
    them in "<key>.blocks" (one byte per block), so a cache_dir that outlives the mount can be
    reused by later mounts. A file too large for the cache altogether is instead read through a
    small in-memory buffer of recent blocks.
    """

    def __init__(self, cache, key):
        self.cache = cache
        self.data_path = os.path.join(cache.cache_dir, f"{key}.data")
        self.blocks_path = os.path.join(cache.cache_dir, f"{key}.blocks")
        self.size_on_disk = 0
        self.last_used = time.time()
        # Guards the state below, and is waited on (via changed) for blocks to arrive
        self.lock = threading.Lock()
        self.changed = threading.Condition(self.lock)
        # Serializes positioned I/O on platforms without os.pread / os.pwrite (i.e. Windows)
        self._io_lock = threading.Lock()
        self.node = None
        self.n_blocks = 0
        self._size = 0
        self._blocks = None
        self._buffer = None
        self._fd = None
        self._open_count = 0
        self._streams = []
        # Blocks being fetched by bounded requests, outside the lock
        self._fetching = set()

    def open(self, node):
        with self.lock:
            if self._open_count == 0:
                # Any file with this content will do, since the cache is keyed on content
                self.node = node
                self._size = node.size or 0
                self.n_blocks = -(-self._size // BLOCK_SIZE)
                if self._size > self.cache.max_size:
                    self._buffer = OrderedDict()
                else:
                    self._fd = os.open(
                        self.data_path,
                        os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0),
                        0o600,
                    )
                    if self._blocks is None:
                        self._blocks = self._load_blocks()
            self._open_count += 1
            self.last_used = time.time()

    def release(self):
        with self.lock:
            self._open_count -= 1
            if self._open_count > 0:
                return
            # Streams store nothing once abandoned, so they needn't have finished yet
            self._close_streams()
            self._buffer = None
            fd, self._fd = self._fd, None
        if fd is not None:
            os.close(fd)
        # Now evictable, so if the cache ran over while it was open, it may be what brings it back under
        self.cache.add_size(0)

    def evict(self):
        """Delete this file from the cache, unless it's in use, returning the space freed"""
        if not self.lock.acquire(blocking=False):
            return 0
        try:
            if self._open_count > 0:
                return 0
            for path in (self.data_path, self.blocks_path):
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass
            self._blocks = None
            freed, self.size_on_disk = self.size_on_disk, 0
            return freed
        except OSError:
            return 0
        finally:
            self.lock.release()

    def read(self, length, offset):
        if offset >= self._size or length <= 0:
            return b""
        length = min(length, self._size - offset)
        first_block = offset // BLOCK_SIZE
        last_block = (offset + length - 1) // BLOCK_SIZE
        self.last_used = time.time()

        if self._buffer is None:
            with self.lock:
                self._download(first_block, last_block)
            return self._pread(length, offset)

        # In memory, a block at a time, since the buffer needn't hold every block of a read at once
        chunks = []
        end = offset + length
        with self.lock:
            while offset < end:
                block = offset // BLOCK_SIZE
                start = offset - block * BLOCK_SIZE
                # Returns with the block buffered, and the lock still held, so it's still there to copy
                self._download(block, block)
                chunk = self._buffer[block][start : start + end - offset]
                chunks.append(chunk)
                offset += len(chunk)
        return b"".join(chunks)

    def has_block(self, block):
        if self._buffer is not None:
            return block in self._buffer
        return self._blocks[block]

    def store_block(self, block, data):
        """Caches a downloaded block, with the lock held"""
        if self._buffer is not None:
            self._buffer[block] = data
            self._buffer.move_to_end(block)
            while len(self._buffer) > MEMORY_BUFFER_BLOCKS:
                self._buffer.popitem(last=False)
        else:
            self._pwrite(data, block * BLOCK_SIZE)
            self._blocks[block] = 1
            self.size_on_disk += len(data)
            self._save_blocks()
            # Before waking readers, so that any eviction has happened by the time they see the block.
            # Safe with the lock held, since eviction never waits on another file's lock.
            self.cache.add_size(len(data))
        self.changed.notify_all()

    def _download(self, first_block, last_block):
        """Downloads whichever of the blocks aren't cached, with the lock held (though released while
        waiting on the network)"""
        # Let streams reading ahead of this read carry on, so they stay ahead of it
        for s in self._streams:
            if (
                first_block < s.next_block <= last_block + STREAM_READAHEAD_BLOCKS + 1
                and s.want_block < last_block
            ):
                s.want_block = last_block
                self.changed.notify_all()

        block = first_block
        while block <= last_block:
            if self.has_block(block):
                block += 1
                continue
            if block in self._fetching:
                self.changed.wait()
                continue

            failed = next(
                (
                    s
                    for s in self._streams
                    if s.error
                    and s.next_block <= block <= s.next_block + MAX_STREAM_SKIP_BLOCKS
                ),
                None,
            )
            if failed:
                raise failed.error
            self._streams = [s for s in self._streams if not s.done]

            stream = self._find_stream(block)
            if stream is None and (block == 0 or self.has_block(block - 1)):
                # A read continuing on from cached data (or from the start of the file) looks
                # sequential, so request the rest of the file in one go rather than block by block
                stream = self._open_stream(block)
            if stream is not None:
                if block > stream.want_block:
                    stream.want_block = block
                    self.changed.notify_all()
                # The stream may be stopped by another reader while this waits, so rather than wait
                # on it specifically, look again from the top once anything changes
                self.changed.wait()
                continue

            # Random access: fetch just the missing blocks this read needs, in one bounded request
            end_block = block
            while (
                end_block < last_block
                and not self.has_block(end_block + 1)
                and end_block + 1 not in self._fetching
            ):
                end_block += 1
            blocks = range(block, end_block + 1)
            self._fetching.update(blocks)
            try:
                self.lock.release()
                try:
                    data = self._fetch_range(block, end_block)
                finally:
                    self.lock.acquire()
                for b in blocks:
                    self.store_block(b, data[b - block])
            finally:
                self._fetching.difference_update(blocks)
                self.changed.notify_all()
            block = end_block + 1

    def _find_stream(self, block):
        reachable = [
            s
            for s in self._streams
            if not s.done
            and s.next_block <= block <= s.next_block + MAX_STREAM_SKIP_BLOCKS
        ]
        if not reachable:
            return None
        stream = max(reachable, key=lambda s: s.next_block)
        # Keep the list in least-recently-used order
        self._streams.remove(stream)
        self._streams.append(stream)
        return stream

    def _open_stream(self, block):
        if len(self._streams) >= MAX_STREAMS_PER_FILE:
            self._streams.pop(0).abandoned = True
            self.changed.notify_all()
        stream = _BlockStream(self, block)
        self._streams.append(stream)
        return stream

    def _fetch_range(self, first_block, last_block):
        start_byte = first_block * BLOCK_SIZE
        end_byte = min((last_block + 1) * BLOCK_SIZE, self._size) - 1
        stream = self.node.open(mode="rb", start_byte=start_byte, end_byte=end_byte)
        try:
            return [
                _read_exactly(stream, self.block_length(block))
                for block in range(first_block, last_block + 1)
            ]
        finally:
            stream.close()

    def block_length(self, block):
        return min(BLOCK_SIZE, self._size - block * BLOCK_SIZE)

    def _close_streams(self):
        for stream in self._streams:
            stream.abandoned = True
        self._streams = []
        self.changed.notify_all()

    def _load_blocks(self):
        try:
            with open(self.blocks_path, "rb") as f:
                blocks = bytearray(f.read())
            data_size = os.path.getsize(self.data_path)
        except OSError:
            return bytearray(self.n_blocks)

        cached = [b for b, is_cached in enumerate(blocks) if is_cached]
        if len(blocks) != self.n_blocks or (
            cached
            and data_size < (cached[-1] * BLOCK_SIZE) + self.block_length(cached[-1])
        ):
            # Left over from an interrupted write, or otherwise inconsistent; start over
            return bytearray(self.n_blocks)
        return blocks

    def _save_blocks(self):
        try:
            with open(self.blocks_path, "wb") as f:
                f.write(self._blocks)
        except OSError:
            # Only needed to reuse the cache in a later mount
            pass

    def _pread(self, length, offset):
        if hasattr(os, "pread"):
            return os.pread(self._fd, length, offset)
        with self._io_lock:
            os.lseek(self._fd, offset, os.SEEK_SET)
            return os.read(self._fd, length)

    def _pwrite(self, data, offset):
        view = memoryview(data)
        if hasattr(os, "pwrite"):
            while view:
                written = os.pwrite(self._fd, view, offset)
                view = view[written:]
                offset += written
            return
        with self._io_lock:
            os.lseek(self._fd, offset, os.SEEK_SET)
            while view:
                view = view[os.write(self._fd, view) :]


def _read_exactly(stream, length, should_stop=None):
    chunks = []
    remaining = length
    while remaining > 0:
        if should_stop and should_stop():
            return None
        chunk = stream.read(min(remaining, STREAM_CHUNK_SIZE))
        if not chunk:
            raise OSError(
                errno.EIO,
                f"The file's contents ended {remaining} bytes short of its listed size",
            )
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _cache_key(node):
    # A file's hash uniquely identifies its contents, so files with the same contents share one
    # cached copy, and a file that changes on Redivis is never served from a stale one
    if node.hash:
        return node.hash.hex()
    return f"{re.sub(r'[^A-Za-z0-9._-]', '_', str(node.id))}-{node.size}"


def _size_on_disk(stat_result):
    # Cached files are sparse, so count the blocks actually allocated where the platform reports them
    if hasattr(stat_result, "st_blocks"):
        return stat_result.st_blocks * 512
    return stat_result.st_size


class RedivisFS(Operations):
    def __init__(self, directory, cache_dir, max_cache_size=None):
        self.directory = directory
        self.cache_dir = str(cache_dir)
        self._cache = _Cache(self.cache_dir, max_cache_size)
        self._file_handles = {}
        self._next_fh = 1
        self._lock = threading.Lock()
        self._mounted_at = int(time.time())

    def _get_node(self, path):
        """Get the file or directory node for the given path"""
        if path == "/":
            return self.directory

        # Remove leading slash and get node
        clean_path = path.lstrip("/")
        try:
            node = self.directory.get(clean_path)
        except exceptions.ValueError:
            raise FuseOSError(errno.ENOENT)

        if node is None:
            raise FuseOSError(errno.ENOENT)
        else:
            return node

    def getattr(self, path, fh=None):
        """Get file attributes"""
        node = self._get_node(path)

        # Default attributes
        attrs = {
            "st_uid": os.getuid(),
            "st_gid": os.getgid(),
            "st_atime": self._mounted_at,
            "st_mtime": self._mounted_at,
            "st_ctime": self._mounted_at,
        }

        if hasattr(node, "children"):  # Directory
            attrs["st_mode"] = stat.S_IFDIR | 0o755
            attrs["st_nlink"] = 2
            attrs["st_size"] = 0
        else:  # File
            attrs["st_mode"] = stat.S_IFREG | 0o644
            attrs["st_nlink"] = 1
            attrs["st_size"] = node.size or 0
            if hasattr(node, "added_at") and node.added_at:
                # The omission of ctime is intentional – this should always reflect when the directory was mounted
                attrs["st_mtime"] = int(node.added_at.timestamp())
                attrs["st_atime"] = int(node.added_at.timestamp())

        return attrs

    def readdir(self, path, fh):
        """List directory contents"""
        node = self._get_node(path)

        if not hasattr(node, "children"):
            raise FuseOSError(errno.ENOTDIR)

        entries = [".", ".."]
        for child_name in node.children.keys():
            entries.append(child_name)

        for entry in entries:
            yield entry

    def open(self, path, flags):
        """Open a file"""
        node = self._get_node(path)

        if hasattr(node, "children"):
            raise FuseOSError(errno.ENOENT)

        # Only allow read access
        if (flags & os.O_WRONLY) or (flags & os.O_RDWR):
            raise FuseOSError(errno.EACCES)

        # All handles on files with the same contents share one cached copy
        cached_file = self._cache.get(_cache_key(node))
        try:
            cached_file.open(node)
        except OSError as e:
            raise FuseOSError(e.errno or errno.EIO)

        with self._lock:
            fh = self._next_fh
            self._next_fh += 1
            self._file_handles[fh] = cached_file

        return fh

    def read(self, path, length, offset, fh):
        """Read from a file"""
        with self._lock:
            cached_file = self._file_handles.get(fh)
        if cached_file is None:
            raise FuseOSError(errno.EBADF)

        try:
            return cached_file.read(length, offset)
        except Exception:
            # Map any I/O error to a generic EIO for FUSE
            raise FuseOSError(errno.EIO)

    def release(self, path, fh):
        """Close a file"""
        with self._lock:
            cached_file = self._file_handles.pop(fh, None)
        if cached_file is not None:
            try:
                cached_file.release()
            except Exception:
                pass
        return 0

    def close(self):
        """Release every open file, e.g. once the filesystem has been unmounted"""
        with self._lock:
            handles = list(self._file_handles)
        for fh in handles:
            self.release(None, fh)

    def statfs(self, path):
        """Get filesystem statistics"""
        return {
            "f_bsize": 4096,
            "f_blocks": 1000000,
            "f_bavail": 1000000,
            "f_bfree": 1000000,
        }


def _run_fuse_and_cleanup(fs, mount_path, remove_cache_dir):
    """Run the FUSE event loop, then remove the mount directory (and a temporary cache) when it exits."""
    try:
        FUSE(
            fs,
            str(mount_path),
            nothreads=False,
            foreground=True,
            max_threads=os.cpu_count() or 2,
        )
    except Exception as e:
        print(e)
        pass
    finally:
        fs.close()
        try:
            mount_path.rmdir()
        except OSError:
            pass
        if remove_cache_dir:
            shutil.rmtree(fs.cache_dir, ignore_errors=True)


def mount_directory(directory, path, foreground, cache_dir=None, max_cache_size=None):

    mount_path = path.expanduser()

    if mount_path.exists():
        raise exceptions.ValueError(f"Mount path {mount_path} already exists")

    if max_cache_size is not None and max_cache_size < 0:
        raise exceptions.ValueError("max_cache_size must not be negative")

    # Without an explicit cache_dir, files are cached in a private temporary directory that is
    # removed on unmount. An explicit cache_dir is kept, so later mounts can reuse it.
    remove_cache_dir = cache_dir is None
    if remove_cache_dir:
        cache_dir = tempfile.mkdtemp(prefix="redivis_mount_cache_")
        # In case the process exits while still mounted, and the FUSE thread never cleans up
        atexit.register(shutil.rmtree, cache_dir, ignore_errors=True)
    else:
        cache_dir = os.path.expanduser(str(cache_dir))
        os.makedirs(cache_dir, mode=0o700, exist_ok=True)

    mount_path.mkdir(parents=True)

    # Create and start FUSE filesystem
    fs = RedivisFS(directory, cache_dir, max_cache_size)
    print(f"Mounted directory at {mount_path}")
    if foreground:
        _run_fuse_and_cleanup(fs, mount_path, remove_cache_dir)
    else:
        mount_thread = threading.Thread(
            target=_run_fuse_and_cleanup,
            args=(fs, mount_path, remove_cache_dir),
            daemon=True,
        )
        mount_thread.start()
        return mount_thread
