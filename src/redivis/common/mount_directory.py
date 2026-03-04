import os
import stat
import errno
import threading

from ..common import exceptions
from mfusepy import FUSE, FuseOSError, Operations


class RedivisFS(Operations):
    def __init__(self, directory):
        self.directory = directory
        self._file_handles = {}
        self._next_fh = 1
        self._fh_lock = threading.Lock()

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
            "st_atime": 0,
            "st_mtime": 0,
            "st_ctime": 0,
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
                attrs["st_mtime"] = int(node.added_at.timestamp())

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

        with self._fh_lock:
            fh = self._next_fh
            self._next_fh += 1
            # Add a per-handle lock so that operations on the same handle
            # remain serialized without blocking other handles.
            self._file_handles[fh] = {
                "node": node,
                "stream": None,
                "position": 0,
                "lock": threading.Lock(),
            }

        return fh

    def read(self, path, length, offset, fh):
        """Read from a file"""
        # First, look up the handle and its per-handle lock under the
        # global file-handle lock, then release it before doing I/O.
        with self._fh_lock:
            handle = self._file_handles.get(fh)
            if handle is None:
                raise FuseOSError(errno.EBADF)
            handle_lock = handle.get("lock")

        # Fallback in case an older handle dict is missing "lock"
        if handle_lock is None:
            handle_lock = threading.Lock()
            with self._fh_lock:
                current = self._file_handles.get(fh)
                if current is None:
                    raise FuseOSError(errno.EBADF)
                if "lock" not in current:
                    current["lock"] = handle_lock
                else:
                    handle_lock = current["lock"]

        with handle_lock:
            # Snapshot the current handle state under the global lock,
            # then perform I/O without holding _fh_lock.
            with self._fh_lock:
                handle = self._file_handles.get(fh)
                if handle is None:
                    raise FuseOSError(errno.EBADF)
                node = handle["node"]
                stream = handle["stream"]
                position = handle["position"]

            try:
                # Create or reuse stream
                if not stream or position != offset:
                    if stream:
                        stream.close()
                    stream = node.open(mode="rb", start_byte=offset)
                    position = offset

                data = stream.read(length)
                position += len(data)

            except Exception:
                # Map any I/O error to a generic EIO for FUSE
                raise FuseOSError(errno.EIO)

            # Update shared handle state under the global lock, ensuring the
            # handle still exists.
            with self._fh_lock:
                current = self._file_handles.get(fh)
                if current is None:
                    # Handle was released while we were reading; close the
                    # local stream and report EBADF.
                    if stream:
                        try:
                            stream.close()
                        except Exception:
                            pass
                    raise FuseOSError(errno.EBADF)
                current["stream"] = stream
                current["position"] = position

            return data

    def release(self, path, fh):
        """Close a file"""
        # Remove the handle from the table under the global lock, but perform
        # any potentially slow close operations without holding _fh_lock.
        stream_to_close = None
        with self._fh_lock:
            handle = self._file_handles.pop(fh, None)
            if handle is not None:
                stream_to_close = handle.get("stream")

        if stream_to_close:
            try:
                stream_to_close.close()
            except Exception:
                pass
        return 0

    def statfs(self, path):
        """Get filesystem statistics"""
        return {
            "f_bsize": 4096,
            "f_blocks": 1000000,
            "f_bavail": 1000000,
            "f_bfree": 1000000,
        }


def _run_fuse_and_cleanup(fs, mount_path):
    """Run the FUSE event loop and remove the mount directory when it exits."""
    try:
        FUSE(
            fs,
            str(mount_path),
            nothreads=False,
            foreground=True,
            max_threads=os.cpu_count(),
        )
    except Exception as e:
        print(e)
        pass
    finally:
        try:
            mount_path.rmdir()
        except OSError:
            pass


def mount_directory(directory, path, foreground):

    mount_path = path.expanduser()

    if mount_path.exists():
        raise exceptions.ValueError(f"Mount path {mount_path} already exists")

    mount_path.mkdir(parents=True)

    # Create and start FUSE filesystem
    fs = RedivisFS(directory)
    print(f"Mounted directory at {mount_path}")
    if foreground:
        _run_fuse_and_cleanup(fs, mount_path)
    else:
        mount_thread = threading.Thread(
            target=_run_fuse_and_cleanup,
            args=(fs, mount_path),
            daemon=True,
        )
        mount_thread.start()
        return mount_thread
