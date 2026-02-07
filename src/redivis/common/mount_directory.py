import os
import stat
import errno
import threading


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
        return self.directory.get(clean_path)

    def getattr(self, path, fh=None):
        """Get file attributes"""
        node = self._get_node(path)
        if not node:
            raise FuseOSError(errno.ENOENT)

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
        if not node or not hasattr(node, "children"):
            raise FuseOSError(errno.ENOTDIR)

        entries = [".", ".."]
        for child_name in node.children.keys():
            entries.append(child_name)

        for entry in entries:
            yield entry

    def open(self, path, flags):
        """Open a file"""
        node = self._get_node(path)
        if not node or hasattr(node, "children"):
            raise FuseOSError(errno.ENOENT)

        # Only allow read access
        if (flags & os.O_WRONLY) or (flags & os.O_RDWR):
            raise FuseOSError(errno.EACCES)

        with self._fh_lock:
            fh = self._next_fh
            self._next_fh += 1
            self._file_handles[fh] = {"node": node, "stream": None, "position": 0}

        return fh

    def read(self, path, length, offset, fh):
        """Read from a file"""
        if fh not in self._file_handles:
            raise FuseOSError(errno.EBADF)

        handle = self._file_handles[fh]
        node = handle["node"]

        try:
            # Create or reuse stream
            if not handle["stream"] or handle["position"] != offset:
                if handle["stream"]:
                    handle["stream"].close()
                handle["stream"] = node.open(start_byte=offset)
                handle["position"] = offset

            data = handle["stream"].read(length)
            handle["position"] += len(data)
            return data

        except Exception as e:
            raise FuseOSError(errno.EIO)

    def release(self, path, fh):
        """Close a file"""
        if fh in self._file_handles:
            handle = self._file_handles[fh]
            if handle["stream"]:
                handle["stream"].close()
            del self._file_handles[fh]
        return 0

    def statfs(self, path):
        """Get filesystem statistics"""
        return {
            "f_bsize": 4096,
            "f_blocks": 1000000,
            "f_bavail": 1000000,
            "f_bfree": 1000000,
        }


def mount_directory(directory, path, foreground):

    # Ensure mount point exists

    mount_path = path.expanduser()
    mount_path.mkdir(parents=True, exist_ok=True)

    if not mount_path.is_dir():
        raise ValueError(f"Mount path {path} is not a directory")

    # Check if mount point is empty
    if any(mount_path.iterdir()):
        raise ValueError(f"Mount path {path} is not empty")

    # Create and start FUSE filesystem
    fs = RedivisFS(directory)
    print(f"Mounted directory at {mount_path}")
    fuse = FUSE(fs, str(mount_path), nothreads=False, foreground=foreground)
