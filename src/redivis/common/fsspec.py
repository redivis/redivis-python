from fsspec import AbstractFileSystem
from ..classes.Table import Table
from ..classes.Directory import Directory
from ..classes.File import File
from . import exceptions


class RedivisFileSystem(AbstractFileSystem):
    """FSSpec filesystem implementation for Redivis."""

    protocol = "redivis"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def _parse_path(path):
        """Parse redivis://table_ref/path/to/file into (table_ref, file_path).

        The table_ref is everything before the first '/', and file_path is
        the remainder (may be empty).
        """
        if path.startswith("redivis://"):
            path = path[len("redivis://") :]
        path = path.strip("/")

        parts = path.split("/", 1)
        table_ref = parts[0]
        file_path = parts[1] if len(parts) > 1 else ""
        return table_ref, file_path

    def _get_table_and_directory(self, table_ref):
        table = Table(table_ref)
        directory = table.to_directory()  # returns root Directory
        return table, directory

    def ls(self, path, detail=False, **kwargs):
        table_ref, file_path = self._parse_path(path)
        table, root_dir = self._get_table_and_directory(table_ref)

        if file_path:
            node = root_dir.get(file_path)
        else:
            node = root_dir

        if node is None:
            raise FileNotFoundError(f"No such file or directory: {path}")

        if isinstance(node, File):
            info = self._file_info(table_ref, node)
            return [info] if detail else [info["name"]]

        entries = []
        for child in node.list():
            if isinstance(child, File):
                entries.append(self._file_info(table_ref, child))
            else:
                entries.append(
                    {
                        "name": f"{table_ref}/{child.path}",
                        "size": 0,
                        "type": "directory",
                    }
                )

        if detail:
            return entries
        return [e["name"] for e in entries]

    def _file_info(self, table_ref, file_obj):
        return {
            "name": f"{table_ref}/{file_obj.path}",
            "size": file_obj.size,
            "type": "file",
        }

    def info(self, path, **kwargs):
        table_ref, file_path = self._parse_path(path)

        if not file_path:
            return {"name": path, "size": 0, "type": "directory"}

        table, root_dir = self._get_table_and_directory(table_ref)
        node = root_dir.get(file_path)

        if node is None:
            raise FileNotFoundError(f"No such file or directory: {path}")

        if isinstance(node, Directory):
            return {"name": path, "size": 0, "type": "directory"}

        return self._file_info(table_ref, node)

    def _open(self, path, mode="rb", **kwargs):
        """Open a file from Redivis. Always returns a binary stream;
        fsspec handles text-mode wrapping externally."""
        if mode not in ("rb", "r", "rt"):
            raise exceptions.ValueError(
                f"Redivis filesystem is read-only; mode '{mode}' is not supported"
            )
        table_ref, file_path = self._parse_path(path)
        table = Table(table_ref)
        return table.file(file_path).open("rb")


def register():
    """Register the Redivis filesystem with fsspec."""
    try:
        from fsspec.registry import register_implementation

        register_implementation("redivis", RedivisFileSystem, clobber=True)
    except ImportError:
        pass
