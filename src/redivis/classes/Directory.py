import warnings

from .Base import Base
from .File import File
import os
from pathlib import Path

import time
import concurrent.futures
from tqdm.auto import tqdm
from threading import Event
from typing import Literal, Optional, List, Union


class Directory(Base):
    def __init__(
        self,
        *,
        path: Union[str, Path],
        table: Optional["Table"] = None,
        query: Optional["Query"] = None,
        parent: Optional["Directory"] = None,
    ) -> None:
        if not table and not query:
            raise ValueError("All directories must either belong to a table or query.")

        self.path = path
        self.name = Path(path).name
        self.table = table
        self.query = query
        self.parent = parent
        self.children = {}
        self._lastCachedAt = time.time()

    def __repr__(self) -> str:
        return f"<Dir {str(self.path)}>"

    def list(
        self,
        *,
        mode: Literal["all", "files", "directories"] = "all",
        recursive: bool = False,
        max_results: Optional[int] = None,
    ) -> List[Union["Directory", File]]:
        if max_results is None:
            max_results = float("inf")
        children: List[Union["Directory", File]] = []
        # TODO: support glob pattern? or regex?
        for child in self.children.values():
            if len(children) >= max_results:
                break
            if recursive and isinstance(child, Directory):
                if mode == "all" or mode == "directories":
                    children.append(child)
                children.extend(
                    child.list(
                        mode=mode,
                        recursive=True,
                        max_results=max_results - len(children),
                    )
                )
            else:
                if (
                    mode == "all"
                    or (mode == "files" and isinstance(child, File))
                    or (mode == "directories" and isinstance(child, Directory))
                ):
                    children.append(child)
        return children

    def get(self, path: Union[str, Path]) -> Union["Directory", File, None]:
        is_explicit_dir = str(path).endswith("/")
        # This is to handle the case when the file was constructed via a file id, in legacy code
        if isinstance(path, str):
            # TODO: remove this after a bit
            split = path.split(".")
            client_id = split[0]
            if (
                len(split) == 2
                and len(client_id) == 14
                and len(client_id.split("-")) == 2
                and len(client_id.split("-")[0]) == 4
            ):
                warnings.warn(
                    "Passing file ids is deprecated, please use file names instead. E.g.: table.file('filename.png')",
                    FutureWarning,
                    stacklevel=2,
                )
                files = self.list(mode="files", recursive=True)
                for f in files:
                    if f.id == path:
                        return f
                return None

        # Normalize input to a Path
        if not isinstance(path, Path):
            path = Path(path)

        if path.is_absolute():
            # Need to use os.path.relpath instead of Path.relative_to to allow for paths that traverse above the current directory
            relative = Path(os.path.relpath(path, self.path))
        else:
            relative = path

        parts = list(relative.parts)
        if not parts:
            return self

        node = self
        for idx, part in enumerate(parts):
            # Handle current directory references
            if part == "." or part == "":
                continue

            # Handle parent directory traversal
            if part == "..":
                if node.parent is None:
                    # Can't traverse above the root of this tree
                    raise ValueError("Path traverses above the root directory")
                node = node.parent
                # If '..' is the last segment, return the directory reached
                if idx == len(parts) - 1:
                    return node
                continue

            # Walk into children
            child = node.children.get(part)
            if not child:
                return None
            if isinstance(child, Directory):
                node = child
            else:
                # Must be the last part to match a file
                if idx == len(parts) - 1:
                    if is_explicit_dir:
                        return None
                    return child
                return None

        # If we finished traversing parts without returning, we resolved to a directory
        return node

    def mount(
        self, path: Optional[Union[str, Path]] = None, *, foreground: bool = False
    ) -> None:
        if (
            os.getenv("REDIVIS_NOTEBOOK_ID") is not None
            and os.getenv("REDIVIS_NOTEBOOK_ENABLE_FUSE") != "TRUE"
        ):
            raise RuntimeError(
                "Mounting directories is not supported within the default Redivis Notebooks. You must configure a custom machine to call directory.mount()"
            )
        from ..common.mount_directory import mount_directory

        if path is None:
            if self.parent is not None:
                default_name = self.name
            elif self.table:
                default_name = self.table.properties["name"]
            else:
                default_name = "redivis_mount"

            path = Path.cwd() / default_name
        elif isinstance(path, str):
            path = Path(path)

        mount_directory(self, path, foreground=foreground)
        return path

    def download(
        self,
        path: Optional[Union[str, Path]] = None,
        *,
        max_results: Optional[int] = None,
        max_parallelization: Optional[int] = None,
        overwrite: bool = False,
        progress: bool = True,
    ) -> None:
        if isinstance(path, str):
            path = Path(path)
        if path is None:
            path = Path.cwd() / self.name

        if path.exists():
            if path.is_file():
                if overwrite:
                    path.unlink()
                else:
                    raise FileExistsError(
                        f"Destination path '{path}' exists and is a file; set overwrite=True to replace it."
                    )

        path.mkdir(parents=True, exist_ok=True)
        cancel_event = Event()

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(
                100, max_parallelization if max_parallelization is not None else 50
            ),
        ) as executor:
            futures = []
            files_to_download = self.list(
                mode="files", recursive=True, max_results=max_results
            )

            file_count = 0
            total_bytes = 0
            for f in files_to_download:
                total_bytes += f.size

            pbar = tqdm(
                unit="B",
                leave=False,
                unit_scale=True,
                mininterval=0.1,
                total=total_bytes if progress else 0,
                desc=f"{file_count}/{len(files_to_download)} files",
                disable=not progress,
            )

            def on_progress(chunk_bytes: int) -> None:
                pbar.update(chunk_bytes)

            # Create and submit download tasks with per-file progress callbacks
            for file in files_to_download:
                file_relative_path = (Path("/") / file.path).relative_to(self.path)
                dest_path = os.path.join(path, str(file_relative_path))

                futures.append(
                    executor.submit(
                        file.download,
                        path=dest_path,
                        overwrite=overwrite,
                        progress=False,
                        on_progress=on_progress if progress else None,
                        cancel_event=cancel_event,
                    )
                )

            not_done = futures
            try:
                while not_done and not cancel_event.is_set():
                    # next line 'sleeps' this main thread, letting the thread pool run
                    freshly_done, not_done = concurrent.futures.wait(
                        not_done, timeout=0.5
                    )
                    for future in freshly_done:
                        # Call result() on any finished threads to raise any exceptions encountered.
                        future.result()
                        file_count += 1
                        pbar.set_description(
                            f"{file_count}/{len(files_to_download)} files"
                        )
            except KeyboardInterrupt:
                print("KeyboardInterrupt")
            finally:
                cancel_event.set()

                for future in not_done:
                    # Only cancels futures that were never started
                    future.cancel()

                # Shutdown all background threads, now that they should know to exit early.
                executor.shutdown(wait=True, cancel_futures=True)
                pbar.close()

    def _add_file(self, file: File) -> None:
        relative_path_parts = (Path("/") / file.path).relative_to(self.path).parts

        if len(relative_path_parts) == 1:
            self.children[relative_path_parts[0]] = file
            file.directory = self
        else:
            subdir = self.children.get(str(relative_path_parts[0]))
            if not subdir:
                subdir = Directory(
                    parent=self,
                    path=self.path / relative_path_parts[0],
                    table=self.table,
                    query=self.query,
                )
                self.children[(str(relative_path_parts[0]))] = subdir
            subdir._add_file(file)
