from .Base import Base
import os
from pathlib import Path

import time
import concurrent.futures
from tqdm.auto import tqdm
from threading import Event


class Directory(Base):
    def __init__(self, *, path, table=None, query=None, parent=None):
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
        return f"<Dir {str(self.path)}/>"

    def list(self, *, recursive=False, max_results=None):
        children = []
        counter = 0
        # TODO: support glob pattern? or regex?
        for child in self.children.values():
            if recursive and isinstance(child, Directory):
                children.append(child)
                children.extend(child.list(recursive=True))
            else:
                children.append(child)
            counter += 1
            if max_results is not None and counter >= max_results:
                break
        return children

    def list_files(self, *, recursive=True, max_results=None):
        # TODO: support glob pattern? or regex?
        # if pattern and not re.match(pattern, str(file_relative_path)):
        #     continue
        files = []
        counter = 0
        for child in self.children.values():
            if isinstance(child, Directory):
                if recursive:
                    files.extend(child.list_files(recursive=True))
            else:
                files.append(child)
            counter += 1
            if max_results is not None and counter >= max_results:
                break
        return files

    def get(self, path):
        # Normalize input to a Path
        if isinstance(path, str):
            input_path = Path(path)
        elif isinstance(path, Path):
            input_path = path
        else:
            return None

        # Attempt to make input_path relative to this directory's path if absolute/anchored
        try:
            if input_path.is_absolute() or input_path.anchor:
                relative = input_path.relative_to(self.path)
            else:
                relative = input_path
        except Exception:
            # Not under this directory
            return None

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
                    return None
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
                    return child
                return None

        # If we finished traversing parts without returning, we resolved to a directory
        return node

    def download_files(
        self,
        path=None,
        *,
        max_results=None,
        max_parallelization=None,
        overwrite=False,
        pattern=None,
        recursive=True,
        progress=True,
    ):
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
            files_to_download = self.list_files(
                recursive=recursive, pattern=pattern, max_results=max_results
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

            def on_progress(chunk_bytes: int):
                pbar.update(chunk_bytes)

            # Create and submit download tasks with per-file progress callbacks
            for file in files_to_download:
                file_relative_path = file.path.relative_to(self.path)
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

    def _add_file(self, file):
        relative_path_parts = file.path.relative_to(self.path).parts

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
