from .Base import Base
import os
import time
import pathlib

from ..common import exceptions
from ..common.api_request import make_request
from tqdm.auto import tqdm
import re

from ..common.retryable_download import perform_parallel_download


class Export(Base):
    def __init__(
        self,
        id,
        *,
        table=None,
        properties=None,
    ):
        self.id = id
        self.table = table
        self.properties = properties
        self.uri = (self.properties or {}).get("uri") or f"/exports/{id}"

    def __repr__(self):
        return f"<Export {self.id}>"

    def get(self):
        self.properties = make_request(method="GET", path=self.uri)
        return self

    def download_files(
        self,
        *,
        path=None,
        overwrite=False,
        progress=True,
        max_parallelization=None,
        max_concurrency=None,
    ):
        self.wait_for_finish()
        file_count = self.properties["fileCount"]
        escaped_table_name = re.sub(
            r"\W+", "_", self.properties.get("table", {}).get("name", "table")
        ).lower()
        is_dir = False
        if path:
            path = os.path.expanduser(path)
        if path is None or (os.path.exists(path) and os.path.isdir(path)):
            is_dir = True
            if path is None:
                path = os.getcwd()
            if file_count > 1:
                path = os.path.join(path, escaped_table_name)
        elif path.endswith(os.sep) or (not os.path.exists(path) and "." not in path):
            is_dir = True
        elif file_count > 1:
            raise exceptions.ValueError(
                f"Path '{path}' is a file, but the export consists of multiple files. Please specify the path to a directory"
            )

        if (
            overwrite is False
            and os.path.exists(path)
            and (not is_dir or file_count > 1)
        ):
            raise exceptions.ValueError(
                f"File already exists at '{path}'. Set parameter overwrite=True to overwrite existing files."
            )

        # Make sure output directory exists
        if is_dir:
            pathlib.Path(path).mkdir(exist_ok=True, parents=True)
        else:
            pathlib.Path(path).parent.mkdir(exist_ok=True, parents=True)

        uris = [
            f"{self.uri}/download?filePart={file_number}"
            for file_number in range(file_count)
        ]
        download_paths = [
            str(
                pathlib.Path(path)
                / f"{escaped_table_name if file_count == 1 else str(file_number).zfill(6)}.{self.properties['format']}"
                if is_dir
                else path
            )
            for file_number in range(file_count)
        ]

        perform_parallel_download(
            uris=uris,
            download_paths=download_paths,
            overwrite=overwrite,
            max_parallelization=max_parallelization,
            max_concurrency=max_concurrency,
            total_bytes=self.properties.get("size"),
            progress=progress,
        )

        return download_paths

    def wait_for_finish(self, *, progress=True):
        iter_count = 0
        if progress:
            pbar = tqdm(
                total=100, leave=False, unit="%", unit_scale=True, mininterval=0.1
            )
            pbar.set_description(f"Preparing download...")

        while True:
            if self.properties["status"] == "completed":
                if progress:
                    pbar.close()
                break
            elif self.properties["status"] == "failed":
                if progress:
                    pbar.close()
                raise exceptions.JobError(
                    kind=self.properties.get("kind"),
                    message=self.properties.get("errorMessage"),
                    status=self.properties.get("status"),
                )
            elif self.properties["status"] == "cancelled":
                if progress:
                    pbar.close()
                raise exceptions.JobError(
                    kind=self.properties.get("kind"),
                    message=self.properties.get("errorMessage", "Export was cancelled"),
                    status=self.properties.get("status"),
                )
            else:
                iter_count += 1
                if progress:
                    pbar.update(self.properties["percentCompleted"] - pbar.n)
                time.sleep(min(iter_count * 0.5, 2))
                self.get()
