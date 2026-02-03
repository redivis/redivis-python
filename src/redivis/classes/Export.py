from .Base import Base
import os
import time
import concurrent.futures
import pathlib
from ..common.api_request import make_request
from tqdm.auto import tqdm
import re
import urllib
from threading import Event

from ..common.retryable_download import perform_retryable_download


class Export(Base):
    def __init__(
        self,
        id,
        *,
        table=None,
        properties=None,
    ):
        self.table = table
        self.properties = properties
        self.uri = (self.properties or {}).get("uri") or f"/exports/{id}"

    def get(self):
        self.properties = make_request(method="GET", path=self.uri)
        return self

    def download_files(
        self, *, path=None, overwrite=False, progress=True, max_parallelization=None
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
            raise Exception(
                f"Path '{path}' is a file, but the export consists of multiple files. Please specify the path to a directory"
            )

        if (
            overwrite is False
            and os.path.exists(path)
            and (not is_dir or file_count > 1)
        ):
            raise Exception(
                f"File already exists at '{path}'. Set parameter overwrite=True to overwrite existing files."
            )

        # Make sure output directory exists
        if is_dir:
            pathlib.Path(path).mkdir(exist_ok=True, parents=True)
        else:
            pathlib.Path(path).parent.mkdir(exist_ok=True, parents=True)

        pbar = None
        if progress:
            pbar = tqdm(
                total=self.properties["size"],
                leave=False,
                unit="iB",
                unit_scale=True,
                mininterval=0.1,
            )

        cancel_event = Event()

        output_file_paths = []

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(
                100, max_parallelization if max_parallelization is not None else 50
            ),
        ) as executor:
            futures = [
                executor.submit(
                    perform_retryable_download,
                    path=f"{self.uri}/download",
                    pbar=pbar,
                    cancel_event=cancel_event,
                    query={"filePart": file_number},
                    overwrite=overwrite,
                    filename=(
                        pathlib.Path(path)
                        / f"{escaped_table_name if file_count == 1 else str(file_number).zfill(6)}.{self.properties['format']}"
                        if is_dir
                        else path
                    ),
                )
                for file_number in range(file_count)
            ]

            not_done = futures

            try:
                while not_done and not cancel_event.is_set():
                    # next line 'sleeps' this main thread, letting the thread pool run
                    freshly_done, not_done = concurrent.futures.wait(
                        not_done, timeout=0.2
                    )
                    for future in freshly_done:
                        # Call result() on any finished threads to raise any exceptions encountered.
                        output_file_paths.append(future.result())
            finally:
                cancel_event.set()
                # Shutdown all background threads, now that they should know to exit early.
                executor.shutdown(wait=True, cancel_futures=True)

        if pbar:
            pbar.close()

        return output_file_paths

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
                raise Exception(
                    f"Export job failed with message: {self.properties['errorMessage']}"
                )
            elif self.properties["status"] == "cancelled":
                if progress:
                    pbar.close()
                raise Exception(f"Export job was cancelled")
            else:
                iter_count += 1
                if progress:
                    pbar.update(self.properties["percentCompleted"] - pbar.n)
                time.sleep(min(iter_count * 0.5, 2))
                self.get()


def get_filename(s):
    fname = re.findall("filename\*=([^;]+)", s, flags=re.IGNORECASE)
    if not len(fname):
        fname = re.findall("filename=([^;]+)", s, flags=re.IGNORECASE)

    if len(fname) and "utf-8''" in fname[0].lower():
        fname = re.sub("utf-8''", "", fname[0], flags=re.IGNORECASE)
        fname = urllib.unquote(fname).decode("utf8")
    elif len(fname):
        fname = fname[0]
    else:
        fname = ""
    # clean space and double quotes
    return fname.strip().strip('"')
