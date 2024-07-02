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
from contextlib import closing


class Export(Base):
    def __init__(
        self,
        id,
        *,
        table=None,
        properties={},
    ):
        self.table = table
        self.properties = properties
        self.uri = self.properties["uri"]

    def get(self):
        self.properties = make_request(method="GET", path=self.uri)
        return self

    def download_files(self, *, path, overwrite, progress=True):
        # TODO: if overwriting file, first check if file is the same size, and if so check md5 hash, and skip if identical (need md5 in header)
        self.wait_for_finish()
        file_count = self.properties["fileCount"]
        is_dir = False
        if path is None or (os.path.exists(path) and os.path.isdir(path)):
            is_dir = True
            if path is None:
                path = os.getcwd()
            if file_count > 1:
                if not hasattr(self.table.properties, "name"):
                    self.table.get()
                escaped_table_name = re.sub(
                    r"\W+", "_", self.table.properties["name"]
                ).lower()
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
                total=self.properties["size"], leave=False, unit="iB", unit_scale=True
            )

        cancel_event = Event()
        # Code to use multiprocess... would simplify exiting on stop, but progress doesn't currently work
        # with concurrent.futures.ProcessPoolExecutor(max_workers=len(read_session["streams"]), mp_context=mp.get_context('fork')) as executor:

        output_file_paths = []
        # TODO: this should use async, not threads
        # See https://github.com/googleapis/python-bigquery/blob/main/google/cloud/bigquery/_pandas_helpers.py#L920
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(os.cpu_count() * 5, file_count)
        ) as executor:
            futures = [
                executor.submit(
                    download_file,
                    uri=f"{self.uri}/download",
                    download_path=path,
                    file_number=file_number,
                    is_dir=is_dir,
                    overwrite=overwrite,
                    pbar=pbar,
                    cancel_event=cancel_event,
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
            pbar = tqdm(total=100, leave=False, unit="%", unit_scale=True)
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
    if not fname:
        fname = re.findall("filename=([^;]+)", s, flags=re.IGNORECASE)
    if "utf-8''" in fname[0].lower():
        fname = re.sub("utf-8''", "", fname[0], flags=re.IGNORECASE)
        fname = urllib.unquote(fname).decode("utf8")
    else:
        fname = fname[0]
    # clean space and double quotes
    return fname.strip().strip('"')


def download_file(
    *,
    uri,
    file_number,
    download_path,
    is_dir=False,
    overwrite=False,
    pbar=None,
    cancel_event,
):
    with closing(
        make_request(
            method="GET",
            path=uri,
            query={"filePart": file_number},
            stream=True,
            parse_response=False,
        )
    ) as r:
        if is_dir:
            file_name = get_filename(r.headers.get("Content-Disposition"))
            download_path = os.path.join(download_path, file_name)

        if overwrite is False and os.path.exists(download_path):
            raise Exception(
                f"File already exists at '{download_path}'. Set parameter overwrite=True to overwrite existing files."
            )

        with open(download_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if cancel_event.is_set():
                    break
                if pbar:
                    pbar.update(len(chunk))
                f.write(chunk)

    return download_path
