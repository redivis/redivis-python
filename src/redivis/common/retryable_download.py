from tqdm.auto import tqdm
import os
import pathlib
from base64 import b64decode
import time
import hashlib
from ..common.api_request import make_request
from contextlib import closing
from requests import RequestException
from urllib3.exceptions import HTTPError


def perform_retryable_download(
    #     IMPORTANT: if updating params, make sure to update retry call signature below
    path,
    *,
    filename=None,
    progress=True,
    pbar=None,
    cancel_event=None,
    query=None,
    overwrite=False,
    size=None,
    md5_hash=None,
    start_byte=0,
    on_progress=None,
    retry_count=0,
):
    if size is not None and md5_hash is not None:
        exact_file_exists = check_filename(
            filename, overwrite, retry_count, size, md5_hash, on_progress
        )
        if exact_file_exists:
            return filename

    try:
        with closing(
            make_request(
                method="GET",
                path=path,
                stream=True,
                query=query,
                parse_response=False,
                headers={"Range": f"bytes={start_byte}-"} if start_byte else None,
            )
        ) as r:
            size = r.headers.get("x-redivis-size")
            md5_hash = r.headers.get("x-redivis-hash")
            exact_file_exists = check_filename(
                filename, overwrite, retry_count, size, md5_hash, on_progress
            )
            if exact_file_exists:
                return filename

            # Make sure output directory exists
            pathlib.Path(filename).parent.mkdir(exist_ok=True, parents=True)

            with open(filename, "wb" if retry_count is 0 else "ab") as f:
                if progress and not pbar:
                    pbar = tqdm(
                        total=int(r.headers["content-length"]),
                        leave=False,
                        unit="iB",
                        unit_scale=True,
                        mininterval=0.1,
                    )

                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    retry_count = 0
                    if cancel_event and cancel_event.is_set():
                        os.remove(filename)
                        return None
                    if pbar:
                        pbar.update(len(chunk))
                    f.write(chunk)
                    if on_progress:
                        on_progress(len(chunk))
    except (RequestException, HTTPError) as e:
        if retry_count < 10:
            time.sleep(retry_count)
            return perform_retryable_download(
                path=path,
                filename=filename,
                filename_cb=filename_cb,
                progress=progress,
                pbar=pbar,
                query=query,
                size=size,
                md5_hash=md5_hash,
                overwrite=overwrite,
                cancel_event=cancel_event,
                start_byte=(
                    os.path.getsize(filename)
                    if filename and os.path.exists(filename)
                    else 0
                ),
                on_progress=on_progress,
                retry_count=retry_count + 1,
            )
        else:
            print("Download connection failed after too many retries, giving up.")
            raise e

    if progress:
        pbar.close()

    return filename


def check_filename(filename, overwrite, retry_count, size, md5_hash, on_progress):
    if retry_count == 0 and os.path.exists(filename):
        if (
            size is not None
            and md5_hash is not None
            and os.path.getsize(filename) == int(size)
        ):
            if isinstance(md5_hash, str):
                md5_hash = b64decode(md5_hash)

            file_hash = hashlib.md5()
            with open(filename, "rb") as f:
                for byte_block in iter(lambda: f.read(8192), b""):
                    file_hash.update(byte_block)

            if file_hash.digest() == md5_hash:
                if on_progress:
                    on_progress(size)
                return filename
        elif not overwrite:
            raise Exception(
                f"File already exists at '{filename}'. Set parameter overwrite=True to overwrite existing files."
            )
    return None
