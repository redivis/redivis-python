from tqdm.auto import tqdm
import os
import pathlib
import time
from ..common.api_request import make_request
from contextlib import closing
from requests import RequestException
from urllib3.exceptions import HTTPError


def perform_retryable_download(
    #     IMPORTANT: if updating params, make sure to update retry call signature below
    path,
    *,
    filename=None,
    filename_cb,
    progress=True,
    pbar=None,
    cancel_event=None,
    query=None,
    overwrite=False,
    start_byte=0,
    on_progress=None,
    retry_count=0,
):
    headers = {}
    if start_byte:
        headers["Range"] = f"bytes={start_byte}-"

    try:
        with closing(
            make_request(
                method="GET",
                path=path,
                stream=True,
                query=query,
                parse_response=False,
                headers=headers,
            )
        ) as r:
            if not filename:
                filename = filename_cb(r)

            # Make sure output directory exists
            pathlib.Path(filename).parent.mkdir(exist_ok=True, parents=True)

            if retry_count == 0 and overwrite is False and os.path.exists(filename):
                raise Exception(
                    f"File already exists at '{filename}'. Set parameter overwrite=True to overwrite existing files."
                )

            with open(filename, "wb" if retry_count is 0 else "ab") as f:
                if progress and not pbar:
                    pbar = tqdm(
                        total=int(r.headers["content-length"]),
                        leave=False,
                        unit="iB",
                        unit_scale=True,
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
