import io
import re
import time
import requests
import os
import logging
from tqdm.utils import CallbackIOWrapper

from ..common import exceptions

verify_ssl = (
    False
    if os.getenv("REDIVIS_API_ENDPOINT", "https://redivis.com/api/v1").find(
        "https://localhost", 0
    )
    == 0
    else True
)

MAX_RETRIES = 10

# NB: temp upload URLs are signed, so they need no other credentials. They must never be
# sent the user's Redivis token, since they're proxied to Google Cloud Storage.


def perform_resumable_upload(data, size=None, temp_upload_url=None, progressbar=None):
    retry_count = 0
    start_byte = 0
    is_file = True if hasattr(data, "read") else False
    file_size = size

    if file_size is None:
        file_size = os.stat(data.name).st_size if is_file else len(data)

    resumable_url = initiate_resumable_upload(file_size, temp_upload_url)

    while True:
        if progressbar:
            progressbar.update(start_byte - progressbar.n)
        if is_file:
            data.seek(start_byte)
            chunk = (
                CallbackIOWrapper(progressbar.update, data, "read")
                if progressbar
                else data
            )
        else:
            chunk = data[start_byte:]

        try:
            res = requests.put(
                url=resumable_url,
                verify=verify_ssl,
                headers={
                    "Content-Length": str(file_size - start_byte),
                    # An empty upload has no byte range to send
                    "Content-Range": (
                        f"bytes {start_byte}-{file_size - 1}/{file_size}"
                        if file_size
                        else "bytes */0"
                    ),
                },
                data=chunk,
            )
            res.raise_for_status()
            if res.status_code == 308:
                # GCS only persisted part of the data; send the rest
                start_byte = get_committed_bytes(res)
                continue
            return
        except requests.RequestException as e:
            if retry_count >= MAX_RETRIES:
                raise exceptions.NetworkError(
                    message=f"A network error occurred. Upload failed after {retry_count} retries.",
                    original_exception=e,
                ) from e

            retry_count += 1
            time.sleep(retry_count)
            logging.debug(
                "A network error occurred. Retrying last chunk of resumable upload."
            )
            start_byte = retry_partial_upload(
                file_size=file_size, resumable_url=resumable_url
            )
            if start_byte is None:
                # The upload session expired, so start a new one
                resumable_url = initiate_resumable_upload(file_size, temp_upload_url)
                start_byte = 0
            elif start_byte >= file_size:
                return


def initiate_resumable_upload(size, temp_upload_url, retry_count=0):
    did_request_complete = False
    try:
        res = requests.post(
            temp_upload_url,
            verify=verify_ssl,
            headers={
                "x-upload-content-length": str(size),
                "x-goog-resumable": "start",
            },
        )
        did_request_complete = True
        res.raise_for_status()
        return res.headers["location"]

    except requests.RequestException as e:
        if did_request_complete:
            raise exceptions.NetworkError(original_exception=e)
        else:
            if retry_count >= MAX_RETRIES:
                raise exceptions.NetworkError(
                    message=f"A network error occurred. Upload failed after {retry_count} retries.",
                    original_exception=e,
                ) from e
            time.sleep(retry_count + 1)
            return initiate_resumable_upload(
                size, temp_upload_url, retry_count=retry_count + 1
            )


def retry_partial_upload(*, retry_count=0, file_size, resumable_url):
    """Asks GCS how much of a resumable upload it has received.

    Returns the byte to resume from (the file size if the upload is complete), or
    None if the upload session no longer exists, and a new one must be started.
    """
    logging.debug("Attempting to resume upload")

    try:
        res = requests.put(
            url=resumable_url,
            verify=verify_ssl,
            headers={"Content-Length": "0", "Content-Range": f"bytes */{file_size}"},
        )

        if res.status_code == 404 or res.status_code == 410:
            return None

        res.raise_for_status()

        if res.status_code == 200 or res.status_code == 201:
            return file_size
        elif res.status_code == 308:
            return get_committed_bytes(res)
        else:
            raise exceptions.RedivisError(
                "An unknown error occurred. Please try again."
            )
    except requests.RequestException as e:
        if retry_count >= MAX_RETRIES:
            raise exceptions.NetworkError(original_exception=e) from e

        time.sleep(retry_count + 1)
        return retry_partial_upload(
            retry_count=retry_count + 1,
            file_size=file_size,
            resumable_url=resumable_url,
        )


def get_committed_bytes(res):
    """The number of bytes GCS has persisted, from its 308 response to an upload."""
    range_header = res.headers.get("Range")
    # If GCS hasn't received any bytes, the header will be missing
    if not range_header:
        return 0
    match = re.match(r"bytes=0-(\d+)", range_header)
    if not match:
        raise exceptions.RedivisError("An unknown error occurred. Please try again.")
    return int(match.group(1)) + 1


def perform_standard_upload(data, temp_upload_url=None, progressbar=None):
    # Progress is reported as the data is read, which needs a file-like object
    if not hasattr(data, "read"):
        data = io.BytesIO(data)
    start_position = data.tell()
    start_progress = progressbar.n if progressbar else 0

    for retry_count in range(MAX_RETRIES + 1):
        try:
            res = requests.put(
                url=temp_upload_url,
                data=(
                    CallbackIOWrapper(progressbar.update, data, "read")
                    if progressbar
                    else data
                ),
                verify=verify_ssl,
            )
            res.raise_for_status()
            return
        except requests.RequestException as e:
            if retry_count >= MAX_RETRIES:
                raise exceptions.NetworkError(
                    message=f"A network error occurred. Upload failed after {retry_count} retries.",
                    original_exception=e,
                ) from e
            time.sleep(retry_count + 1)
            # The failed attempt read (some of) the data, so send it again from where it
            # started
            data.seek(start_position)
            if progressbar:
                progressbar.update(start_progress - progressbar.n)
