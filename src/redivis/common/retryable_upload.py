import re
import math
import time
import requests
import os
import logging
from tqdm.utils import CallbackIOWrapper
from urllib.parse import quote as quote_uri

from .auth import get_auth_token

verify_ssl = (
    False
    if os.getenv("REDIVIS_API_ENDPOINT", "https://redivis.com/api/v1").find(
        "https://localhost", 0
    )
    == 0
    else True
)


def perform_resumable_upload(
    data, size=None, temp_upload_url=None, proxy_url=None, progressbar=None
):
    retry_count = 0
    start_byte = 0
    is_file = True if hasattr(data, "read") else False
    file_size = size

    if file_size is None:
        file_size = os.stat(data.name).st_size if is_file else len(data)

    chunk_size = file_size
    # chunk_size = min(file_size, 2**30)
    headers = {"Authorization": f"Bearer {get_auth_token()}"}

    if proxy_url:
        temp_upload_url = f"{proxy_url}?url={quote_uri(temp_upload_url)}"

    resumable_url = initiate_resumable_upload(file_size, temp_upload_url, headers)

    while (
        start_byte < file_size or start_byte == 0
    ):  # handle empty upload for start_byte == 0
        end_byte = min(start_byte + chunk_size - 1, file_size - 1)
        if progressbar:
            progressbar.update(start_byte - progressbar.n)
        if is_file:
            data.seek(start_byte)
            if progressbar:
                chunk = CallbackIOWrapper(progressbar.update, data, "read")
            else:
                chunk = data
        else:
            if start_byte != 0:
                chunk = data[start_byte : end_byte + 1]
            else:
                chunk = data

        try:
            res = requests.put(
                url=resumable_url,
                verify=verify_ssl,
                headers={
                    **headers,
                    **{
                        "Content-Length": f"{end_byte - start_byte + 1}",
                        "Content-Range": f"bytes {start_byte}-{end_byte}/{file_size}",
                    },
                },
                data=chunk,
            )
            res.raise_for_status()

            start_byte += chunk_size
            retry_count = 0  # reset retry_count after a successfully uploaded chunk
        except Exception as e:
            if retry_count > 10:
                print("A network error occurred. Upload failed after too many retries.")
                raise e

            retry_count += 1
            time.sleep(retry_count)
            print("A network error occurred. Retrying last chunk of resumable upload.")
            start_byte = retry_partial_upload(
                file_size=file_size, resumable_url=resumable_url, headers=headers
            )


def initiate_resumable_upload(size, temp_upload_url, headers, retry_count=0):
    did_request_complete = False
    try:
        res = requests.post(
            temp_upload_url,
            verify=verify_ssl,
            headers={
                **headers,
                **{
                    "x-upload-content-length": str(size),
                    "x-goog-resumable": "start",
                },
            },
        )
        did_request_complete = True
        res.raise_for_status()
        return res.headers["location"]

    except Exception as e:
        if did_request_complete:
            raise e
        else:
            if retry_count > 10:
                print("A network error occurred. Upload failed after too many retries.")
                raise e
            time.sleep(retry_count + 1)
            initiate_resumable_upload(
                size, temp_upload_url, headers, retry_count=retry_count + 1
            )


def retry_partial_upload(*, retry_count=0, file_size, resumable_url, headers):
    logging.debug("Attempting to resume upload")

    try:
        res = requests.put(
            url=resumable_url,
            verify=verify_ssl,
            headers={
                **headers,
                **{"Content-Length": "0", "Content-Range": f"bytes */{file_size}"},
            },
        )

        if res.status_code == 404:
            return 0

        res.raise_for_status()

        if res.status_code == 200 or res.status_code == 201:
            return file_size
        elif res.status_code == 308:
            range_header = res.headers["Range"] if "Range" in res.headers else None
            if range_header:
                match = re.match(r"bytes=0-(\d+)", range_header)
                if match.group(0) and not math.isnan(int(match.group(1))):
                    return int(match.group(1)) + 1
                else:
                    raise Exception("An unknown error occurred. Please try again.")
            # If GCS hasn't received any bytes, the header will be missing
            else:
                return 0
        else:
            raise Exception("An unknown error occurred. Please try again.")
    except Exception as e:
        if retry_count > 10:
            raise e

        time.sleep(retry_count + 1)
        return retry_partial_upload(
            retry_count=retry_count + 1,
            file_size=file_size,
            resumable_url=resumable_url,
            headers=headers,
        )


def perform_standard_upload(
    data, temp_upload_url=None, proxy_url=None, retry_count=0, progressbar=None
):
    original_url = temp_upload_url
    try:
        if progressbar:
            data = CallbackIOWrapper(progressbar.update, data, "read")

        headers = {"Authorization": f"Bearer {get_auth_token()}"}

        if proxy_url:
            temp_upload_url = f"{proxy_url}?url={quote_uri(temp_upload_url)}"

        res = requests.put(
            url=temp_upload_url, data=data, headers=headers, verify=verify_ssl
        )
        res.raise_for_status()
    except Exception as e:
        if retry_count > 10:
            print("A network error occurred. Upload failed after too many retries.")
            raise e
        time.sleep(retry_count + 1)
        return perform_standard_upload(
            data=data,
            temp_upload_url=original_url,
            proxy_url=proxy_url,
            retry_count=retry_count + 1,
            progressbar=progressbar,
        )
