import json
import time
import re
import math
import os
import requests
from ..common.api_request import make_request, make_paginated_request

# 8MB
MAX_CHUNK_SIZE = 2 ** 23


class Upload:
    def __init__(self, uri, properties=None):
        # TODO: reconsider this scheme
        self.uri = uri
        self.properties = properties

    def __getitem__(self, key):
        return self.properties[key]

    def __str__(self):
        return json.dumps(self.properties, indent=2)

    def get(self):
        self.properties = make_request(
            method="GET",
            path=self.uri,
        )

    def delete(self):
        make_request(
            method="DELETE",
            path=self.uri,
        )

    def upload_file(self, data):
        start_byte = 0
        retry_count = 0
        chunk_size = MAX_CHUNK_SIZE
        is_file = True if hasattr(data, "read") else False
        file_size = os.stat(data.name).st_size if is_file else len(data)

        res = make_request(
            path=f"{self.uri}/resumableUri",
            method="POST",
            payload={"size": file_size},
        )
        resumable_uri = res["uri"]

        while start_byte < file_size:
            end_byte = min(start_byte + chunk_size - 1, file_size - 1)
            if is_file:
                data.seek(start_byte)
                chunk = data.read(end_byte - start_byte + 1)
            else:
                chunk = data[start_byte:end_byte]

            try:
                res = requests.put(
                    url=resumable_uri,
                    headers={
                        "Content-Length": f"{end_byte - start_byte + 1}",
                        "Content-Range": f"bytes {start_byte}-{end_byte}/{file_size}",
                    },
                    data=chunk,
                )
                res.raise_for_status()

                start_byte += chunk_size

                make_request(
                    path=self.uri,
                    method="PATCH",
                    payload={"percentCompleted": (end_byte + 1) / file_size * 100},
                )
                retry_count = 0
            except Exception as e:
                if retry_count > 20:
                    print(
                        "A network error occurred. Upload failed after too many retries."
                    )

                    self.properties = make_request(
                        path=self.uri,
                        method="PATCH",
                        payload={
                            "errorMessage": "A network error occurred. Upload failed after too many retries."
                        },
                    )

                    raise e

                retry_count += 1
                time.sleep(retry_count)
                print(e)
                print("An error occurred. Retrying last chunk of resumable upload")
                start_byte = retry_partial_upload(
                    file_size=file_size, resumable_uri=resumable_uri
                )

        return

    def list_variables(self):
        # TODO
        return


def retry_partial_upload(*, retry_count=0, file_size, resumable_uri):
    print("Attempting to resume upload")

    try:
        res = requests.put(
            url=resumable_uri,
            headers={"Content-Range": f"bytes */{file_size}"},
        )

        if res.status_code == 404:
            return 0

        res.raise_for_status()

        if res.status_code == 200 or res.status_code == 201:
            return file_size
        elif res.status_code == 308:
            range_header = res.headers["Range"]

            if range_header:
                match = re.match(r"bytes=0-(\d+)", range_header)
                if match.group(0) and not math.isnan(int(match.group(1))):
                    return int(match.group(1)) + 1
                else:
                    raise Exception("An unknown error occurred. Please try again.")
            # If GCS hasn't received any bytes, the header will be missing
            else:
                return 0
    except Exception as e:
        if retry_count > 10:
            raise e

        retry_partial_upload(
            retry_count=retry_count + 1,
            file_size=file_size,
            resumable_uri=resumable_uri,
        )
