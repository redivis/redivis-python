import json
import time
import re
import math
import os
import logging
import requests
from urllib.parse import quote as quote_uri
import warnings

from ..common.list_rows import list_rows

from .Variable import Variable
from ..common.api_request import make_request, make_paginated_request


# 8MB
MAX_CHUNK_SIZE = 2 ** 23


class Upload:
    def __init__(
        self,
        name,
        *,
        table,
        properties=None,
    ):
        self.table = table
        self.name = name
        self.uri = f"{table.uri}/uploads/{quote_uri(self.name)}"
        self.properties = properties

    def __getitem__(self, key):
        return (
            self.properties[key] if self.properties and key in self.properties else None
        )

    def __str__(self):
        return json.dumps(self.properties, indent=2)

    def create(
        self,
        *,
        type="delimited",
        delimiter=None,
        schema=None,
        has_header_row=True,
        skip_bad_records=False,
        allow_quoted_newlines=False,
        quote_character='"',
        if_not_exists=False,
    ):

        if schema and type != "stream":
            warnings.warn(
                "The schema option is ignored for uploads that aren't of type `stream`"
            )

        if if_not_exists and self.exists():
            return self

        response = make_request(
            method="POST",
            path=f"{self.table.uri}/uploads",
            payload={
                "name": self.name,
                "type": type,
                "schema": schema,
                "hasHeaderRow": has_header_row,
                "skipBadRecords": skip_bad_records,
                "allowQuotedNewlines": allow_quoted_newlines,
                "quoteCharacter": quote_character,
                "delimiter": delimiter,
            },
        )
        self.properties = response
        self.uri = self.properties["uri"]
        return self

    def delete(self):
        make_request(
            method="DELETE",
            path=self.uri,
        )

    def exists(self):
        try:
            make_request(method="GET", path=self.uri)
            return True
        except Exception as err:
            if err.args[0]["status"] != 404:
                raise err
            return False

    def get(self):
        self.properties = make_request(
            method="GET",
            path=self.uri,
        )
        self.uri = self.properties["uri"]

    def insert_rows(self, rows, *, update_schema=False):
        response = make_request(
            method="POST",
            path=f"{self.uri}/rows",
            payload={"rows": rows, "updateSchema": update_schema},
        )
        return response

    def list_variables(self, *, max_results=10000):
        variables = make_paginated_request(
            path=f"{self.uri}/variables", page_size=1000, max_results=max_results
        )
        return [
            Variable(variable["name"], properties=variable, upload=self)
            for variable in variables
        ]

    def list_rows(self, max_results=None, *, variables=None):
        if not self.properties or not hasattr(self.properties, "numRows"):
            self.get()

        max_results = (
            min(max_results, int(self.properties["numRows"]))
            if max_results is not None
            else self.properties["numRows"]
        )

        mapped_variables = get_mapped_variables(variables, self.uri)

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            type="tuple",
        )

    def to_dataframe(self, max_results=None, *, limit=None, variables=None):
        if not self.properties or not hasattr(self.properties, "numRows"):
            self.get()

        max_results = (
            min(max_results, int(self.properties["numRows"]))
            if max_results is not None
            else self.properties["numRows"]
        )

        mapped_variables = get_mapped_variables(variables, self.uri)

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            type="dataframe",
        )

    def upload_file(
        self,
        data,
        *,
        remove_on_fail=False,
        wait_for_finish=True,
        raise_on_fail=True,
    ):
        try:
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
                    chunk = data[start_byte : end_byte + 1]

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
                    print("An error occurred. Retrying last chunk of resumable upload")
                    start_byte = retry_partial_upload(
                        file_size=file_size, resumable_uri=resumable_uri
                    )
            if wait_for_finish:
                while True:
                    time.sleep(2)
                    self.get()
                    if self["status"] == "completed" or self["status"] == "failed":
                        if self["status"] == "failed" and raise_on_fail:
                            raise Exception(self["errorMessage"])
                        break
                    else:
                        logging.debug("Upload is still in progress...")
        except Exception as e:
            if remove_on_fail:
                self.delete()
            raise Exception(str(e))
        return

    def variable(self, name):
        return Variable(name, upload=self)


def retry_partial_upload(*, retry_count=0, file_size, resumable_uri):
    logging.debug("Attempting to resume upload")

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


def get_mapped_variables(variables, uri):
    all_variables = make_paginated_request(path=f"{uri}/variables", page_size=1000)

    if variables is None:
        return all_variables
    else:
        lower_variable_names = [variable.lower() for variable in variables]
        variables_list = list(
            filter(
                lambda variable: variable["name"].lower() in lower_variable_names,
                all_variables,
            )
        )
        variables_list.sort(
            key=lambda variable: lower_variable_names.index(variable["name"].lower())
        )
        return variables_list
