import json
import time
import re
import io
import math
import os
import logging
import csv
import gzip
from collections import namedtuple
import pandas as pd
import requests
from urllib.parse import quote as quote_uri
import warnings

from ..common.util import set_dataframe_types
from .Variable import Variable
from ..common.api_request import make_request, make_paginated_request, make_rows_request


# 8MB
MAX_CHUNK_SIZE = 2 ** 23


class Upload:
    def __init__(
        self,
        name,
        *,
        table,
        properties={},
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

    def list_rows(self, max_results=None, *, limit=None, variables=None):
        if limit and max_results is None:
            warnings.warn(
                "The limit parameter has been renamed to max_results, and will be removed in a future version of this library",
                DeprecationWarning,
            )
            max_results = limit

        # This allows us to persist casing of the passed variable names
        original_variable_names = variables
        all_variables = make_paginated_request(
            path=f"{self.uri}/variables", page_size=1000
        )

        if variables is None:
            variables_list = all_variables
            original_variable_names = [variable["name"] for variable in all_variables]
        else:
            lower_variable_names = [variable.lower() for variable in variables]
            variables_list = list(
                filter(
                    lambda variable: variable["name"].lower() in lower_variable_names,
                    all_variables,
                )
            )
            variables_list.sort(
                key=lambda variable: lower_variable_names.index(
                    variable["name"].lower()
                )
            )

        Row = namedtuple(
            "Row",
            original_variable_names,
        )

        if not self.properties:
            self.get()

        max_results = (
            min(max_results, int(self.properties["numRows"]))
            if max_results is not None
            else self.properties["numRows"]
        )
        res = make_rows_request(
            uri=self.uri,
            max_results=max_results,
            query={
                "selectedVariables": ",".join(
                    map(lambda variable: variable["name"], variables_list)
                ),
            },
        )

        fd = res.raw
        if res.headers.get("content-encoding") == "gzip":
            fd = gzip.GzipFile(fileobj=fd, mode="r")

        reader = csv.reader(io.TextIOWrapper(fd))

        return [Row(*row) for row in reader]

    def to_dataframe(self, max_results=None, *, limit=None, variables=None):
        original_variable_names = variables
        all_variables = make_paginated_request(
            path=f"{self.uri}/variables", page_size=1000
        )

        if variables is None:
            variables_list = all_variables
            original_variable_names = [variable["name"] for variable in all_variables]
        else:
            lower_variable_names = [variable.lower() for variable in variables]
            variables_list = list(
                filter(
                    lambda variable: variable["name"].lower() in lower_variable_names,
                    all_variables,
                )
            )
            variables_list.sort(
                key=lambda variable: lower_variable_names.index(
                    variable["name"].lower()
                )
            )

        if limit and max_results is None:
            warnings.warn(
                "The limit parameter has been renamed to max_results, and will be removed in a future version of this library",
                DeprecationWarning,
            )
            max_results = limit

        if not self.properties:
            self.get()

        max_results = (
            min(max_results, int(self.properties["numRows"]))
            if max_results is not None
            else self.properties["numRows"]
        )
        res = make_rows_request(
            uri=self.uri,
            max_results=max_results,
            query={
                "selectedVariables": ",".join(
                    map(lambda variable: variable["name"], variables_list)
                ),
            },
        )

        df = pd.read_csv(
            res.raw,
            dtype="string",
            names=original_variable_names,
            compression="gzip"
            if res.headers.get("content-encoding") == "gzip"
            else None,
        )

        if variables is None:
            return set_dataframe_types(df, variables_list)
        else:
            return set_dataframe_types(
                df,
                map(
                    lambda variable, variable_name: {
                        "name": variable_name,
                        "type": variable["type"],
                    },
                    variables_list,
                    variables,
                ),
            )

    def create(
        self,
        *,
        type="delimited",
        delimiter=None,
        schema=None,
        has_header_row=True,
        skip_bad_records=False,
        allow_quoted_newlines=False,
        quote_character=None,
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

    def upload_file(
        self,
        data,
        *,
        create_if_needed=True,
        delimiter=None,
        schema=None,
        has_header_row=True,
        skip_bad_records=False,
        allow_quoted_newlines=False,
        quote_character=None,
        remove_on_fail=False,
        wait_for_finish=True,
        raise_on_fail=True,
    ):
        if create_if_needed and not self.exists():
            self.create(
                delimiter=delimiter,
                schema=schema,
                has_header_row=has_header_row,
                skip_bad_records=skip_bad_records,
                allow_quoted_newlines=allow_quoted_newlines,
                quote_character=quote_character,
            )
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
                    print(e)
                    print("An error occurred. Retrying last chunk of resumable upload")
                    start_byte = retry_partial_upload(
                        file_size=file_size, resumable_uri=resumable_uri
                    )
            if wait_for_finish:
                while True:
                    time.sleep(2)
                    self.get()
                    if self["status"] == "completed" or self["status"] == "failed":
                        break
                    else:
                        logging.debug("Upload is still in progress...")
        except Exception as e:
            if remove_on_fail:
                self.delete()
            if raise_on_fail:
                raise Exception(self["errorMessage"] or str(e))
        return


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
