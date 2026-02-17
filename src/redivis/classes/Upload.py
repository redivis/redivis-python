import time
import os
import json
import logging
from urllib.parse import quote as quote_uri
import warnings
import io
import pathlib
import uuid
from tqdm.auto import tqdm

from ..common.TabularReader import TabularReader
from ..common.util import convert_data_to_parquet

from .Variable import Variable
from ..common.api_request import make_request, make_paginated_request
from ..common.retryable_upload import perform_resumable_upload, perform_standard_upload

MAX_SIMPLE_UPLOAD_SIZE = 2**20  # 1MB
MIN_RESUMABLE_UPLOAD_SIZE = 2**25  # 32MB


class Upload(TabularReader):
    def __init__(
        self,
        name,
        *,
        table,
        properties=None,
    ):
        super().__init__(is_upload=True)
        self.table = table
        self.name = name
        self.uri = (
            properties["uri"]
            if "uri" in (properties or {})
            else (f"{table.uri}/uploads/{quote_uri(self.name.replace('.', '_'),'')}")
        )
        self.properties = properties

    def delete(self):
        make_request(
            method="DELETE",
            path=self.uri,
        )

    def exists(self):
        try:
            make_request(method="HEAD", path=self.uri)
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

    def variable(self, name):
        return Variable(name, upload=self)

    def create(
        self,
        content=None,
        # content can be:
        # - a file path (string or pathlib.Path)
        # - a file-like object (has a .read() method)
        # - raw bytes or a bytearray
        # - one of the data types returned by the to_ methods
        # - None (in which case, a transfer specification must be provided)
        # - TODO: a directory path (string or pathlib.Path)
        # - TODO: a Redivis Directory
        # - TODO: a Redivis File
        # - TODO: a Redivis Query (need BE support)
        # - TODO: a Redivis Table
        # - TODO: an array of any of the above, except for tables + queries
        *,
        type=None,  # TODO: add rawFiles (doesn't support Table or Query)
        transfer_specification=None,
        delimiter=None,
        schema=None,
        metadata=None,
        has_header_row=True,
        skip_bad_records=False,
        has_quoted_newlines=None,
        quote_character=None,
        escape_character=None,
        null_markers=None,
        rename_on_conflict=False,
        replace_on_conflict=False,
        allow_jagged_rows=False,
        if_not_exists=False,
        remove_on_fail=False,
        wait_for_finish=True,
        raise_on_fail=True,
        progress=True,
    ):
        if replace_on_conflict is True and rename_on_conflict is True:
            raise Exception(
                "Invalid parameters. replace_on_conflict and rename_on_conflict cannot both be True."
            )

        if not isinstance(content, list):
            content = [content]

        payload = {
            "name": self.name,
            "type": type,
            "schema": schema,
            "metadata": metadata,
            "hasHeaderRow": has_header_row,
            "skipBadRecords": skip_bad_records,
            "hasQuotedNewlines": has_quoted_newlines,
            "allowJaggedRows": allow_jagged_rows,
            "quoteCharacter": quote_character,
            "escapeCharacter": escape_character,
            "nullMarkers": null_markers,
            "delimiter": delimiter,
            "transferSpecification": transfer_specification,  # TODO: consider transfer specification with many files / sources
        }

        if not self.name and content and len(content) == 1:
            # If we've passed a file path, use that as the upload name
            if len(content) == 1 and isinstance(content[0], (str, pathlib.Path)):
                self.name = pathlib.Path(content[0]).name
            # If we've passed a file-like object with a name attribute, use that as the upload name
            elif hasattr(content[0], "name"):
                self.name = pathlib.Path(content[0].name).name

        # If we have a name, check for name conflicts
        if self.name != "":
            if self.properties is None or "uri" not in self.properties:
                self.uri = f"{self.table.uri}/uploads/{quote_uri(self.name.replace('.', '_'), '')}"
            if self.exists():
                if if_not_exists:
                    return self
                elif replace_on_conflict:
                    self.delete()
                elif not rename_on_conflict:
                    raise Exception(
                        f"An upload with the name {self.name} already exists on this version of the table. If you want to upload this file anyway, set the parameter rename_on_conflict=True or replace_on_conflict=True."
                    )

        if type == "stream" or transfer_specification:
            response = make_request(
                method="POST",
                path=f"{self.table.uri}/uploads",
                payload=payload,
            )
            self.properties = response
            self.uri = self.properties["uri"]
        else:
            payload["totalFileCount"] = len(content)
            response = make_request(
                method="POST",
                path=f"{self.table.uri}/uploads",
                payload=payload,
            )
            self.properties = response
            self.uri = self.properties["uri"]

            # TODO: parallelize this (and add max_parallelization above)
            for data in content:
                did_reopen_file = False
                tempfile_to_remove = None
                size = None
                if isinstance(data, str) and len(data) > 4096:  # maximum path length
                    raise Exception(
                        "`content` argument was a string, but is an invalid file path. To upload data as content, make sure that the `content` argument is provided as binary data."
                    )

                if isinstance(data, str) or isinstance(data, pathlib.PurePath):
                    did_reopen_file = True
                    data = open(data, "rb")

                if data is not None and not hasattr(data, "read"):
                    if isinstance(data, (bytes, bytearray)):
                        size = len(data)
                        data = io.BytesIO(data)
                    else:
                        tempfile_to_remove = convert_data_to_parquet(data)
                        did_reopen_file = True
                        type = "parquet"
                        data = open(tempfile_to_remove, "rb")

                pbar_bytes = None
                # If file isn't in binary mode, we need to reopen, otherwise uploading of non-text files will fail
                if hasattr(data, "mode") and "b" not in data.mode:
                    data = open(data.name, "rb")
                    did_reopen_file = True

                if size is None:
                    size = os.stat(data.name).st_size

                if progress:
                    pbar_bytes = tqdm(
                        total=size,
                        unit="B",
                        leave=False,
                        unit_scale=True,
                        mininterval=0.1,
                    )

                if size >= MIN_RESUMABLE_UPLOAD_SIZE or True:
                    perform_resumable_upload(
                        data=data,
                        size=size,
                        progressbar=pbar_bytes,
                        url=f"{os.getenv('REDIVIS_API_ENDPOINT')}{self.uri}/files?uploadType=resumable&size={size}&name={quote_uri(self.name or str(uuid.uuid4()), '')}",
                    )
                else:
                    # TODO: handle if size is unknown
                    perform_standard_upload(
                        data=data,
                        url=f"{os.getenv('REDIVIS_API_ENDPOINT')}{self.uri}/files?uploadType=resumable&size={size}&name={quote_uri(self.name or str(uuid.uuid4()), '')}",
                        progressbar=pbar_bytes,
                    )

                if did_reopen_file:
                    data.close()
                if progress:
                    pbar_bytes.close()
                if tempfile_to_remove is not None:
                    os.remove(tempfile_to_remove)

        try:
            # TODO: clean up streaming uploads, validate their status (should we have some sort of "finish" event for them?)
            if type != "stream" and wait_for_finish:
                while True:
                    time.sleep(2)
                    self.get()
                    if (
                        self.properties["status"] == "completed"
                        or self.properties["status"] == "failed"
                    ):
                        if self.properties["status"] == "failed" and raise_on_fail:
                            raise Exception(self.properties["errorMessage"])
                        break
                    else:
                        logging.debug("Upload is still in progress...")
        except Exception as e:
            if remove_on_fail and self.properties["status"] == "failed":
                self.delete()
            raise Exception(str(e))

        return self
