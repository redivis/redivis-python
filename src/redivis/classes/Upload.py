import os
from urllib.parse import quote as quote_uri
import warnings
import io
import pathlib
import shutil
import tempfile
from tqdm.auto import tqdm

from ..common import exceptions
from ..common.config import resolve_config
from ..common.TabularReader import TabularReader
from ..common.util import convert_data_to_parquet

from .Variable import Variable
from ..common.api_request import make_request, make_paginated_request
from ..common.imports import (
    compute_md5_base64,
    create_import,
    finalize_import,
    upload_import_file,
    wait_for_import,
)


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
            else self._uri_for_name(self.name)
        )
        self.properties = properties
        # The import that created this upload, if it was created here
        self._import = None

    def __repr__(self):
        return f"<Upload {self.table.uri}.{self.name}>"

    def delete(self):
        make_request(
            method="DELETE",
            path=self.uri,
        )

    def exists(self):
        try:
            make_request(method="HEAD", path=self.uri)
            return True
        except exceptions.NotFoundError:
            return False

    def get(self):
        self.properties = make_request(
            method="GET",
            path=self.uri,
        )
        self.uri = self.properties["uri"]
        return self

    def insert_rows(self, rows):
        response = make_request(
            method="POST",
            path=f"{self.uri}/rows",
            payload={"rows": rows},
        )
        return response

    def list_variables(self, *, max_results=None):
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
        data=None,
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
        progress=None,
    ):
        progress = resolve_config("progress", progress)

        if data is not None and content is None:
            content = data
            warnings.warn(
                "Deprecation warning: the `data` named argument has been renamed to `content`",
                FutureWarning,
                stacklevel=2,
            )

        if isinstance(content, str) and len(content) > 4096:  # maximum path length
            raise exceptions.ValueError(
                "`content` argument was a string, but is an invalid file path. To upload data as content, make sure that the `content` argument is provided as binary data."
            )

        if replace_on_conflict is True and rename_on_conflict is True:
            raise exceptions.ValueError(
                "Invalid parameters. replace_on_conflict and rename_on_conflict cannot both be True."
            )

        is_import = content is not None or transfer_specification is not None
        if is_import:
            from .Table import resolve_table_for_write

            # Writes go through the table's own URI, rather than a workflow-scoped one
            resolve_table_for_write(self.table)

        content_file_name = None
        if isinstance(content, (str, pathlib.PurePath)):
            content_file_name = pathlib.Path(content).name
        elif hasattr(content, "name") and isinstance(content.name, str):
            content_file_name = pathlib.Path(content.name).name

        if not self.name and content_file_name:
            self.name = content_file_name

        # Check for a conflicting upload before uploading anything
        if self.name != "":
            if self.properties is None or "uri" not in self.properties:
                self.uri = self._uri_for_name(self.name)
            if self.exists():
                if if_not_exists:
                    return self
                elif replace_on_conflict is True:
                    self.delete()
                elif rename_on_conflict is not True:
                    raise exceptions.ValueError(
                        f"An upload with the name {self.name} already exists on this version of the table. If you want to upload this file anyway, set the parameter rename_on_conflict=True or replace_on_conflict=True."
                    )

        if schema and type != "stream":
            warnings.warn(
                "The schema option is ignored for uploads that aren't of type `stream`"
            )

        if not is_import:
            # E.g., a stream upload, which rows are then inserted into
            response = make_request(
                method="POST",
                path=f"{self.table.uri}/uploads",
                payload={
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
                },
            )
            self.properties = response
            self.uri = self.properties["uri"]
            # The API may have named it differently, e.g. with rename_on_conflict
            self.name = self.properties.get("name", self.name)
            return self

        did_reopen_file = False
        tempfile_to_remove = None
        try:
            if content is not None:
                if isinstance(content, (str, pathlib.PurePath)):
                    content = open(content, "rb")
                    did_reopen_file = True
                elif isinstance(content, (bytes, bytearray)):
                    content = io.BytesIO(content)
                elif not hasattr(content, "read"):
                    tempfile_to_remove = convert_data_to_parquet(content)
                    type = "parquet"
                    content = open(tempfile_to_remove, "rb")
                    did_reopen_file = True

                # If file isn't in binary mode, we need to reopen, otherwise uploading
                # of non-text files will fail
                if hasattr(content, "mode") and "b" not in content.mode:
                    if did_reopen_file:
                        content.close()
                    content = open(content.name, "rb")
                    did_reopen_file = True

                # The content is read more than once (for its md5, and again if a
                # request has to be retried), so it's copied first if it can't be
                if not getattr(content, "seekable", lambda: False)():
                    unseekable = content
                    content = tempfile.TemporaryFile()
                    shutil.copyfileobj(unseekable, content)
                    content.seek(0)
                    if did_reopen_file:
                        unseekable.close()
                    did_reopen_file = True

                # Content is uploaded from its current position, e.g. for an
                # io.BytesIO that's been partly read
                position = content.tell()
                size = content.seek(0, io.SEEK_END) - position
                content.seek(position)

            payload = {
                "name": self.name or None,
                "type": type,
                "metadata": metadata,
                "hasHeaderRow": has_header_row,
                "skipBadRecords": skip_bad_records,
                "hasQuotedNewlines": has_quoted_newlines,
                "allowJaggedRows": allow_jagged_rows,
                "quoteCharacter": quote_character,
                "escapeCharacter": escape_character,
                "nullMarkers": null_markers,
                "delimiter": delimiter,
            }
            if content is not None:
                payload = {**payload, "totalFileCount": 1, "totalSize": size}
            else:
                source_path = transfer_specification.get("sourcePath")
                payload = {
                    **payload,
                    "sourceType": transfer_specification.get("sourceType"),
                    "sourcePaths": (
                        [source_path]
                        if source_path is not None
                        else transfer_specification.get("sourcePaths")
                    ),
                    "identity": transfer_specification.get("identity"),
                }

            import_ = create_import(self.table, payload)
            self._import = import_
            # The API may have named it differently, e.g. with rename_on_conflict. The
            # upload is still found by its name.
            self.name = import_.get("name") or self.name
            self.uri = self._uri_for_name(self.name)

            if content is not None:
                pbar_bytes = None
                if progress:
                    pbar_bytes = tqdm(
                        total=size,
                        unit="B",
                        leave=False,
                        unit_scale=True,
                        mininterval=0.1,
                    )
                try:
                    upload_import_file(
                        import_,
                        name=(
                            content_file_name
                            or import_.get("name")
                            or (f"data.{type}" if type else "data")
                        ),
                        source=content,
                        size=size,
                        md5=compute_md5_base64(content),
                        progress=pbar_bytes.update if pbar_bytes is not None else None,
                    )
                finally:
                    if pbar_bytes is not None:
                        pbar_bytes.close()
                import_ = finalize_import(import_)
                self._import = import_
        finally:
            if did_reopen_file:
                content.close()
            if tempfile_to_remove is not None:
                os.remove(tempfile_to_remove)

        self.properties = import_
        if wait_for_finish:
            import_ = wait_for_import(import_, raise_on_fail=False)
            self._import = import_
            try:
                self.get()
            except exceptions.NotFoundError:
                # E.g., the import failed before the upload could be created
                if import_["status"] == "completed":
                    raise
                self.properties = import_
            else:
                if import_["status"] != "completed":
                    if remove_on_fail:
                        self.delete()
                    # Report the import's status, since the upload's may lag behind it
                    self.properties = {
                        **self.properties,
                        "status": import_["status"],
                        "errorMessage": import_.get("errorMessage")
                        or self.properties.get("errorMessage"),
                    }

            if import_["status"] != "completed" and raise_on_fail:
                raise exceptions.JobError(
                    message=import_.get("errorMessage"),
                    status=import_["status"],
                    kind=self.properties.get("kind"),
                )

        return self

    def _uri_for_name(self, name):
        return f"{self.table.uri}/uploads/{quote_uri(name.replace('.', '_'), '')}"
