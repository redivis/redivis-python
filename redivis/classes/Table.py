import time
import json
import csv
import gzip
import logging
import io
from collections import namedtuple
from ..common.util import set_dataframe_types
import pandas as pd
from urllib.parse import quote as quote_uri
import warnings


from .Upload import Upload
from .Variable import Variable
from ..common.api_request import make_request, make_paginated_request, make_rows_request


class Table:
    def __init__(
        self,
        name,
        *,
        sample=False,
        dataset=None,
        project=None,
        properties=None,
    ):
        parent = dataset or project
        owner = parent.user or parent.organization
        sample_string = ":sample" if sample else ""
        version_string = f":{dataset.version}" if dataset else ""
        self.name = name
        self.dataset = dataset
        self.project = project
        self.identifier = (
            f"{owner.name}.{parent.name}{version_string}.{self.name}{sample_string}"
        )
        self.uri = f"/tables/{quote_uri(self.identifier)}"
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
        self.properties = make_request(method="GET", path=self.uri)
        self.uri = self.properties["uri"]
        return self

    def create(self, *, description=None, upload_merge_strategy="append"):
        response = make_request(
            method="POST",
            path=f"{self.dataset.uri}/tables",
            payload={
                "name": self.name,
                "description": description,
                "uploadMergeStrategy": upload_merge_strategy,
            },
        )
        self.properties = response
        self.uri = self.properties["uri"]
        return self

    def update(self, *, name=None, description=None, upload_merge_strategy=None):
        payload = {}
        if name:
            payload["name"] = name
        if upload_merge_strategy:
            payload["uploadMergeStrategy"] = upload_merge_strategy
        if description is not None:
            payload["description"] = description

        response = make_request(
            method="PATCH",
            path=f"{self.uri}",
            payload=payload,
        )
        self.properties = response
        return

    def delete(self):
        make_request(
            method="DELETE",
            path=self.uri,
        )
        return

    def upload(
        self,
        name,
        *,
        type="delimited",
        schema=None,
        has_header_row=True,
        skip_bad_records=False,
        allow_quoted_newlines=False,
        quote_character='"',
        delimiter=None,
        #     This is deprecated
        data=None,
        remove_on_fail=True,
    ):
        upload = Upload(
            table=self,
            name=name,
        )
        if data is not None:
            warnings.warn(
                "Passing data directly to the upload constructor is deprecated. Please call table.upload().upload_file(file) instead.",
                DeprecationWarning,
            )
            upload.create(
                schema=schema,
                type=type,
                has_header_row=has_header_row,
                skip_bad_records=skip_bad_records,
                allow_quoted_newlines=allow_quoted_newlines,
                quote_character=quote_character,
                delimiter=delimiter,
            )
            upload.upload_file(
                data,
                create_if_needed=True,
                wait_for_finish=True,
                raise_on_fail=True,
                remove_on_fail=remove_on_fail,
            )

        return upload

    def list_uploads(self, *, max_results=None):
        uploads = make_paginated_request(
            path=f"{self.uri}/uploads", max_results=max_results
        )
        return [Upload(upload) for upload in uploads]

    def list_variables(self, *, max_results=None):
        variables = make_paginated_request(
            path=f"{self.uri}/variables", page_size=1000, max_results=max_results
        )
        return [
            Variable(variable["name"], table=self, properties=variable)
            for variable in variables
        ]

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
