import time
import json
import logging
from collections import namedtuple
from ..common.util import set_dataframe_types
import pandas as pd
from urllib.parse import quote as quote_uri

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
        return self.properties[key]

    def __str__(self):
        return json.dumps(self.properties, indent=2)

    def exists(self):
        try:
            make_request(method="GET", path=self.uri)
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
            payload["mergeStrategy"] = upload_merge_strategy
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

    def upload(self, *, name, data, type, remove_on_fail=True):
        response = make_request(
            method="POST",
            path=f"{self.uri}/uploads",
            payload={"name": name, "type": type},
        )
        upload = Upload(uri=response["uri"])
        try:
            upload.upload_file(data)
            while True:
                time.sleep(2)
                upload.get()
                if upload["status"] == "completed":
                    break
                elif upload["status"] == "failed":
                    raise Exception(upload["errorMessage"])
                else:
                    logging.debug("Upload is still in progress...")
        except Exception as e:
            if remove_on_fail:
                print("An error occurred. Deleting upload.")
                upload.delete()
            raise e

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
        return [Variable(variable) for variable in variables]

    def list_rows(self, max_results=None, *, limit=None):
        # TODO: this is deprectated and should ultimately be removed
        if limit and max_results is None:
            max_results = limit

        variables = make_paginated_request(path=f"{self.uri}/variables")
        Row = namedtuple(
            "Row",
            [variable["name"] for variable in variables],
        )

        if not self.properties:
            self.get()

        max_results = (
            min(max_results, int(self.properties["numRows"]))
            if max_results is not None
            else self.properties["numRows"]
        )
        rows = make_rows_request(
            uri=self.uri,
            max_results=max_results,
            query={
                "selectedVariables": ",".join(
                    map(lambda variable: variable["name"], variables)
                ),
            },
        )

        return [Row(*row) for row in rows]

    def to_dataframe(self, max_results=None, *, limit=None, offset_start=0):
        variables = make_paginated_request(path=f"{self.uri}/variables")
        # TODO: this is deprectated and should ultimately be removed
        if limit and max_results is None:
            max_results = limit

        if not self.properties:
            self.get()

        max_results = (
            min(max_results, int(self.properties["numRows"]))
            if max_results is not None
            else self.properties["numRows"]
        )
        rows = make_rows_request(
            uri=self.uri,
            max_results=max_results,
            query={
                "selectedVariables": ",".join(
                    map(lambda variable: variable["name"], variables)
                ),
            },
        )
        if len(rows) == 0:
            return pd.DataFrame(
                rows,
                columns=[variable["name"] for variable in variables],
            )

        df = pd.DataFrame(
            rows, dtype="string", columns=[variable["name"] for variable in variables]
        )
        return set_dataframe_types(df, variables)

    def download(self):
        # TODO
        return
