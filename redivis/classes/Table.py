import time
import json
import logging
from collections import namedtuple
import requests
import pandas as pd

from .Upload import Upload
from .Variable import Variable
from ..common.api_request import make_request, make_paginated_request


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
        owner = parent.organization or parent.user
        sample_string = ":sample" if sample else ""
        version_string = f":{dataset.version}" if dataset else ""
        self.name = name
        self.dataset = dataset
        self.project = project
        self.uri = f"/tables/{owner.name}.{parent.name}:{version_string}.{self.name}{sample_string}"
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

    def update(self, *, name=None, description=None, merge_strategy=None):
        # TODO: don't pass up None if the user didn't provide it
        response = make_request(
            method="PATCH",
            path=f"{self.uri}",
            payload={
                "name": name,
                "description": description,
                "mergeStrategy": merge_strategy,
            },
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

    def list_uploads(self):
        uploads = make_paginated_request(path=f"{self.uri}/uploads")
        return [Upload(upload) for upload in uploads]

    def list_variables(self):
        variables = make_paginated_request(path=f"{self.uri}/variables", page_size=1000)
        return [Variable(variable) for variable in variables]

    def list_rows(self, limit, *, offset_start=0):
        variables = make_paginated_request(path=f"{self.uri}/variables")
        Row = namedtuple(
            "Row",
            [variable["name"] for variable in variables],
        )

        if not self.properties:
            self.get()

        page = 0
        page_size = 10000

        max_results = (
            min(limit, int(self.properties["numRows"]))
            if limit is not None
            else self.properties.numRows
        )

        rows = ""
        while page * page_size < max_results:
            results = make_request(
                method="get",
                path=f"{self.uri}/rows",
                parse_response=False,
                query={
                    "selectedVariables": ",".join(
                        map(lambda variable: variable["name"], variables)
                    ),
                    "startIndex": page * page_size + offset_start,
                    "max_results": page_size
                    if (page + 1) * page_size < max_results
                    else max_results - page * page_size,
                },
            )
            rows += results
            page += 1

        return [Row(*json.loads(row)) for row in rows.split("\n")]

    def to_data_frame(self, limit, *, offset_start=0):
        #  TODO: this won't work if we don't have any rows
        rows = self.list_rows(limit=limit, offset_start=offset_start)
        return pd.DataFrame(rows, columns=rows[0]._fields)

    def download(self):
        # TODO
        return
