from ..common.api_request import make_request
import json
import os
import time
from ..common.list_rows import list_rows
import warnings


class Query:
    def __init__(
        self,
        query,
        *,
        default_project=None,
        default_dataset=None,
    ):
        if not default_project and not default_dataset:
            if os.getenv("REDIVIS_DEFAULT_PROJECT"):
                default_project = os.getenv("REDIVIS_DEFAULT_PROJECT")
            elif os.getenv("REDIVIS_DEFAULT_DATASET"):
                default_dataset = os.getenv("REDIVIS_DEFAULT_DATASET")

        self.properties = make_request(
            method="post",
            path="/queries",
            payload={
                "query": query,
                "defaultProject": default_project if default_project else None,
                "defaultDataset": default_dataset if default_dataset else None,
            },
        )
        self.uri = f"/queries/{self.properties['id']}"

    def __getitem__(self, key):
        return (
            self.properties[key] if self.properties and key in self.properties else None
        )

    def __str__(self):
        return json.dumps(self.properties, indent=2)

    def get(self):
        self.properties = make_request(method="GET", path=self.uri)
        return self

    def list_rows(self, max_results=None, *, limit=None):
        if limit and max_results is None:
            warnings.warn(
                "The limit parameter has been renamed to max_results, and will be removed in a future version of this library",
                DeprecationWarning,
            )
            max_results = limit

        self._wait_for_finish()
        variables = self.properties["outputSchema"]

        max_results = (
            min(max_results, int(self.properties["outputNumRows"]))
            if max_results is not None
            else self.properties["outputNumRows"]
        )

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            mapped_variables=variables,
            type="tuple",
        )

    def to_dataframe(self, max_results=None, *, limit=None):
        if limit and max_results is None:
            warnings.warn(
                "The limit parameter has been renamed to max_results, and will be removed in a future version of this library",
                DeprecationWarning,
            )
            max_results = limit

        self._wait_for_finish()
        variables = self.properties["outputSchema"]

        max_results = (
            min(max_results, int(self.properties["outputNumRows"]))
            if max_results is not None
            else self.properties["outputNumRows"]
        )

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            mapped_variables=variables,
            type="dataframe",
        )

    def _wait_for_finish(self):
        while True:
            if self.properties["status"] == "completed":
                break
            elif self.properties["status"] == "failed":
                raise Exception(
                    f"Query job failed with message: {self.properties['errorMessage']}"
                )
            elif self.properties["status"] == "cancelled":
                raise Exception(f"Query job was cancelled")
            else:
                time.sleep(2)
                self.get()
