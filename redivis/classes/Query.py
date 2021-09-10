from ..common.api_request import make_request, make_rows_request
from ..common.util import set_dataframe_types
import json
import time
from collections import namedtuple
import pandas as pd


class Query:
    def __init__(self, query, *, default_project=None, default_dataset=None):
        self.properties = make_request(
            method="post",
            path="/queries",
            payload={
                "query": query,
                "defaultProject": default_project if default_project else None,
                "defaultDataset": default_dataset if default_dataset else None,
            },
        )
        print(default_project)
        self.uri = f"/queries/{self.properties['id']}"

    def __getitem__(self, key):
        return self.properties[key]

    def __str__(self):
        return json.dumps(self.properties, indent=2)

    def get(self):
        self.properties = make_request(method="GET", path=self.uri)
        return self

    def list_rows(self, max_results=None, *, limit=None):
        if limit and max_results is None:
            max_results = limit

        self._wait_for_finish()
        variables = self.properties["outputSchema"]
        Row = namedtuple(
            "Row",
            [variable["name"] for variable in variables],
        )

        max_results = (
            min(max_results, int(self.properties["outputNumRows"]))
            if max_results is not None
            else self.properties["outputNumRows"]
        )

        rows = make_rows_request(uri=self.uri, max_results=max_results)
        return [Row(*row) for row in rows]

    def to_dataframe(self, max_results=None, *, limit=None):
        if limit and max_results is None:
            max_results = limit

        self._wait_for_finish()
        variables = self.properties["outputSchema"]

        max_results = (
            min(max_results, int(self.properties["outputNumRows"]))
            if max_results is not None
            else self.properties["outputNumRows"]
        )

        rows = make_rows_request(uri=self.uri, max_results=max_results)
        if len(rows) == 0:
            return pd.DataFrame(
                rows,
                columns=[variable["name"] for variable in variables],
            )

        df = pd.DataFrame(
            rows,
            dtype="string",
            columns=[variable["name"] for variable in variables],
        )

        return set_dataframe_types(df, variables)

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
