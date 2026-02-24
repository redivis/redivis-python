import os
import time
from .Variable import Variable
from ..common import exceptions

from ..common.TabularReader import TabularReader
from ..common.api_request import make_request, make_paginated_request


class Query(TabularReader):
    def __init__(
        self,
        query,
        *,
        default_workflow=None,
        default_dataset=None,
    ):
        super().__init__(is_query=True)
        self.did_initiate = False
        if not default_workflow and not default_dataset:
            if os.getenv("REDIVIS_DEFAULT_WORKFLOW"):
                default_workflow = os.getenv("REDIVIS_DEFAULT_WORKFLOW")
            elif os.getenv("REDIVIS_DEFAULT_DATASET"):
                default_dataset = os.getenv("REDIVIS_DEFAULT_DATASET")

        self.payload = {"query": query}
        self.directory = None
        if default_workflow:
            self.payload["defaultWorkflow"] = default_workflow
        if default_dataset:
            self.payload["defaultDataset"] = default_dataset

    def __repr__(self):
        return f"<Query {self.payload.get('id', '')}>"

    def get(self):
        self._initiate()
        self.properties = make_request(method="GET", path=self.uri)
        return self

    # def dry_run(self):
    #   TODO

    def variable(self, name):
        # TODO: dry run (?) + cache variables
        self._wait_for_finish()
        return Variable(name, query=self)

    def list_variables(self, *, max_results=None):
        # TODO: dry run (?) + cache variables
        self._wait_for_finish()
        variables = make_paginated_request(
            path=f"{self.uri}/variables", page_size=1000, max_results=max_results
        )
        return [
            Variable(variable["name"], query=self, properties=variable)
            for variable in variables
        ]

    def _initiate(self):
        if not self.did_initiate:
            self.did_initiate = True
            self.properties = make_request(
                method="post",
                path="/queries",
                payload=self.payload,
            )
            self.uri = self.properties["uri"]

    def _wait_for_finish(self):
        self._initiate()
        while True:
            if self.properties["status"] == "completed":
                break
            elif self.properties["status"] == "failed":
                raise exceptions.JobError(
                    message=self.properties.get("errorMessage"),
                    status=self.properties.get("status"),
                    kind=self.properties.get("kind"),
                )
            elif self.properties["status"] == "cancelled":
                raise exceptions.JobError(
                    message="Query Job was cancelled",
                    status=self.properties.get("status"),
                    kind=self.properties.get("kind"),
                )
            else:
                time.sleep(2)
                self.get()
