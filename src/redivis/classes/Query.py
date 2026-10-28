import os
import threading
import time
from contextlib import closing
from .Variable import Variable
from ..common import exceptions

from ..common.TabularReader import TabularReader
from ..common.api_request import make_request, make_paginated_request
from ..common.config import get_config


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
        self._initiate_lock = threading.Lock()
        if not default_workflow and not default_dataset:
            if get_config("default_workflow"):
                default_workflow = get_config("default_workflow")
            elif get_config("default_dataset"):
                default_dataset = get_config("default_dataset")

        self.payload = {"query": query}
        self.directory = None
        if default_workflow:
            self.payload["defaultWorkflow"] = default_workflow
        if default_dataset:
            self.payload["defaultDataset"] = default_dataset

    def __repr__(self):
        return f"<Query {self.properties.get('id', '[uninitiated]')}>"

    # A lock can't be pickled (or copied), so each copy gets its own
    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_initiate_lock"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._initiate_lock = threading.Lock()

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

    def _initiate(self, list_rows=None):
        """Run the query, if it hasn't been run yet.

        The query is only created once, however many callers are waiting on it.
        With `list_rows`, the API responds with the query's rows rather than the
        query if it finishes quickly, in which case the caller that created it
        gets that response, to read the rows from.
        """
        with self._initiate_lock:
            if self.did_initiate:
                return None

            response = make_request(
                method="post",
                path="/queries",
                payload=(
                    self.payload
                    if list_rows is None
                    else {**self.payload, "listRows": list_rows}
                ),
                parse_response=False,
                stream=True,
            )
            self.did_initiate = True

            # Content-Location is the URI of the rows, and Location that of the
            # query. (Older APIs ignore listRows, and always respond with the query.)
            if not response.headers.get("Content-Location"):
                with closing(response):
                    self.properties = response.json()
                self.uri = self.properties["uri"]
                return None

            query_id = response.headers["Location"].rstrip("/").split("/")[-1]
            self.uri = f"/queries/{query_id}"
            self.properties = {
                "kind": "query",
                "id": query_id,
                "uri": self.uri,
                "status": "completed",
            }
            return response

    def _wait_for_finish(self, list_rows=None):
        """Wait for the query to finish, running it if need be.

        Returns the response containing the query's rows, if running the query
        with `list_rows` responded with them (see _initiate()).
        """
        rows_response = self._initiate(list_rows)
        if rows_response is not None:
            return rows_response

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
