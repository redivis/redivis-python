from .Base import Base
from ..common.api_request import make_request
from .Table import Table
import os
from urllib.parse import quote as quote_uri
import time
import logging


class Transform(Base):
    def __init__(self, name, *, workflow=None, properties={}):
        from .Workflow import Workflow  # avoid circular import

        if not workflow:
            if len(name.split(".")) == 3:
                workflow = Workflow(".".join(name.split(".")[0:2]))
                name = name.split(".")[-1]
            elif os.getenv("REDIVIS_DEFAULT_WORKFLOW"):
                workflow = Workflow(os.getenv("REDIVIS_DEFAULT_WORKFLOW"))
            else:
                raise Exception(
                    "Invalid transform specifier, must be the fully qualified reference if no workflow is specified"
                )

        if isinstance(workflow, str):
            workflow = Workflow(workflow)

        self.workflow = workflow
        self.name = name

        self.qualified_reference = properties.get(
            "qualifiedReference", f"{self.workflow.qualified_reference}.{self.name}"
        )
        self.scoped_reference = properties.get("scopedReference", self.name)
        self.uri = properties.get(
            "uri", f"/transforms/{quote_uri(self.qualified_reference, '')}"
        )
        self.properties = properties

    def source_tables(self):
        self.get()
        return [
            Table(source_table["qualifiedReference"], properties=source_table)
            for source_table in self.properties["sourceTables"]
        ]

    def output_table(self):
        self.get()
        return Table(
            self.properties["outputTable"]["qualifiedReference"],
            properties=self.properties["outputTable"],
        )

    def get(self):
        self.properties = make_request(
            method="GET",
            path=self.uri,
        )
        self.uri = self.properties["uri"]

        return self

    def exists(self):
        try:
            make_request(method="HEAD", path=self.uri)
            return True
        except Exception as err:
            if err.args[0]["status"] != 404:
                raise err
            return False

    def run(self, *, wait_for_finish=True):
        self.properties = make_request(
            method="POST",
            path=f"{self.uri}/run",
        )
        self.uri = self.properties["uri"]

        if wait_for_finish:
            while True:
                time.sleep(2)
                self.get()
                if self.properties.get("currentJob") and self.properties["currentJob"][
                    "status"
                ] in ["completed", "failed"]:
                    if self.properties["currentJob"]["status"] == "failed":
                        raise Exception(self.properties["currentJob"]["errorMessage"])
                    break
                elif self.properties.get("currentJob"):
                    logging.debug("Transform is still in progress...")
                else:
                    break

        return self

    def cancel(self):
        self.properties = make_request(
            method="POST",
            path=f"{self.uri}/cancel",
        )
        self.uri = self.properties["uri"]
