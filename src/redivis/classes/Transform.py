from .Base import Base
from ..common.api_request import make_request
from .Table import Table
import os
from urllib.parse import quote as quote_uri
import time
import logging
import warnings
from ..common.util import get_warning


class Transform(Base):
    def __init__(self, name, *, workflow=None, properties=None):
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

        self.qualified_reference = (properties or {}).get(
            "qualifiedReference", f"{self.workflow.qualified_reference}.{self.name}"
        )
        self.scoped_reference = (properties or {}).get("scopedReference", self.name)
        self.uri = (properties or {}).get(
            "uri", f"/transforms/{quote_uri(self.qualified_reference, '')}"
        )
        self.properties = properties

    def referenced_tables(self):
        self.get()
        return [
            Table(source_table["qualifiedReference"], properties=source_table)
            for source_table in self.properties["referencedTables"]
        ]

    def source_tables(self):
        warnings.warn(
            get_warning("source_tables_deprecation"), FutureWarning, stacklevel=2
        )
        return self.referenced_tables()

    def source_table(self):
        self.get()
        return Table(
            self.properties["sourceTable"]["qualifiedReference"],
            properties=self.properties["sourceTable"],
        )

    def update(self, *, name=None, source_table=None):
        payload = {}

        if name is not None:
            payload["name"] = name
        if source_table is not None:
            if isinstance(source_table, Table):
                payload["sourceTable"] = source_table.qualified_reference
            else:
                payload["sourceTable"] = source_table

        response = make_request(
            method="PATCH",
            path=f"{self.uri}",
            payload=payload,
        )
        self.properties = response
        return self

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
                current_job = self.properties.get("currentJob") or self.properties.get(
                    "lastRunJob"
                )
                if current_job and current_job["status"] in [
                    "completed",
                    "failed",
                ]:
                    if current_job["status"] == "failed":
                        raise Exception(current_job["errorMessage"])
                    break
                else:
                    logging.debug("Transform is still in progress...")

        return self

    def cancel(self):
        self.properties = make_request(
            method="POST",
            path=f"{self.uri}/cancel",
        )
        self.uri = self.properties["uri"]
