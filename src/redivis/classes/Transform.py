from .Base import Base
from ..common import exceptions
from ..common.api_request import make_request
from .Table import Table, resolve_table_for_write
import os
from urllib.parse import quote as quote_uri
import warnings
from ..common.util import get_warning, split_reference, wait_for_run


class Transform(Base):
    def __init__(self, name, *, workflow=None, properties=None):
        from .Workflow import Workflow  # avoid circular import

        if not workflow:
            if name is not None and len(split_reference(name)) == 3:
                parts = split_reference(name)
                workflow = Workflow(".".join(parts[0:2]))
                name = parts[-1]
            elif os.getenv("REDIVIS_DEFAULT_WORKFLOW"):
                workflow = Workflow(os.getenv("REDIVIS_DEFAULT_WORKFLOW"))
            else:
                raise exceptions.ValueError(
                    "Invalid transform specifier, must be the fully qualified reference if no workflow is specified"
                )

        if isinstance(workflow, str):
            workflow = Workflow(workflow)

        self.workflow = workflow
        self.name = name

        # A transform without a name (yet) is named by the API when it's created
        self.qualified_reference = (properties or {}).get(
            "qualifiedReference",
            (
                f"{self.workflow.qualified_reference}.{self.name}"
                if self.name is not None
                else None
            ),
        )
        self.scoped_reference = (properties or {}).get("scopedReference", self.name)
        self.uri = (properties or {}).get(
            "uri",
            (
                f"/transforms/{quote_uri(self.qualified_reference, '')}"
                if self.qualified_reference is not None
                else None
            ),
        )
        self.properties = properties

    def __repr__(self):
        return f"<Transform {self.qualified_reference}>"

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

    def create(self, *, source_table, query=None):
        payload = {}
        if self.name is not None:
            payload["name"] = self.name
        if query is not None:
            payload["query"] = query
        if isinstance(source_table, Table):
            resolve_table_for_write(source_table)
            payload["sourceTable"] = source_table.qualified_reference
        else:
            payload["sourceTable"] = source_table

        response = make_request(
            method="POST",
            path=f"{self.workflow.uri}/transforms",
            payload=payload,
        )
        update_properties(self, response)
        return self

    def update(self, *, name=None, source_table=None, query=None):
        payload = {}

        if name is not None:
            payload["name"] = name
        if query is not None:
            payload["query"] = query
        if source_table is not None:
            if isinstance(source_table, Table):
                resolve_table_for_write(source_table)
                payload["sourceTable"] = source_table.qualified_reference
            else:
                payload["sourceTable"] = source_table

        response = make_request(
            method="PATCH",
            path=f"{self.uri}",
            payload=payload,
        )
        self.properties = response
        self.uri = self.properties["uri"]
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
        except exceptions.NotFoundError:
            return False

    def run(self, *, wait_for_finish=True):
        self.properties = make_request(
            method="POST",
            path=f"{self.uri}/run",
        )
        self.uri = self.properties["uri"]

        if wait_for_finish:
            wait_for_run(self, self.properties)

        return self

    def cancel(self):
        self.properties = make_request(
            method="POST",
            path=f"{self.uri}/cancel",
        )
        self.uri = self.properties["uri"]
        return self


def update_properties(instance, properties):
    instance.properties = properties
    instance.qualified_reference = properties["qualifiedReference"]
    instance.scoped_reference = properties["scopedReference"]
    instance.name = properties["name"]
    instance.uri = properties["uri"]
