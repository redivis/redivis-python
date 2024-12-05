from .Query import Query
from .Table import Table
from .Base import Base
from urllib.parse import quote as quote_uri
from ..common.api_request import make_request, make_paginated_request


class Workflow(Base):
    def __init__(self, name, *, user, properties=None):
        self.user = user
        self.name = name

        self.qualified_reference = (
            properties["qualifiedReference"]
            if "qualifiedReference" in (properties or {})
            else (f"{self.user.name}.{self.name}")
        )
        self.scoped_reference = (
            properties["scopedReference"]
            if "scopedReference" in (properties or {})
            else f"{self.name}"
        )
        self.uri = f"/workflows/{quote_uri(self.qualified_reference, '')}"
        self.properties = properties

    def list_tables(self, *, max_results=None):
        tables = make_paginated_request(
            path=f"{self.uri}/tables",
            page_size=100,
            max_results=max_results,
        )
        return [
            Table(table["name"], workflow=self, properties=table) for table in tables
        ]

    def exists(self):
        try:
            make_request(method="HEAD", path=self.uri)
            return True
        except Exception as err:
            if err.args[0]["status"] != 404:
                raise err
            return False

    def get(self):
        properties = make_request(method="GET", path=self.uri)
        update_properties(self, properties)
        return self

    def query(self, query):
        return Query(query, default_workflow=self.qualified_reference)

    def table(self, name):
        return Table(name, workflow=self)


def update_properties(instance, properties):
    instance.properties = properties
    instance.qualified_reference = properties["qualifiedReference"]
    instance.scoped_reference = properties["scopedReference"]
    instance.name = properties["name"]
    instance.uri = properties["uri"]
