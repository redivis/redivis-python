from .Query import Query
from .Table import Table
from .Base import Base
from urllib.parse import quote as quote_uri
from ..common.api_request import make_paginated_request


class Project(Base):
    def __init__(self, name, *, user, properties={}):
        self.user = user
        self.name = name
        self.identifier = f"{self.user.name}.{self.name}"
        self.uri = f"/projects/{quote_uri(self.identifier, '')}"
        self.properties = {
            **{
                "kind": "project",
                "name": name,
                "uri": self.uri
            },
            **properties
        }

    def list_tables(self, *, max_results=None, include_dataset_tables=False):
        tables = make_paginated_request(
            path=f"{self.uri}/tables",
            page_size=100,
            max_results=max_results,
            query={"includeDatasetTables": include_dataset_tables},
        )
        return [
            Table(table["name"], project=self, properties=table) for table in tables
        ]

    def query(self, query):
        return Query(query, default_project=self.identifier)

    def table(self, name):
        return Table(name, project=self)
