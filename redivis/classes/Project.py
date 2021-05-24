from .Query import Query
from .Table import Table
from ..common.api_request import make_paginated_request


class Project:
    def __init__(self, name, *, user, properties=None):
        self.user = user
        self.name = name
        self.uri = f"/projects/{self.user.name}.{self.name}"
        self.properties = properties

    def query(self, query):
        return Query(query, default_project=self)

    def table(self, name):
        return Table(name, project=self)

    def list_tables(self, *, max_results=None, include_dataset_tables=False):
        tables = make_paginated_request(
            path=f"{self.uri}/tables",
            page_size=100,
            max_results=max_results,
            query={"includeDatasetTables": include_dataset_tables},
        )
        return [
            Table(table["name"], dataset=self, properties=table) for table in tables
        ]
