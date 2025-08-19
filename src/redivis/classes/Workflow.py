import os
from .Datasource import Datasource
from .Query import Query
from .Table import Table
from .Notebook import Notebook
from .Transform import Transform

from .Base import Base
from urllib.parse import quote as quote_uri
from ..common.api_request import make_request, make_paginated_request


class Workflow(Base):
    def __init__(self, name, *, user=None, organization=None, properties=None):
        from .User import User  # avoid circular import
        from .Organization import Organization  # avoid circular import

        if not user and not organization:
            if len(name.split(".")) == 2:
                username = name.split(".")[0]
                name = name.split(".")[1]
                organization = Organization(username)
                user = User(username)
            elif os.getenv("REDIVIS_DEFAULT_USER"):
                user = User(os.getenv("REDIVIS_DEFAULT_USER"))
            elif os.getenv("REDIVIS_DEFAULT_ORGANIZATION"):
                organization = Organization(os.getenv("REDIVIS_DEFAULT_ORGANIZATION"))
            else:
                raise Exception(
                    "Invalid workflow specifier, must be the fully qualified reference if no owner is specified"
                )

        if isinstance(user, str):
            user = User(user)

        if isinstance(organization, str):
            organization = Organization(user)

        self.user = user
        self.organization = organization
        self.name = name

        self.scoped_reference = (properties or {}).get(
            "scopedReference",
            self.name,
        )
        self.qualified_reference = (properties or {}).get(
            "qualifiedReference",
            f"{(self.organization or self.user).name}.{self.scoped_reference}",
        )

        self.uri = f"/workflows/{quote_uri(self.qualified_reference, '')}"
        self.properties = properties

    def list_datasources(self, *, max_results=None):
        data_sources = make_paginated_request(
            path=f"{self.uri}/dataSources",
            page_size=100,
            max_results=max_results,
        )
        return [
            Datasource(data_source["id"], workflow=self, properties=data_source)
            for data_source in data_sources
        ]

    def list_tables(self, *, max_results=None):
        tables = make_paginated_request(
            path=f"{self.uri}/tables",
            page_size=100,
            max_results=max_results,
        )
        return [
            Table(table["name"], workflow=self, properties=table) for table in tables
        ]

    def list_notebooks(self, *, max_results=None):
        notebooks = make_paginated_request(
            path=f"{self.uri}/notebooks",
            page_size=100,
            max_results=max_results,
        )
        return [
            Notebook(name=notebook["name"], workflow=self, properties=notebook)
            for notebook in notebooks
        ]

    def list_transforms(self, *, max_results=None):
        transforms = make_paginated_request(
            path=f"{self.uri}/transforms",
            page_size=100,
            max_results=max_results,
        )
        return [
            Transform(name=transform["name"], workflow=self, properties=transform)
            for transform in transforms
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

    def notebook(self, name):
        return Notebook(name, workflow=self)

    def transform(self, name):
        return Transform(name, workflow=self)

    def datasource(self, source):
        return Datasource(source, workflow=self)


def update_properties(instance, properties):
    instance.properties = properties
    instance.qualified_reference = properties["qualifiedReference"]
    instance.scoped_reference = properties["scopedReference"]
    instance.name = properties["name"]
    instance.uri = properties["uri"]
