import os
from .Datasource import Datasource
from .Parameter import Parameter
from .Query import Query
from .Table import Table
from .Notebook import Notebook
from .Transform import Transform

from .Base import Base
from urllib.parse import quote as quote_uri
from ..common.util import split_reference

from ..common import exceptions
from ..common.api_request import make_request, make_paginated_request
from ..common.config import get_config


class Workflow(Base):
    def __init__(self, name, *, user=None, organization=None, properties=None):
        from .User import User  # avoid circular import
        from .Organization import Organization  # avoid circular import

        if not user and not organization:
            if len(split_reference(name)) == 2:
                username, name = split_reference(name)
                organization = Organization(username)
                user = User(username)
            elif get_config("default_user"):
                user = User(get_config("default_user"))
            elif get_config("default_organization"):
                organization = Organization(get_config("default_organization"))
            else:
                raise exceptions.ValueError(
                    "Invalid workflow specifier, must be the fully qualified reference if no owner is specified"
                )

        if isinstance(user, str):
            user = User(user)

        if isinstance(organization, str):
            organization = Organization(organization)

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

    def __repr__(self):
        return f"<Workflow {self.qualified_reference}>"

    def _rectify_ambiguous_owner(self):
        # A reference like "owner.workflow_name" could be owned by either
        if self.user and self.organization:
            if (self.properties or {}).get("owner"):
                if self.properties["owner"]["kind"] == "user":
                    self.organization = None
                else:
                    self.user = None
            elif self.organization.exists():
                self.user = None
            else:
                self.organization = None

    def create(self, *, description=None, is_public=False):
        self._rectify_ambiguous_owner()
        owner = self.organization or self.user

        properties = make_request(
            method="POST",
            path=f"{owner.uri}/workflows",
            payload={
                "name": self.name,
                "description": description,
                "isPublic": is_public,
            },
        )
        update_properties(self, properties)
        return self

    def delete(self):
        make_request(method="DELETE", path=self.uri)
        return

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

    def list_tables(self, *, max_results=None, include_datasources=False):
        tables = make_paginated_request(
            path=f"{self.uri}/tables",
            query={"includeDataSources": "true"} if include_datasources else {},
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

    def list_parameters(self, *, max_results=None):
        parameters = make_paginated_request(
            path=f"{self.uri}/parameters",
            page_size=100,
            max_results=max_results,
        )
        return [
            Parameter(name=parameter["name"], workflow=self, properties=parameter)
            for parameter in parameters
        ]

    def exists(self):
        try:
            make_request(method="HEAD", path=self.uri)
            return True
        except exceptions.NotFoundError:
            return False

    def update_variables(self, variables):
        make_request(
            method="PATCH",
            path=f"{self.uri}/variables",
            payload={"variables": variables},
        )

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

    def transform(self, name=None):
        return Transform(name, workflow=self)

    def parameter(self, name):
        return Parameter(name, workflow=self)

    def datasource(self, source):
        return Datasource(source, workflow=self)


def update_properties(instance, properties):
    instance.properties = properties
    instance.qualified_reference = properties["qualifiedReference"]
    instance.scoped_reference = properties["scopedReference"]
    instance.name = properties["name"]
    instance.uri = properties["uri"]
    instance._rectify_ambiguous_owner()
