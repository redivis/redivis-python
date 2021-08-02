from .Table import Table
from .Query import Query
from urllib.parse import quote as quote_uri

from ..common.api_request import make_request, make_paginated_request
import json


class Dataset:
    def __init__(
        self,
        name,
        *,
        version="current",
        user=None,
        organization=None,
        properties=None,
    ):
        self.name = name
        self.version = version
        self.user = user
        self.organization = organization
        self.identifier = (
            f"{(self.organization or self.user).name}.{self.name}:{self.version}"
        )
        self.uri = f"/datasets/{quote_uri(self.identifier)}"
        self.properties = properties

    def __getitem__(self, key):
        return self.properties[key]

    def __str__(self):
        return json.dumps(self.properties, indent=2)

    def table(self, name, *, sample=False):
        return Table(name, dataset=self, sample=sample)

    def exists(self):
        try:
            make_request(method="GET", path=self.uri)
        except Exception as err:
            if err.args[0]["status"] != 404:
                raise err
            return False

        return True

    def get(self):
        self.properties = make_request(method="GET", path=self.uri)
        self.uri = self.properties["uri"]
        return self

    def create(self, *, public_access_level="none", description=None):
        if self.organization:
            path = f"/organizations/{self.organization.name}/datasets"
        else:
            path = f"/users/{self.user.name}/datasets"

        self.properties = make_request(
            method="POST",
            path=path,
            payload={
                "name": self.name,
                "publicAccessLevel": public_access_level,
                "description": description,
            },
        )
        return self

    def update(self, *, name=None, public_access_level=None, description=None):
        payload = {}
        if name:
            payload["name"] = name
        if public_access_level:
            payload["publicAccessLevel"] = public_access_level
        if description is not None:
            payload["description"] = description

        self.properties = make_request(
            method="POST",
            path=self.uri,
            payload=payload,
        )
        return self

    def delete(self):
        make_request(
            method="DELETE",
            path=self.uri,
        )
        return

    def create_next_version(self, ignore_if_exists=False):
        if not self.properties or not hasattr(self.properties, "nextVersion"):
            self.get()

        if not self.properties["nextVersion"]:
            make_request(method="POST", path=f"{self.uri}/versions")
        elif not ignore_if_exists:
            raise Exception(
                f"Next version already exists at {self.properties['nextVersion']['datasetUri']}. To avoid this error, set argument ignore_if_exists to True"
            )

        return Dataset(
            name=self.name,
            user=self.user,
            organization=self.organization,
            version="next",
        ).get()

    def release(self):
        make_request(
            method="POST",
            path=f"{self.uri}/versions/next/release",
        )
        return Dataset(
            name=self.name,
            user=self.user,
            organization=self.organization,
            version="current",
        ).get()

    def list_tables(self, max_results):
        tables = make_paginated_request(
            path=f"{self.uri}/tables", page_size=100, max_results=max_results
        )
        return [
            Table(table["name"], dataset=self, properties=table) for table in tables
        ]

    def query(self, query):
        return Query(query, default_dataset=self.identifier)
