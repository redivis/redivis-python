from .Table import Table
from .Query import Query
from .Base import Base
from urllib.parse import quote as quote_uri
import warnings

from ..common.api_request import make_request, make_paginated_request

class Dataset(Base):
    def __init__(
        self,
        name,
        *,
        version="current",  # TODO: should be version_tag, version would reference a version class (?) dataset().version("next").create()? .version("next").release()?
        user=None,
        organization=None,
        properties={},
    ):
        self.name = name
        self.version = version
        self.user = user
        self.organization = organization
        self.identifier = (
            f"{(self.organization or self.user).name}.{self.name}:{self.version}"
        )
        self.uri = f"/datasets/{quote_uri(self.identifier, '')}"
        self.properties = {
            **{
                  "kind": 'dataset',
                  "name": name,
                  "uri": self.uri
            },
            **properties
        }



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

    def create_next_version(self, *, if_not_exists=False, ignore_if_exists=None):
        if ignore_if_exists is not None:
            warnings.warn(
                "The ignore_if_exists parameter has been renamed to if_not_exists, and will be removed in a future version of this library",
                FutureWarning,
                stacklevel=2
            )
            if_not_exists = ignore_if_exists

        if not self.properties or not hasattr(self.properties, "nextVersion"):
            self.get()

        if not self.properties["nextVersion"]:
            make_request(method="POST", path=f"{self.uri}/versions")
        elif not if_not_exists:
            raise Exception(
                f"Next version already exists at {self.properties['nextVersion']['datasetUri']}. To avoid this error, set argument ignore_if_exists to True"
            )

        return Dataset(
            name=self.name,
            user=self.user,
            organization=self.organization,
            version="next",
        ).get()

    def delete(self):
        make_request(
            method="DELETE",
            path=self.uri,
        )
        return

    def exists(self):
        try:
            make_request(method="GET", path=self.uri)
            return True
        except Exception as err:
            if err.args[0]["status"] != 404:
                raise err
            return False

    def get(self):
        self.properties = make_request(method="GET", path=self.uri)
        self.uri = self.properties["uri"]
        return self

    def list_tables(self, max_results):
        tables = make_paginated_request(
            path=f"{self.uri}/tables", page_size=100, max_results=max_results
        )
        return [
            Table(table["name"], dataset=self, properties=table) for table in tables
        ]

    def query(self, query):
        return Query(query, default_dataset=self.identifier)

    def release(self):
        res = make_request(
            method="POST",
            path=f"{self.uri}/versions/next/release",
        )
        self.version = f'v{res["tag"]}'
        self.identifier = (
            f"{(self.organization or self.user).name}.{self.name}:{self.version}"
        )
        self.uri = f"/datasets/{quote_uri(self.identifier, '')}"
        return Dataset(
            name=self.name,
            user=self.user,
            organization=self.organization,
            version="current",
        ).get()

    def table(self, name, *, sample=False):
        return Table(name, dataset=self, sample=sample)

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
