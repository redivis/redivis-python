from .Table import Table
from .Query import Query
from .Base import Base
from urllib.parse import quote as quote_uri

from ..common.api_request import make_request, make_paginated_request

class Dataset(Base):
    def __init__(
        self,
        name,
        *,
        version="current",  # TODO: should be version_tag, version would reference a version class (?) dataset().version("next").create()? .version("next").release()?
        user=None,
        organization=None,
        properties=None,
    ):
        self.name = name
        self.user = user
        self.organization = organization

        if version and version != "current" and version != "next" and not version.lower().startswith("v"):
            version = f"v{version}"

        self.qualified_reference = properties["qualifiedReference"] if "qualifiedReference" in (properties or {}) else (
            f"{(self.organization or self.user).name}.{self.name}:{version}"
        )
        self.scoped_reference = properties["scopedReference"] if "scopedReference" in (properties or {}) else f"{self.name}:{version}"
        self.uri = f"/datasets/{quote_uri(self.qualified_reference, '')}"
        self.properties = properties

    def create(self, *, public_access_level="none", description=None):
        if self.organization:
            path = f"/organizations/{self.organization.name}/datasets"
        else:
            path = f"/users/{self.user.name}/datasets"

        properties = make_request(
            method="POST",
            path=path,
            payload={
                "name": self.name,
                "publicAccessLevel": public_access_level,
                "description": description,
            },
        )
        update_properties(self, properties)
        return self

    def create_next_version(self, *, if_not_exists=False):
        if not self.properties or "nextVersion" not in self.properties:
            self.get()

        if not self.properties["nextVersion"]:
            make_request(method="POST", path=f"{self.uri}/versions")
        elif not if_not_exists:
            raise Exception(
                f"Next version already exists at {self.properties['nextVersion']['datasetUri']}. To avoid this error, set argument if_not_exists to True"
            )

        next_version_dataset = Dataset(
            name=self.name,
            user=self.user,
            organization=self.organization,
            version="next",
        )

        return next_version_dataset.get()

    def delete(self):
        make_request(
            method="DELETE",
            path=self.uri,
        )
        return

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

    def list_tables(self, max_results=None):
        tables = make_paginated_request(
            path=f"{self.uri}/tables", page_size=100, max_results=max_results
        )
        return [
            Table(table["name"], dataset=self, properties=table) for table in tables
        ]

    def query(self, query):
        return Query(query, default_dataset=self.qualified_reference)

    def release(self):
        version_res = make_request(
            method="POST",
            path=f"{self.uri}/versions/next/release",
        )
        self.uri = version_res["datasetUri"]
        self.get()
        return self

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

        res = make_request(
            method="PATCH",
            path=self.uri,
            payload=payload,
        )
        update_properties(self, res)
        return self

def update_properties(instance, properties):
    instance.properties = properties
    instance.qualified_reference = properties["qualifiedReference"]
    instance.scoped_reference = properties["scopedReference"]
    instance.name = properties["name"]
    instance.uri = properties["uri"]
