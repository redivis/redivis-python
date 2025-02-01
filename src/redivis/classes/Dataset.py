from .Table import Table
from .Version import Version
from .Query import Query
from .Base import Base
from urllib.parse import quote as quote_uri
import re

from ..common.api_request import make_request, make_paginated_request


class Dataset(Base):
    def __init__(
        self,
        name,
        *,
        version=None,
        user=None,
        organization=None,
        properties=None,
    ):
        self.name = name.split(":")[0]
        self.user = user
        self.organization = organization

        reference_id = ""
        reference_id_split = re.sub(
            r":(v\d+[._]\d+|current|next|sample)", "", name
        ).split(":")
        if len(reference_id_split) > 1:
            reference_id = reference_id_split[1]

        if reference_id:
            reference_id = f":{reference_id}"

        if not version:
            version_search = re.search(r":(v\d+[._]\d+|current|next)", name)
            if version_search:
                version = version_search[1]

        if (
            version
            and version != "current"
            and version != "next"
            and not version.lower().startswith("v")
        ):
            version = f"v{version}"

        self.version_tag = version
        self.scoped_reference = (
            properties["scopedReference"]
            if "scopedReference" in (properties or {})
            else f"{self.name}{reference_id}{f':{version}' if version else ''}"
        )
        self.qualified_reference = (
            properties["qualifiedReference"]
            if "qualifiedReference" in (properties or {})
            else (f"{(self.organization or self.user).name}.{self.scoped_reference}")
        )

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
            name=self.scoped_reference,
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

    def list_versions(self, max_results=None):
        versions = make_paginated_request(
            path=f"{self.uri}/versions", page_size=100, max_results=max_results
        )
        return [
            Version(version["tag"], dataset=self, properties=version)
            for version in versions
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

    def unrelease(self):
        version_res = make_request(
            method="POST",
            path=f"{self.uri}/versions/current/unrelease",
        )
        self.uri = version_res["datasetUri"]
        self.get()
        return self

    def version(self, tag=None):
        if not self.version_tag and not tag:
            self.get()

        tag = self.version_tag if tag is None else tag
        return Version(tag, dataset=self)

    def table(self, name):
        return Table(name, dataset=self)

    def add_labels(self, labels):
        self.get()
        self.update(
            labels=list(
                set(self.properties.get("labels", []))
                | set(label.lower() for label in labels)
            )
        )
        return self

    def remove_labels(self, labels):
        self.get()
        self.update(
            labels=list(
                set(self.properties.get("labels", []))
                - set(label.lower() for label in labels)
            )
        )
        return self

    def update(
        self, *, name=None, public_access_level=None, description=None, labels=None
    ):
        payload = {}
        if name:
            payload["name"] = name
        if public_access_level:
            payload["publicAccessLevel"] = public_access_level
        if description is not None:
            payload["description"] = description
        if labels is not None:
            payload["labels"] = labels

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
