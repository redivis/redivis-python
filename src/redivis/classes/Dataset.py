from .Table import Table
from .Version import Version
import os
import warnings
from .Query import Query
from .Base import Base
from urllib.parse import quote as quote_uri
from ..common.util import split_reference
import re

from ..common import exceptions
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
        if not user and not organization:
            from .User import User
            from .Organization import Organization

            if len(split_reference(name)) == 2:
                username, name = split_reference(name)
                organization = Organization(username)
                user = User(username)
            elif os.getenv("REDIVIS_DEFAULT_USER"):
                user = User(os.getenv("REDIVIS_DEFAULT_USER"))
            elif os.getenv("REDIVIS_DEFAULT_ORGANIZATION"):
                organization = Organization(os.getenv("REDIVIS_DEFAULT_ORGANIZATION"))
            else:
                raise exceptions.ValueError(
                    "Invalid dataset specifier, must be the fully qualified reference if no owner is specified"
                )

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
        self.scoped_reference = (properties or {}).get(
            "scopedReference",
            f"{self.name}{reference_id}{f':{version}' if version else ''}",
        )
        self.qualified_reference = (properties or {}).get(
            "qualifiedReference",
            f"{(self.organization or self.user).name}.{self.scoped_reference}",
        )

        self.uri = f"/datasets/{quote_uri(self.qualified_reference, '')}"
        self.properties = properties

    def __repr__(self):
        return f"<Dataset {self.qualified_reference}>"

    def _rectify_ambiguous_owner(self):
        if self.user and self.organization:
            if (self.properties or {}).get("owner"):
                if self.properties.get("owner")["kind"] == "user":
                    self.organization = None
                else:
                    self.user = None
            elif self.organization.exists():
                self.user = None
            else:
                self.organization = None

    def create(self, *, public_access_level="none", description=None):
        self._rectify_ambiguous_owner()
        if self.organization:
            path = f"{self.organization.uri}/datasets"
        else:
            path = f"{self.user.uri}/datasets"

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
            raise exceptions.ValueError(
                f"Next version already exists at {self.properties['nextVersion']['datasetUri']}. To avoid this error, set argument if_not_exists to True"
            )

        self._rectify_ambiguous_owner()
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
        except exceptions.NotFoundError:
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

    def release(self, *, release_notes=None):
        version_res = make_request(
            method="POST",
            path=f"{self.uri}/versions/next/release",
            payload={"releaseNotes": release_notes},
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

    def previous_version(self):
        version = self.version().previous_version()
        return version.dataset() if version else None

    def next_version(self):
        version = self.version().next_version()
        return version.dataset() if version else None

    def table(self, name):
        return Table(name, dataset=self)

    def add_labels(self, labels):
        warnings.warn(
            "This method is deprecated, and will be removed in a future version. Use dataset.update(labels_to_add=[...]) instead",
            FutureWarning,
            stacklevel=2,
        )
        return self.update(labels_to_add=labels)

    def remove_labels(self, labels):
        warnings.warn(
            "This method is deprecated, and will be removed in a future version. Use dataset.update(labels_to_remove=[...]) instead",
            FutureWarning,
            stacklevel=2,
        )
        return self.update(labels_to_remove=labels)

    def update(
        self, 
        *, 
        name=None, 
        public_access_level=None, 
        description=None, 
        methodology_markdown=None, 
        usage_markdown=None, 
        labels=None, 
        labels_to_add=None, 
        labels_to_remove=None, 
        tags=None, 
        tags_to_add=None, 
        tags_to_remove=None
    ):
        payload = {}
        if name:
            payload["name"] = name
        if public_access_level:
            payload["publicAccessLevel"] = public_access_level
        if description is not None:
            payload["description"] = description
        if methodology_markdown is not None:
            payload["methodologyMarkdown"] = methodology_markdown
        if usage_markdown is not None:
            payload["usageMarkdown"] = usage_markdown
        if labels is not None:
            payload["labels"] = labels
        if labels_to_add is not None:
            payload["labelsToAdd"] = labels_to_add
        if labels_to_remove is not None:
            payload["labelsToRemove"] = labels_to_remove
        if tags is not None:
            payload["tags"] = tags
        if tags_to_add is not None:
            payload["tagsToAdd"] = tags_to_add
        if tags_to_remove is not None:
            payload["tagsToRemove"] = tags_to_remove

        res = make_request(
            method="PATCH",
            path=self.uri,
            payload=payload,
        )
        update_properties(self, res)
        return self

    def update_variables(self, variables):
        make_request(
            method="PATCH",
            path=f"{self.uri}/variables",
            payload={"variables": variables},
        )


def update_properties(instance, properties):
    instance.properties = properties
    instance.qualified_reference = properties["qualifiedReference"]
    instance.scoped_reference = properties["scopedReference"]
    instance.name = properties["name"]
    instance.uri = properties["uri"]
    if (properties.get("version") or {}).get("tag"):
        # In the form used in references (v1_0, rather than v1.0)
        instance.version_tag = properties["version"]["tag"].replace(".", "_")
    instance._rectify_ambiguous_owner()
