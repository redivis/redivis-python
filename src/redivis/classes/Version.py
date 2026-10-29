import logging
import time

from ..common import exceptions

from .Base import Base
from ..common.api_request import make_request
from ..common.util import normalize_version_tag


class Version(Base):
    def __init__(self, tag, *, dataset, properties=None):
        # e.g. v1.0, even if given in the form used in references (v1_0)
        self.tag = normalize_version_tag(tag)
        self._dataset = dataset

        self.uri = (
            properties["uri"]
            if "uri" in (properties or {})
            else (
                f"{dataset.uri}/versions/{normalize_version_tag(tag, for_reference=True)}"
            )
        )
        self.properties = properties

    def __repr__(self):
        return f"<Version {self._dataset.qualified_reference}>"

    def get(self):
        self.properties = make_request(
            method="GET",
            path=self.uri,
        )
        self.uri = self.properties["uri"]
        return self

    def exists(self):
        try:
            make_request(method="HEAD", path=self.uri)
            return True
        except exceptions.NotFoundError:
            return False

    def update(self, *, label=None, release_notes=None):
        payload = {}
        if label is not None:
            payload["label"] = label
        if release_notes is not None:
            payload["releaseNotes"] = release_notes

        self.properties = make_request(
            method="PATCH",
            path=self.uri,
            payload=payload,
        )
        self.uri = self.properties["uri"]
        return self

    def delete(self, *, wait_for_finalization=False):
        self.properties = make_request(method="DELETE", path=self.uri)
        self.uri = self.properties["uri"]
        if wait_for_finalization:
            self._wait_for_finalization()
        return self

    def undelete(self):
        self.properties = make_request(method="POST", path=f"{self.uri}/undelete")
        self.uri = self.properties["uri"]
        return self

    def previous_version(self):
        if "previousVersion" not in (self.properties or {}):
            self.get()

        if not self.properties["previousVersion"]:
            return None

        return Version(
            tag=self.properties["previousVersion"]["tag"], dataset=self._dataset
        )

    def next_version(self):
        if "nextVersion" not in (self.properties or {}):
            self.get()

        if not self.properties["nextVersion"]:
            return None

        return Version(tag=self.properties["nextVersion"]["tag"], dataset=self._dataset)

    def _wait_for_finalization(self):
        # Releasing, unreleasing, or deleting a version takes effect immediately, but its data is moved in the
        # background, and the dataset can't be released, unreleased, or deleted again until that finishes
        while self.properties.get("isFinalizing"):
            logging.debug("Version is still finalizing...")
            time.sleep(2)
            self.get()

    def dataset(self):
        # Avoids circular import
        from .Dataset import Dataset

        return Dataset(
            self._dataset.scoped_reference,
            organization=self._dataset.organization,
            user=self._dataset.user,
            version=self.tag,
        )
