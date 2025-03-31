from .Base import Base
from ..common.api_request import make_request


class Version(Base):
    def __init__(self, tag, *, dataset, properties={}):
        if tag != "current" and tag != "next" and not tag.lower().startswith("v"):
            tag = f"v{tag}"

        self.tag = tag
        self._dataset = dataset

        self.uri = (
            properties["uri"]
            if "uri" in (properties or {})
            else (f"{dataset.uri}/versions/{self.tag}")
        )
        self.properties = properties

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
        except Exception as err:
            if err.args[0]["status"] != 404:
                raise err
            return False

    def delete(self):
        self.properties = make_request(method="DELETE", path=self.uri)
        self.uri = self.properties["uri"]

    def undelete(self):
        self.properties = make_request(method="POST", path=f"{self.uri}/undelete")
        self.uri = self.properties["uri"]

    def previous_version(self):
        if "previousVersion" not in self.properties:
            self.get()

        if not self.properties["previousVersion"]:
            return None

        return Version(
            tag=self.properties["previousVersion"]["tag"], dataset=self._dataset
        )

    def next_version(self):
        if "nextVersion" not in self.properties:
            self.get()

        if not self.properties["nextVersion"]:
            return None

        return Version(tag=self.properties["nextVersion"]["tag"], dataset=self._dataset)

    def dataset(self):
        # Avoids circular import
        from .Dataset import Dataset

        return Dataset(
            self._dataset.scopedReference,
            organization=self._dataset.organization,
            user=self._dataset.user,
            version=self.tag,
        )
