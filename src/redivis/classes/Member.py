import warnings

from .Base import Base
from .User import User
from ..common.api_request import make_request
from ..common import exceptions


class Member(Base):
    def __init__(self, name, *, organization, properties=None):
        self.name = name
        self.organization = organization
        self.user = User(
            name,
            properties=(properties or {}).get("user"),
        )
        self.uri = (
            properties["uri"]
            if "uri" in (properties or {})
            else (f"{organization.uri}/members/{self.name}")
        )
        self.properties = properties

    def __repr__(self):
        return f"<Member {self.organization.name}.{self.name}>"

    def get(self):
        self.properties = make_request(
            method="GET",
            path=self.uri,
        )
        self.uri = self.properties["uri"]
        self.user.properties = self.properties["user"]
        return self

    def add_labels(self, labels):
        warnings.warn(
            "This method is deprecated, and will be removed in a future version. Use member.update(labels_to_add=[...]) instead",
            FutureWarning,
            stacklevel=2,
        )
        return self.update(labels_to_add=labels)

    def remove_labels(self, labels):
        warnings.warn(
            "This method is deprecated, and will be removed in a future version. Use member.update(labels_to_remove=[...]) instead",
            FutureWarning,
            stacklevel=2,
        )
        return self.update(labels_to_remove=labels)

    def add_comment(self, text):
        make_request(
            method="POST",
            path=f"{self.uri}/comments",
            payload={"text": text},
        )
        return self

    def update(self, *, status=None, labels=None, labels_to_add=None, labels_to_remove=None):
        payload = {}
        if status is not None:
            payload["status"] = status
        if labels is not None:
            payload["labels"] = labels
        if labels_to_add is not None:
            payload["labelsToAdd"] = labels_to_add
        if labels_to_remove is not None:
            payload["labelsToRemove"] = labels_to_remove

        response = make_request(
            method="PATCH",
            path=f"{self.uri}",
            payload=payload,
        )
        self.properties = response
        self.user.properties = response["user"]
        return self

    def exists(self):
        try:
            make_request(method="HEAD", path=self.uri)
            return True
        except exceptions.NotFoundError:
            return False
