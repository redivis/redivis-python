import time
from .Base import Base
from .User import User
from ..common.api_request import make_request


class Member(Base):
    def __init__(self, name, *, organization, properties={}):
        self.name = name
        self.organization = organization
        self.user = User(
            name, properties=properties["user"] if "user" in properties else {}
        )
        self.uri = (
            properties["uri"]
            if "uri" in (properties or {})
            else (f"{organization.uri}/members/{self.name}")
        )
        self.properties = properties

    def get(self):
        self.properties = make_request(
            method="GET",
            path=self.uri,
        )
        self.uri = self.properties["uri"]
        self.user.properties = self.properties["user"]
        return self

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

    def update(self, *, labels=None, status=None):
        payload = {}
        if labels is not None:
            payload["labels"] = labels
        if status is not None:
            payload["status"] = status

        response = make_request(
            method="PATCH",
            path=f"{self.uri}",
            payload=payload,
        )
        self.properties = response
        self.user.properties = response["user"]
        return

    def exists(self):
        try:
            make_request(method="HEAD", path=self.uri)
            return True
        except Exception as err:
            if err.args[0]["status"] != 404:
                raise err
            return False
