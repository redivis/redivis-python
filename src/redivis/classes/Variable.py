import time
from .Base import Base
from ..common.api_request import make_request


class Variable(Base):
    def __init__(
        self,
        name,
        *,
        table=None,
        upload=None,
        properties=None,
    ):
        self.name = name
        self.table = table
        self.upload = upload
        self.uri = (
            properties["uri"]
            if "uri" in (properties or {})
            else (f"{table.uri if table else upload.uri}/variables/{self.name}")
        )
        self.properties = properties

    def get(self, wait_for_statistics=False):
        self.properties = make_request(
            method="GET",
            path=self.uri,
        )
        self.uri = self.properties["uri"]
        while (
            wait_for_statistics and self.properties["statistics"]["status"] == "running"
        ):
            time.sleep(2)
            self.properties = make_request(
                method="GET",
                path=self.uri,
            )

        return self

    def update(self, *, label=None, description=None, value_labels=None):
        payload = {}
        if label is not None:
            payload["label"] = label
        if description is not None:
            payload["description"] = description
        if value_labels is not None:
            payload["valueLabels"] = value_labels

        response = make_request(
            method="PATCH",
            path=f"{self.uri}",
            payload=payload,
        )
        self.properties = response
        return

    def exists(self):
        try:
            make_request(method="HEAD", path=self.uri)
            return True
        except Exception as err:
            if err.args[0]["status"] != 404:
                raise err
            return False
