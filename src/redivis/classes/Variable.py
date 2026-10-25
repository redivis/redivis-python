import time

from ..common import exceptions

from .Base import Base
from ..common.api_request import make_request
from urllib.parse import quote as quote_uri


class Variable(Base):
    def __init__(
        self,
        name,
        *,
        table=None,
        upload=None,
        query=None,
        properties=None,
    ):
        self.name = name
        self.table = table
        self.upload = upload
        self.query = query

        if "uri" in (properties or {}):
            self.uri = properties["uri"]
        elif table:
            self.uri = f"{table.uri}/variables/{quote_uri(self.name, '')}"
        elif upload:
            self.uri = f"{upload.uri}/variables/{quote_uri(self.name, '')}"
        else:
            self.uri = f"{query.uri}/variables/{quote_uri(self.name, '')}"

        self.properties = properties

    def __repr__(self):
        if self.properties and self.properties.get("type"):
            return f"<Variable {self.name} ({self.properties.get('type')})>"
        return f"<Variable {self.name}>"

    def get(self):
        self.properties = make_request(
            method="GET",
            path=self.uri,
        )
        self.uri = self.properties["uri"]

        return self

    def get_statistics(self):
        statistics = make_request(
            method="GET",
            path=f"{self.uri}/statistics",
        )
        while statistics["status"] == "running" or statistics["status"] == "queued":
            time.sleep(2)
            statistics = make_request(
                method="GET",
                path=f"{self.uri}/statistics",
            )

        return statistics

    def update(self, *, label=None, description=None, value_labels=None):
        from .Table import is_workflow_scoped_uri, resolve_table_for_write

        if self.table and is_workflow_scoped_uri(self.uri):
            resolve_table_for_write(self.table)
            self.uri = f"{self.table.uri}/variables/{quote_uri(self.name, '')}"

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
        return self

    def exists(self):
        try:
            make_request(method="HEAD", path=self.uri)
            return True
        except exceptions.NotFoundError:
            return False
