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
        properties={},
    ):
        self.name = name
        self.table = table
        self.upload = upload
        self.uri = f"{table.uri if table else upload.uri}/variables/{self.name}"
        self.properties = {
            **{
                "kind": "variable",
                "name": "name",
                "uri": self.uri
            },
            **properties
        }

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

    def exists(self):
        try:
            make_request(method="GET", path=self.uri)
            return True
        except Exception as err:
            if err.args[0]["status"] != 404:
                raise err
            return False
