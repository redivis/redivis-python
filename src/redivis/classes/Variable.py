import json
import time

from ..common.api_request import make_request


class Variable:
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
        self.properties = properties
        self.uri = f"{table.uri if table else upload.uri}/variables/{self.name}"

    def __getitem__(self, key):
        return self.properties[key]

    def __str__(self):
        return json.dumps(self.properties, indent=2)

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
