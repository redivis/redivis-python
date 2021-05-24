import json

from ..common.api_request import make_request, make_paginated_request


class Upload:
    def __init__(self, uri, properties=None):
        # TODO: reconsider this scheme
        self.uri = uri
        self.properties = properties

    def __getitem__(self, key):
        return self.properties[key]

    def __str__(self):
        return json.dumps(self.properties, indent=2)

    def get(self):
        self.properties = make_request(
            method="GET",
            path=self.uri,
        )

    def upload_file(self, data):
        # TODO: resumable uploads
        make_request(method="PUT", path=self.uri, payload=data, parse_payload=False)
        return

    def list_variables(self):
        # TODO
        return
