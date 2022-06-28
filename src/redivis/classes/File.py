import json
from ..common.api_request import make_request
from urllib.parse import quote as quote_uri

class File:
    def __init__(
        self,
        id,
        *,
        properties=None,
    ):
        self.id = id
        self.uri = f"/rawFiles/{quote_uri(id, '')}"
        self.properties = properties

    def __getitem__(self, key):
        return (
            self.properties[key] if self.properties and key in self.properties else None
        )

    def __str__(self):
        return json.dumps(self.properties, indent=2)

    def get(self):
        self.properties = make_request(method="GET", path=self.uri)
        self.uri = self.properties["uri"]
        return self

    def exists(self):
        try:
            make_request(method="GET", path=self.uri)
            return True
        except Exception as err:
            if err.args[0]["status"] != 404:
                raise err
            return False

    def download(self, path=None, *, stream=False):
        if path is not None:
            with make_request(method="GET", path=f'{self.uri}/download', stream=True, parse_response=False) as r:
                with open(path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
        else:
            return make_request(method="GET", path=f'{self.uri}/download', parse_response=False, stream=stream)
