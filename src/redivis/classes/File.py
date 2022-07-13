import json
import os
from io import BytesIO
import pathlib
from ..common.api_request import make_request
from urllib.parse import quote as quote_uri

class File:
    def __init__(
        self,
        id,
        *,
        table=None,
        properties=None,
    ):
        self.id = id
        self.table = table
        self.uri = f"/rawFiles/{quote_uri(id, '')}"
        self.properties = properties or {"kind":"rawFile", "id":id, "uri": self.uri}

    def __getitem__(self, key):
        return (
            self.properties[key] if self.properties and key in self.properties else None
        )

    def __str__(self):
        return json.dumps(self.properties, indent=2)

    def __repr__(self):
        return str(self)

    def get(self):
        res = make_request(method="HEAD", path=self.uri, parse_response=False)
        parse_headers(self, res)
        return self

    def download(self, path=None, *, overwrite=False):
        is_dir = False
        if path is None:
            path = os.getcwd()
            is_dir = True
        elif path.endswith('/'):
            is_dir = True
        elif os.path.exists(path) and os.path.isdir(path):
            is_dir = True

        with make_request(method="GET", path=f'{self.uri}', stream=True, parse_response=False) as r:
            parse_headers(self, r)
            name = self.properties["name"]

            file_name = os.path.join(path, name) if is_dir else path

            if overwrite is False and os.path.exists(file_name):
                raise Exception(f"File already exists at '{file_name}'. Set parameter overwrite=False to overwrite existing files.")

            # Make sure output directory exists
            pathlib.Path(file_name).parent.mkdir(exist_ok=True, parents=True)

            with open(file_name, 'wb') as f:
                for chunk in r.iter_content(chunk_size=None):
                    f.write(chunk)

    def read(self, *, as_text=False):
        r = make_request(method="GET", path=f'{self.uri}', parse_response=False)
        if as_text:
            return r.text
        else:
            return r.content

    def stream(self):
        return BytesIO(make_request(method="GET", path=f'{self.uri}', parse_response=False, stream=True).content)


def parse_headers(file, res):
    file.properties["name"] = res.headers['x-redivis-filename']
    file.properties["md5Hash"] = res.headers['digest'].replace("md5=", "")
    file.properties["contentType"] = res.headers['content-type']
    file.properties["size"] = res.headers['content-length']