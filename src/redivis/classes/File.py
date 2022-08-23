from tqdm.auto import tqdm
import os
from io import BytesIO
import pathlib
from .Base import Base
from ..common.api_request import make_request
from urllib.parse import quote as quote_uri


class File(Base):
    def __init__(
        self,
        id,
        *,
        table=None,
        properties={},
    ):
        self.id = id
        self.table = table
        self.uri = f"/rawFiles/{quote_uri(id, '')}"
        self.properties = {
            **{"kind": "rawFile", "id": id, "uri": self.uri},
            **properties
        }

    def get(self):
        res = make_request(method="HEAD", path=self.uri, parse_response=False)
        parse_headers(self, res)
        return self

    def download(self, path=None, *, overwrite=False, progress=True, on_progress=None):
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
                raise Exception(f"File already exists at '{file_name}'. Set parameter overwrite=True to overwrite existing files.")

            # Make sure output directory exists
            pathlib.Path(file_name).parent.mkdir(exist_ok=True, parents=True)

            with open(file_name, 'wb') as f:
                if progress:
                    pbar = tqdm(total=self.properties['size'], leave=False, unit='iB', unit_scale=True)
                for chunk in r.iter_content(chunk_size=1024*1024):
                    if progress:
                        pbar.update(len(chunk))
                    f.write(chunk)
                    if on_progress:
                        on_progress(len(chunk))

                if progress:
                    pbar.close()

        return file_name

    def read(self, *, as_text=False):
        r = make_request(method="GET", path=f'{self.uri}', parse_response=False)
        parse_headers(self, r)
        if as_text:
            return r.text
        else:
            return r.content

    def stream(self):
        r = make_request(method="GET", path=f'{self.uri}', parse_response=False, stream=True)
        parse_headers(self, r)
        return BytesIO(r.content)


def parse_headers(file, res):
    file.properties["name"] = res.headers['x-redivis-filename']
    file.properties["md5Hash"] = res.headers['digest'].replace("md5=", "")
    file.properties["contentType"] = res.headers['content-type']
    file.properties["size"] = int(res.headers['content-length'])