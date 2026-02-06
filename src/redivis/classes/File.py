import os
import io
import warnings
from base64 import b64decode
from pathlib import Path
import time
from requests import RequestException
from urllib3.exceptions import HTTPError
from .Base import Base
from ..common.api_request import make_request
from urllib.parse import quote as quote_uri

from ..common.retryable_download import perform_retryable_download


class File(Base):

    def __init__(
        self,
        id,
        name,
        *,
        directory,
        table=None,
        query=None,
        properties=None,
    ):
        if not table and not query:
            raise ValueError("All files must either belong to a table or query.")
        if properties is None:
            properties = {}

        self.id = id
        self.path = name if isinstance(name, Path) else Path(f"./{name}")
        self.name = self.path.name
        self.table = table
        self.query = query
        self.directory = directory
        self.uri = f"/rawFiles/{quote_uri(id, '')}"
        self.size = properties.get("size")
        self.hash = properties.get("md5_hash")
        if self.hash:
            self.hash = b64decode(self.hash)
        self.added_at = properties.get("added_at")
        self.properties = properties

    def get(self):
        warnings.warn(
            "This method is deprecated. Nothing to fetch; all file metadata already exists at file.properties",
            FutureWarning,
            stacklevel=2,
        )
        return self

    def __repr__(self) -> str:
        return f"<File {str(self.path)}>"

    def download(
        self,
        path=None,
        *,
        overwrite=False,
        progress=True,
        on_progress=None,
        cancel_event=None,
    ):
        if path:
            path = os.path.expanduser(path)

        is_dir = False
        if path is None:
            path = os.getcwd()
            is_dir = True
        elif path.endswith(os.sep):
            is_dir = True
        elif os.path.exists(path) and os.path.isdir(path):
            is_dir = True

        query_params = {}
        if self.table:
            query_params["tableId"] = self.table.properties.get("id")
        elif self.query:
            query_params["queryId"] = self.query.properties.get("id")

        return perform_retryable_download(
            path=self.uri,
            filename=os.path.join(path, self.name) if is_dir else path,
            size=self.size,
            md5_hash=self.hash,
            query=query_params,
            progress=progress,
            on_progress=on_progress,
            cancel_event=cancel_event,
            overwrite=overwrite,
        )

    def read(self, *, as_text=False, start_byte=0, end_byte=None):
        range_headers = {}
        if end_byte:
            range_headers["Range"] = f"bytes={int(start_byte)}-{int(end_byte)}"
        elif start_byte:
            range_headers["Range"] = f"bytes={int(start_byte)}-"

        r = make_request(
            method="GET",
            path=f"{self.uri}",
            parse_response=False,
            headers=range_headers,
        )
        if as_text:
            return r.text
        else:
            return r.content

    def stream(self, *, start_byte=0, end_byte=None):
        warnings.warn(
            "file.stream() has been deprecated, please use file.open() instead",
            FutureWarning,
            stacklevel=2,
        )
        return self.open(start_byte=start_byte, end_byte=end_byte)

    def open(self, *, start_byte=0, end_byte=None):
        if start_byte:
            start_byte = int(start_byte)
        if end_byte:
            end_byte = int(end_byte)

        stream = Stream(
            uri=self.uri, start_byte=start_byte, end_byte=end_byte, file=self
        )
        return stream


class Stream(io.BufferedIOBase):
    def __init__(self, uri, start_byte=0, end_byte=None, file=None):
        super().__init__()
        self.uri = uri
        self.retry_count = 0
        self.bytes_read = 0
        self.start_byte = start_byte
        self.end_byte = end_byte
        self._file = file
        self._total_bytes = None
        self._iter_chunk_size = 1024 * 1024
        self._closed = False
        self.response = None

    def _close_response(self):
        if self.response:
            self.response.raw.close()
            self.response = None

    def _raise_if_closed(self):
        if self._closed:
            raise OSError(5, "Stream closed")

    def _get_response(self, recreate=False):
        self._raise_if_closed()

        if not recreate and self.response:
            return self.response
        try:
            # If recreating, make sure the previous response is closed
            self._close_response()

            range_headers = {}
            start_byte = self.start_byte + self.bytes_read

            if self._total_bytes is not None and start_byte >= self._total_bytes:
                # Don't attempt to read past the last byte; this will raise an error from the Redivis API
                return None

            if self.end_byte:
                range_headers["Range"] = f"bytes={start_byte}-{self.end_byte}"
            elif start_byte:
                range_headers["Range"] = f"bytes={start_byte}-"

            r = make_request(
                method="GET",
                path=self.uri,
                parse_response=False,
                stream=True,
                headers=range_headers,
            )

            self._total_bytes = self._file.size
            self.retry_count = 0
            self.response = r
            return r
        except (RequestException, HTTPError) as e:
            if self.retry_count >= 10:
                print("File download failed after too many retries, giving up.")
                raise e
            time.sleep(self.retry_count)
            self.retry_count += 1
            return self._get_response(True)

    def read(self, size=-1):
        r = self._get_response()
        if not r:
            return b""
        try:
            chunk = self.response.raw.read(size)
            self.bytes_read += len(chunk)
            return chunk
        except (RequestException, HTTPError) as e:
            if self.retry_count >= 10:
                print("File download failed after too many retries, giving up.")
                raise e
            time.sleep(self.retry_count)
            self.retry_count += 1
            self._get_response(True)
            return self.read(size)

    def read1(self, size=-1):
        return self.read(size)

    def readall(self):
        return self.read()

    def readinto(self, buffer):
        r = self._get_response()
        if not r:
            return 0
        try:
            bytes_read = self.response.raw.readinto(buffer)
            self.bytes_read += bytes_read
            return bytes_read
        except (RequestException, HTTPError) as e:
            if self.retry_count >= 10:
                print("File download failed after too many retries, giving up.")
                raise e
            time.sleep(self.retry_count)
            self.retry_count += 1
            self._get_response(True)
            return self.readinto(buffer)

    def raw(self):
        return self.response.raw

    @property
    def closed(self):
        return self._closed

    def close(self):
        self._close_response()
        self._closed = True

    def readline(self, size=-1):
        self._get_response()
        try:
            line = self.response.raw.readline(size)
            self.bytes_read += len(line)
            return line
        except (RequestException, HTTPError) as e:
            if self.retry_count >= 10:
                print("File download failed after too many retries, giving up.")
                raise e
            time.sleep(self.retry_count)
            self.retry_count += 1
            self.response = self._get_response(True)
            return self.readline(size)

    def readlines(self, hint=-1):
        self._get_response()
        if hint is None:
            hint = -1
        lines = []
        while (hint == -1 or self.bytes_read < hint) and not self._closed:
            # Try/catch handled in self.readline()
            line = self.readline()
            if line:
                lines.append(self.readline())
            else:
                break
        return lines

    def seek(self, offset, whence=os.SEEK_SET):
        self._raise_if_closed()
        current_bytes_read = self.bytes_read
        if whence == os.SEEK_SET:
            self.bytes_read = offset
        elif whence == os.SEEK_CUR:
            self.bytes_read += offset
        elif whence == os.SEEK_END:
            if self._total_bytes is None:
                self._get_response()
            self.bytes_read = self._total_bytes + offset

        if current_bytes_read != self.bytes_read:
            self._close_response()

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.bytes_read

    def writable(self):
        return False

    def __iter__(self):
        return self

    def __next__(self):
        chunk = self.read(self._iter_chunk_size)
        if chunk == b"":
            self._closed = True
            self.response.close()
            raise StopIteration
        return chunk
