"""Imports: how uploads (Upload.create() and Table.add_files()) get data into a table.

An import is created on a table, its files are uploaded to it, and it's finalized, after
which the API processes it as a job. Small files are uploaded in a single request; larger
ones through a resumable upload session, in chunks, so that an interrupted upload resumes
from what was received rather than starting over. Every file is verified by its md5.
"""

import base64
import hashlib
import io
import logging
import os
import pathlib
import threading
import time
import uuid

import niquests

from . import exceptions
from .api_request import make_request
from .auth import get_verify_ssl
from .config import get_config
from .retryable_upload import get_committed_bytes
from .util import raise_api_error

# Files up to this size are uploaded in a single request; larger ones through an upload
# session. These are module attributes, read when used, so that tests can lower them.
DIRECT_UPLOAD_MAX_SIZE = 2**25  # 32MiB
# Upload session chunks start at this size, and adapt to how long they take to send,
# staying a power of two (and so a multiple of the 256KiB that every chunk but the last
# must be) between the min and max sizes
INITIAL_CHUNK_SIZE = 2**23  # 8MiB
MIN_CHUNK_SIZE = 2**20  # 1MiB
MAX_CHUNK_SIZE = 2**27  # 128MiB
# A chunk that takes longer than this halves the next one; one that takes less than
# half of it doubles the next one
TARGET_CHUNK_SECONDS = 8
# Upload session failures in a row, without any progress, before giving up
MAX_CONSECUTIVE_FAILURES = 10
MD5_READ_SIZE = 2**23  # 8MiB

POLL_INITIAL_INTERVAL = 2
POLL_BACKOFF = 1.5
POLL_MAX_INTERVAL = 30

TERMINAL_STATUSES = ("completed", "failed", "cancelled")
RETRYABLE_STATUSES = (408, 429, 500, 502, 503, 504)
# An upload session that was cancelled, or has expired, answers with one of these
DEAD_SESSION_STATUSES = (400, 404, 410, 499)


def create_import(table, payload):
    """Creates an import on the table. Keys of `payload` whose value is None are omitted."""
    # Imports are writes, which must go through the table's own URI rather than a
    # workflow-scoped one
    from ..classes.Table import resolve_table_for_write

    resolve_table_for_write(table)
    return make_request(
        method="POST",
        path=f"{table.uri}/imports",
        payload={
            # Lets the API recognize a retried request, so it never creates two imports
            "idempotencyKey": str(uuid.uuid4()),
            **{key: value for key, value in payload.items() if value is not None},
        },
        idempotent=True,
    )


def upload_import_file(import_, name, source, size, md5, progress=None):
    """Uploads a file to an import.

    `source` is a file path, bytes, or a seekable binary file object positioned at the
    start of the file's `size` bytes. `md5` is its base64-encoded md5, from
    compute_md5_base64(). `progress`, if given, is called with the change in the number
    of bytes uploaded, which may be negative after a failed request.
    """
    file, start, should_close = _open_source(source)
    tracker = _ProgressTracker(progress)
    try:
        path = f"{import_['uri']}/files/{encode_import_file_name(name)}"
        if size <= DIRECT_UPLOAD_MAX_SIZE:
            make_request(
                method="POST",
                path=path,
                payload=_FileSlice(file, start, 0, size, tracker),
                parse_payload=False,
                parse_response=False,
                headers={
                    "Content-Length": str(size),
                    "Content-MD5": md5,
                    "Content-Type": "application/octet-stream",
                },
                idempotent=True,
            )
        else:
            _upload_in_chunks(
                open_session=lambda: make_request(
                    method="POST",
                    path=f"{path}/uploadSession",
                    payload={"size": size, "md5Hash": md5},
                    idempotent=True,
                )["uri"],
                file=file,
                start=start,
                size=size,
                tracker=tracker,
            )
        tracker.set(size)
    finally:
        if should_close:
            file.close()


def finalize_import(import_):
    return make_request(
        method="POST", path=f"{import_['uri']}/finalize", idempotent=True
    )


def wait_for_import(import_, raise_on_fail=True):
    """Waits for an import to finish, returning its final properties.

    Raises a JobError if it failed or was cancelled, unless `raise_on_fail` is False.
    """
    interval = POLL_INITIAL_INTERVAL
    while import_.get("status") not in TERMINAL_STATUSES:
        time.sleep(interval)
        interval = min(interval * POLL_BACKOFF, POLL_MAX_INTERVAL)
        import_ = make_request(method="GET", path=import_["uri"], idempotent=True)
        logging.debug(f"Import is {import_.get('status')}...")

    if import_["status"] != "completed" and raise_on_fail:
        raise exceptions.JobError(
            message=import_.get("errorMessage"),
            status=import_["status"],
            kind=import_.get("kind"),
        )
    return import_


def encode_import_file_name(name):
    """The file name as unpadded base64url, as the API expects it in paths."""
    return base64.urlsafe_b64encode(name.encode("utf-8")).decode("ascii").rstrip("=")


def compute_md5_base64(source):
    """The base64-encoded md5 of a source (see upload_import_file()), read in pieces."""
    file, start, should_close = _open_source(source)
    try:
        md5 = hashlib.md5()
        file.seek(start)
        while True:
            piece = file.read(MD5_READ_SIZE)
            if not piece:
                break
            md5.update(piece)
        file.seek(start)
        return base64.b64encode(md5.digest()).decode("ascii")
    finally:
        if should_close:
            file.close()


def _open_source(source):
    """Returns (file object, start position, whether to close it)."""
    if isinstance(source, (str, pathlib.PurePath)):
        return open(source, "rb"), 0, True
    if isinstance(source, (bytes, bytearray, memoryview)):
        return io.BytesIO(source), 0, True
    return source, source.tell(), False


def _upload_in_chunks(*, open_session, file, start, size, tracker):
    api_endpoint = get_config("api_endpoint")
    session_uri = open_session()
    chunk_size = INITIAL_CHUNK_SIZE
    # How much of the file the session has, or None if it has to be asked
    offset = None
    failures = 0
    most_committed = 0

    while True:
        url = f"{api_endpoint}{session_uri}"
        response = status = error = None
        sent_at = time.monotonic()
        try:
            # NB: the upload session is its own authorization, as a signed URL is, so
            # these requests are never sent credentials
            if offset is None:
                # Ask what the session has received, e.g. after a failed chunk
                response = niquests.put(
                    url=url,
                    verify=get_verify_ssl(),
                    headers={"Content-Length": "0", "Content-Range": f"bytes */{size}"},
                )
            else:
                end = min(offset + chunk_size, size)
                response = niquests.put(
                    url=url,
                    verify=get_verify_ssl(),
                    headers={
                        "Content-Length": str(end - offset),
                        "Content-Range": f"bytes {offset}-{end - 1}/{size}",
                    },
                    data=_FileSlice(file, start, offset, end - offset, tracker),
                )
            status = response.status_code
        except niquests.RequestException as e:
            error = e

        if status in (200, 201):
            return

        if status == 308:
            committed = get_committed_bytes(response)
            if offset is not None:
                # Adapt the size of the chunks to how long they take to send
                elapsed = time.monotonic() - sent_at
                if elapsed > TARGET_CHUNK_SECONDS:
                    chunk_size = max(MIN_CHUNK_SIZE, chunk_size // 2)
                elif elapsed < TARGET_CHUNK_SECONDS / 2:
                    chunk_size = min(MAX_CHUNK_SIZE, chunk_size * 2)
            if committed > most_committed:
                most_committed = committed
                failures = 0
            tracker.set(committed)
            if committed < size:
                offset = committed
                continue
            # The session has every byte, but hasn't said it's complete, so ask again
            failures += 1
        elif status in DEAD_SESSION_STATUSES:
            # The session was cancelled or has expired. Asking for it again opens a new
            # one, unless the import itself is no longer accepting files
            new_session_uri = open_session()
            if new_session_uri == session_uri:
                raise exceptions.RedivisError(
                    "The upload failed because its import is no longer accepting files: it "
                    "may have been cancelled, or expired. Imports must be finalized within "
                    "20 hours of being created."
                )
            logging.debug("Upload session expired, resuming in a new one")
            session_uri = new_session_uri
            offset = None
            failures += 1
            if failures <= MAX_CONSECUTIVE_FAILURES:
                continue
        elif status is not None and status not in RETRYABLE_STATUSES:
            raise_api_error(response_text=response.text, response=response)
        else:
            failures += 1

        if failures > MAX_CONSECUTIVE_FAILURES:
            raise exceptions.NetworkError(
                message=f"Upload failed after {MAX_CONSECUTIVE_FAILURES} retries",
                original_exception=error,
            )
        logging.debug(f"Upload chunk failed ({status or error}), retrying")
        time.sleep(min(failures, 30))
        chunk_size = max(MIN_CHUNK_SIZE, chunk_size // 2)
        offset = None
        tracker.set(most_committed)


class _ProgressTracker:
    """Reports a file's upload progress, as changes to a callback (e.g. tqdm.update)."""

    def __init__(self, callback):
        self.callback = callback
        self.reported = 0
        self.lock = threading.Lock()

    def set(self, uploaded):
        if self.callback is None:
            return
        with self.lock:
            delta = uploaded - self.reported
            self.reported = uploaded
        if delta:
            self.callback(delta)


class _FileSlice(io.RawIOBase):
    """`length` bytes of a file, from `offset`, as a request body.

    The file starts at `start` in the file object. Reports progress as it's read, and is
    rewound (by seek(0)) to be sent again.
    """

    def __init__(self, file, start, offset, length, tracker):
        super().__init__()
        self.file = file
        self.start = start
        self.offset = offset
        self.length = length
        self.tracker = tracker
        self.position = 0

    def __len__(self):
        return self.length - self.position

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_CUR:
            offset += self.position
        elif whence == os.SEEK_END:
            offset += self.length
        self.position = max(0, min(offset, self.length))
        return self.position

    def read(self, size=-1):
        remaining = self.length - self.position
        if size is None or size < 0 or size > remaining:
            size = remaining
        if size == 0:
            return b""
        self.file.seek(self.start + self.offset + self.position)
        data = self.file.read(size)
        self.position += len(data)
        self.tracker.set(self.offset + self.position)
        return data

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)
