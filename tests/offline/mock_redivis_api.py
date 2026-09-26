"""A minimal in-process stand-in for the Redivis API.

It implements just enough of the endpoints to exercise the client's read paths
(listRows, read sessions, exports, raw files) and its retry / authentication handling,
with knobs in STATE for injecting failures. Tests get a fresh STATE per test via
the `api` fixture in conftest.py.
"""

import base64
import io
import json
import re
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pyarrow
import pyarrow.parquet

TABLE_PATH = "/api/v1/tables/a.b.mock"
DEFAULT_NUM_ROWS = 500
BATCH_ROWS = 100
VARIABLES = [
    {"name": "id", "type": "integer"},
    {"name": "name", "type": "string"},
    {"name": "val", "type": "float"},
]


def jwt(scope):
    """An access token whose payload carries `scope`, as the client reads it."""
    payload = base64.urlsafe_b64encode(json.dumps({"scope": scope}).encode())
    return f"header.{payload.decode().rstrip('=')}.signature"


# Cached credentials for the client to send, holding the default scope so the
# client never tries to upgrade them
FAKE_CREDENTIALS = {
    "access_token": jwt("data.edit organization.write workflow.write"),
    "expires_at": 9e12,
    "refresh_token": "refresh",
}

STATE = {}


def reset():
    STATE.clear()
    STATE.update(
        {
            # The table's contents and metadata
            "num_rows": DEFAULT_NUM_ROWS,
            "num_bytes": 1000,
            "container_kind": "table",
            "upload_status": "completed",
            "query_bytes": 1000,
            # Reject unauthenticated requests, other than listRows, which (like
            # the real API) can be read anonymously
            "require_auth": False,
            # Return 503 for the next N requests
            "fail_503": 0,
            # Queued responses for the next requests, as (status, body). A status
            # of None lets that request through to the normal handler.
            "response_script": [],
            # The next listRows response stops after this many rows
            "truncate_rows_once": 0,
            # The next listRows request drops the connection without responding
            "fail_rows_once": False,
            # Raw file contents, by file id
            "raw_files": {},
            # The Range header of each rawFiles request (None if absent)
            "raw_file_ranges": [],
            # The next rawFiles response drops the connection after this many bytes
            "drop_raw_file_after": 0,
            # rawFiles responses ignore the Range header, sending the whole file
            "ignore_raw_file_range": False,
            # Everything received, as (method, path, query, had_authorization)
            "requests": [],
            "post_body_lengths": [],
        }
    )


reset()


def paths(*fragments):
    """The request paths received, optionally filtered to those containing a fragment."""
    return [
        path
        for _, path, _, _ in STATE["requests"]
        if not fragments or any(fragment in path for fragment in fragments)
    ]


def schema_for(names):
    types = {"id": pyarrow.int64(), "name": pyarrow.string(), "val": pyarrow.float64()}
    return pyarrow.schema([(name, types[name]) for name in names])


def columns_for(names, start, count):
    ids = range(start, start + count)
    values = {
        "id": pyarrow.array(ids, pyarrow.int64()),
        "name": pyarrow.array([f"row-{i}" for i in ids], pyarrow.string()),
        "val": pyarrow.array([i / 2 for i in ids], pyarrow.float64()),
    }
    return [values[name] for name in names]


def arrow_stream(names, start, count, *, sentinel):
    sink = io.BytesIO()
    schema = schema_for(names)
    with pyarrow.ipc.new_stream(sink, schema) as writer:
        for batch_start in range(start, start + count, BATCH_ROWS):
            batch_count = min(BATCH_ROWS, start + count - batch_start)
            writer.write_batch(
                pyarrow.record_batch(
                    columns_for(names, batch_start, batch_count), schema=schema
                )
            )
        if sentinel:
            # The empty end-of-stream batch that readStreams emit with eosSentinel=true
            writer.write_batch(
                pyarrow.record_batch(columns_for(names, 0, 0), schema=schema)
            )
    return sink.getvalue()


def parquet_file(count):
    names = [v["name"] for v in VARIABLES]
    sink = io.BytesIO()
    pyarrow.parquet.write_table(
        pyarrow.table(columns_for(names, 0, count), schema=schema_for(names)), sink
    )
    return sink.getvalue()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        self._handle("GET")

    def do_HEAD(self):
        self._handle("HEAD")

    def do_POST(self):
        self._handle("POST")

    def do_PATCH(self):
        self._handle("PATCH")

    def _send(self, body, *, status=200, content_type="application/json"):
        if not isinstance(body, bytes):
            body = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _send_arrow(self, body):
        self._send(body, content_type="application/vnd.apache.arrow.stream")

    def _send_raw_file(self, file_id):
        data = STATE["raw_files"][file_id]
        range_header = self.headers.get("Range")
        STATE["raw_file_ranges"].append(range_header)

        if STATE["ignore_raw_file_range"]:
            range_header = None
        start, end = 0, len(data) - 1
        if range_header:
            match = re.fullmatch(r"bytes=(\d+)-(\d*)", range_header)
            start = int(match[1])
            end = min(int(match[2]), end) if match[2] else end
        body = data[start : end + 1]

        self.send_response(206 if range_header else 200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        if range_header:
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        self.end_headers()

        if STATE["drop_raw_file_after"]:
            body = body[: STATE["drop_raw_file_after"]]
            STATE["drop_raw_file_after"] = 0
            self.close_connection = True
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # The client stopped reading partway, as streamed reads do when abandoned
            self.close_connection = True

    def _intercepted(self, method, path, query):
        """Apply any injected failures, returning True if a response was sent."""
        STATE["requests"].append((method, path, query, "Authorization" in self.headers))

        if STATE["fail_503"]:
            STATE["fail_503"] -= 1
            self._send(
                {"status": 503, "error": "unavailable", "error_description": "busy"},
                status=503,
            )
            return True

        if STATE["response_script"]:
            status, body = STATE["response_script"].pop(0)
            if status is not None:
                self._send(body, status=status)
                return True
            return False

        if (
            STATE["require_auth"]
            and "Authorization" not in self.headers
            and not path.endswith("/rows")
        ):
            self._send(
                {
                    "status": 401,
                    "error": "invalid_token",
                    "error_description": "No credentials were provided.",
                },
                status=401,
            )
            return True

        return False

    def _handle(self, method):
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = {k: v[0] for k, v in urllib.parse.parse_qs(parsed.query).items()}
        if method in ("POST", "PATCH"):
            STATE["post_body_lengths"].append(len(body))

        if self._intercepted(method, path, query):
            return

        num_rows = STATE["num_rows"]
        all_names = [v["name"] for v in VARIABLES]
        query_properties = {
            "id": "q1",
            "kind": "query",
            "uri": "/queries/q1",
            "status": "completed",
            "outputNumBytes": STATE["query_bytes"],
            "outputNumRows": num_rows,
            "finishedAt": 1758800000123,
        }

        if method == "POST":
            if path == "/api/v1/queries":
                return self._send(query_properties)
            if path.endswith("/readSessions"):
                max_results = json.loads(body or "{}").get("maxResults")
                if max_results is not None:
                    num_rows = min(num_rows, max_results)
                STATE["session_rows"] = num_rows
                half = num_rows // 2
                return self._send(
                    {
                        "streams": [
                            {"id": "s0", "estimatedRows": half},
                            {"id": "s1", "estimatedRows": num_rows - half},
                        ],
                        "numRows": num_rows,
                    }
                )
            if path.endswith("/exports"):
                return self._send(
                    {
                        "id": "e1",
                        "uri": "/exports/e1",
                        "status": "completed",
                        "fileCount": 1,
                        "format": "parquet",
                        "size": len(parquet_file(num_rows)),
                    }
                )
            return self._send({"ok": True, "bytes": len(body)})

        if method == "PATCH":
            return self._send({"ok": True})

        if path.endswith("/variables"):
            return self._send({"results": VARIABLES, "nextPageToken": None})

        if path.startswith("/api/v1/rawFiles/"):
            return self._send_raw_file(urllib.parse.unquote(path.rsplit("/", 1)[1]))

        if path.endswith("/rows"):
            if STATE["fail_rows_once"]:
                STATE["fail_rows_once"] = False
                self.close_connection = True
                return
            names = (
                query["selectedVariables"].split(",")
                if "selectedVariables" in query
                else all_names
            )
            count = min(int(query.get("maxResults", num_rows)), num_rows)
            if STATE["truncate_rows_once"]:
                count = min(count, STATE["truncate_rows_once"])
                STATE["truncate_rows_once"] = 0
            return self._send_arrow(arrow_stream(names, 0, count, sentinel=False))

        if path.startswith("/api/v1/readStreams/"):
            num_rows = STATE.get("session_rows", num_rows)
            half = num_rows // 2
            start, size = (0, half) if path.endswith("/s0") else (half, num_rows - half)
            offset = int(query.get("offset", 0))
            return self._send_arrow(
                arrow_stream(
                    all_names,
                    start + offset,
                    size - offset,
                    sentinel=query.get("eosSentinel") == "true",
                )
            )

        if path == "/api/v1/exports/e1/download":
            return self._send(
                parquet_file(num_rows), content_type="application/octet-stream"
            )

        if path == "/api/v1/queries/q1":
            return self._send(query_properties)

        if "/uploads/" in path:
            completed = STATE["upload_status"] == "completed"
            return self._send(
                {
                    "uri": "/uploads/u1",
                    "name": "My Upload",
                    "status": STATE["upload_status"],
                    "numRows": num_rows,
                    "numBytes": STATE["num_bytes"] if completed else None,
                }
            )

        if path == TABLE_PATH:
            return self._send(
                {
                    "kind": "table",
                    "name": "mock",
                    "qualifiedReference": "a.b.mock",
                    "scopedReference": "mock",
                    "uri": "/tables/a.b.mock",
                    "numRows": num_rows,
                    "numBytes": STATE["num_bytes"],
                    "container": {"kind": STATE["container_kind"]},
                }
            )

        return self._send({"status": 404, "error": "not_found"}, status=404)


def start():
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}/api/v1"
