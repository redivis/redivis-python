"""Repros for two listRows behaviors, against the dev environment.

1. GET /tables/:table/rows?format=arrow fails with "Cannot stream a single arrow response
   for tables with pending uploads" when the table's (unreleased) version has rows in more
   than one BigQuery table.
2. maxResults=0 responds with a plain-text "OK", rather than an empty result.

Run from the repo root:

    REDIVIS_API_ENDPOINT=https://local.host:8443/api/v1 \
    REDIVIS_CONFIG_DIR=~/.redivis/environments/local.host_8443 \
    PYTHONPATH=src python listrows_repro.py
"""

import time
import uuid

import redivis
from redivis.common.exceptions import RedivisError

OWNER = redivis.organization("demo")
CSV = b"id,name\n1,a\n2,b\n3,c\n"


def list_rows(table, **query):
    """Request listRows directly, and summarize the response (or error)."""
    try:
        response = redivis.make_api_request(
            path=f"{table.uri}/rows",
            query={"format": "arrow", **query},
            parse_response=False,
        )
    except RedivisError as error:
        return f"error: {error}"
    content_type = response.headers.get("Content-Type", "").split(";")[0]
    body = response.content
    return f"{response.status_code} {content_type}, {len(body)} bytes" + (
        f": {body!r}" if len(body) < 20 else ""
    )


def read_session(table, **payload):
    try:
        session = redivis.make_api_request(
            method="POST",
            path=f"{table.uri}/readSessions",
            payload={"requestedStreamCount": 2, "format": "arrow", **payload},
        )
    except RedivisError as error:
        return f"error: {error}"
    return f"numRows={session['numRows']}, {len(session['streams'])} stream(s)"


def report(label, result):
    print(f"  {label:<55} {result}")


def new_dataset():
    return OWNER.dataset(f"listrows_repro_{uuid.uuid4().hex[:8]}").create()


def upload(table, name, content=CSV):
    table.upload(name).create(content, progress=False)


def delete(dataset):
    reference = dataset.qualified_reference.rsplit(":", 1)[0]
    for _ in range(30):
        try:
            redivis.dataset(reference).delete()
            return
        except RedivisError as error:
            if "being released" not in str(error):
                raise
            time.sleep(2)


datasets = []
try:
    print("\n1. Unreleased tables whose rows span several BigQuery tables\n")

    dataset = new_dataset()
    datasets.append(dataset)

    one_upload = dataset.table("one_upload").create()
    upload(one_upload, "first.csv")
    report("one upload (works)", list_rows(one_upload))

    two_uploads = dataset.table("two_uploads").create()
    upload(two_uploads, "first.csv")
    upload(two_uploads, "second.csv")
    report("two uploads", list_rows(two_uploads))
    report(
        "two uploads, maxResults=3 (only reaches the first)",
        list_rows(two_uploads, maxResults=3),
    )
    report("two uploads, maxResults=4", list_rows(two_uploads, maxResults=4))
    report(
        "two uploads, format=csv (only arrow is refused)",
        list_rows(two_uploads, format="csv"),
    )
    report("two uploads, readSession (works)", read_session(two_uploads))

    # The previous version's rows count as another table, for the append strategy
    dataset.release()
    next_version = dataset.create_next_version()
    appended = next_version.table("one_upload")
    upload(appended, "second.csv")
    report("next version, append onto released rows", list_rows(appended))

    replaced = new_dataset()
    datasets.append(replaced)
    table = replaced.table("data").create(upload_merge_strategy="replace")
    upload(table, "first.csv")
    replaced.release()
    replaced_next = replaced.create_next_version().table("data")
    upload(replaced_next, "second.csv")
    report("next version, replace (works)", list_rows(replaced_next))

    print("\n2. Empty results: maxResults=0, or a table with no rows\n")

    # An unreleased version's tables, each with one upload
    unreleased = new_dataset()
    datasets.append(unreleased)
    with_rows = unreleased.table("with_rows").create()
    upload(with_rows, "data.csv")
    without_rows = unreleased.table("without_rows").create()
    upload(without_rows, "header_only.csv", b"id,name\n")
    report("unreleased, maxResults=0", list_rows(with_rows, maxResults=0))
    report(
        "unreleased, maxResults=0, format=csv",
        list_rows(with_rows, maxResults=0, format="csv"),
    )
    report(
        "unreleased, maxResults=0, readSession", read_session(with_rows, maxResults=0)
    )
    report("unreleased, no rows", list_rows(without_rows))
    report("unreleased, no rows, readSession", read_session(without_rows))
    report(
        "unreleased, maxResults=1 (for comparison)", list_rows(with_rows, maxResults=1)
    )

    # The same tables, once released. Reads of a table that's still being released go
    # through the unreleased path; the API doesn't say when its release finishes, but
    # a released table's read sessions always have a stream.
    unreleased.release()
    released = redivis.dataset(unreleased.qualified_reference)
    for _ in range(60):
        if read_session(released.table("with_rows"), maxResults=0).endswith(
            "1 stream(s)"
        ):
            break
        time.sleep(5)
    report(
        "released, maxResults=0", list_rows(released.table("with_rows"), maxResults=0)
    )
    report("released, no rows", list_rows(released.table("without_rows")))

    # An unreleased version appending to a released one: the previous version's rows
    # make for a (zero-row) table to read
    report("next version, append, maxResults=0", list_rows(appended, maxResults=0))
finally:
    for dataset in datasets:
        delete(dataset)
