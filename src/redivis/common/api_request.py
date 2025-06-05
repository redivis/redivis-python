import requests
import logging
import os
import json
import platform
import warnings
from urllib.parse import unquote
import time

from .auth import get_auth_token, refresh_credentials
from .._version import __version__


def make_request(
    *,
    method="GET",
    path="",
    query=None,
    payload=None,
    parse_payload=True,
    parse_response=True,
    stream=False,
    files=None,
    headers={},
    retry_count=0,
):
    original_parameters = locals().copy()
    args = get_request_args(
        method=method,
        path=path,
        query=query,
        payload=payload,
        parse_payload=parse_payload,
        stream=stream,
        files=files,
    )

    logging.debug(f"Making API '{method}' request to '{args['url']}'")
    r = requests.request(**args)

    return process_request_response(r, parse_response, method, original_parameters)


def make_paginated_request(
    *, path, query={}, page_size=100, max_results=None, parse_response=True
):
    logging.debug(f"Making paginated API request to '{path}'")

    page = 0
    results = []
    next_page_token = None

    while True:
        if max_results is not None and len(results) >= max_results:
            break

        response = make_request(
            method="get",
            path=path,
            parse_response=True,
            query={
                **query,
                **{
                    "pageToken": next_page_token,
                    "maxResults": (
                        page_size
                        if max_results is None or (page + 1) * page_size < max_results
                        else max_results - page * page_size
                    ),
                },
            },
        )
        page += 1
        results += response["results"]
        next_page_token = response["nextPageToken"]
        if not next_page_token:
            break

    return results


def get_request_args(
    method,
    path,
    query=None,
    payload=None,
    parse_payload=True,
    stream=False,
    files=None,
    headers={},
):
    api_endpoint = __get_api_endpoint()
    verify_ssl = (
        False
        if api_endpoint.find("https://localhost", 0) == 0
        or os.getenv("REDIVIS_ENV") == "development"
        or os.getenv("REDIVIS_ENV") == "test"
        or os.getenv("REDIVIS_ENV") == "staging"
        else True
    )
    url = f"{api_endpoint}{path}"

    method = method.upper()
    headers = {
        **{
            "Authorization": f"Bearer {get_auth_token()}",
            "User-Agent": f"redivis-python/{__version__} ({platform.platform()}; Python/{platform.python_version()})",
        },
        **headers,
    }

    if parse_payload and payload:
        payload = json.dumps(payload)
        headers["Content-Type"] = "application/json"

    return {
        "method": method.upper(),
        "url": url,
        "headers": headers,
        "params": query,
        "verify": verify_ssl,
        "data": payload,
        "stream": stream,
        "files": files,
        "timeout": 125,
    }


previously_printed_warnings = {}


def process_request_response(
    r, parse_response=True, method=None, original_parameters=None
):
    method = method.lower()
    response_json = {}
    try:
        # Retry with exponential backoff on service unavailable
        if r.status_code == 503 and original_parameters["retry_count"] < 10:
            logging.debug("API is currently unavailable, retrying...")
            time.sleep(original_parameters["retry_count"])
            original_parameters["retry_count"] += 1
            return make_request(**original_parameters)

        if r.status_code >= 400 or (
            method != "head" and parse_response and r.text != "OK"
        ):
            if method == "head":
                if "X-REDIVIS-ERROR-PAYLOAD" in r.headers:
                    response_json = json.loads(
                        unquote(r.headers["X-REDIVIS-ERROR-PAYLOAD"])
                    )
                else:
                    # This should never happen
                    response_json = {"error": "unknown_error", "status": r.status_code}
            else:
                response_json = r.json()

        if (
            (
                r.status_code == 401
                or (
                    r.status_code == 403
                    and response_json["error"] == "insufficient_scope"
                )
            )
            and os.getenv("REDIVIS_API_TOKEN") is None
            and os.getenv("REDIVIS_NOTEBOOK_JOB_ID") is None
        ):
            warnings.warn(
                f"{response_json['error']}: {response_json['error_description']}"
            )
            refresh_credentials(
                scope=(
                    response_json["scope"].split(" ")
                    if "scope" in response_json
                    else None
                ),
                amr_values=(
                    response_json["amr_values"]
                    if "amr_values" in response_json
                    else None
                ),
            )
            return make_request(**original_parameters)
    except Exception:
        if method == "head":
            raise Exception(unquote(r.headers["X-REDIVIS-ERROR-PAYLOAD"]))
        else:
            raise Exception(r.text)

    if "X-REDIVIS-WARNING" in r.headers:
        global previously_printed_warnings
        if r.headers["X-REDIVIS-WARNING"] not in previously_printed_warnings:
            warnings.warn(r.headers["X-REDIVIS-WARNING"])
            previously_printed_warnings[r.headers["X-REDIVIS-WARNING"]] = True

    if r.status_code >= 400:
        raise Exception(response_json)
    elif parse_response:
        return response_json
    else:
        return r


def __get_api_endpoint():
    return (
        "https://redivis.com/api/v1"
        if os.getenv("REDIVIS_API_ENDPOINT") is None
        else os.getenv("REDIVIS_API_ENDPOINT")
    )
