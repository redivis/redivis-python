import requests
import logging
import os
import json
import platform
from urllib.parse import unquote

from .auth import get_auth_token, refresh_credentials
from .._version import __version__


def make_request(
    *,
    method,
    path,
    query=None,
    payload=None,
    parse_payload=True,
    parse_response=True,
    stream=False,
    files=None,
    headers={},
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
            "X-Redivis-Client": "redivis-python",
            "X-Redivis-Client-Version": __version__,
            "X-Redivis-Client-Python-Version": platform.python_version(),
            "X-Redivis-Client-System": platform.system(),
            "X-Redivis-Client-System-Version": platform.release(),
            "User-Agent": f"redivis-python/{__version__}",
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


def process_request_response(
    r, parse_response=True, method=None, original_parameters=None
):
    method = method.lower()
    response_json = {}
    try:
        if (
            r.status_code == 401
            and os.getenv("REDIVIS_API_TOKEN") is None
            and os.getenv("REDIVIS_NOTEBOOK_JOB_ID") is None
        ):
            refresh_credentials()
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
                    response_json = {"error": {"status": r.status_code}}
            else:
                response_json = r.json()
    except Exception:
        if method == "head":
            raise Exception(unquote(r.headers["X-REDIVIS-ERROR-PAYLOAD"]))
        else:
            raise Exception(r.text)

    if r.status_code >= 400:
        raise Exception(response_json["error"])
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
