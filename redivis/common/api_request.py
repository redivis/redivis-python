import requests
import logging
import os
import json
from .auth import get_auth_token
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


def make_request(
    *,
    method,
    path,
    query=None,
    payload=None,
    parse_payload=True,
    parse_response=True,
    stream=False,
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
    method = method.lower()
    url = f"{api_endpoint}{path}"

    headers = {"Authorization": f"Bearer {get_auth_token()}"}

    logging.debug(f"Making API '{method}' request to '{url}'")

    if parse_payload and payload:
        payload = json.dumps(payload)

    r = getattr(requests, method)(
        url,
        headers=headers,
        params=query,
        verify=verify_ssl,
        data=payload,
        stream=stream,
    )

    response_json = {}
    try:
        if r.status_code >= 400 or (parse_response and r.text != "OK"):
            response_json = r.json()
    except Exception:
        raise Exception(r.text)

    if r.status_code >= 400:
        raise Exception(response_json["error"])
    elif parse_response:
        return response_json
    else:
        return r


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
                    "maxResults": page_size
                    if max_results is None or (page + 1) * page_size < max_results
                    else max_results - page * page_size,
                },
            },
        )
        page += 1
        results += response["results"]
        next_page_token = response["nextPageToken"]
        if not next_page_token:
            break

    return results


def make_rows_request(*, uri, max_results, query={}):
    res = make_request(
        method="get",
        path=f"{uri}/rows",
        parse_response=False,
        stream=True,
        query={
            **query,
            **{"maxResults": max_results, "format": "csv"},
        },
    )

    return res


def __get_api_endpoint():
    return (
        "https://redivis.com/api/v1"
        if os.getenv("REDIVIS_API_ENDPOINT") is None
        else os.getenv("REDIVIS_API_ENDPOINT")
    )
