import requests
import logging
import os

api_endpoint = (
    "https://redivis.com/api/v1"
    if os.getenv("REDIVIS_API_ENDPOINT") is None
    else os.getenv("REDIVIS_API_ENDPOINT")
)


def make_request(*, method="get", path, query={}):
    method = method.lower()
    url = f"{api_endpoint}{path}"

    headers = {"Authorization": f"Bearer {__get_access_token()}"}

    logging.debug(f"Making API '{method}' request to '{url}'")

    r = requests.get(url, headers=headers, params=query)

    json_response = r.json()

    if r.status_code >= 400:
        raise requests.HTTPError(json_response)

    return __camel_to_snake(json_response)


def make_paginated_request(method, path, identifier, query, max_results):
    print()


def __get_access_token():
    if os.getenv("REDIVIS_API_TOKEN") is None:
        raise EnvironmentError(
            "The environment variable REDIVIS_API_TOKEN must be set."
        )
    return os.environ["REDIVIS_API_TOKEN"]


def __camel_to_snake(json_response):
    return json_response
