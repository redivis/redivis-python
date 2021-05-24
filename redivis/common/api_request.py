import requests
import logging
import os
import re
import json

api_endpoint = (
    "https://redivis.com/api/v1"
    if os.getenv("REDIVIS_API_ENDPOINT") is None
    else os.getenv("REDIVIS_API_ENDPOINT")
)

verifySSL = False if api_endpoint.find("https://localhost", 0) == 0 else True


def make_request(
    *, method, path, query=None, payload=None, parse_payload=True, parse_response=True
):

    method = method.lower()
    url = f"{api_endpoint}{path}"

    headers = {"Authorization": f"Bearer {__get_access_token()}"}

    logging.debug(f"Making API '{method}' request to '{url}'")

    if parse_payload and payload:
        payload = json.dumps(payload)

    r = getattr(requests, method)(
        url, headers=headers, params=query, verify=verifySSL, data=payload
    )

    if r.status_code >= 400:
        raise Exception(r.json()["error"])
    elif (
        parse_response and r.text != "OK"
    ):  # handles deletions, where there is no content
        return r.json()
        # return __invert_case(json_response, __camel_to_snake)
    else:
        return r.text


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


def __get_access_token():
    if os.getenv("REDIVIS_API_TOKEN") is None:
        raise EnvironmentError(
            "The environment variable REDIVIS_API_TOKEN must be set."
        )
    return os.environ["REDIVIS_API_TOKEN"]


def __camel_case_to_snake_case(json_response):

    return json_response


def __camel_to_snake(name):
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def __snake_to_camel(name):
    return re.sub("_([a-z])", lambda match: match.group(1).upper(), name)


def __invert_case(dict_obj, inverter):
    assert type(dict_obj) == dict
    converted_dict_obj = {}
    for name in dict_obj:
        new_name = inverter(name)
        value = dict_obj[name]

        if type(value) == dict:
            converted_dict_obj[new_name] = __invert_case(value, inverter)
        else:
            converted_dict_obj[new_name] = dict_obj[name]
    return converted_dict_obj
