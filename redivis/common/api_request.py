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


def make_request(*, method, path, query=None, payload=None, parse_payload=True):
    method = method.lower()
    url = f"{api_endpoint}{path}"

    headers = {"Authorization": f"Bearer {__get_access_token()}"}

    logging.debug(f"Making API '{method}' request to '{url}'")

    if parse_payload and payload:
        payload = json.dumps(payload)

    r = getattr(requests, method)(
        url, headers=headers, params=query, verify=verifySSL, data=payload
    )

    if r.text != "OK":
        json_response = r.json()
        if r.status_code >= 400:
            raise requests.HTTPError(json_response)
        return json_response

    else:
        r.raise_for_status()
        return

    # return __invert_case(json_response, __camel_to_snake)


def make_paginated_request(*, path, query=None, max_results=100):
    # TODO: proper pagination with max_results. How to use iterators?
    headers = {"Authorization": f"Bearer {__get_access_token()}"}
    url = f"{api_endpoint}{path}"
    logging.debug(f"Making paginated API request to '{url}'")
    requests.get(url, headers=headers, params={"maxResults": max_results}.update(query))


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
