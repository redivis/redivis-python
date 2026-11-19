import niquests
import logging
import os
import json
import platform
import warnings
from email.utils import parsedate_to_datetime
from urllib.parse import unquote
import time

from .auth import get_auth_token, has_credentials, refresh_credentials
from .._version import __version__
from .util import raise_api_error
from .config import get_config


def make_request(
    *,
    method="GET",
    path="",
    query=None,
    payload=None,
    parse_payload=True,
    parse_response=True,
    stream=False,
    headers=None,
    idempotent=False,
    retry_count=0,
    auth_failures=(),
):
    """Makes a request to the API, returning its parsed response.

    A `payload` is sent as JSON, unless `parse_payload` is False, in which case it's
    sent as is: e.g. bytes, or a seekable file-like object, which is rewound (with
    seek(0)) to be sent again on a retry.

    Pass `idempotent=True` for a request that's safe to repeat whatever its method
    (e.g., because it carries an idempotency key), to retry it after connection
    failures and transient errors (408, 429, and 5xx statuses), as GET and HEAD
    requests are after connection failures.
    """
    if headers is None:
        headers = {}

    original_parameters = locals().copy()
    args = get_request_args(
        method=method,
        path=path,
        query=query,
        payload=payload,
        parse_payload=parse_payload,
        stream=stream,
        headers=headers,
    )

    logging.debug(f"Making API '{method}' request to '{args['url']}'")
    try:
        r = niquests.request(**args)
    except (niquests.ConnectionError, niquests.Timeout):
        # Retry a request that failed to connect (or timed out) with backoff. Only
        # idempotent requests are safe to repeat, since we can't know whether the server
        # received the original.
        if (
            (idempotent or method.upper() in ("GET", "HEAD"))
            and retry_count < 10
            and __rewind_request_body(original_parameters)
        ):
            logging.debug("Request failed to connect, retrying...")
            time.sleep(retry_count)
            original_parameters["retry_count"] = retry_count + 1
            return make_request(**original_parameters)
        raise

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
    headers={},
):
    api_endpoint = __get_api_endpoint()
    url = f"{api_endpoint}{path}"

    method = method.upper()
    headers = {
        **{"User-Agent": __get_user_agent()},
        **headers,
    }

    # Only send a token we can obtain without logging in, of whatever scope;
    # otherwise send the request anonymously, since the resource may be publicly
    # accessible. If it isn't, or needs more scope, the server says so, and
    # process_request_response() authenticates and retries.
    if "Authorization" not in headers and has_credentials():
        auth_token = get_auth_token(scope=[], interactive=False)
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

    if parse_payload and payload:
        payload = json.dumps(payload)
        headers["Content-Type"] = "application/json"

    return {
        "method": method.upper(),
        "url": url,
        "headers": headers,
        "params": query,
        "data": payload,
        "stream": stream,
        "timeout": 125,
    }


previously_printed_warnings = {}

# A backstop on authentication retries, in case the server's failures keep
# changing without ever succeeding. See process_request_response()
MAX_AUTH_ATTEMPTS = 5

# The statuses that idempotent requests are retried after. Other requests are only
# retried after a 503
RETRYABLE_STATUSES = (408, 429, 500, 502, 503, 504)
# The longest a Retry-After header is waited for
MAX_RETRY_AFTER = 60


def process_request_response(
    r, parse_response=True, method=None, original_parameters=None
):
    method = method.lower()
    response_json = {}

    # Retry with backoff on service unavailable, or, for idempotent requests, any
    # transient error
    idempotent = original_parameters["idempotent"]
    if (
        (r.status_code == 503 or (idempotent and r.status_code in RETRYABLE_STATUSES))
        and original_parameters["retry_count"] < 10
        and __rewind_request_body(original_parameters)
    ):
        logging.debug(f"API responded with {r.status_code}, retrying...")
        retry_after = __get_retry_after(r) if idempotent else None
        # Release the connection now; otherwise a streamed response holds its
        # socket open for as long as the retries take.
        r.close()
        time.sleep(
            original_parameters["retry_count"] if retry_after is None else retry_after
        )
        original_parameters["retry_count"] += 1
        return make_request(**original_parameters)

    # NB: only the response parsing belongs in this try. Anything else in here
    # (notably the authentication retry below) would have its errors swallowed
    # and reported as an unrelated API error.
    if r.status_code >= 400 or (method != "head" and parse_response and r.text != "OK"):
        try:
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
        except Exception:
            if method == "head":
                error_payload = r.headers.get("X-REDIVIS-ERROR-PAYLOAD")
                response_text = (
                    unquote(error_payload) if error_payload is not None else r.text
                )
                raise_api_error(response_text=response_text, response=r)

            else:
                raise_api_error(response_text=r.text, response=r)

    is_auth_failure = r.status_code == 401 or (
        r.status_code == 403 and response_json.get("error") == "insufficient_scope"
    )
    # Authenticating is only worth retrying while it makes progress, i.e. each
    # attempt fails differently than the ones before it: an anonymous request
    # that's rejected, then a scope upgrade after logging in, and so on. Once a
    # failure repeats, re-authenticating can't help, and retrying anyway would
    # loop forever rather than surfacing the server's error.
    auth_failure = (
        json.dumps(
            {
                "status": r.status_code,
                "authenticated": "Authorization" in r.request.headers,
                "error": response_json.get("error"),
                "error_description": response_json.get("error_description"),
                "scope": response_json.get("scope"),
                "amr_values": response_json.get("amr_values"),
            },
            sort_keys=True,
        )
        if is_auth_failure
        else None
    )

    if (
        is_auth_failure
        and os.getenv("REDIVIS_API_TOKEN") is None
        and os.getenv("REDIVIS_DEFAULT_NOTEBOOK") is None
        and auth_failure not in original_parameters["auth_failures"]
        and len(original_parameters["auth_failures"]) < MAX_AUTH_ATTEMPTS
        and __rewind_request_body(original_parameters)
    ):
        warnings.warn(
            f"{response_json.get('error')}: {response_json.get('error_description', 'Authentication is required to access this resource.')}"
        )
        refresh_credentials(
            scope=(
                response_json["scope"].split(" ") if "scope" in response_json else None
            ),
            amr_values=(
                response_json["amr_values"] if "amr_values" in response_json else None
            ),
        )
        r.close()
        original_parameters["auth_failures"] = (
            *original_parameters["auth_failures"],
            auth_failure,
        )
        return make_request(**original_parameters)

    if "X-REDIVIS-WARNING" in r.headers:
        global previously_printed_warnings
        if r.headers["X-REDIVIS-WARNING"] not in previously_printed_warnings:
            warnings.warn(r.headers["X-REDIVIS-WARNING"])
            previously_printed_warnings[r.headers["X-REDIVIS-WARNING"]] = True

    if r.status_code >= 400:
        return raise_api_error(response_json=response_json, response=r)
    elif parse_response:
        return response_json
    else:
        return r


def __rewind_request_body(original_parameters):
    """Prepare a request to be sent again, reporting whether that's possible.

    A file-like payload was already consumed by the first attempt, so it has to be
    rewound — re-sending one as-is would upload nothing at all.
    """
    payload = original_parameters.get("payload")

    if not hasattr(payload, "read"):
        return True

    if not hasattr(payload, "seek") or not getattr(payload, "seekable", lambda: True)():
        return False

    payload.seek(0)
    return True


def __get_retry_after(r):
    """The seconds to wait that a response's Retry-After header asks for, if any."""
    value = r.headers.get("Retry-After")
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            seconds = parsedate_to_datetime(value).timestamp() - time.time()
        except (TypeError, ValueError, IndexError, OverflowError):
            return None
    return min(max(seconds, 0), MAX_RETRY_AFTER)


def __get_user_agent():
    return f"redivis-python/{__version__} ({platform.platform()}; Python/{platform.python_version()})"


def __get_api_endpoint():
    return get_config("api_endpoint")
