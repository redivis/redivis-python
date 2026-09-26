"""Retry and authentication handling in make_request()."""

import io

import pytest
import requests
from redivis.common import api_request, auth, exceptions
from redivis.common.util import raise_api_error

from mock_redivis_api import FAKE_CREDENTIALS

TABLE = "/tables/a.b.mock"


# --- 503 retries -------------------------------------------------------------


@pytest.mark.parametrize("method", ["GET", "HEAD", "PATCH"])
def test_idempotent_requests_retry_503s(api, method):
    api["fail_503"] = 3

    api_request.make_request(method=method, path=TABLE, parse_response=False)

    assert len(api["requests"]) == 4


def test_persistent_503s_eventually_raise(api):
    api["fail_503"] = 1000

    with pytest.raises(exceptions.APIError):
        api_request.make_request(method="GET", path=TABLE)

    assert len(api["requests"]) == 11


def test_retried_request_resends_its_whole_file_body(api):
    api["fail_503"] = 2
    content = b"x" * 5000

    response = api_request.make_request(
        method="PATCH",
        path=TABLE,
        parse_payload=False,
        files={"metadata": '{"a": 1}', "data": io.BytesIO(content)},
    )

    lengths = api["post_body_lengths"]
    assert len(lengths) == 3
    assert len(set(lengths)) == 1 and lengths[0] > len(content)
    assert response["ok"]


def test_unseekable_body_is_not_retried(api):
    class Unseekable:
        def read(self, *args):
            return b"data"

        def seekable(self):
            return False

    api["fail_503"] = 1

    with pytest.raises(exceptions.APIError):
        api_request.make_request(
            method="PATCH",
            path=TABLE,
            parse_payload=False,
            files={"data": Unseekable()},
        )

    assert len(api["requests"]) == 1


# --- connection hygiene ------------------------------------------------------


@pytest.fixture
def responses(monkeypatch):
    """Every Response the client receives, in order."""
    received = []
    original = requests.request

    def record(**kwargs):
        response = original(**kwargs)
        received.append(response)
        return response

    monkeypatch.setattr(requests, "request", record)
    return received


def test_503_retries_close_the_responses_they_discard(api, responses):
    api["fail_503"] = 3

    final = api_request.make_request(
        method="GET", path=TABLE, stream=True, parse_response=False
    )

    assert len(responses) == 4
    assert all(r.raw.closed for r in responses[:3])
    assert not final.raw.closed  # still the caller's to read
    final.close()


def test_auth_retries_close_the_responses_they_discard(api, responses, monkeypatch):
    monkeypatch.setattr(api_request, "refresh_credentials", lambda **kwargs: None)
    api["response_script"] = [(401, {"status": 401, "error": "invalid_token"})]

    final = api_request.make_request(
        method="GET", path=TABLE, stream=True, parse_response=False
    )

    assert len(responses) == 2
    assert responses[0].raw.closed
    assert not final.raw.closed
    final.close()


# --- authentication retries --------------------------------------------------

UNAUTHENTICATED = (401, {"status": 401, "error": "invalid_token"})
INSUFFICIENT_SCOPE = (
    403,
    {"status": 403, "error": "insufficient_scope", "scope": "data.data"},
)
PASS = (None, None)


@pytest.fixture
def logins(monkeypatch):
    """Stub out re-authentication, recording the scope of each attempt."""
    attempts = []

    def refresh_credentials(scope=None, amr_values=None):
        attempts.append(scope)
        monkeypatch.setattr(auth, "cached_credentials", dict(FAKE_CREDENTIALS))

    monkeypatch.setattr(api_request, "refresh_credentials", refresh_credentials)
    return attempts


def test_repeated_auth_failure_stops_after_one_attempt(api, logins):
    # e.g. a refreshed token the server still rejects
    api["response_script"] = [UNAUTHENTICATED] * 5

    with pytest.raises(exceptions.APIError, match="invalid_token"):
        api_request.make_request(method="GET", path=TABLE)

    assert len(logins) == 1
    assert len(api["requests"]) == 2


def test_anonymous_login_then_scope_upgrade(anonymous, logins):
    anonymous["response_script"] = [UNAUTHENTICATED, INSUFFICIENT_SCOPE, PASS]

    response = api_request.make_request(method="GET", path=TABLE)

    assert response["name"] == "mock"
    assert logins == [None, ["data.data"]]


def test_logging_in_counts_as_progress_even_with_the_same_error(anonymous, logins):
    anonymous["response_script"] = [UNAUTHENTICATED, UNAUTHENTICATED, PASS]

    api_request.make_request(method="GET", path=TABLE)

    assert len(logins) == 2
    assert [had_auth for *_, had_auth in anonymous["requests"]] == [False, True, True]


def test_cycling_auth_failures_stop_at_the_first_repeat(api, logins):
    a = (401, {"status": 401, "error": "a"})
    b = (401, {"status": 401, "error": "b"})
    api["response_script"] = [a, b, a, PASS]

    with pytest.raises(exceptions.APIError):
        api_request.make_request(method="GET", path=TABLE)

    assert len(logins) == 2


def test_ever_changing_auth_failures_are_capped(api, logins):
    api["response_script"] = [
        (401, {"status": 401, "error": f"error-{i}"}) for i in range(20)
    ]

    with pytest.raises(exceptions.APIError):
        api_request.make_request(method="GET", path=TABLE)

    assert len(logins) == api_request.MAX_AUTH_ATTEMPTS


def test_auth_retry_resends_its_whole_file_body(api, logins):
    api["response_script"] = [UNAUTHENTICATED, INSUFFICIENT_SCOPE, PASS]
    content = b"x" * 5000

    response = api_request.make_request(
        method="PATCH",
        path=TABLE,
        parse_payload=False,
        files={"metadata": '{"a": 1}', "data": io.BytesIO(content)},
    )

    lengths = api["post_body_lengths"]
    assert len(lengths) == 3
    assert len(set(lengths)) == 1 and lengths[0] > len(content)
    assert response["ok"]


def test_unseekable_body_is_not_retried_after_auth_failure(api, logins):
    class Unseekable:
        def read(self, *args):
            return b"data"

        def seekable(self):
            return False

    api["response_script"] = [UNAUTHENTICATED]

    with pytest.raises(exceptions.APIError, match="invalid_token"):
        api_request.make_request(
            method="PATCH",
            path=TABLE,
            parse_payload=False,
            files={"data": Unseekable()},
        )

    assert logins == []
    assert len(api["requests"]) == 1


def test_unauthenticated_request_is_sent_without_a_token(anonymous):
    api_request.make_request(method="GET", path=TABLE)

    assert [had_auth for *_, had_auth in anonymous["requests"]] == [False]


# --- error reporting ---------------------------------------------------------


def test_raise_api_error_reads_the_status_from_an_error_response():
    # A requests.Response for an error status is falsy
    response = requests.Response()
    response.status_code = 404

    with pytest.raises(exceptions.NotFoundError):
        raise_api_error(response_text="missing", response=response)
