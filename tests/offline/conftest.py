"""Fixtures for tests that run against an in-process mock of the Redivis API.

Unlike the tests in the parent directory, these need no Redivis server or
credentials: `pytest tests/offline`
"""

import time

import pytest

import mock_redivis_api
from mock_redivis_api import FAKE_CREDENTIALS


@pytest.fixture(scope="session")
def mock_endpoint():
    server, endpoint = mock_redivis_api.start()
    yield endpoint
    server.shutdown()


@pytest.fixture
def api(mock_endpoint, monkeypatch, tmp_path):
    """Point the client at a freshly reset mock API, as an authenticated user.

    Returns the mock's STATE, for injecting failures and inspecting requests.
    """
    from redivis.common import auth

    mock_redivis_api.reset()
    monkeypatch.setenv("REDIVIS_API_ENDPOINT", mock_endpoint)
    monkeypatch.delenv("REDIVIS_API_TOKEN", raising=False)
    monkeypatch.delenv("REDIVIS_DEFAULT_NOTEBOOK", raising=False)

    # Never read, write, or delete the real credentials file
    monkeypatch.setattr(auth, "redivis_dir", tmp_path / ".redivis")
    monkeypatch.setattr(
        auth, "credentials_file", tmp_path / ".redivis" / "python_credentials"
    )
    monkeypatch.setattr(auth, "cached_credentials", dict(FAKE_CREDENTIALS))

    # Never open a browser; tests that exercise the login flow replace this
    def unexpected_login(*args, **kwargs):
        raise AssertionError("an interactive OAuth login was triggered")

    monkeypatch.setattr(auth, "perform_oauth_login", unexpected_login)

    # Retries back off with time.sleep; don't actually wait
    monkeypatch.setattr(time, "sleep", lambda seconds: None)

    return mock_redivis_api.STATE


@pytest.fixture
def anonymous(api, monkeypatch):
    """Like `api`, but with no credentials available at all."""
    from redivis.common import auth

    monkeypatch.setattr(auth, "cached_credentials", None)
    return api


@pytest.fixture
def table(api):
    import redivis

    return redivis.table("a.b.mock")
