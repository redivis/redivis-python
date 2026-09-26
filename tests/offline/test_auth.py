"""Credential discovery and the OAuth login flow."""

import json
import os
import stat
import time

import pytest
import redivis
from redivis.common import auth

from mock_redivis_api import jwt

# The fixtures replace perform_oauth_login with a guard, so keep the real one
real_login = auth.perform_oauth_login

TOKEN = {
    "access_token": jwt("data.edit organization.write workflow.write"),
    "expires_at": 9e12,
    "expires_in": 3600,
    "refresh_token": "refresh",
}


class FakeResponse:
    def __init__(self, body):
        self.status_code = 200
        self._body = body

    def json(self):
        return self._body

    def raise_for_status(self):
        pass


@pytest.fixture
def oauth_server(monkeypatch):
    """Stand in for the OAuth device flow, recording the scope each login requests."""
    requested_scopes = []

    def post(url, **kwargs):
        if url.endswith("/oauth/device_authorization"):
            requested_scopes.append(json.loads(kwargs["data"])["scope"].split(" "))
            return FakeResponse(
                {
                    "verification_uri_complete": "https://example.test",
                    "device_code": "d",
                    "interval": 0,
                }
            )
        return FakeResponse(TOKEN)

    monkeypatch.setattr(auth.requests, "post", post)
    monkeypatch.setattr("webbrowser.open", lambda *args, **kwargs: False)
    monkeypatch.setattr(auth, "perform_oauth_login", real_login)
    return requested_scopes


def test_login_always_requests_the_default_scope(api, oauth_server):
    auth.perform_oauth_login(scope=["data.data"])

    [scope] = oauth_server
    assert set(auth.default_scope) <= set(scope)
    assert "data.data" in scope
    assert len(scope) == len(set(scope))


def test_login_for_a_default_scope_doesnt_duplicate_it(api, oauth_server):
    auth.perform_oauth_login(scope=["data.edit"])

    [scope] = oauth_server
    assert sorted(scope) == sorted(auth.default_scope)


def test_login_creates_the_credentials_directory(anonymous, oauth_server):
    assert not auth.redivis_dir.exists()

    auth.perform_oauth_login(scope=None)

    assert (
        json.loads(auth.credentials_file.read_text())["access_token"]
        == TOKEN["access_token"]
    )


def test_private_resource_prompts_a_login_then_succeeds(anonymous, oauth_server):
    anonymous["require_auth"] = True

    table = redivis.table("a.b.mock").get()

    assert table.properties["name"] == "mock"
    assert len(oauth_server) == 1
    assert [had_auth for *_, had_auth in anonymous["requests"]] == [False, True]


def test_has_credentials(anonymous):
    assert not auth.has_credentials()

    auth.redivis_dir.mkdir()
    auth.credentials_file.write_text(json.dumps(TOKEN))
    assert auth.has_credentials()
    assert auth.cached_credentials["access_token"] == TOKEN["access_token"]


def test_has_credentials_with_an_api_token(anonymous, monkeypatch):
    monkeypatch.setenv("REDIVIS_API_TOKEN", "token")

    assert auth.has_credentials()


def test_the_credentials_scope_is_read_from_their_token(api):
    # Scopes whose base64url encoding includes "-" and "_", which plain base64
    # decoding would mangle, with and without padding to restore
    for scope in ["data.edit workflow.write ~~~?>>", "data.edit data.data"]:
        auth.cached_credentials["access_token"] = jwt(scope)

        assert auth.get_current_credential_scope() == scope.split(" ")


def test_a_token_about_to_expire_is_refreshed_before_its_used(api, oauth_server):
    auth.cached_credentials["access_token"] = "about-to-expire"
    auth.cached_credentials["expires_at"] = time.time() + 60

    assert auth.get_auth_token() == TOKEN["access_token"]
    assert auth.cached_credentials["expires_at"] == TOKEN["expires_at"]


def test_a_token_that_isnt_about_to_expire_is_used_as_is(api, oauth_server):
    token = auth.cached_credentials["access_token"]

    assert auth.get_auth_token() == token


def file_mode(path):
    return stat.S_IMODE(os.stat(path).st_mode)


def test_credentials_are_saved_privately(anonymous, oauth_server):
    auth.perform_oauth_login(scope=None)

    assert file_mode(auth.credentials_file) == 0o600


def test_refreshed_credentials_stay_private(api, oauth_server):
    # e.g. a file written by an earlier version of the client
    auth.redivis_dir.mkdir()
    auth.credentials_file.write_text("{}")
    os.chmod(auth.credentials_file, 0o644)
    auth.cached_credentials["expires_at"] = time.time()

    auth.get_auth_token()

    assert file_mode(auth.credentials_file) == 0o600
    assert json.loads(auth.credentials_file.read_text())["access_token"] == (
        TOKEN["access_token"]
    )
