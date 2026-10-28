"""Credential discovery and the OAuth login flow."""

import json
import os
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest
import redivis
from redivis.common import auth, exceptions

from .mock_redivis_api import jwt

# Windows has no POSIX permission bits (os.chmod only sets a file read-only), so the
# tests of them only run elsewhere
posix_permissions_only = pytest.mark.skipif(
    sys.platform == "win32", reason="Windows has no POSIX permission bits"
)

# The fixtures replace perform_oauth_login with a guard, so keep the real one
real_login = auth.perform_oauth_login

TOKEN = {
    "access_token": jwt("data.edit organization.write workflow.write"),
    "expires_at": 9e12,
    "expires_in": 3600,
    "refresh_token": "refresh",
}


class FakeResponse:
    def __init__(self, body, status_code=200):
        self.status_code = status_code
        self._body = body
        self.text = body if isinstance(body, str) else json.dumps(body)

    def json(self):
        if isinstance(self._body, str):
            raise ValueError("not JSON")
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

    monkeypatch.setattr(auth.niquests, "post", post)
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
    assert not auth.get_redivis_dir().exists()

    auth.perform_oauth_login(scope=None)

    assert (
        json.loads(auth.get_credentials_file().read_text())["access_token"]
        == TOKEN["access_token"]
    )
    if sys.platform != "win32":  # See posix_permissions_only
        assert file_mode(auth.get_credentials_dir()) == 0o700


def test_the_credentials_directory_can_be_overridden(tmp_path):
    # It's resolved on import, so check it in a fresh interpreter, importing the same
    # copy of the package as these tests
    package_root = str(Path(redivis.__file__).parents[1])
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from redivis.common import auth; print(auth.get_credentials_file())",
        ],
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                filter(None, [package_root, os.getenv("PYTHONPATH")])
            ),
            "REDIVIS_CONFIG_DIR": str(tmp_path / "config"),
            "REDIVIS_API_ENDPOINT": "http://localhost:8080/api/v1",
        },
        capture_output=True,
        text=True,
        check=True,
    )

    assert Path(result.stdout.strip()) == (
        tmp_path / "config" / "environments" / "localhost_8080" / "python_credentials"
    )


def test_private_resource_prompts_a_login_then_succeeds(anonymous, oauth_server):
    anonymous["require_auth"] = True

    table = redivis.table("a.b.mock").get()

    assert table.properties["name"] == "mock"
    assert len(oauth_server) == 1
    assert [had_auth for *_, had_auth in anonymous["requests"]] == [False, True]


def test_has_credentials(anonymous):
    assert not auth.has_credentials()

    auth.get_credentials_dir().mkdir(parents=True)
    auth.get_credentials_file().write_text(json.dumps(TOKEN))
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


def test_a_rotated_refresh_token_replaces_the_one_that_was_used(api, monkeypatch):
    sent = []

    def post(url, **kwargs):
        sent.append(kwargs["data"]["refresh_token"])
        return FakeResponse({**TOKEN, "refresh_token": "refresh-2"})

    monkeypatch.setattr(auth.niquests, "post", post)
    auth.cached_credentials["expires_at"] = time.time()

    auth.get_auth_token()

    assert sent == ["refresh"]
    saved = json.loads(auth.get_credentials_file().read_text())
    assert saved["refresh_token"] == "refresh-2"


def test_credentials_another_process_refreshed_are_used(api, monkeypatch):
    """Rather than refreshing with the refresh token they replaced, which the server
    takes as a sign that it was stolen."""
    # e.g. a second notebook kernel, sharing the credentials file
    theirs = {
        **TOKEN,
        "access_token": jwt("data.edit organization.write workflow.write") + "-theirs",
        "refresh_token": "refresh-2",
    }
    auth.get_credentials_dir().mkdir(parents=True)
    auth.get_credentials_file().write_text(json.dumps(theirs))
    monkeypatch.setattr(
        auth.niquests,
        "post",
        lambda url, **kwargs: pytest.fail("refreshed with a replaced refresh token"),
    )
    auth.cached_credentials["expires_at"] = time.time()

    assert auth.get_auth_token() == theirs["access_token"]
    assert auth.cached_credentials["refresh_token"] == "refresh-2"


def file_mode(path):
    return stat.S_IMODE(os.stat(path).st_mode)


@posix_permissions_only
def test_credentials_are_saved_privately(anonymous, oauth_server):
    auth.perform_oauth_login(scope=None)

    assert file_mode(auth.get_credentials_file()) == 0o600


@posix_permissions_only
def test_refreshed_credentials_stay_private(api, oauth_server):
    # e.g. a file written by an earlier version of the client
    auth.get_credentials_dir().mkdir(parents=True)
    auth.get_credentials_file().write_text("{}")
    os.chmod(auth.get_credentials_file(), 0o644)
    auth.cached_credentials["expires_at"] = time.time()

    auth.get_auth_token()

    assert file_mode(auth.get_credentials_file()) == 0o600
    assert json.loads(auth.get_credentials_file().read_text())["access_token"] == (
        TOKEN["access_token"]
    )


def test_a_failed_write_leaves_the_previous_credentials_intact(api):
    auth.write_cached_credentials()
    previous = auth.get_credentials_file().read_text()
    # Fails partway through serializing, after some of the file is written
    auth.cached_credentials["access_token"] = object()

    with pytest.raises(TypeError):
        auth.write_cached_credentials()

    assert auth.get_credentials_file().read_text() == previous
    assert os.listdir(auth.get_credentials_dir()) == [auth.get_credentials_file().name]


def test_a_server_error_while_refreshing_keeps_the_credentials(api, monkeypatch):
    auth.write_cached_credentials()
    monkeypatch.setattr(
        auth.niquests,
        "post",
        lambda url, **kwargs: FakeResponse("<html>Bad gateway</html>", 502),
    )

    with pytest.raises(exceptions.APIError):
        auth.refresh_credentials()

    assert auth.cached_credentials["refresh_token"] == "refresh"
    assert auth.get_credentials_file().exists()


def test_a_rejected_refresh_token_prompts_a_login(api, monkeypatch):
    auth.write_cached_credentials()
    monkeypatch.setattr(
        auth.niquests,
        "post",
        lambda url, **kwargs: FakeResponse({"error": "invalid_grant"}, 400),
    )
    logins = []

    def login(**kwargs):
        logins.append(kwargs)
        auth.cached_credentials = dict(TOKEN)

    monkeypatch.setattr(auth, "perform_oauth_login", login)

    assert auth.refresh_credentials() == TOKEN["access_token"]
    assert len(logins) == 1
    assert not auth.get_credentials_file().exists()


@pytest.mark.parametrize(
    "api_endpoint, environment_dir",
    [
        ("https://redivis.com/api/v1", None),
        ("https://REDIVIS.com:443/api/v1", None),
        ("http://localhost:8080/api/v1", "localhost_8080"),
        ("https://local.host:8443/api/v1", "local.host_8443"),
    ],
)
def test_each_environment_has_its_own_credentials(
    anonymous, api_endpoint, environment_dir
):
    # Those for redivis.com are kept directly in the config directory
    credentials_dir = auth.get_redivis_dir()
    if environment_dir:
        credentials_dir = credentials_dir / "environments" / environment_dir
    redivis.config(api_endpoint=api_endpoint)
    try:
        assert auth.get_credentials_file() == credentials_dir / "python_credentials"
    finally:
        redivis.config(api_endpoint=None)
