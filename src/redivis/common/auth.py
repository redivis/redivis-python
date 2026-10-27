import os
import json
import time
import sys
import tempfile
import warnings
import base64
from pathlib import Path
import re
from urllib.parse import urlparse
import niquests

from . import exceptions
from .util import raise_api_error
from .config import get_config

cached_credentials = None
# The file that cached_credentials were loaded from, which changes when config_dir does
cached_credentials_file = None
default_scope = ["data.edit", "organization.write", "workflow.write"]
client_id = "7YGtYWuQot1TEe0pHB3EPSj5"


# Overridable for environments where the home directory isn't writable, or is
# shared between users
def get_redivis_dir():
    return Path(get_config("config_dir") or "~/.redivis").expanduser()


# Credentials are only valid for the environment that issued them. Those for
# redivis.com are kept directly in the config directory, and any other API
# endpoint's host (e.g. "localhost_8080") has its own subdirectory. The JS and R
# clients name these directories the same way.
def get_environment_host():
    url = urlparse(get_config("api_endpoint"))
    host = url.hostname
    if url.port and url.port != {"http": 80, "https": 443}.get(url.scheme):
        host = f"{host}:{url.port}"
    return re.sub(r"[^\w.-]", "_", host)


def get_credentials_dir():
    host = get_environment_host()
    if host == "redivis.com":
        return get_redivis_dir()
    return get_redivis_dir() / "environments" / host


def get_credentials_file():
    return get_credentials_dir() / "python_credentials"


def get_verify_ssl():
    return not get_config("api_endpoint").startswith("https://localhost")


# Note that https is optional, since traffic can happen over http if all in the same cluster
def get_base_url():
    return re.match(r"(https?://.*?)(/|$)", get_config("api_endpoint")).group(1)


def load_cached_credentials():
    global cached_credentials, cached_credentials_file

    credentials_file = get_credentials_file()
    if (
        cached_credentials is not None
        and cached_credentials_file is not None
        and cached_credentials_file != credentials_file
    ):
        cached_credentials = None

    if cached_credentials is None and credentials_file.is_file():
        try:
            with open(credentials_file, "r") as f:
                cached_credentials = json.load(f)
            cached_credentials_file = credentials_file
        except Exception as e:
            """ignore"""

    return cached_credentials


def has_credentials():
    """Whether an auth token can be obtained without an interactive login.

    Used to decide whether a request should be attempted anonymously; publicly
    accessible resources can be read without any credentials at all.
    """
    if os.getenv("REDIVIS_API_TOKEN"):
        return True

    return load_cached_credentials() is not None


def get_auth_token(scope=None):
    global cached_credentials

    if not scope:
        scope = default_scope

    if os.getenv("REDIVIS_API_TOKEN"):
        if os.getenv("REDIVIS_DEFAULT_NOTEBOOK") is None and bool(
            getattr(sys, "ps1", sys.flags.interactive)
        ):
            warnings.warn(
                """Setting the REDIVIS_API_TOKEN for interactive sessions is deprecated and highly discouraged.
Please delete the token on Redivis and remove it from your code, and follow the authentication prompts here instead.

This environment variable should only ever be set in a non-interactive environment, such as in an automated script or service.
"""
            )
        return os.environ["REDIVIS_API_TOKEN"]
    else:
        load_cached_credentials()

    missing_scope = list(set(scope) - set(get_current_credential_scope()))

    if (
        cached_credentials is not None
        and "expires_at" in cached_credentials
        and "access_token" in cached_credentials
        and len(missing_scope) == 0
    ):
        # Refresh a token that expires in the next 5 minutes, rather than
        # sending one the server is about to reject
        if cached_credentials["expires_at"] < (time.time() + 5 * 60):
            return refresh_credentials()
        else:
            return cached_credentials["access_token"]
    else:
        perform_oauth_login(
            scope=missing_scope if len(missing_scope) > 0 else scope,
            upgrade_credentials=bool(len(missing_scope)),
        )

        return cached_credentials["access_token"]


def clear_cached_credentials():
    global cached_credentials
    cached_credentials = None
    get_credentials_file().unlink(missing_ok=True)


def perform_oauth_login(scope, amr_values=None, upgrade_credentials=False):
    global cached_credentials
    import webbrowser

    # Always request the default scope alongside whatever else was asked for. A
    # token without it (e.g. from a login prompted by one resource's required
    # scope) would make get_auth_token() prompt again on the very next request.
    scope = list(dict.fromkeys([*default_scope, *(scope or [])]))

    challenge, verifier = get_pkce()

    res = niquests.post(
        f"{get_base_url()}/oauth/device_authorization",
        verify=get_verify_ssl(),
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "client_id": client_id,
                "scope": " ".join(scope),
                "amr_values": amr_values,
                "code_challenge": challenge,
                "code_challenge_method": "S256",
                "access_type": "offline",
            }
        ),
    )
    res.raise_for_status()
    parsed_response = res.json()

    did_open = webbrowser.open(parsed_response["verification_uri_complete"])

    if did_open:
        print("Please authenticate with your Redivis account. Opening browser to:")
        print(parsed_response["verification_uri_complete"])
    else:
        print("Please visit the URL below to authenticate with your Redivis account:")
        print(parsed_response["verification_uri_complete"])

    headers = {}
    if upgrade_credentials and cached_credentials:
        headers["Authorization"] = f"Bearer {cached_credentials['access_token']}"

    started_polling_at = time.time()
    while True:
        if time.time() - started_polling_at > 60 * 10:
            raise exceptions.RedivisError(
                "Timed out after 10 minutes while waiting for device authorization"
            )

        time.sleep(parsed_response["interval"] or 5)

        res = niquests.post(
            f"{get_base_url()}/oauth/token",
            verify=get_verify_ssl(),
            headers=headers,
            data={
                "client_id": client_id,
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": parsed_response["device_code"],
                "code_verifier": verifier,
            },
        )

        if res.status_code == 200:
            break
        elif res.status_code == 400:
            if res.json()["error"] == "authorization_pending":
                """authorization pending"""
            else:
                raise_api_error(response_json=res.json(), response=res)
        else:
            raise_api_error(response_json=res.json(), response=res)

    cached_credentials = res.json()
    write_cached_credentials()

    return cached_credentials


def write_cached_credentials():
    global cached_credentials_file

    credentials_dir = get_credentials_dir()
    credentials_file = get_credentials_file()
    cached_credentials_file = credentials_file
    # The file holds a refresh token, so keep it private to this user
    credentials_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    # Write a temporary file and swap it into place, so that another process
    # (e.g. a second notebook kernel) never reads a partially written file.
    # mkstemp creates it readable only by this user, so the token is never
    # exposed to others, even briefly, and replacing the file also replaces
    # the permissions of one written by an earlier version of the client.
    fd, temp_path = tempfile.mkstemp(dir=credentials_dir, prefix=".python_credentials.")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(cached_credentials, f, indent=2)
        os.replace(temp_path, credentials_file)
    except BaseException:
        Path(temp_path).unlink(missing_ok=True)
        raise


def refresh_credentials(scope=None, amr_values=None):
    global cached_credentials

    if scope or amr_values:
        perform_oauth_login(
            scope=scope or get_current_credential_scope(),
            amr_values=amr_values,
            upgrade_credentials=True,
        )
    elif cached_credentials is None:
        # The request was made anonymously because no credentials were available.
        # The resource isn't public, so we need to authenticate now.
        perform_oauth_login(scope=default_scope)
    elif "refresh_token" in cached_credentials:
        res = niquests.post(
            f"{get_base_url()}/oauth/token",
            verify=get_verify_ssl(),
            data={
                "client_id": client_id,
                "grant_type": "refresh_token",
                "refresh_token": cached_credentials["refresh_token"],
            },
        )
        if res.status_code in (400, 401):
            # The refresh token was rejected (e.g. revoked or expired), so the
            # user needs to log in again
            clear_cached_credentials()
        elif res.status_code >= 400:
            # Anything else (e.g. a 503 during a deploy) says nothing about the
            # refresh token, so keep it for a later call to retry with
            try:
                response_json = res.json()
            except Exception:
                raise_api_error(response_text=res.text, response=res)
            raise_api_error(response_json=response_json, response=res)
        else:
            refresh_response = res.json()
            cached_credentials["access_token"] = refresh_response["access_token"]
            cached_credentials["expires_at"] = refresh_response["expires_at"]
            cached_credentials["expires_in"] = refresh_response["expires_in"]
            write_cached_credentials()
    else:
        clear_cached_credentials()

    return get_auth_token()


def get_current_credential_scope():
    try:
        if cached_credentials is not None:
            base64_payload = cached_credentials["access_token"].split(".")[1]
            # JWT segments are unpadded base64url; restore the padding the
            # decoder requires
            padded_base64_payload = base64_payload + "=" * (-len(base64_payload) % 4)
            return json.loads(base64.urlsafe_b64decode(padded_base64_payload))[
                "scope"
            ].split(" ")
    except Exception as e:
        """ignore"""

    return default_scope


def get_pkce():
    import hashlib
    import base64
    import secrets

    verifier = secrets.token_urlsafe(64)
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
        .decode()
        .replace("=", "")
    )
    return challenge, verifier
