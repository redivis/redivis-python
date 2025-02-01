import os
import json
import time
import sys
import warnings
import base64
from pathlib import Path
import re
import requests

redivis_dir = Path.home() / ".redivis"
cached_credentials = None
verify_ssl = (
    os.getenv("REDIVIS_API_ENDPOINT", "https://redivis.com").find(
        "https://localhost", 0
    )
    != 0
)
credentials_file = redivis_dir / "python_credentials"
default_scope = ["data.edit", "organization.write"]
client_id = "7YGtYWuQot1TEe0pHB3EPSj5"
# Note that https is optional, since traffic can happen over http if all in the same cluster
base_url = re.match(
    r"(https?://.*?)(/|$)", os.getenv("REDIVIS_API_ENDPOINT", "https://redivis.com")
).group(1)


def get_auth_token(scope=None):
    global cached_credentials

    if not scope:
        scope = default_scope

    if os.getenv("REDIVIS_API_TOKEN"):
        if os.getenv("REDIVIS_NOTEBOOK_JOB_ID") is None and bool(
            getattr(sys, "ps1", sys.flags.interactive)
        ):
            warnings.warn(
                """Setting the REDIVIS_API_TOKEN for interactive sessions is deprecated and highly discouraged.
Please delete the token on Redivis and remove it from your code, and follow the authentication prompts here instead.

This environment variable should only ever be set in a non-interactive environment, such as in an automated script or service.
"""
            )
        return os.environ["REDIVIS_API_TOKEN"]
    elif cached_credentials is None and credentials_file.is_file():
        try:
            with open(credentials_file, "r") as f:
                cached_credentials = json.load(f)
        except Exception as e:
            """ignore"""

    missing_scope = list(set(scope) - set(get_current_credential_scope()))

    if (
        cached_credentials is not None
        and "expires_at" in cached_credentials
        and "access_token" in cached_credentials
        and len(missing_scope) == 0
    ):
        if cached_credentials["expires_at"] < (time.time() - 5 * 60):
            return refresh_credentials()
        else:
            return cached_credentials["access_token"]
    else:
        if not redivis_dir.is_dir():
            redivis_dir.mkdir()

        perform_oauth_login(
            scope=missing_scope if len(missing_scope) > 0 else scope,
            upgrade_credentials=bool(len(missing_scope)),
        )

        return cached_credentials["access_token"]


def clear_cached_credentials():
    global cached_credentials
    cached_credentials = None
    credentials_file.unlink(missing_ok=True)


def perform_oauth_login(scope, amr_values=None, upgrade_credentials=False):
    global cached_credentials
    import webbrowser

    challenge, verifier = get_pkce()

    res = requests.post(
        f"{base_url}/oauth/device_authorization",
        verify=verify_ssl,
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
            raise Exception("Timed out waiting for device authorization")

        time.sleep(parsed_response["interval"] or 5)

        res = requests.post(
            f"{base_url}/oauth/token",
            verify=verify_ssl,
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
                raise Exception(res.json())
        else:
            raise Exception(res.json())

    cached_credentials = res.json()

    with open(credentials_file, "w") as f:
        json.dump(cached_credentials, f, indent=2)

    return cached_credentials


def refresh_credentials(scope=None, amr_values=None):
    global cached_credentials

    if scope or amr_values:
        perform_oauth_login(
            scope=scope or get_current_credential_scope(),
            amr_values=amr_values,
            upgrade_credentials=True,
        )
    elif "refresh_token" in cached_credentials:
        res = requests.post(
            f"{base_url}/oauth/token",
            verify=verify_ssl,
            data={
                "client_id": client_id,
                "grant_type": "refresh_token",
                "refresh_token": cached_credentials["refresh_token"],
            },
        )
        if res.status_code >= 400:
            clear_cached_credentials()
        else:
            refresh_response = res.json()
            cached_credentials["access_token"] = refresh_response["access_token"]
            cached_credentials["expires_at"] = refresh_response["expires_at"]
            cached_credentials["expires_in"] = refresh_response["expires_in"]

            with open(credentials_file, "w") as f:
                json.dump(cached_credentials, f, indent=2)
    else:
        clear_cached_credentials()

    return get_auth_token()


def get_current_credential_scope():
    try:
        if cached_credentials is not None:
            base64_payload = cached_credentials["access_token"].split(".")[1]
            # IMPORTANT: b64decode requires that the string length be a multiple of 4, with "=" at the end for padding
            padded_base64_payload = f"{base64_payload}{'=' * (len(base64_payload) % 4)}"
            return json.loads(base64.b64decode(padded_base64_payload))["scope"].split(
                " "
            )
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
