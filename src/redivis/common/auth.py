import os


def get_auth_token():
    if os.getenv("REDIVIS_API_TOKEN") is None:
        raise EnvironmentError(
            "The environment variable REDIVIS_API_TOKEN must be set."
        )
    return os.environ["REDIVIS_API_TOKEN"]
