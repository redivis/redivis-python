import os
import re

from . import exceptions


def _parse_reference(value, name):
    # Accept class instances (e.g., redivis.workflow("owner.name")) as well as reference strings
    if isinstance(value, str):
        return _parse_string(value, name)
    reference = getattr(value, "qualified_reference", None) or getattr(
        value, "name", None
    )
    if not isinstance(reference, str):
        raise exceptions.ValueError(
            f"{name} must be a reference string, or a Redivis object"
        )
    return reference


def _parse_string(value, name):
    if not isinstance(value, str) or not value:
        raise exceptions.ValueError(f"{name} must be a non-empty string")
    return value


def _parse_path(value, name):
    if isinstance(value, os.PathLike):
        value = os.fspath(value)
    return _parse_string(value, name)


def _parse_url(value, name):
    if not re.match(r"^https?://[^/]", _parse_string(value, name)):
        raise exceptions.ValueError(f'{name} must be an http(s) URL, but got "{value}"')
    return value.rstrip("/")


# Environment variables are strings, so these are also accepted
def _parse_positive_integer(value, name):
    if isinstance(value, str) and value.strip().isdigit():
        value = int(value)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise exceptions.ValueError(
            f"{name} must be a positive integer, but got {value!r}"
        )
    return value


def _parse_boolean(value, name):
    if isinstance(value, str):
        if value.strip().lower() in ("1", "true", "yes", "on"):
            return True
        if value.strip().lower() in ("0", "false", "no", "off"):
            return False
    if not isinstance(value, bool):
        raise exceptions.ValueError(f"{name} must be a boolean, but got {value!r}")
    return value


# Each setting falls back to its environment variable when it hasn't been set
# via config(), and then to its default, if it has one. Environment variable
# names are always REDIVIS_ followed by the setting's name in uppercase; any
# others are older names that are still honored.
_settings = {
    "default_workflow": (("REDIVIS_DEFAULT_WORKFLOW",), _parse_reference, None),
    "default_dataset": (("REDIVIS_DEFAULT_DATASET",), _parse_reference, None),
    "default_user": (("REDIVIS_DEFAULT_USER",), _parse_reference, None),
    "default_organization": (("REDIVIS_DEFAULT_ORGANIZATION",), _parse_reference, None),
    "api_endpoint": (
        ("REDIVIS_API_ENDPOINT",),
        _parse_url,
        "https://redivis.com/api/v1",
    ),
    "config_dir": (("REDIVIS_CONFIG_DIR",), _parse_path, None),
    "tmp_dir": (("REDIVIS_TMP_DIR", "REDIVIS_TMPDIR"), _parse_path, None),
    "max_parallelization": (
        ("REDIVIS_MAX_PARALLELIZATION",),
        _parse_positive_integer,
        None,
    ),
    "progress": (("REDIVIS_PROGRESS",), _parse_boolean, True),
}

_overrides = {}

# Distinguishes an argument that wasn't passed from one passed as None
_UNSET = object()


def config(
    *,
    default_workflow=_UNSET,
    default_dataset=_UNSET,
    default_user=_UNSET,
    default_organization=_UNSET,
    api_endpoint=_UNSET,
    config_dir=_UNSET,
    tmp_dir=_UNSET,
    max_parallelization=_UNSET,
    progress=_UNSET,
):
    """Sets library-wide defaults, and returns the current configuration.

    Unset values fall back to their environment variables (REDIVIS_DEFAULT_WORKFLOW,
    REDIVIS_API_ENDPOINT, etc.); pass None to clear a value that was previously set.

    Parameters
    ----------
    default_workflow : str | Workflow | None
        Qualified reference of the workflow ("owner.workflow") that unqualified
        table, query, parameter, transform, and notebook references resolve within
    default_dataset : str | Dataset | None
        Qualified reference of the dataset ("owner.dataset") that unqualified table
        and query references resolve within, when there's no default workflow
    default_user : str | User | None
        Owner of unqualified dataset and workflow references
    default_organization : str | Organization | None
        Owner of unqualified dataset and workflow references, when there's no
        default user
    api_endpoint : str | None
        Base URL of the Redivis API. Defaults to "https://redivis.com/api/v1"
    config_dir : str | os.PathLike | None
        Directory where credentials are cached. Defaults to "~/.redivis"
    tmp_dir : str | os.PathLike | None
        Directory for temporary files, such as data being converted to another
        format. Defaults to a new temporary directory
    max_parallelization : int | None
        Default for the max_parallelization argument of methods that read, upload,
        or download in parallel. Each method has its own default when this isn't set
    progress : bool | None
        Default for the progress argument, which shows progress bars. Defaults to True

    Returns
    -------
    dict
        The value of every setting, or None for those that aren't set
    """
    updates = {key: value for key, value in locals().items() if value is not _UNSET}
    # Validate everything before setting anything, so that an invalid value
    # doesn't leave a partial update
    parsed = {
        key: None if value is None else _settings[key][1](value, key)
        for key, value in updates.items()
    }
    for key, value in parsed.items():
        if value is None:
            _overrides.pop(key, None)
        else:
            _overrides[key] = value

    return {key: get_config(key) for key in _settings}


def get_config(key):
    if key in _overrides:
        return _overrides[key]
    env_variables, parse, default = _settings[key]
    for env_variable in env_variables:
        if os.getenv(env_variable):
            return parse(os.getenv(env_variable), env_variable)
    return default


def resolve_config(key, value, fallback=None):
    """Returns an argument's value or, when it's None, its configured default,
    and then the fallback"""
    if value is not None:
        return value
    configured = get_config(key)
    return fallback if configured is None else configured
