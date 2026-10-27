"""Library-wide settings, via redivis.config() and their environment variables."""

import inspect
import json
import os

import pytest
import redivis
from redivis.common import auth, exceptions
from redivis.common.config import resolve_config

from .test_auth import TOKEN

ENV_VARIABLES = [
    "REDIVIS_DEFAULT_WORKFLOW",
    "REDIVIS_DEFAULT_DATASET",
    "REDIVIS_MAX_PARALLELIZATION",
    "REDIVIS_PROGRESS",
    "REDIVIS_TMP_DIR",
    "REDIVIS_TMPDIR",
]


@pytest.fixture(autouse=True)
def clean_config(monkeypatch):
    for name in ENV_VARIABLES:
        monkeypatch.delenv(name, raising=False)
    yield
    redivis.config(
        **{key: None for key in inspect.signature(redivis.config).parameters}
    )


def test_falls_back_to_environment_variables(monkeypatch):
    assert redivis.config()["default_workflow"] is None
    monkeypatch.setenv("REDIVIS_DEFAULT_WORKFLOW", "jane.from_env")
    assert redivis.config()["default_workflow"] == "jane.from_env"


def test_set_values_take_precedence_until_cleared_with_none(monkeypatch):
    monkeypatch.setenv("REDIVIS_DEFAULT_WORKFLOW", "jane.from_env")
    config = redivis.config(default_workflow="jane.from_config")
    assert config["default_workflow"] == "jane.from_config"
    # Arguments that aren't passed leave their values as they are
    assert redivis.config()["default_workflow"] == "jane.from_config"
    assert redivis.config(default_workflow=None)["default_workflow"] == "jane.from_env"


def test_rejects_unknown_options():
    with pytest.raises(TypeError):
        redivis.config(default_table="x")


def test_validates_values_before_setting_any_of_them():
    for invalid in [
        {"max_parallelization": 0},
        {"max_parallelization": 2.5},
        {"max_parallelization": True},
        {"api_endpoint": "redivis.com"},
        {"progress": "sometimes"},
        {"default_workflow": "jane.wf", "default_dataset": 12},
    ]:
        with pytest.raises(exceptions.ValueError):
            redivis.config(**invalid)
    assert redivis.config()["default_workflow"] is None


def test_accepts_redivis_objects_as_references():
    config = redivis.config(default_workflow=redivis.workflow("jane.wf"))
    assert config["default_workflow"] == "jane.wf"
    assert redivis.current_workflow().name == "wf"


def test_parses_environment_variables(monkeypatch):
    monkeypatch.setenv("REDIVIS_MAX_PARALLELIZATION", "4")
    monkeypatch.setenv("REDIVIS_PROGRESS", "false")
    assert redivis.config()["max_parallelization"] == 4
    assert redivis.config()["progress"] is False

    monkeypatch.setenv("REDIVIS_MAX_PARALLELIZATION", "many")
    with pytest.raises(exceptions.ValueError, match="REDIVIS_MAX_PARALLELIZATION"):
        redivis.config()
    monkeypatch.delenv("REDIVIS_MAX_PARALLELIZATION")


def test_defaults(monkeypatch):
    monkeypatch.delenv("REDIVIS_CONFIG_DIR", raising=False)
    config = redivis.config()
    assert config["api_endpoint"] == os.environ.get(
        "REDIVIS_API_ENDPOINT", "https://redivis.com/api/v1"
    )
    assert config["progress"] is True
    assert config["config_dir"] is None
    assert str(auth.get_redivis_dir()) == os.path.expanduser("~/.redivis")
    assert config["max_parallelization"] is None


def test_tmp_dir_honors_the_legacy_environment_variable(monkeypatch, tmp_path):
    monkeypatch.setenv("REDIVIS_TMPDIR", str(tmp_path))
    assert redivis.config()["tmp_dir"] == str(tmp_path)


def test_arguments_take_precedence_over_config():
    assert resolve_config("progress", None) is True
    assert resolve_config("max_parallelization", None, 7) == 7

    redivis.config(progress=False, max_parallelization=3)
    assert resolve_config("progress", None) is False
    assert resolve_config("progress", True) is True
    assert resolve_config("max_parallelization", None, 7) == 3
    assert resolve_config("max_parallelization", 0, 7) == 0


def test_default_workflow_resolves_unqualified_tables(api):
    redivis.config(default_workflow="jane.wf")
    table = redivis.table("mock")
    assert table.workflow.qualified_reference == "jane.wf"


def test_credentials_are_reloaded_when_config_dir_changes(anonymous, tmp_path):
    for name in ["a", "b"]:
        credentials_dir = tmp_path / name / "environments" / auth.get_environment_host()
        credentials_dir.mkdir(parents=True)
        (credentials_dir / "python_credentials").write_text(
            json.dumps({**TOKEN, "access_token": f"token_{name}"})
        )

    redivis.config(config_dir=tmp_path / "a")
    assert auth.has_credentials()
    assert auth.cached_credentials["access_token"] == "token_a"

    redivis.config(config_dir=tmp_path / "b")
    assert auth.has_credentials()
    assert auth.cached_credentials["access_token"] == "token_b"
