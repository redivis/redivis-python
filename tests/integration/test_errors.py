"""How errors from the API surface to callers."""

import pytest

import redivis
from redivis import exceptions


def test_missing_dataset(live):
    dataset = live.owner.dataset(live.unique_name("missing"))

    assert not dataset.exists()
    with pytest.raises(exceptions.NotFoundError) as excinfo:
        dataset.get()
    assert excinfo.value.status_code == 404
    assert "Not found" in str(excinfo.value)


def test_missing_table(tiny_table):
    table = tiny_table.dataset.table("does_not_exist")

    assert not table.exists()
    with pytest.raises(exceptions.NotFoundError):
        table.get()


def test_missing_variable(tiny_table):
    variable = tiny_table.variable("does_not_exist")

    assert not variable.exists()
    with pytest.raises(exceptions.NotFoundError):
        variable.get()


def test_unknown_endpoint_has_a_readable_error(live):
    with pytest.raises(exceptions.NotFoundError) as excinfo:
        redivis.make_api_request(method="GET", path="/not_an_endpoint")

    assert "Invalid API endpoint" in str(excinfo.value)
    assert "Invalid API endpoint" in repr(excinfo.value)


def test_invalid_request_raises_an_api_error(scratch_dataset):
    with pytest.raises(exceptions.APIError) as excinfo:
        scratch_dataset.table("").create()

    assert 400 <= excinfo.value.status_code < 500
