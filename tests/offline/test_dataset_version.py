"""Validation of the version argument when referencing a dataset."""

import pytest
import redivis
from redivis.common import exceptions


@pytest.mark.parametrize(
    "version, expected_tag",
    [
        ("current", "current"),
        ("next", "next"),
        ("v1.0", "v1.0"),
        ("v1_0", "v1_0"),
        ("V1.0", "V1.0"),
        ("1.0", "v1.0"),
        ("7_2", "v7_2"),
        ("13.4", "v13.4"),
    ],
)
def test_valid_versions(version, expected_tag):
    dataset = redivis.organization("org").dataset("dataset", version=version)
    assert dataset.version_tag == expected_tag
    assert dataset.qualified_reference == f"org.dataset:{expected_tag}"


@pytest.mark.parametrize(
    "version", ["v6", "8", "v", "vv1._", "foo", "7.3.1", "v1.0.0", "1.", "Current"]
)
def test_invalid_versions_raise(version):
    with pytest.raises(exceptions.ValueError, match="Invalid version"):
        redivis.organization("org").dataset("dataset", version=version)


def test_version_in_name():
    dataset = redivis.organization("org").dataset("dataset:abc1:v2_1")
    assert dataset.version_tag == "v2_1"
    assert dataset.qualified_reference == "org.dataset:abc1:v2_1"


def test_no_version():
    dataset = redivis.organization("org").dataset("dataset")
    assert dataset.version_tag is None
    assert dataset.qualified_reference == "org.dataset"
