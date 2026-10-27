"""Organizations, and the datasets and members they have."""

import secrets

import pytest

import redivis

from . import helpers

pytestmark = pytest.mark.organization


def test_exists(organization, live):
    assert organization.exists()
    assert not redivis.organization(live.unique_name("missing")).exists()


def test_list_datasets(organization):
    datasets = organization.list_datasets(max_results=5)

    assert 0 < len(datasets) <= 5
    for dataset in datasets:
        assert dataset.organization.name == organization.name
        assert dataset.properties["owner"]["kind"] == "organization"


def test_list_datasets_by_label(organization, live, new_dataset):
    if live.owner.name != organization.name:
        pytest.skip("needs the test datasets to be owned by the organization")
    # Labels may only have letters, numbers, and underscores
    label = f"pytest_{secrets.token_hex(4)}"
    dataset = new_dataset()
    dataset.update(labels=[label])
    helpers.add_data(dataset)
    # Unreleased datasets aren't listed
    dataset.release()

    listed = organization.list_datasets(labels=[label])

    assert [d.properties["id"] for d in listed] == [dataset.properties["id"]]


def test_list_members(organization, live):
    members = organization.list_members()

    assert members
    names = {member.name for member in members}
    assert live.user.name in names


def test_get_member(organization, live):
    member = organization.member(live.user.name)

    assert member.exists()
    member.get()
    assert member.user.name == live.user.name
    assert member.properties["user"]["name"] == live.user.name


def test_list_workflows(organization):
    for workflow in organization.list_workflows(max_results=3):
        assert workflow.organization.name == organization.name
