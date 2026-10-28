"""The authenticated user, and listing what users own."""

import redivis

from . import helpers


def test_current_user_is_the_authenticated_user(live):
    user = redivis.current_user()

    assert user.name == live.user.name
    assert user.properties["kind"] == "user"
    assert user.uri == f"/users/{user.name}"


def test_list_datasets(live):
    datasets = live.user.list_datasets(max_results=3)

    assert len(datasets) <= 3
    for dataset in datasets:
        assert isinstance(dataset, redivis.dataset)
        assert dataset.properties["kind"] == "dataset"
        assert dataset.qualified_reference.startswith(f"{live.user.name}.")


def test_list_workflows(live):
    workflows = live.user.list_workflows(max_results=3)

    assert len(workflows) <= 3
    for workflow in workflows:
        assert isinstance(workflow, redivis.workflow)
        assert workflow.qualified_reference.startswith(f"{live.user.name}.")


def test_owner_lists_the_datasets_it_owns(live, new_dataset):
    dataset = new_dataset()
    helpers.add_data(dataset)
    # Unreleased datasets aren't listed
    dataset.release()

    listed = {d.properties["id"] for d in live.owner.list_datasets()}

    assert dataset.properties["id"] in listed
