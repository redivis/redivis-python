"""Creating, updating, versioning, and deleting datasets."""

import pytest

import redivis
from redivis import exceptions

from . import helpers


def test_create(live, new_dataset):
    dataset = new_dataset(description="A description")

    assert dataset.exists()
    assert dataset.properties["description"] == "A description"
    assert dataset.properties["publicAccessLevel"] == "none"
    assert dataset.qualified_reference.startswith(f"{live.owner.name}.")
    # A new dataset starts out with an unreleased version, which it references
    assert dataset.version_tag == "next"
    assert dataset.properties["version"]["isReleased"] is False


def test_get_by_reference(new_dataset):
    dataset = new_dataset()

    fetched = redivis.dataset(dataset.qualified_reference).get()

    assert fetched.properties["id"] == dataset.properties["id"]
    assert fetched.name == dataset.name


def test_update(new_dataset):
    dataset = new_dataset()

    dataset.update(
        description="Updated",
        methodology_markdown="# Methodology",
        usage_markdown="Usage notes",
        labels_to_add=["pytest", "other"],
    )
    assert dataset.properties["description"] == "Updated"
    assert dataset.properties["methodologyMarkdown"] == "# Methodology"
    assert dataset.properties["usageMarkdown"] == "Usage notes"
    assert set(dataset.properties["labels"]) == {"pytest", "other"}

    dataset.update(labels_to_remove=["other"])
    assert dataset.properties["labels"] == ["pytest"]

    # Re-fetching reflects what was saved
    assert dataset.get().properties["description"] == "Updated"


def test_rename(new_dataset):
    dataset = new_dataset()
    new_name = f"{dataset.name}_renamed"

    dataset.update(name=new_name)

    assert dataset.name == new_name
    assert new_name in dataset.qualified_reference
    assert dataset.get().name == new_name


def test_delete(live):
    dataset = live.owner.dataset(live.unique_name("delete")).create()
    assert dataset.exists()

    dataset.delete()

    assert not dataset.exists()


def test_create_next_version_when_one_exists(new_dataset):
    dataset = new_dataset()

    with pytest.raises(exceptions.ValueError, match="already exists"):
        dataset.create_next_version()

    next_version = dataset.create_next_version(if_not_exists=True)
    assert next_version.version_tag == "next"
    assert next_version.properties["id"] == dataset.properties["id"]


def test_release_and_version_history(new_dataset):
    dataset = new_dataset()
    helpers.add_data(dataset)

    dataset.release(release_notes="First release")

    assert dataset.version_tag == "v1_0"
    assert dataset.properties["version"]["isReleased"] is True
    version = dataset.version().get()
    assert version.properties["tag"] == "v1.0"
    assert version.properties["releaseNotes"] == "First release"
    assert dataset.previous_version() is None

    next_version = dataset.create_next_version()
    assert next_version.version_tag == "next"
    # A version can only be released if it changed something
    helpers.add_data(next_version, "more_data")
    next_version.release()
    second_tag = next_version.properties["version"]["tag"]
    assert second_tag != "v1.0"
    assert next_version.version_tag == second_tag.replace(".", "_")

    tags = {v.properties["tag"] for v in next_version.list_versions()}
    assert tags == {"v1.0", second_tag}
    assert next_version.previous_version().version_tag == "v1.0"

    # The released versions remain readable by their tags
    assert redivis.dataset(f"{dataset.qualified_reference.rsplit(':', 1)[0]}:v1.0").exists()


def test_version_update(new_dataset):
    dataset = new_dataset()
    helpers.add_data(dataset)
    dataset.release()

    version = dataset.version().update(label="Initial", release_notes="Notes")

    assert version.properties["label"] == "Initial"
    assert version.get().properties["releaseNotes"] == "Notes"


def test_list_tables(new_dataset):
    dataset = new_dataset()
    dataset.table("first").create()
    dataset.table("second").create(is_file_index=True)

    tables = dataset.list_tables()

    assert sorted(t.name for t in tables) == ["first", "second"]
    assert all(isinstance(t, redivis.table) for t in tables)
