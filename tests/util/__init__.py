import redivis
import os

#  This ignores insecure https requests warnings from the console
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

os.environ["REDIVIS_API_ENDPOINT"] = "https://localhost:8443/api/v1"


def get_user_name():
    return "imathews"


def get_dataset_name():
    return "A test dataset"


def get_table_name():
    return "A test table"


def get_table():
    return (
        redivis.user(get_user_name())
        .dataset(get_dataset_name(), version="next")
        .table(get_table_name())
    )


def get_dataset():
    return redivis.user(get_user_name()).dataset(get_dataset_name(), version="next")


def delete_test_dataset():
    dataset = redivis.user(get_user_name()).dataset(get_dataset_name())
    if dataset.exists():
        dataset.delete()


def create_test_dataset():
    dataset = redivis.user(get_user_name()).dataset(get_dataset_name())

    if not dataset.exists():
        dataset.create()

    dataset = dataset.create_next_version(
        ignore_if_exists=True
    )  # Make sure we have a version to upload to
    return dataset


def clear_test_data():
    table = (
        redivis.user(get_user_name())
        .dataset(get_dataset_name(), version="next")
        .table(get_table_name())
    )
    if table.exists():
        table.delete()


def populate_test_data():
    create_test_dataset()
    table = (
        redivis.user(get_user_name())
        .dataset(get_dataset_name(), version="next")
        .table(get_table_name())
    )

    if not table.exists():
        table.create(description="Some info", upload_merge_strategy="replace")

    if not table.get().properties["numRows"]:
        with open(
            os.path.join(os.path.dirname(__file__), "../data/tiny.csv"), "rb"
        ) as f:
            table.upload(name="local.csv").create().upload_file(
                type="delimited", data=f
            )
