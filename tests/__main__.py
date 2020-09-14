import os, sys, traceback

os.environ["REDIVIS_API_ENDPOINT"] = "https://localhost:8443/api/v1"

import logging
from redivis import create_dataset, Dataset, Table

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

dataset_name = "some random name"

try:
    dataset = create_dataset(
        user="imathews", name=dataset_name, public_access_level="none"
    )

    print(dataset)

    table = dataset.create_table(
        name="A table", description="Some info", upload_merge_strategy="replace"
    )

    print(table)

    table.create_upload(name="tiny.csv", type="delimited", data="a,b\n1,2\n3,4")
    with open(os.path.join("./tests", "tiny.csv"), "rb") as f:
        table.create_upload(name="tiny.csv", type="delimited", data=f)

    dataset.release()

    dataset.create_version()

    table = Table(f"imathews.{dataset_name}:next.a_table")

    with open(os.path.join("./tests", "tiny.csv"), "rb") as f:
        table.create_upload(name="tiny.csv", type="delimited", data=f)

    dataset.release()

except Exception as e:
    traceback.print_exc(file=sys.stdout)
    print(e)

print("Deleting dataset")
# Dataset(f"imathews.{dataset_name}").delete()
