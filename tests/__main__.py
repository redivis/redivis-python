import os
import logging
import redivis

os.environ["REDIVIS_API_ENDPOINT"] = "https://localhost:8443/api/v1"
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


dataset = redivis.Dataset("imathews.test")
dataset.get()

print(dataset)

print(dataset["version"]["isReleased"])
#
#
# redivis.list_datasets(owner="imathews")
# redivis.list_tables(dataset="imathews.optum")
# redivis.list_tables(project="imathews.my_project")
#
# dataset = redivis.get_dataset(identifier="imathews.optum")
#
# dataset = redivis.Dataset("imathews.optum").exists()
#
# table = redivis.get_table(identifier="imathews.optum.test_table")
#
#
# table.clear_uploads()
# table.create_upload(autocreate_next_version=True)
#
# if "next_version" not in dataset:
#     dataset.create_next_version(ignore_if_exists=True)
#
# dataset.get()
#
#
# dataset.exists()
# dataset.list_tables(max_results=10)
# dataset.create_table
#
#
# dataset = redivis.create_dataset(name="dataset", public_access_level="none")
