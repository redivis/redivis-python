import json


from .methods import list_tables
from .api_request import make_request


class Dataset:
    def __init__(self, identifier, properties=None):
        self.identifier = identifier
        self.properties = properties

    def __getitem__(self, key):
        return self.properties[key]

    def __str__(self):
        return json.dumps(self.properties, indent=2)

    def exists(self):
        return

    def get(self):
        self.properties = make_request(
            method="GET", path=f"/datasets/{self.identifier}"
        )
        return

    def list_tables(self, max_results):
        return list_tables(dataset=self.identifer)

    def list_versions(self, max_results):
        return

    def create_next_version(self, ignore_if_exists=False, upload_merge_strategy=None):
        return

    def release_next_version(self):
        return

    def create_table(self):
        return

    def delete(self):
        return


class Version:
    def __init__(self, properties):
        for key in properties:
            self[key] = properties[key]
