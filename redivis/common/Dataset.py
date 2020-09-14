import json
import requests

from .Table import *
from .api_request import make_request


class Dataset:
    def __init__(self, identifier, properties=None):
        self.uri = f"/datasets/{identifier}"
        self.properties = properties

    def __getitem__(self, key):
        return self.properties[key]

    def __str__(self):
        return json.dumps(self.properties, indent=2)

    def exists(self):
        try:
            make_request(method="GET", path=self.uri)
        except requests.HTTPError as err:
            # TODO: check status code
            return False

        return True

    def get(self):
        self.properties = make_request(method="GET", path=self.uri)

        self.uri = self.properties["uri"]
        return self

    def update(self, name, public_access_level, description):
        self.properties = make_request(
            method="POST",
            path=self.uri,
            payload={
                "name": name,
                "publicAccessLevel": public_access_level,
                "description": description,
            },
        )
        return self

    def list_tables(self, max_results):
        # TODO
        return

    def list_versions(self, max_results):
        # TODO
        return

    def create_version(self, ignore_if_exists=False):
        if not self.properties or not hasattr(self.properties, "nextVersion"):
            self.get()

        if not self.properties["nextVersion"]:
            response = make_request(method="POST", path=f"{self.uri}/versions")
            return Dataset(response["datasetUri"].replace("/datasets/", "")).get()
        elif ignore_if_exists:
            return Dataset(
                self.properties["nextVersion"]["datasetUri"].replace("/datasets/", "")
            ).get()
        else:
            raise Exception(
                f"Next version already exists at {self.properties['nextVersion']['datasetUri']}. To avoid this error, set argumeent ignore_if_exists to True"
            )

    def release(self):
        response = make_request(
            method="POST",
            path=f"{self.uri}/versions/next/release",
        )
        self.uri = response["datasetUri"]
        return self.get()

    def create_table(self, *, name, description, upload_merge_strategy):
        response = make_request(
            method="POST",
            path=f"{self.uri}/tables",
            payload={
                "name": name,
                "description": description,
                "uploadMergeStrategy": upload_merge_strategy,
            },
        )
        return Table(response["uri"].replace("/tables/", ""), response)

    def delete(self):
        make_request(
            method="DELETE",
            path=self.uri,
        )
        return


def create_dataset(
    *, user=None, organization=None, name, public_access_level, description=None
):
    if user:
        path = f"/users/{user}/datasets"
    elif organization:
        path = f"/organizations/{organization}/datasets"
    else:
        raise Exception("Must provide a user or organization")

    response = make_request(
        method="POST",
        path=path,
        payload={
            "name": name,
            "publicAccessLevel": public_access_level,
            "description": description,
        },
    )

    return Dataset(
        identifier=response["uri"].replace("/datasets/", ""), properties=response
    )
