import warnings
from urllib.parse import quote as quote_uri
from .Dataset import Dataset
from .Project import Project
from ..common.api_request import make_paginated_request

class User:
    def __init__(self, name):
        self.name = name
        self.uri = f"/users/{quote_uri(self.name)}"

    def dataset(self, name, *, version="current"):
        return Dataset(name, user=self, version=version)

    def project(self, name):
        return Project(name, user=self)

    def list_datasets(self, max_results=None):
        datasets = make_paginated_request(
            path=f"{self.uri}/datasets", page_size=100, max_results=max_results
        )
        return [
            Dataset(dataset["name"], user=self, properties=dataset) for dataset in datasets
        ]