import warnings
from .Dataset import Dataset
from urllib.parse import quote as quote_uri
from ..common.api_request import make_paginated_request

class Organization:
    def __init__(self, name):
        self.name = name
        self.uri = f"/organizations/{quote_uri(self.name)}"

    def dataset(self, name, *, version="current"):
        return Dataset(name, organization=self, version=version)

    def list_datasets(self, max_results=None):
        datasets = make_paginated_request(
            path=f"{self.uri}/datasets", page_size=100, max_results=max_results
        )
        return [
            Dataset(dataset["name"], organization=self, properties=dataset) for dataset in datasets
        ]
