from .Dataset import Dataset
from .Member import Member
from .Base import Base
from .Secret import Secret

from urllib.parse import quote as quote_uri
from ..common.api_request import make_paginated_request


class Organization(Base):
    def __init__(self, name):
        self.name = name
        self.uri = f"/organizations/{quote_uri(self.name, '')}"
        self.properties = {"kind": "organization", "uri": self.uri, "userName": name}

    def dataset(self, name, *, version=None):
        return Dataset(name, organization=self, version=version)

    def member(self, name):
        return Member(name, organization=self)

    def secret(self, name):
        return Secret(name, organization=self)

    def list_datasets(self, max_results=None, labels=None):
        query = {}
        if labels:
            query["labels"] = ",".join(labels)

        datasets = make_paginated_request(
            path=f"{self.uri}/datasets",
            page_size=100,
            max_results=max_results,
            query=query,
        )
        return [
            Dataset(dataset["name"], organization=self, properties=dataset)
            for dataset in datasets
        ]

    def list_members(self, max_results=None, labels=None, statuses=None):
        query = {}
        if labels:
            query["labels"] = ",".join(labels)
        if statuses:
            query["statuses"] = ",".join(statuses)

        members = make_paginated_request(
            path=f"{self.uri}/members",
            page_size=500,
            max_results=max_results,
            query=query,
        )
        return [
            Member(member["user"]["name"], organization=self, properties=member)
            for member in members
        ]
