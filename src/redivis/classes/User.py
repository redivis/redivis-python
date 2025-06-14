from urllib.parse import quote as quote_uri
from .Dataset import Dataset
from .Base import Base
from .Secret import Secret
from .Workflow import Workflow
from ..common.api_request import make_paginated_request
import warnings


class User(Base):
    def __init__(self, name, properties={}):
        self.name = name
        self.uri = f"/users/{quote_uri(self.name, '')}"
        self.properties = {
            **{"kind": "user", "userName": name, "uri": self.uri},
            **properties,
        }

    def dataset(self, name, *, version=None):
        return Dataset(name, user=self, version=version)

    def project(self, name):
        warnings.warn(
            "Projects have been renamed to Workflows, please update your code to: user.workflow()",
            FutureWarning,
            stacklevel=2,
        )
        return Workflow(name, user=self)

    def workflow(self, name):
        return Workflow(name, user=self)

    def secret(self, name):
        return Secret(name, user=self)

    def list_datasets(self, max_results=None):
        datasets = make_paginated_request(
            path=f"{self.uri}/datasets", page_size=100, max_results=max_results
        )
        return [
            Dataset(dataset["name"], user=self, properties=dataset)
            for dataset in datasets
        ]

    def list_projects(self, max_results=None):
        warnings.warn(
            "Projects have been renamed to Workflows, please update your code to: user.list_workflows()",
            FutureWarning,
            stacklevel=2,
        )
        return self.list_workflows(max_results)

    def list_workflows(self, max_results=None):
        workflows = make_paginated_request(
            path=f"{self.uri}/workflows", page_size=100, max_results=max_results
        )
        return [
            Workflow(workflow["name"], user=self, properties=workflow)
            for workflow in workflows
        ]
