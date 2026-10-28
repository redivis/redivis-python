from .Base import Base
from ..common import exceptions
from ..common.api_request import make_request
from ..common.util import split_reference
import os
from ..common.config import get_config
from urllib.parse import quote as quote_uri


class Parameter(Base):
    def __init__(self, name, *, workflow=None, properties=None):
        from .Workflow import Workflow  # avoid circular import

        if not workflow:
            if name is not None and len(split_reference(name)) == 3:
                parts = split_reference(name)
                workflow = Workflow(".".join(parts[0:2]))
                name = parts[-1]
            elif get_config("default_workflow"):
                workflow = Workflow(get_config("default_workflow"))
            else:
                raise exceptions.ValueError(
                    "Invalid parameter specifier, must be the fully qualified reference if no workflow is specified"
                )

        if isinstance(workflow, str):
            workflow = Workflow(workflow)

        self.workflow = workflow
        self.name = name

        self.properties = properties
        self.uri = (
            properties["uri"]
            if "uri" in (properties or {})
            else f"{workflow.uri}/parameters/{quote_uri(name, '')}"
        )

    def __repr__(self):
        return f"<Parameter @{self.workflow.qualified_reference}.{self.name}>"

    def exists(self):
        try:
            make_request(method="HEAD", path=self.uri)
            return True
        except exceptions.NotFoundError:
            return False

    def get(self):
        self.properties = make_request(path=self.uri)
        self.uri = self.properties["uri"]
        return self

    def get_values(self):
        self.get()
        return self.properties["values"]

    def delete(self):
        make_request(path=self.uri, method="DELETE")
        return

    def update(self, *, name=None, type=None, values=None):
        payload = {}
        if name is not None:
            payload["name"] = name
        if type is not None:
            payload["type"] = type
        if values is not None:
            payload["values"] = values

        self.properties = make_request(path=self.uri, method="PATCH", payload=payload)
        self.uri = self.properties["uri"]
        return self

    def create(self, *, type=None, values):
        payload = {"name": self.name, "values": values}

        if type is not None:
            payload["type"] = type

        self.properties = make_request(
            path=f"{self.workflow.uri}/parameters", method="POST", payload=payload
        )
        self.uri = self.properties["uri"]
        return self
