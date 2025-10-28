from .Base import Base
from ..common.api_request import make_request
import os


class Parameter(Base):
    def __init__(self, name, *, workflow=None, properties=None):
        from .Workflow import Workflow  # avoid circular import

        if not workflow:
            if len(name.split(".")) == 3:
                workflow = Workflow(".".join(name.split(".")[0:2]))
                name = name.split(".")[-1]
            elif os.getenv("REDIVIS_DEFAULT_WORKFLOW"):
                workflow = Workflow(os.getenv("REDIVIS_DEFAULT_WORKFLOW"))
            else:
                raise Exception(
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
            else f"{workflow.uri}/parameters/{name}"
        )

    def exists(self):
        try:
            make_request(method="HEAD", path=self.uri)
            return True
        except Exception as err:
            if err.args[0]["status"] != 404:
                raise err
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

        self.properties = make_request(path=self.uri, method="PATCH")
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
