from .Table import Table
from .Base import Base
from ..common.api_request import make_request

from urllib.parse import quote as quote_uri
import os


class DataSource(Base):
    def __init__(self, reference, *, workflow=None, properties=None):
        if not workflow:
            from .Workflow import Workflow

            if os.getenv("REDIVIS_DEFAULT_WORKFLOW"):
                workflow = Workflow(os.getenv("REDIVIS_DEFAULT_WORKFLOW"))
            else:
                raise Exception(
                    "REDIVIS_DEFAULT_WORKFLOW must be set if no workflow is specified in the DataSource constructor"
                )
        self.workflow = workflow
        self.uri = (properties or {}).get(
            "uri", f"{workflow.uri}/dataSources/{quote_uri(reference)}"
        )
        self.properties = properties

    def get(self):
        self.properties = make_request(
            method="GET",
            path=self.uri,
        )
        self.uri = self.properties["uri"]

        return self

    def exists(self):
        try:
            make_request(method="HEAD", path=self.uri)
            return True
        except Exception as err:
            if err.args[0]["status"] != 404:
                raise err
            return False

    def update(self, *, source_dataset=None, source_workflow=None, mapped_tables=None):
        # avoid circular import
        from .Workflow import Workflow
        from .Dataset import Dataset

        if isinstance(source_workflow, Workflow):
            source_workflow = source_workflow.qualified_reference
        if isinstance(source_dataset, Dataset):
            source_dataset = source_dataset.qualified_reference
        if isinstance(mapped_tables, list):
            mapped_tables_dict = {}
            for source, dest in mapped_tables:
                if isinstance(source, Table):
                    source = source.qualified_reference
                if isinstance(dest, Table):
                    dest = dest.qualified_reference
                mapped_tables_dict[source] = dest
            mapped_tables = mapped_tables_dict

        self.properties = make_request(
            method="PATCH",
            path=self.uri,
            payload={
                "sourceDataset": source_dataset,
                "sourceWorkflow": source_workflow,
                "mappedTables": mapped_tables,
            },
        )
        self.uri = self.properties["uri"]

        return self
