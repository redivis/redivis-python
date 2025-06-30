from .Table import Table
from .Base import Base
from ..common.api_request import make_request

from urllib.parse import quote as quote_uri
import os


class Datasource(Base):
    def __init__(self, source, *, workflow=None, properties=None):
        from .Workflow import Workflow
        from .Dataset import Dataset

        if not workflow:
            if os.getenv("REDIVIS_DEFAULT_WORKFLOW"):
                workflow = Workflow(os.getenv("REDIVIS_DEFAULT_WORKFLOW"))
            else:
                raise Exception(
                    "REDIVIS_DEFAULT_WORKFLOW must be set if no workflow is specified in the Datasource constructor"
                )
        if isinstance(source, Dataset) or isinstance(source, Workflow):
            source = source.qualified_reference

        self.workflow = workflow
        self.uri = (properties or {}).get(
            "uri", f"{workflow.uri}/dataSources/{quote_uri(source)}"
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

    def update(
        self,
        *,
        source_dataset=None,
        source_workflow=None,
        sample=None,
        version=None,
        mapped_tables=None,
    ):
        # avoid circular import
        from .Workflow import Workflow
        from .Dataset import Dataset
        from .Version import Version

        if isinstance(source_workflow, Workflow):
            source_workflow = source_workflow.qualified_reference
        if isinstance(source_dataset, Dataset):
            source_dataset = source_dataset.qualified_reference
        if isinstance(mapped_tables, list):
            mapped_tables_dict = {}
            for source, dest in mapped_tables:
                if isinstance(source, Table):
                    source = source.scoped_reference
                if isinstance(dest, Table):
                    dest = dest.scoped_reference
                mapped_tables_dict[source] = dest
            mapped_tables = mapped_tables_dict

        if sample is not None or version is not None:
            if not source_dataset and not source_workflow:
                self.get()
                source_dataset = (self.properties.get("sourceDataset") or {}).get(
                    "qualifiedReference"
                )
            if not source_dataset or source_workflow:
                if sample:
                    raise Exception(
                        "Sampling is not applicable to datasources that reference a workflow"
                    )
                if version:
                    raise Exception(
                        "Versions are not applicable to datasources that reference a workflow"
                    )

        if isinstance(version, Version):
            version = version.tag

        source_dataset = Dataset(source_dataset, version=version).qualified_reference

        if sample == False:
            source_dataset.replace(":sample", "")
        elif sample == True and not ":sample" in source_dataset:
            source_dataset += ":sample"

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
