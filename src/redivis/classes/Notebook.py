from .Table import Table
from .Base import Base
from ..common.api_request import make_request
from tqdm.auto import tqdm
import time
import logging

import pathlib
import os
from urllib.parse import quote as quote_uri

from ..common.retryable_upload import perform_resumable_upload, perform_standard_upload
from ..common.util import convert_data_to_parquet


class Notebook(Base):
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
                    "Invalid notebook specifier, must be the fully qualified reference if no workflow is specified"
                )

        if isinstance(workflow, str):
            workflow = Workflow(workflow)

        self.workflow = workflow
        self.name = name

        self.qualified_reference = (properties or {}).get(
            "qualifiedReference", f"{self.workflow.qualified_reference}.{self.name}"
        )
        self.scoped_reference = (properties or {}).get("scopedReference", self.name)
        self.uri = (properties or {}).get(
            "uri", f"/notebooks/{quote_uri(self.qualified_reference, '')}"
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

    def run(self, *, wait_for_finish=True):
        self.properties = make_request(
            method="POST",
            path=f"{self.uri}/run",
        )
        self.uri = self.properties["uri"]

        if wait_for_finish:
            while True:
                time.sleep(2)
                self.get()
                if self.properties.get("currentJob") and self.properties["currentJob"][
                    "status"
                ] in ["completed", "failed"]:
                    if self.properties["currentJob"]["status"] == "failed":
                        raise Exception(self.properties["currentJob"]["errorMessage"])
                    break
                elif self.properties.get("currentJob"):
                    logging.debug("Notebook is still in progress...")
                else:
                    break

        return self

    def stop(self):
        self.properties = make_request(
            method="POST",
            path=f"{self.uri}/stop",
        )
        self.uri = self.properties["uri"]
        return self

    def source_tables(self):
        self.get()
        return [
            Table(source_table["qualifiedReference"], properties=source_table)
            for source_table in self.properties["sourceTables"]
        ]

    def output_table(self):
        self.get()
        return Table(
            self.properties["outputTable"]["qualifiedReference"],
            properties=self.properties["outputTable"],
        )

    def create_output_table(
        self, data=None, *, name=None, append=False, geography_variables=None
    ):
        if not self.properties.get("currentJob"):
            self.get()

        current_notebook_job_id = self.properties.get("currentJob")["id"]

        should_remove_tempfile = True
        temp_file_path = None
        try:
            if isinstance(data, str) or isinstance(data, pathlib.PurePath):
                data = str(data)
                if data.endswith(".parquet"):
                    should_remove_tempfile = False
                    temp_file_path = data
                else:
                    raise Exception(
                        "Only paths to parquet files (ending in .parquet) are supported when a string argument is provided"
                    )
            else:
                temp_file_path = convert_data_to_parquet(data)

            size = os.stat(temp_file_path).st_size
            pbar_bytes = tqdm(total=size, unit="B", leave=False, unit_scale=True)

            res = make_request(
                method="POST",
                path=f"/notebookJobs/{current_notebook_job_id}/tempUploads",
                payload={"tempUploads": [{"size": size, "resumable": size > 5e7}]},
            )
            temp_upload = res["results"][0]

            with open(temp_file_path, "rb") as f:
                if temp_upload["resumable"]:
                    perform_resumable_upload(
                        data=f,
                        progressbar=pbar_bytes,
                        proxy_url=f"{os.getenv('REDIVIS_API_ENDPOINT')}/notebookJobs/{current_notebook_job_id}/tempUploadProxy",
                        temp_upload_url=temp_upload["url"],
                    )
                else:
                    perform_standard_upload(
                        data=f,
                        temp_upload_url=temp_upload["url"],
                        proxy_url=f"{os.getenv('REDIVIS_API_ENDPOINT')}/notebookJobs/{current_notebook_job_id}/tempUploadProxy",
                        progressbar=pbar_bytes,
                    )

            pbar_bytes.close()
            if should_remove_tempfile:
                os.remove(temp_file_path)

            res = make_request(
                method="PUT",
                path=f"/notebookJobs/{current_notebook_job_id}/outputTable",
                payload={
                    "name": name,
                    "append": append,
                    "geographyVariables": geography_variables,
                    "tempUploadId": temp_upload["id"],
                },
            )

            return Table(name=res["name"], properties=res)
        except Exception as e:
            if (
                temp_file_path
                and os.path.exists(temp_file_path)
                and should_remove_tempfile
            ):
                os.remove(temp_file_path)
            raise e
