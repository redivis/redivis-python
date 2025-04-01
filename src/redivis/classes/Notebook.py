from .Table import Table
from .Base import Base
from ..common.api_request import make_request
from tqdm.auto import tqdm

import pathlib
import os

from ..common.retryable_upload import perform_resumable_upload, perform_standard_upload
from ..common.util import convert_data_to_parquet


class Notebook(Base):
    def __init__(
        self,
        current_notebook_job_id,
    ):
        self.current_notebook_job_id = current_notebook_job_id

    def create_output_table(
        self, data=None, *, name=None, append=False, geography_variables=None
    ):
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
                path=f"/notebookJobs/{self.current_notebook_job_id}/tempUploads",
                payload={"tempUploads": [{"size": size, "resumable": size > 5e7}]},
            )
            temp_upload = res["results"][0]

            with open(temp_file_path, "rb") as f:
                if temp_upload["resumable"]:
                    perform_resumable_upload(
                        data=f,
                        progressbar=pbar_bytes,
                        proxy_url=f"{os.getenv('REDIVIS_API_ENDPOINT')}/notebookJobs/{self.current_notebook_job_id}/tempUploadProxy",
                        temp_upload_url=temp_upload["url"],
                    )
                else:
                    perform_standard_upload(
                        data=f,
                        temp_upload_url=temp_upload["url"],
                        proxy_url=f"{os.getenv('REDIVIS_API_ENDPOINT')}/notebookJobs/{self.current_notebook_job_id}/tempUploadProxy",
                        progressbar=pbar_bytes,
                    )

            pbar_bytes.close()
            if should_remove_tempfile:
                os.remove(temp_file_path)

            res = make_request(
                method="PUT",
                path=f"/notebookJobs/{self.current_notebook_job_id}/outputTable",
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
