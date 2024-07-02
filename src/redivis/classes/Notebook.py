from .Table import Table
from .Base import Base
from ..common.api_request import make_request
from tqdm.auto import tqdm

import pathlib
import uuid
import os

from ..common.retryable_upload import perform_resumable_upload, perform_standard_upload


class Notebook(Base):
    def __init__(
        self,
        current_notebook_job_id,
    ):
        self.current_notebook_job_id = current_notebook_job_id

    def create_output_table(
        self, data=None, *, name=None, append=False, geography_variables=None
    ):
        temp_file_path = f"/tmp/redivis/out/{uuid.uuid4()}"
        try:
            pathlib.Path(temp_file_path).parent.mkdir(exist_ok=True, parents=True)

            import geopandas
            import pandas as pd
            import pyarrow as pa
            import pyarrow.dataset as pa_dataset
            import pyarrow.parquet as pa_parquet
            from dask.dataframe import DataFrame as dask_df

            if isinstance(data, geopandas.GeoDataFrame):
                if geography_variables is None:
                    geography_variables = list(data.select_dtypes("geometry"))
                data.to_wkt().to_parquet(path=temp_file_path, index=False)
            elif isinstance(data, pd.DataFrame):
                data.to_parquet(path=temp_file_path, index=False)
            elif isinstance(data, pa_dataset.Dataset):
                pa_dataset.write_dataset(
                    data,
                    temp_file_path,
                    format="parquet",
                    basename_template="part-{i}.parquet",
                    max_partitions=1,
                )
                temp_file_path = f"{temp_file_path}/part-0.parquet"
            elif isinstance(data, pa.Table):
                pa_parquet.write_table(data, temp_file_path)
            elif isinstance(data, dask_df):
                data.to_parquet(temp_file_path, write_index=False)
                temp_file_path = f"{temp_file_path}/part.0.parquet"
            else:
                # importing polars is causing an IllegalInstruction error on ARM + Docker. Import inline to avoid crashes elsewhwere
                # TODO: revert once fixed upstream
                import polars

                if isinstance(data, polars.LazyFrame):
                    data.sink_parquet(temp_file_path)
                elif isinstance(data, polars.DataFrame):
                    data.write_parquet(temp_file_path)
                else:
                    raise Exception(
                        "Unknown datatype provided to notebook.create_output_table. Must either by a file path, or an instance of pandas.DataFrame, pyarrow.Dataset, pyarrow.Table, dask.DataFrame, polars.LazyFrame, or polars.DataFrame"
                    )

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
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            raise e
