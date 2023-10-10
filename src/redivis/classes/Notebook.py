from .Table import Table
from .Base import Base
from ..common.api_request import make_request
import pathlib
import uuid
import os
import pyarrow as pa
import pyarrow.dataset as pa_dataset
import pyarrow.parquet as pa_parquet
import pandas as pd

class Notebook(Base):
    def __init__(
        self,
        current_notebook_job_id,
    ):
        self.current_notebook_job_id = current_notebook_job_id

    def create_output_table(self, data=None, *, name=None, append=False, geography_variables=None):
        if type(data) == str:
            temp_file_path = str
        else:
            temp_file_path = f"/tmp/redivis/out/{uuid.uuid4()}"
            pathlib.Path(temp_file_path).parent.mkdir(exist_ok=True, parents=True)
            
            import geopandas

            if isinstance(data, geopandas.GeoDataFrame):
                if geography_variables is None:
                    geography_variables = list(data.select_dtypes('geometry'))
                data.to_wkt().to_parquet(path=temp_file_path, index=False)
            elif isinstance(data, pd.DataFrame):
                data.to_parquet(path=temp_file_path, index=False)
            elif isinstance(data, pa_dataset.Dataset):
                pa_dataset.write_dataset(data, temp_file_path, format='parquet', basename_template='part-{i}.parquet', max_partitions=1)
                temp_file_path = f'{temp_file_path}/part-0.parquet'
            elif isinstance(data, pa.Table):
                pa_parquet.write_table(data, temp_file_path)
            else:
                # importing polars is causing an IllegalInstruction error on ARM + Docker. Import inline to avoid crashes elsewhwere
                # TODO: revert once fixed upstream
                import polars
                if isinstance(data, polars.LazyFrame):
                    data.sink_parquet(temp_file_path)
                elif isinstance(data, polars.DataFrame):
                    data.write_parquet(temp_file_path)
                else:
                    raise Exception('Unknown datatype provided to notebook.create_output_table. Must either by a file path, or an instance of pandas.DataFrame, pyarrow.Dataset, pyarrow.Table, dask.DataFrame, polars.LazyFrame, or polars.DataFrame')

        with open(temp_file_path, 'rb') as f:
            res = make_request(
                method="PUT",
                path=f"/notebookJobs/{self.current_notebook_job_id}/outputTable",
                query={"name": name, "append": append, "geographyVariables": geography_variables},
                payload=f,
                parse_payload=False
            )

        if type(data) != str:
            os.remove(temp_file_path)

        return Table(name=res["name"], properties=res)
