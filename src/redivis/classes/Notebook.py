from .Table import Table
from .Base import Base
from ..common.api_request import make_request
import pathlib
import uuid
import os
import geopandas


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

            if isinstance(data, geopandas.GeoDataFrame):
                if geography_variables is None:
                    geography_variables = list(data.select_dtypes('geometry'))
                data = data.to_wkt()

            data.to_parquet(path=temp_file_path)

        with open(temp_file_path, 'rb') as f:
            res = make_request(
                method="PUT",
                path=f"/notebookJobs/{self.current_notebook_job_id}/outputTable",
                query={"name": name, "append": append, geography_variables: geography_variables},
                payload=f,
                parse_payload=False
            )

        os.remove(temp_file_path)

        return Table(name=res["name"], properties=res)
