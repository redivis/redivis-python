from .Base import Base
import os
import time
import warnings
from .Variable import Variable
from .File import File
from pathlib import Path
import tempfile

from ..common.api_request import make_request, make_paginated_request
from ..common.list_rows import list_rows
from ..common.util import get_geography_variable, get_warning, arrow_table_to_pandas


class Query(Base):
    def __init__(
        self,
        query,
        *,
        default_workflow=None,
        default_dataset=None,
    ):
        self.did_initiate = False
        if not default_workflow and not default_dataset:
            if os.getenv("REDIVIS_DEFAULT_WORKFLOW"):
                default_workflow = os.getenv("REDIVIS_DEFAULT_WORKFLOW")
            elif os.getenv("REDIVIS_DEFAULT_DATASET"):
                default_dataset = os.getenv("REDIVIS_DEFAULT_DATASET")

        self.payload = {"query": query}
        if default_workflow:
            self.payload["defaultWorkflow"] = default_workflow
        if default_dataset:
            self.payload["defaultDataset"] = default_dataset

    def get(self):
        self._initiate()
        self.properties = make_request(method="GET", path=self.uri)
        return self

    # def dry_run(self):
    #   TODO

    def to_arrow_dataset(
        self,
        max_results=None,
        *,
        progress=True,
        batch_preprocessor=None,
        max_parallelization=os.cpu_count(),
    ):
        self._wait_for_finish()

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            mapped_variables=self.properties["outputSchema"],
            output_type="arrow_dataset",
            progress=progress,
            coerce_schema=False,
            batch_preprocessor=batch_preprocessor,
            max_parallelization=max_parallelization,
        )

    def to_arrow_table(
        self,
        max_results=None,
        *,
        progress=True,
        batch_preprocessor=None,
        max_parallelization=os.cpu_count(),
    ):
        self._wait_for_finish()

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            mapped_variables=self.properties["outputSchema"],
            output_type="arrow_table",
            progress=progress,
            coerce_schema=False,
            batch_preprocessor=batch_preprocessor,
            max_parallelization=max_parallelization,
        )

    def to_polars_lazyframe(
        self,
        max_results=None,
        *,
        progress=True,
        batch_preprocessor=None,
        max_parallelization=os.cpu_count(),
    ):
        self._wait_for_finish()

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            mapped_variables=self.properties["outputSchema"],
            output_type="polars_lazyframe",
            progress=progress,
            coerce_schema=False,
            batch_preprocessor=batch_preprocessor,
            max_parallelization=max_parallelization,
        )

    def to_dask_dataframe(
        self,
        max_results=None,
        *,
        progress=True,
        batch_preprocessor=None,
        max_parallelization=os.cpu_count(),
    ):
        self._wait_for_finish()

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            mapped_variables=self.properties["outputSchema"],
            output_type="dask_dataframe",
            progress=progress,
            coerce_schema=False,
            batch_preprocessor=batch_preprocessor,
            max_parallelization=max_parallelization,
        )

    def to_pandas_dataframe(
        self,
        max_results=None,
        *,
        progress=True,
        dtype_backend="pyarrow",
        date_as_object=False,
        batch_preprocessor=None,
        max_parallelization=os.cpu_count(),
    ):
        self._wait_for_finish()

        arrow_table = list_rows(
            uri=self.uri,
            max_results=max_results,
            mapped_variables=self.properties["outputSchema"],
            output_type="arrow_table",
            progress=progress,
            coerce_schema=False,
            batch_preprocessor=batch_preprocessor,
            max_parallelization=max_parallelization,
        )

        return arrow_table_to_pandas(
            arrow_table, dtype_backend, date_as_object, max_parallelization
        )

    def to_geopandas_dataframe(
        self,
        max_results=None,
        *,
        geography_variable="",
        progress=True,
        dtype_backend="pyarrow",
        date_as_object=False,
        batch_preprocessor=None,
        max_parallelization=os.cpu_count(),
    ):
        import geopandas

        self._wait_for_finish()

        arrow_table = list_rows(
            uri=self.uri,
            max_results=max_results,
            mapped_variables=self.properties["outputSchema"],
            output_type="arrow_table",
            progress=progress,
            coerce_schema=False,
            batch_preprocessor=batch_preprocessor,
            max_parallelization=max_parallelization,
        )

        df = arrow_table_to_pandas(
            arrow_table, dtype_backend, date_as_object, max_parallelization
        )

        if geography_variable is not None:
            geography_variable = get_geography_variable(
                self.properties["outputSchema"], geography_variable
            )
            if geography_variable is None:
                raise Exception(
                    'Unable to find a variable with type=="geography" in the query results'
                )

        df[geography_variable["name"]] = geopandas.GeoSeries.from_wkt(
            df[geography_variable["name"]]
        )
        df = geopandas.GeoDataFrame(
            data=df, geometry=geography_variable["name"], crs="EPSG:4326"
        )

        return df

    def to_dataframe(self, max_results=None, *, geography_variable="", progress=True):
        warnings.warn(get_warning("dataframe_deprecation"), FutureWarning, stacklevel=2)
        self._wait_for_finish()

        arrow_table = list_rows(
            uri=self.uri,
            max_results=max_results,
            mapped_variables=self.properties["outputSchema"],
            output_type="arrow_table",
            progress=progress,
            coerce_schema=False,
        )

        df = arrow_table.to_pandas(self_destruct=True)

        if geography_variable is not None:
            geography_variable = get_geography_variable(
                self.properties["outputSchema"], geography_variable
            )

        if geography_variable is not None:
            import geopandas

            warnings.warn(
                get_warning("geodataframe_deprecation"), FutureWarning, stacklevel=2
            )
            df[geography_variable["name"]] = geopandas.GeoSeries.from_wkt(
                df[geography_variable["name"]]
            )
            df = geopandas.GeoDataFrame(
                data=df, geometry=geography_variable["name"], crs="EPSG:4326"
            )

        return df

    def to_arrow_batch_iterator(
        self, max_results=None, *, variables=None, progress=True
    ):
        self._wait_for_finish()

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=self.properties["outputSchema"],
            output_type="arrow_iterator",
            progress=progress,
            coerce_schema=False,
        )

    def list_rows(self, max_results=None, *, progress=True):
        warnings.warn(
            "The list_rows method is deprecated. Please use table.to_arrow_batch_iterator() or table.to_arrow_table().to_pylist() for better performance and memory utilization.",
            FutureWarning,
            stacklevel=2,
        )

        self._wait_for_finish()

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            mapped_variables=self.properties["outputSchema"],
            output_type="tuple",
            progress=progress,
            coerce_schema=False,
        )

    def variable(self, name):
        # TODO: dry run (?) + cache variables
        self._wait_for_finish()
        return Variable(name, query=self)

    def list_variables(self, *, max_results=None):
        # TODO: dry run (?) + cache variables
        self._wait_for_finish()
        variables = make_paginated_request(
            path=f"{self.uri}/variables", page_size=1000, max_results=max_results
        )
        return [
            Variable(variable["name"], query=self, properties=variable)
            for variable in variables
        ]

    def to_directory(
        self, *, file_id_variable="file_id", file_name_variable="file_name"
    ):
        from .Directory import Directory
        import pyarrow

        self._wait_for_finish()

        res = make_request(
            method="get",
            path=f"{self.uri}/rawFiles",
            query={
                "format": "arrow",
                "fileIdVariable": file_id_variable,
                "fileNameVariable": file_name_variable,
            },
            stream=True,
            parse_response=False,
        )

        directory = Directory(path=Path(""), query=self)

        for file_spec in (
            pyarrow.ipc.RecordBatchStreamReader(res.raw).read_all().to_pylist()
        ):
            directory._add_file(
                File(
                    file_spec[file_id_variable],
                    file_spec[file_name_variable],
                    query=self,
                    properties=file_spec,
                    directory=directory,
                )
            )

        self.directory = directory
        return self.directory

    def file(self, path):
        if not self.directory:
            self.to_directory()

        return self.directory.get(path)

    def list_files(
        self, max_results=None, *, file_id_variable=None, file_name_variable=None
    ):
        warnings.warn(
            "This method is deprecated. Please use query.to_directory().list_files() instead",
            FutureWarning,
            stacklevel=2,
        )
        if not self.directory:
            self.to_directory(file_id_variable=file_id_variable)

        return self.directory.list_files(recursive=True, max_results=max_results)

    def download_files(
        self,
        path=None,
        overwrite=False,
        max_results=None,
        file_id_variable=None,
        file_name_variable=None,
        progress=True,
        max_parallelization=None,
    ):
        warnings.warn(
            "This method is deprecated. Please use query.to_directory().download_files() instead",
            FutureWarning,
            stacklevel=2,
        )
        if not self.directory:
            self.to_directory(
                file_id_variable=file_id_variable, file_name_variable=file_name_variable
            )

        return self.directory.download_files(
            path=path,
            max_results=max_results,
            overwrite=overwrite,
            max_parallelization=max_parallelization,
            progress=progress,
        )

    def to_stata(self):
        import pyarrow as pa
        from pystata import stata

        with tempfile.TemporaryDirectory() as tmpdirname:
            load_script_res = make_request(
                method="GET",
                path=f"{self.uri}/script",
                query={"type": "stata", "filePath": f"{tmpdirname}/part-0.csv"},
                parse_response=False,
            )
            ds = self.to_arrow_dataset()
            pa.dataset.write_dataset(
                ds,
                base_dir=tmpdirname,
                existing_data_behavior="overwrite_or_ignore",
                basename_template="part-{i}.csv",
                format="csv",
            )
            stata.run("clear")
            stata.run(load_script_res.text, quietly=True)
            stata.run("describe")

    def to_sas(self, name=None):
        if name is None:
            raise Exception(
                'A SAS dataset name must be provided. E.g., query.to_sas("mydata")'
            )
        import pyarrow as pa
        import saspy
        from IPython import get_ipython

        ip = get_ipython()

        with tempfile.TemporaryDirectory() as tmpdirname:
            load_script_res = make_request(
                method="GET",
                path=f"{self.uri}/script",
                query={
                    "type": "sas",
                    "filePath": f"{tmpdirname}/part-0.csv",
                    "sasDatasetName": name,
                },
                parse_response=False,
            )
            ds = self.to_arrow_dataset()
            pa.dataset.write_dataset(
                ds,
                base_dir=tmpdirname,
                existing_data_behavior="overwrite_or_ignore",
                basename_template="part-{i}.csv",
                format="csv",
            )
            ip.run_cell_magic("SAS", "", load_script_res.text)

    def _initiate(self):
        if not self.did_initiate:
            self.did_initiate = True
            self.properties = make_request(
                method="post",
                path="/queries",
                payload=self.payload,
            )
            self.uri = self.properties["uri"]

    def _wait_for_finish(self):
        self._initiate()
        while True:
            if self.properties["status"] == "completed":
                break
            elif self.properties["status"] == "failed":
                raise Exception(
                    f"Query job failed with message: {self.properties['errorMessage']}"
                )
            elif self.properties["status"] == "cancelled":
                raise Exception(f"Query job was cancelled")
            else:
                time.sleep(2)
                self.get()
