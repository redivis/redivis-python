from .Base import Base
import os
import time
import warnings
import pyarrow as pa
import pandas as pd

from ..common.api_request import make_request
from ..common.list_rows import list_rows
from ..common.util import get_geography_variable, get_warning


class Query(Base):
    def __init__(
        self,
        query,
        *,
        default_project=None,
        default_dataset=None,
    ):
        if not default_project and not default_dataset:
            if os.getenv("REDIVIS_DEFAULT_PROJECT"):
                default_project = os.getenv("REDIVIS_DEFAULT_PROJECT")
            elif os.getenv("REDIVIS_DEFAULT_DATASET"):
                default_dataset = os.getenv("REDIVIS_DEFAULT_DATASET")

        self.properties = make_request(
            method="post",
            path="/queries",
            payload={
                "query": query,
                "defaultProject": default_project if default_project else None,
                "defaultDataset": default_dataset if default_dataset else None,
            },
        )
        self.uri = f"/queries/{self.properties['id']}"

    def get(self):
        self.properties = make_request(method="GET", path=self.uri)
        return self

    def to_arrow_dataset(self, max_results=None, *, progress=True, batch_preprocessor=None):
        self._wait_for_finish()

        return list_rows(
            uri=self.uri,
            max_results=self.properties["outputNumRows"] if max_results is None else min(max_results, int(
                self.properties["outputNumRows"])),
            mapped_variables=self.properties["outputSchema"],
            output_type="arrow_dataset",
            progress=progress,
            coerce_schema=False,
            batch_preprocessor=batch_preprocessor
        )

    def to_arrow_table(self, max_results=None, *, progress=True, batch_preprocessor=None):
        self._wait_for_finish()

        return list_rows(
            uri=self.uri,
            max_results=self.properties["outputNumRows"] if max_results is None else min(max_results, int(
                self.properties["outputNumRows"])),
            mapped_variables=self.properties["outputSchema"],
            output_type="arrow_table",
            progress=progress,
            coerce_schema=False,
            batch_preprocessor=batch_preprocessor
        )

    def to_polars_lazyframe(self, max_results=None, *, progress=True, batch_preprocessor=None):
        self._wait_for_finish()

        return list_rows(
            uri=self.uri,
            max_results=self.properties["outputNumRows"] if max_results is None else min(max_results, int(
                self.properties["outputNumRows"])),
            mapped_variables=self.properties["outputSchema"],
            output_type="polars_lazyframe",
            progress=progress,
            coerce_schema=False,
            batch_preprocessor=batch_preprocessor
        )

    def to_dask_dataframe(self, max_results=None, *, progress=True, batch_preprocessor=None):
        self._wait_for_finish()

        return list_rows(
            uri=self.uri,
            max_results=self.properties["outputNumRows"] if max_results is None else min(max_results, int(
                self.properties["outputNumRows"])),
            mapped_variables=self.properties["outputSchema"],
            output_type="dask_dataframe",
            progress=progress,
            coerce_schema=False,
            batch_preprocessor=batch_preprocessor
        )

    def to_pandas_dataframe(self, max_results=None, *, geography_variable="", progress=True, dtype_backend='pyarrow', date_as_object=False, batch_preprocessor=None):
        if dtype_backend not in ['numpy', 'numpy_nullable', 'pyarrow']:
            raise Exception(f"Unknown dtype_backend. Must be one of 'pyarrow'|'numpy_nullable'|'numpy'. Default is 'pyarrow'")

        self._wait_for_finish()

        arrow_table = list_rows(
            uri=self.uri,
            max_results=self.properties["outputNumRows"] if max_results is None else min(max_results, int(self.properties["outputNumRows"])),
            mapped_variables=self.properties["outputSchema"],
            output_type="arrow_table",
            progress=progress,
            coerce_schema=False,
            batch_preprocessor=batch_preprocessor
        )

        if dtype_backend == 'numpy_nullable':
            df = arrow_table.to_pandas(self_destruct=True, date_as_object=date_as_object, types_mapper={
                pa.int64(): pd.Int64Dtype(),
                pa.bool_(): pd.BooleanDtype(),
                pa.float64(): pd.Float64Dtype(),
                pa.string(): pd.StringDtype(),
            }.get)
        elif dtype_backend == 'pyarrow':
            df = arrow_table.to_pandas(self_destruct=True, types_mapper=pd.ArrowDtype)
        else:
            df = arrow_table.to_pandas(self_destruct=True, date_as_object=date_as_object)

        if geography_variable is not None:
            geography_variable = get_geography_variable(self.properties["outputSchema"], geography_variable)

        if geography_variable is not None:
            import geopandas
            df[geography_variable["name"]] = geopandas.GeoSeries.from_wkt(df[geography_variable["name"]])
            df = geopandas.GeoDataFrame(data=df, geometry=geography_variable["name"], crs="EPSG:4326")

        return df

    def to_geopandas_dataframe(self, max_results=None, *, geography_variable="", progress=True, dtype_backend='pyarrow', date_as_object=False, batch_preprocessor=None):
        import geopandas

        if dtype_backend not in ['numpy', 'numpy_nullable', 'pyarrow']:
            raise Exception(f"Unknown dtype_backend. Must be one of 'pyarrow'|'numpy_nullable'|'numpy'. Default is 'pyarrow'")

        self._wait_for_finish()

        arrow_table = list_rows(
            uri=self.uri,
            max_results=self.properties["outputNumRows"] if max_results is None else min(max_results, int(self.properties["outputNumRows"])),
            mapped_variables=self.properties["outputSchema"],
            output_type="arrow_table",
            progress=progress,
            coerce_schema=False,
            batch_preprocessor=batch_preprocessor
        )

        if dtype_backend == 'numpy_nullable':
            df = arrow_table.to_pandas(self_destruct=True, date_as_object=date_as_object, types_mapper={
                pa.int64(): pd.Int64Dtype(),
                pa.bool_(): pd.BooleanDtype(),
                pa.float64(): pd.Float64Dtype(),
                pa.string(): pd.StringDtype(),
            }.get)
        elif dtype_backend == 'pyarrow':
            df = arrow_table.to_pandas(self_destruct=True, types_mapper=pd.ArrowDtype)
        else:
            df = arrow_table.to_pandas(self_destruct=True, date_as_object=date_as_object)

        if geography_variable is not None:
            geography_variable = get_geography_variable(self.properties["outputSchema"], geography_variable)
            if geography_variable is None:
                raise Exception('Unable to find a variable with type=="geography" in the query results')

        df[geography_variable["name"]] = geopandas.GeoSeries.from_wkt(df[geography_variable["name"]])
        df = geopandas.GeoDataFrame(data=df, geometry=geography_variable["name"], crs="EPSG:4326")

        return df

    def to_dataframe(self, max_results=None, *, geography_variable="", progress=True):
        warnings.warn(get_warning('dataframe_deprecation'), FutureWarning, stacklevel=2)
        self._wait_for_finish()

        arrow_table = list_rows(
            uri=self.uri,
            max_results=self.properties["outputNumRows"] if max_results is None else min(max_results, int(
                self.properties["outputNumRows"])),
            mapped_variables=self.properties["outputSchema"],
            output_type="arrow_table",
            progress=progress,
            coerce_schema=False
        )

        df = arrow_table.to_pandas(self_destruct=True)

        if geography_variable is not None:
            geography_variable = get_geography_variable(self.properties["outputSchema"], geography_variable)

        if geography_variable is not None:
            import geopandas
            warnings.warn(get_warning('geodataframe_deprecation'), FutureWarning, stacklevel=2)
            df[geography_variable["name"]] = geopandas.GeoSeries.from_wkt(df[geography_variable["name"]])
            df = geopandas.GeoDataFrame(data=df, geometry=geography_variable["name"], crs="EPSG:4326")

        return df

    def to_arrow_batch_iterator(self, max_results=None, *, variables=None, progress=True):
        self._wait_for_finish()

        return list_rows(
            uri=self.uri,
            max_results=self.properties["numRows"] if max_results is None else min(max_results, int(self.properties["numRows"])),
            selected_variables=variables,
            mapped_variables=self.properties["outputSchema"],
            output_type="arrow_iterator",
            progress=progress,
            coerce_schema=hasattr(self.properties, "container") is False or self.properties["container"][
                "kind"] == 'dataset'
        )

    def list_rows(self, max_results=None, *, progress=True):
        warnings.warn(
            "The list_rows method is deprecated. Please use table.to_arrow_batch_iterator() or table.to_arrow_table().to_pylist() for better performance and memory utilization.",
            FutureWarning, stacklevel=2)

        self._wait_for_finish()

        return list_rows(
            uri=self.uri,
            max_results=self.properties["outputNumRows"] if max_results is None else min(max_results, int(
                self.properties["outputNumRows"])),
            mapped_variables=self.properties["outputSchema"],
            output_type="tuple",
            progress=progress,
            coerce_schema=False
        )

    def _wait_for_finish(self):
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
