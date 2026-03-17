from ..common import exceptions
from ..classes.Base import Base

import warnings
import os
import tempfile
from ..classes.File import File
from ..classes.Directory import Directory
from pathlib import Path
from contextlib import closing
from .fetch_rows import make_rows_request
from ..common.api_request import make_request, make_paginated_request
from ..common.util import get_warning
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, Literal
from datetime import datetime, timezone
import weakref

cached_directories: weakref.WeakValueDictionary[str, Directory] = (
    weakref.WeakValueDictionary()
)


class TabularReader(Base):
    def __init__(
        self, is_table=False, is_query=False, is_upload=False, is_read_stream=False
    ):
        self._is_table = is_table
        self._is_query = is_query
        self._is_upload = is_upload
        self._is_read_stream = is_read_stream
        self.directory = None
        self.properties = {}

    # TODO: prefix? pattern? Don't cache if these are present
    def to_directory(
        self,
        *,
        file_id_variable: Optional[str] = None,
        file_name_variable: Optional[str] = None,
    ) -> Directory:
        # TODO: add file / directory methods to uploads
        if self._is_upload:
            raise exceptions.ValueError(
                "Listing files on uploads is not currently supported"
            )
        if self._is_read_stream:
            raise exceptions.ValueError(
                "Listing files on read streams is not supported"
            )

        # We need to check that the query has finished.
        # However, for tables, we don't need to fetch the table metadata before building the directory
        #   and it's important that we don't, since the cached_directories map is based on the user-provided URI
        # TODO: in the future, we should map all URIs to a canonical URI for caching
        if self._is_query:
            check_is_ready(self)

        if self.directory is None and cached_directories.get(self.uri):
            self.directory = cached_directories[self.uri]

        if self.directory and self._is_query:
            # Queries are immutable, always use the cached version
            return self.directory

        import pyarrow

        # Compute this here, to get the timestamp before the request was sent
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        headers = {}
        if self.directory:
            headers["If-Modified-Since"] = datetime.fromtimestamp(
                self.directory._last_cached_at / 1000, tz=timezone.utc
            ).strftime("%a, %d %b %Y %H:%M:%S GMT")

        with closing(
            make_request(
                method="get",
                path=f"{self.uri}/rawFiles",
                headers=headers,
                query={
                    "format": "arrow",
                    "fileIdVariable": file_id_variable,
                    "fileNameVariable": file_name_variable,
                },
                stream=True,
                parse_response=False,
            )
        ) as res:
            if res.status_code == 304:
                self.directory._last_cached_at = now_ms
                return self.directory

            directory = Directory(
                path=Path("/"),
                query=self if self._is_query else None,
                table=self if self._is_table else None,
            )

            for file_spec in (
                pyarrow.ipc.RecordBatchStreamReader(res.raw).read_all().to_pylist()
            ):
                directory._add_file(
                    File(
                        file_spec["file_id"],
                        file_spec[file_name_variable or "file_name"],
                        query=self if self._is_query else None,
                        table=self if self._is_table else None,
                        properties=file_spec,
                        directory=directory,
                    )
                )

            self.directory = directory
            self.directory._last_cached_at = now_ms
            cached_directories[self.uri] = self.directory
            return self.directory

    def file(self, path: Union[str, Path]) -> File:
        if self.directory is None and cached_directories.get(self.uri):
            self.directory = cached_directories[self.uri]

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        if self.directory is None or (
            now_ms - self.directory._last_cached_at > 30000  # cache for 30s
        ):
            self.to_directory()

        node = self.directory.get(path)
        if isinstance(node, Directory):
            raise exceptions.ValueError(f"{path} is a directory, not a file")
        if node is None:
            raise exceptions.NotFoundError(f"No file found at path '{path}'")
        return node

    def list_files(
        self,
        max_results: Optional[int] = None,
        *,
        file_id_variable: Optional[str] = None,
        file_name_variable: Optional[str] = None,
    ) -> List[File]:
        self.to_directory(
            file_id_variable=file_id_variable, file_name_variable=file_name_variable
        )

        return self.directory.list(
            mode="files", recursive=True, max_results=max_results
        )

    def to_read_streams(self, target_count=os.cpu_count(), *, variables=None):
        from ..classes.ReadStream import ReadStream

        if self._is_read_stream:
            raise exceptions.ValueError(
                "Cannot call to_read_streams() on a ReadStream."
            )

        if target_count is None:
            target_count = os.cpu_count() or 1
        if target_count < 1:
            raise exceptions.ValueError("target_count must be at least 1")

        payload = {"requestedStreamCount": target_count, "format": "arrow"}

        if variables is not None:
            payload["selectedVariables"] = variables

        read_session = make_request(
            method="post",
            path=f"{self.uri}/readSessions",
            parse_response=True,
            payload=payload,
        )
        read_streams = [
            ReadStream(
                id=stream["id"],
                table=self if self._is_table else None,
                query=self if self._is_query else None,
                upload=self if self._is_upload else None,
                selected_variables=variables,
                properties=stream,
            )
            for stream in read_session["streams"]
        ]
        return read_streams

    def download_files(
        self,
        path: Optional[Union[str, Path]] = None,
        overwrite: bool = False,
        max_results: Optional[int] = None,
        file_id_variable: Optional[str] = None,
        file_name_variable: Optional[str] = None,
        progress: bool = True,
        max_parallelization: Optional[int] = None,
    ) -> None:
        warnings.warn(
            "This method is deprecated. Please use to_directory().download() instead",
            FutureWarning,
            stacklevel=2,
        )
        self.to_directory(
            file_id_variable=file_id_variable, file_name_variable=file_name_variable
        )

        return self.directory.download(
            path=path,
            max_results=max_results,
            overwrite=overwrite,
            max_parallelization=max_parallelization,
            progress=progress,
        )

    def to_arrow_dataset(
        self,
        max_results: Optional[int] = None,
        *,
        variables: Optional[Iterable[str]] = None,
        progress: bool = True,
        batch_preprocessor: Optional[Any] = None,
        max_parallelization: int = os.cpu_count() or 1,
    ) -> Any:
        mapped_variables, selected_variables, coerce_schema = get_mapped_variables(
            self, variables
        )

        return make_rows_request(
            uri=self.uri,
            instance=self,
            max_results=max_results,
            selected_variables=selected_variables,
            mapped_variables=mapped_variables,
            output_type="arrow_dataset",
            progress=progress,
            coerce_schema=coerce_schema,
            batch_preprocessor=batch_preprocessor,
            use_export_api=should_use_export_api(self),
            max_parallelization=max_parallelization,
        )

    def to_arrow_table(
        self,
        max_results: Optional[int] = None,
        *,
        variables: Optional[Iterable[str]] = None,
        progress: bool = True,
        batch_preprocessor: Optional[Any] = None,
        max_parallelization: int = os.cpu_count() or 1,
    ) -> Any:
        mapped_variables, selected_variables, coerce_schema = get_mapped_variables(
            self, variables
        )

        return make_rows_request(
            uri=self.uri,
            instance=self,
            max_results=max_results,
            selected_variables=selected_variables,
            mapped_variables=mapped_variables,
            output_type="arrow_table",
            progress=progress,
            coerce_schema=coerce_schema,
            batch_preprocessor=batch_preprocessor,
            use_export_api=should_use_export_api(self),
            max_parallelization=max_parallelization,
        )

    def to_polars_lazyframe(
        self,
        max_results: Optional[int] = None,
        *,
        variables: Optional[Iterable[str]] = None,
        progress: bool = True,
        batch_preprocessor: Optional[Any] = None,
        max_parallelization: int = os.cpu_count() or 1,
    ) -> Any:
        mapped_variables, selected_variables, coerce_schema = get_mapped_variables(
            self, variables
        )

        return make_rows_request(
            uri=self.uri,
            instance=self,
            max_results=max_results,
            selected_variables=selected_variables,
            mapped_variables=mapped_variables,
            output_type="polars_lazyframe",
            progress=progress,
            coerce_schema=coerce_schema,
            batch_preprocessor=batch_preprocessor,
            use_export_api=should_use_export_api(self),
            max_parallelization=max_parallelization,
        )

    def to_dask_dataframe(
        self,
        max_results: Optional[int] = None,
        *,
        variables: Optional[Iterable[str]] = None,
        progress: bool = True,
        batch_preprocessor: Optional[Any] = None,
        max_parallelization: int = os.cpu_count() or 1,
    ) -> Any:
        mapped_variables, selected_variables, coerce_schema = get_mapped_variables(
            self, variables
        )

        return make_rows_request(
            uri=self.uri,
            instance=self,
            max_results=max_results,
            selected_variables=selected_variables,
            mapped_variables=mapped_variables,
            output_type="dask_dataframe",
            progress=progress,
            coerce_schema=coerce_schema,
            batch_preprocessor=batch_preprocessor,
            use_export_api=should_use_export_api(self),
            max_parallelization=max_parallelization,
        )

    def to_pandas_dataframe(
        self,
        max_results: Optional[int] = None,
        *,
        variables: Optional[Iterable[str]] = None,
        progress: bool = True,
        dtype_backend: str = "pyarrow",
        date_as_object: bool = False,
        batch_preprocessor: Optional[Any] = None,
        max_parallelization: int = os.cpu_count() or 1,
    ) -> Any:
        mapped_variables, selected_variables, coerce_schema = get_mapped_variables(
            self, variables
        )

        arrow_table = make_rows_request(
            uri=self.uri,
            instance=self,
            max_results=max_results,
            selected_variables=selected_variables,
            mapped_variables=mapped_variables,
            output_type="arrow_table",
            progress=progress,
            coerce_schema=coerce_schema,
            batch_preprocessor=batch_preprocessor,
            use_export_api=should_use_export_api(self),
            max_parallelization=max_parallelization,
        )
        return arrow_table_to_pandas(
            arrow_table, dtype_backend, date_as_object, max_parallelization
        )

    def to_geopandas_dataframe(
        self,
        max_results: Optional[int] = None,
        *,
        variables: Optional[Iterable[str]] = None,
        geography_variable: Union[str, None, Literal[""]] = "",
        progress: bool = True,
        dtype_backend: str = "pyarrow",
        date_as_object: bool = False,
        batch_preprocessor: Optional[Any] = None,
        max_parallelization: int = os.cpu_count() or 1,
    ) -> Any:
        import geopandas

        mapped_variables, selected_variables, coerce_schema = get_mapped_variables(
            self, variables
        )

        arrow_table = make_rows_request(
            uri=self.uri,
            instance=self,
            max_results=max_results,
            selected_variables=selected_variables,
            mapped_variables=mapped_variables,
            output_type="arrow_table",
            progress=progress,
            coerce_schema=coerce_schema,
            batch_preprocessor=batch_preprocessor,
            use_export_api=should_use_export_api(self),
            max_parallelization=max_parallelization,
        )

        df = arrow_table_to_pandas(
            arrow_table, dtype_backend, date_as_object, max_parallelization
        )

        if geography_variable is not None:
            geography_variable = get_geography_variable(
                mapped_variables, geography_variable
            )
            if geography_variable is None:
                raise exceptions.NotFoundError(
                    'Unable to find a variable with type=="geography" in the query results'
                )

        if geography_variable is not None:
            df[geography_variable["name"]] = geopandas.GeoSeries.from_wkt(
                df[geography_variable["name"]]
            )
            df = geopandas.GeoDataFrame(
                data=df, geometry=geography_variable["name"], crs="EPSG:4326"
            )

        return df

    def to_dataframe(
        self,
        max_results: Optional[int] = None,
        *,
        variables: Optional[Iterable[str]] = None,
        geography_variable: Union[str, None, Literal[""]] = "",
        progress: bool = True,
    ) -> Any:
        warnings.warn(get_warning("dataframe_deprecation"), FutureWarning, stacklevel=2)

        mapped_variables, selected_variables, coerce_schema = get_mapped_variables(
            self, variables
        )

        arrow_table = make_rows_request(
            uri=self.uri,
            instance=self,
            max_results=max_results,
            selected_variables=selected_variables,
            mapped_variables=mapped_variables,
            output_type="arrow_table",
            progress=progress,
            coerce_schema=coerce_schema,
            use_export_api=should_use_export_api(self),
        )

        df = arrow_table.to_pandas(self_destruct=True)

        if geography_variable is not None:
            geography_variable = get_geography_variable(
                mapped_variables, geography_variable
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
        self,
        max_results: Optional[int] = None,
        *,
        variables: Optional[Iterable[str]] = None,
        progress: bool = True,
    ) -> Iterable[Any]:
        mapped_variables, selected_variables, coerce_schema = get_mapped_variables(
            self, variables
        )

        return make_rows_request(
            uri=self.uri,
            instance=self,
            max_results=max_results,
            selected_variables=selected_variables,
            mapped_variables=mapped_variables,
            output_type="arrow_iterator",
            progress=progress,
            coerce_schema=coerce_schema,
        )

    def to_sas(
        self,
        name: Optional[str] = None,
        max_results: Optional[int] = None,
        *,
        variables: Optional[Iterable[str]] = None,
        progress: bool = True,
        batch_preprocessor: Optional[Any] = None,
        max_parallelization: int = os.cpu_count(),
        geography_variable: Union[str, None, Literal[""]] = "",
    ) -> None:
        if self._is_read_stream:
            raise exceptions.ValueError("Cannot call to_sas() on a ReadStream.")
        if not name:
            raise exceptions.ValueError(
                'A SAS dataset name must be provided. E.g., table.to_sas("mydata")'
            )
        import pyarrow as pa
        import saspy  # make sure this gets imported here, so that SAS initialization happens
        from IPython import get_ipython

        ip = get_ipython()

        mapped_variables, selected_variables, coerce_schema = get_mapped_variables(
            self, variables
        )

        with tempfile.TemporaryDirectory() as tmpdirname:
            # IMPORTANT: SAS is running as a separate user, need to make sure the directory is readable
            os.chmod(tmpdirname, 0o755)

            if geography_variable == "":

                geo_variables = make_paginated_request(
                    path=f"{self.uri}/variables",
                    query={"type": "geography"},
                )
                if variables is not None:
                    lower_variable_names = [v.lower() for v in variables]
                    geo_variables = [
                        v
                        for v in geo_variables
                        if v["name"].lower() in lower_variable_names
                    ]
                if len(geo_variables) > 1:
                    raise exceptions.ValueError(
                        "Multiple geography variables found; please specify which to use"
                    )
                elif len(geo_variables) == 1:
                    geography_variable = geo_variables[0]["name"]
                else:
                    geography_variable = None

            if geography_variable is None:
                use_export_api = max_results is not None and variables is None and should_use_export_api(self)
                
                # IMPORTANT: always pass selectedVariables if not using the export API to ensure correct ordering
                if selected_variables is None and not use_export_api:
                    selected_variables = [v["name"] for v in mapped_variables]

                load_script = make_request(
                    method="GET",
                    path=f"{self.uri}/script",
                    query={
                        "type": "sas",
                        "filePath": f"{tmpdirname}/part-0.csv",
                        "sasDatasetName": name,
                        "selectedVariables": selected_variables,
                    },
                    parse_response=False,
                ).text

                if use_export_api:
                    self.download(
                        path=f"{tmpdirname}/part-0.csv", format="csv", progress=progress
                    )
                else:
                    ds = self.to_arrow_dataset(
                        max_results=max_results,
                        variables=variables,
                        progress=progress,
                        batch_preprocessor=batch_preprocessor,
                        max_parallelization=max_parallelization,
                    )
                    pa.dataset.write_dataset(
                        ds,
                        base_dir=tmpdirname,
                        existing_data_behavior="overwrite_or_ignore",
                        basename_template="part-{i}.csv",
                        format="csv",
                    )
            else:
                geopandas_df = self.to_geopandas_dataframe(
                    max_results=max_results,
                    geography_variable=geography_variable,
                    progress=progress,
                    variables=variables,
                    batch_preprocessor=batch_preprocessor,
                    max_parallelization=max_parallelization,
                )
                geopandas_df.to_file(f"{tmpdirname}/out.shp")
                load_script = f"""proc mapimport datafile="{tmpdirname}/out.shp" out={name};\nrun;"""

            ip.run_cell_magic("SAS", "", load_script)

    def to_stata(
        self,
        max_results: Optional[int] = None,
        *,
        geography_variable: Union[str, None, Literal[""]] = "",
        variables: Optional[Iterable[str]] = None,
        progress: bool = True,
        batch_preprocessor: Optional[Any] = None,
        max_parallelization: int = os.cpu_count(),
    ) -> None:
        if self._is_read_stream:
            raise exceptions.ValueError("Cannot call to_stata() on a ReadStream.")
        # This will be set if there was an error during Stata initialization in a Redivis notebook
        if os.getenv("STATA_ERROR"):
            raise exceptions.RedivisError(
                f"""An error occurred during Stata initialization. Please make sure you have the correct license and edition specified.\n\nThe error message was:\n\n{os.getenv('STATA_ERROR')}."""
            )
        import pyarrow as pa
        from pystata import stata

        mapped_variables, selected_variables, coerce_schema = get_mapped_variables(
            self, variables
        )

        with tempfile.TemporaryDirectory() as tmpdirname:
            if geography_variable == "":
                geo_variables = make_paginated_request(
                    path=f"{self.uri}/variables",
                    query={"type": "geography"},
                )
                if variables is not None:
                    lower_variable_names = [v.lower() for v in variables]
                    geo_variables = [
                        v
                        for v in geo_variables
                        if v["name"].lower() in lower_variable_names
                    ]
                if len(geo_variables) > 1:
                    raise exceptions.ValueError(
                        "Multiple geography variables found; please specify which to use"
                    )
                elif len(geo_variables) == 1:
                    geography_variable = geo_variables[0]["name"]
                else:
                    geography_variable = None

            if geography_variable is None:
                use_export_api = max_results is not None and variables is None and should_use_export_api(self)
                
                # IMPORTANT: always pass selectedVariables if not using the export API to ensure correct ordering
                if selected_variables is None and not use_export_api:
                    selected_variables = [v["name"] for v in mapped_variables]

                load_script_res = make_request(
                    method="GET",
                    path=f"{self.uri}/script",
                    query={
                        "type": "stata",
                        "filePath": f"{tmpdirname}/part-0.csv",
                        "selectedVariables": selected_variables,
                    },
                    parse_response=False,
                )
                load_script = load_script_res.text
                if use_export_api:
                    self.download(
                        path=f"{tmpdirname}/part-0.csv", format="csv", progress=progress
                    )
                else:
                    ds = self.to_arrow_dataset(
                        max_results=max_results,
                        variables=variables,
                        progress=progress,
                        batch_preprocessor=batch_preprocessor,
                        max_parallelization=max_parallelization,
                    )

                    # IMPORTANT: this reduces the resolution of temporal fields, since Stata doesn't support microsecond precision (which is what we get from BQ)

                    columns = {}

                    for field in ds.schema:
                        t = field.type

                        if pa.types.is_timestamp(t):
                            columns[field.name] = pa.dataset.field(field.name).cast(
                                pa.timestamp("ms", tz=t.tz)
                            )
                        elif pa.types.is_time64(t):
                            # Arrow time32 supports seconds / milliseconds
                            columns[field.name] = pa.dataset.field(field.name).cast(
                                pa.time32("ms")
                            )
                        else:
                            columns[field.name] = pa.dataset.field(field.name)

                    scanner = ds.scanner(columns=columns)

                    pa.dataset.write_dataset(
                        scanner,
                        base_dir=tmpdirname,
                        existing_data_behavior="overwrite_or_ignore",
                        basename_template="part-{i}.csv",
                        format="csv",
                    )
            else:
                geopandas_df = self.to_geopandas_dataframe(
                    max_results=max_results,
                    geography_variable=geography_variable,
                    variables=variables,
                    progress=progress,
                    batch_preprocessor=batch_preprocessor,
                    max_parallelization=max_parallelization,
                )
                geopandas_df.to_file(f"{tmpdirname}/out.shp")
                # spshape2dta creates an out.dta + out_shp.dta file in WORKDIR, but doesn't load it. Only out.dta should be loaded; the other file is linked behind the scenes
                load_script = (
                    f'spshape2dta "{tmpdirname}/out.shp", replace\nuse out.dta, clear'
                )

            stata.run("clear")
            stata.run(load_script, quietly=True)
            stata.run("describe")

    def list_rows(
        self,
        max_results: Optional[int] = None,
        *,
        variables: Optional[Iterable[str]] = None,
        progress: bool = True,
    ):
        raise exceptions.DeprecationError(
            "The list_rows method is deprecated. Please use table.to_arrow_batch_iterator() or table.to_arrow_table().to_pylist() for better performance and memory utilization."
        )


def check_is_ready(self: TabularReader) -> None:
    if self._is_query:
        self._wait_for_finish()
    elif self._is_table:
        if not self.properties or "container" not in self.properties:
            self.get()


def get_mapped_variables(
    self: TabularReader, variables: Optional[Iterable[str]]
) -> Tuple[List[Dict[str, Any]], bool]:
    if self._is_read_stream:
        return get_mapped_variables(
            (
                self.table
                if self.table is not None
                else self.upload if self.upload is not None else self.query
            ),
            variables=self.selected_variables,
        )

    check_is_ready(self)

    coerce_schema = False  # queries and uploads will always have the correct types

    if self._is_table:
        coerce_schema = self.properties["container"]["kind"] == "dataset"

    all_variables = make_paginated_request(path=f"{self.uri}/variables", page_size=1000)

    if variables is None:
        return all_variables, variables, coerce_schema
    else:
        lower_variable_names = [variable.lower() for variable in variables]
        variables_list = list(
            filter(
                lambda variable: variable["name"].lower() in lower_variable_names,
                all_variables,
            )
        )
        variables_list.sort(
            key=lambda variable: lower_variable_names.index(variable["name"].lower())
        )
        return variables_list, variables, coerce_schema


def arrow_table_to_pandas(
    arrow_table: Any, dtype_backend: str, date_as_object: bool, max_parallelization: int
) -> Any:
    import pandas as pd
    import pyarrow as pa

    pa.set_cpu_count(max_parallelization)
    pa.set_io_thread_count(max_parallelization)

    if dtype_backend not in ["numpy", "numpy_nullable", "pyarrow"]:
        raise exceptions.ValueError(
            f"Unknown dtype_backend. Must be one of 'pyarrow'|'numpy_nullable'|'numpy'. Default is 'pyarrow'"
        )

    if dtype_backend == "numpy_nullable":
        df = arrow_table.to_pandas(
            self_destruct=True,
            date_as_object=date_as_object,
            types_mapper={
                pa.int64(): pd.Int64Dtype(),
                pa.bool_(): pd.BooleanDtype(),
                pa.float64(): pd.Float64Dtype(),
                pa.string(): pd.StringDtype(),
            }.get,
        )
    elif dtype_backend == "pyarrow":
        df = arrow_table.to_pandas(self_destruct=True, types_mapper=pd.ArrowDtype)
    else:
        df = arrow_table.to_pandas(self_destruct=True, date_as_object=date_as_object)

    return df


def get_geography_variable(
    variables: List[Dict[str, Any]],
    geography_variable_name: Union[str, None, Literal[""]],
) -> Optional[Dict[str, Any]]:
    if geography_variable_name == "":
        for variable in variables:
            if variable["type"] == "geography":
                return variable

        return None
    else:
        for variable in variables:
            if variable["name"] == geography_variable_name:
                return variable

        raise exceptions.NotFoundError(
            f"The specified geography variable '{geography_variable_name}' could not be found"
        )


def should_use_export_api(self: TabularReader) -> bool:
    if not self._is_table:
        return False
    if not self.properties or "numBytes" not in self.properties:
        self.get()
    return self.properties.get("numBytes") > (
        1e10 if os.getenv("REDIVIS_DEFAULT_NOTEBOOK") is None else 1e11
    )
