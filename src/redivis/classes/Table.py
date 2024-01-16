from .Base import Base
from urllib.parse import quote as quote_uri
import warnings
import os
import time
import glob
import concurrent.futures
import pathlib
import pyarrow as pa
import pandas as pd
from tqdm.auto import tqdm

from .Upload import Upload
from .Variable import Variable
from .File import File
from ..common.list_rows import list_rows
from ..common.api_request import make_request, make_paginated_request
from ..common.util import get_geography_variable, get_warning
from ..common.retryable_upload import perform_resumable_upload, perform_standard_upload


class Table(Base):
    def __init__(
        self,
        name,
        *,
        sample=False,
        dataset=None,
        project=None,
        properties=None,
    ):
        dataset, project = get_table_parents(dataset, project)
        sample_string = ":sample" if sample else ""
        self.name = name
        self.dataset = dataset
        self.project = project

        self.qualified_reference = properties["qualifiedReference"] if "qualifiedReference" in (properties or {}) else (
            f"{dataset.qualified_reference if dataset else project.qualified_reference}.{self.name}{sample_string}"
        )
        self.scoped_reference = properties["scopedReference"] if "scopedReference" in (properties or {}) else f"{self.name}{sample_string}"
        self.uri = f"/tables/{quote_uri(self.qualified_reference, '')}"
        self.properties = properties

    def create(self, *, description=None, upload_merge_strategy="append", is_file_index=False):
        response = make_request(
            method="POST",
            path=f"{self.dataset.uri}/tables",
            payload={
                "name": self.name,
                "description": description,
                "uploadMergeStrategy": upload_merge_strategy,
                "isFileIndex": is_file_index
            },
        )
        update_properties(self, response)
        return self

    def delete(self):
        make_request(
            method="DELETE",
            path=self.uri,
        )
        return

    def get(self):
        properties = make_request(method="GET", path=self.uri)
        update_properties(self, properties)
        return self

    def exists(self):
        try:
            make_request(method="HEAD", path=self.uri)
            return True
        except Exception as err:
            if err.args[0]["status"] != 404:
                raise err
            return False

    def list_uploads(self, *, max_results=None):
        uploads = make_paginated_request(
            path=f"{self.uri}/uploads", max_results=max_results
        )
        return [Upload(upload) for upload in uploads]

    def list_variables(self, *, max_results=None):
        variables = make_paginated_request(
            path=f"{self.uri}/variables", page_size=1000, max_results=max_results
        )
        return [
            Variable(variable["name"], table=self, properties=variable)
            for variable in variables
        ]

    def add_file(self, name, data):
        warnings.warn(
            "The table.add_file() method is deprecated, and will be removed soon. Please use the table.add_files() method for improved performance and future compatibility.",
            FutureWarning, stacklevel=2)

        response = make_request(
            method="POST",
            path=f"{self.uri}/rawFiles",
            query={"name": name},
            payload=data,
            parse_payload=False
        )
        return File(response["id"], table=self, properties=response)

    def add_files(self, *, files=None, directory=None, progress=True):
        if (files is None) == (directory is None):
            raise Exception("Either files or directory must be specified")

        total_size = 0

        if directory:
            files = []
            if not directory.endswith('/'):
                directory += '/'
            for filename in glob.iglob(directory + '**/*', recursive=True):
                if os.path.isfile(filename):
                    size = os.stat(filename).st_size
                    total_size += size
                    files.append({
                        "path": filename,
                        "name": os.path.relpath(filename, directory),
                        "size": size
                    })
        else:
            for file in files:
                if "name" not in file:
                    if "data" in file:
                        raise Exception('All file specifications with a "data" key must specify a name')

                    file["name"] = os.path.basename(file["path"])

                if "data" in file:
                    if isinstance(file["data"], str):
                        file["data"] = bytes(file["data"], 'utf-8')
                    file["size"] = len(file["data"])
                else:
                    file["size"] = os.stat(file["path"]).st_size

                total_size += file["size"]

        pbar_bytes = None
        pbar_count = None
        if progress:
            pbar_count = tqdm(total=len(files), leave=False, unit=' files')
            pbar_bytes = tqdm(total=total_size, leave=False, unit='B', unit_scale=True)

        try:
            current_batch_timestamp = time.time()
            uploaded_files = []
            current_batch_files = []
            current_temp_uploads_batch = []
            target_batch_size = 1e8
            current_batch_size = 0

            for i, file in enumerate(files):
                if i % 1000 == 0:
                    res = make_request(
                        method="POST",
                        path=f"{self.uri}/tempUploads",
                        payload={
                            "tempUploads": [
                                {"size": file["size"], "name": file["name"], "resumable": file["size"] > 5e7}
                                for file in files[i:i+1000]
                            ]
                        },
                    )
                    current_temp_uploads_batch = res["results"]

                current_batch_files.append({"file": file, "temp_upload": current_temp_uploads_batch[i % 1000]})
                current_batch_size += file["size"]

                if len(current_batch_files) >= 1000 or i == len(files) - 1 or current_batch_size > target_batch_size:
                    def upload(batch_file):
                        nonlocal pbar_bytes
                        file = batch_file["file"]
                        temp_upload = batch_file["temp_upload"]

                        if temp_upload["resumable"]:
                            data = open(file["path"], "rb") if "path" in file else file["data"]
                            perform_resumable_upload(data=data, progressbar=pbar_bytes,
                                                     temp_upload_url=temp_upload["url"])
                        else:
                            data = open(file["path"], "rb") if "path" in file else file["data"]
                            perform_standard_upload(data=data, temp_upload_url=temp_upload["url"],
                                                    progressbar=pbar_bytes)

                        if "path" in file:
                            data.close()

                    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                        futures = [executor.submit(upload, batch_file)
                                   for batch_file in current_batch_files]

                        not_done = futures
                        try:
                            while not_done:
                                # next line 'sleeps' this main thread, letting the thread pool run
                                freshly_done, not_done = concurrent.futures.wait(not_done, timeout=0.5)
                                for future in freshly_done:
                                    # Call result() on any finished threads to raise any exceptions encountered.
                                    future.result()
                        finally:
                            for future in not_done:
                                # Only cancels futures that were never started
                                future.cancel()
                            # Shutdown all background threads, now that they should know to exit early.
                            executor.shutdown(wait=True, cancel_futures=True)

                    if time.time() - current_batch_timestamp > 60:
                        target_batch_size = target_batch_size / 2
                    elif time.time() - current_batch_timestamp < 15:
                        target_batch_size = target_batch_size * 2

                    response = make_request(
                        method="POST",
                        path=f"{self.uri}/rawFiles",
                        payload={"files": [{"name": batch_file["file"]["name"], "tempUploadId": batch_file["temp_upload"]["id"]} for batch_file in current_batch_files]},
                    )
                    uploaded_files += [
                        File(f["id"], table=self, properties=f)
                        for f in response["results"]
                    ]
                    if progress:
                        pbar_count.update(len(current_batch_files))

                    current_batch_timestamp = time.time()
                    current_batch_files = []
                    current_batch_size = 0

            if progress:
                pbar_bytes.close()
                pbar_count.close()

            return uploaded_files
        except Exception as e:
            if progress:
                pbar_bytes.close()
                pbar_count.close()

            raise e


    def list_files(self, max_results=None, *, file_id_variable=None):
        if file_id_variable:
            variable = Variable(file_id_variable, table=self)
            if not variable.get().properties['isFileId']:
                raise Exception(f'The variable {file_id_variable} does not represent a file id.')
        else:
            variables = make_paginated_request(
                path=f"{self.uri}/variables", max_results=2, query={"isFileId": True}
            )
            if len(variables) == 0:
                raise Exception(f"No variable containing file ids was found on this table")
            elif len(variables) > 1:
                raise Exception(f"This table contains multiple variables representing a file id. Please specify the variable with file ids you want to download via the 'file_id_variable' parameter.")

            file_id_variable = variables[0]["name"]

        rows = self.to_arrow_table(max_results=max_results, variables=[file_id_variable], progress=False).to_pylist()
        return [
            File(row[file_id_variable], table=self)
            for row in rows
        ]

    def download_files(self, path=None, *, overwrite=False, max_results=None, file_id_variable=None, progress=True):
        files = self.list_files(max_results, file_id_variable=file_id_variable)
        if path is None:
            path = os.getcwd()

        if progress:
            pbar_count = tqdm(total=len(files), leave=False, unit=' files')
            pbar_bytes = tqdm(unit='B', leave=False, unit_scale=True, bar_format='{n_fmt} ({rate_fmt})')

        if not os.path.exists(path):
            pathlib.Path(path).mkdir(exist_ok=True, parents=True)


        def on_progress(bytes):
            nonlocal pbar_bytes
            pbar_bytes.update(bytes)

        def download(file):
            nonlocal progress
            nonlocal pbar_count

            file.download(path, overwrite=overwrite, progress=False, on_progress=on_progress if progress else None)
            if progress:
                pbar_count.update()

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(download, file) for file in files]

            not_done = futures
            try:
                while not_done:
                    # next line 'sleeps' this main thread, letting the thread pool run
                    freshly_done, not_done = concurrent.futures.wait(not_done, timeout=0.5)
                    for future in freshly_done:
                        # Call result() on any finished threads to raise any exceptions encountered.
                        future.result()
            finally:
                for future in not_done:
                    # Only cancels futures that were never started
                    future.cancel()
                # Shutdown all background threads, now that they should know to exit early.
                executor.shutdown(wait=True, cancel_futures=True)

        if progress:
            pbar_count.close()
            pbar_bytes.close()

    def to_arrow_dataset(self, max_results=None, *, variables=None, progress=True, batch_preprocessor=None):
        if not self.properties or "container" not in self.properties:
            self.get()

        mapped_variables = get_mapped_variables(variables, self.uri)

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="arrow_dataset",
            progress=progress,
            coerce_schema=self.properties["container"]["kind"] == 'dataset',
            batch_preprocessor=batch_preprocessor
        )

    def to_arrow_table(self, max_results=None, *, variables=None, progress=True, batch_preprocessor=None):
        if not self.properties or "container" not in self.properties:
            self.get()

        mapped_variables = get_mapped_variables(variables, self.uri)

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="arrow_table",
            progress=progress,
            coerce_schema=self.properties["container"]["kind"] == 'dataset',
            batch_preprocessor=batch_preprocessor
        )

    def to_polars_lazyframe(self, max_results=None, *, variables=None, progress=True, batch_preprocessor=None):
        if not self.properties or "container" not in self.properties:
            self.get()

        mapped_variables = get_mapped_variables(variables, self.uri)

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="polars_lazyframe",
            progress=progress,
            coerce_schema=self.properties["container"]["kind"] == 'dataset',
            batch_preprocessor=batch_preprocessor
        )

    def to_dask_dataframe(self, max_results=None, *, variables=None, progress=True, batch_preprocessor=None):
        if not self.properties or "container" not in self.properties:
            self.get()

        mapped_variables = get_mapped_variables(variables, self.uri)

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="dask_dataframe",
            progress=progress,
            coerce_schema=self.properties["container"]["kind"] == 'dataset',
            batch_preprocessor=batch_preprocessor
        )

    def to_pandas_dataframe(self, max_results=None, *, variables=None, progress=True, dtype_backend='pyarrow', date_as_object=False, batch_preprocessor=None):
        if dtype_backend not in ['numpy', 'numpy_nullable', 'pyarrow']:
            raise Exception(f"Unknown dtype_backend. Must be one of 'pyarrow'|'numpy_nullable'|'numpy'. Default is 'pyarrow'")

        if not self.properties or "container" not in self.properties:
            self.get()

        mapped_variables = get_mapped_variables(variables, self.uri)
        arrow_table = list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="arrow_table",
            progress=progress,
            coerce_schema=self.properties["container"]["kind"] == 'dataset',
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

        return df

    def to_geopandas_dataframe(self, max_results=None, *, variables=None, geography_variable="", progress=True, dtype_backend='pyarrow', date_as_object=False, batch_preprocessor=None):
        import geopandas

        if dtype_backend not in ['numpy', 'numpy_nullable', 'pyarrow']:
            raise Exception(f"Unknown dtype_backend. Must be one of 'pyarrow'|'numpy_nullable'|'numpy'. Default is 'pyarrow'")

        if not self.properties or "container" not in self.properties:
            self.get()

        mapped_variables = get_mapped_variables(variables, self.uri)
        arrow_table = list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="arrow_table",
            progress=progress,
            coerce_schema=self.properties["container"]["kind"] == 'dataset',
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
            geography_variable = get_geography_variable(mapped_variables, geography_variable)
            if geography_variable is None:
                raise Exception('Unable to find a variable with type=="geography" in the query results')

        if geography_variable is not None:
            df[geography_variable["name"]] = geopandas.GeoSeries.from_wkt(df[geography_variable["name"]])
            df = geopandas.GeoDataFrame(data=df, geometry=geography_variable["name"], crs="EPSG:4326")

        return df

    def to_dataframe(self, max_results=None, *, variables=None, geography_variable="", progress=True):
        warnings.warn(get_warning('dataframe_deprecation'), FutureWarning, stacklevel=2)

        if not self.properties or "container" not in self.properties:
            self.get()

        mapped_variables = get_mapped_variables(variables, self.uri)
        arrow_table = list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="arrow_table",
            progress=progress,
            coerce_schema=self.properties["container"]["kind"] == 'dataset',
        )

        df = arrow_table.to_pandas(self_destruct=True)

        if geography_variable is not None:
            geography_variable = get_geography_variable(mapped_variables, geography_variable)

        if geography_variable is not None:
            import geopandas
            warnings.warn(get_warning('geodataframe_deprecation'), FutureWarning, stacklevel=2)
            df[geography_variable["name"]] = geopandas.GeoSeries.from_wkt(df[geography_variable["name"]])
            df = geopandas.GeoDataFrame(data=df, geometry=geography_variable["name"], crs="EPSG:4326")

        return df

    def to_arrow_batch_iterator(self, max_results=None, *, variables=None, progress=True):
        if not self.properties or "container" not in self.properties:
            self.get()

        mapped_variables = get_mapped_variables(variables, self.uri)
        return list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="arrow_iterator",
            progress=progress,
            coerce_schema=self.properties["container"]["kind"] == 'dataset',
        )

    def list_rows(self, max_results=None, *, variables=None, progress=True):
        warnings.warn("The list_rows method is deprecated. Please use table.to_arrow_batch_iterator() or table.to_arrow_table().to_pylist() for better performance and memory utilization.", FutureWarning, stacklevel=2)

        if not self.properties or "container" not in self.properties:
            self.get()

        mapped_variables = get_mapped_variables(variables, self.uri)
        return list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="tuple",
            progress=progress,
            coerce_schema=self.properties["container"]["kind"] == 'dataset',
        )

    def update(self, *, name=None, description=None, upload_merge_strategy=None):
        payload = {}
        if name:
            payload["name"] = name
        if upload_merge_strategy:
            payload["uploadMergeStrategy"] = upload_merge_strategy
        if description is not None:
            payload["description"] = description

        response = make_request(
            method="PATCH",
            path=f"{self.uri}",
            payload=payload,
        )
        update_properties(self, response)
        return

    def upload(self, name):
        return Upload(name=name, table=self)

    def variable(self, name):
        return Variable(name, table=self)


def get_mapped_variables(variables, uri):
    all_variables = make_paginated_request(path=f"{uri}/variables", page_size=1000)
    if variables is None:
        return all_variables
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
        return variables_list


def get_table_parents(dataset, project):
    from .User import User

    if dataset or project:
        return dataset, project
    elif os.getenv("REDIVIS_DEFAULT_PROJECT") is not None:
        return None, User(os.getenv("REDIVIS_DEFAULT_PROJECT").split(".")[0]).project(
            os.getenv("REDIVIS_DEFAULT_PROJECT").split(".")[1]
        )
    elif os.getenv("REDIVIS_DEFAULT_DATASET") is not None:
        return (
            User(os.getenv("REDIVIS_DEFAULT_DATASET").split(".")[0]).dataset(
                os.getenv("REDIVIS_DEFAULT_DATASET").split(".")[1]
            )
        ), None
    else:
        raise Exception(
            "Cannot reference an unqualified table if the neither the REDIVIS_DEFAULT_PROJECT or REDIVIS_DEFAULT_DATASET environment variables are set."
        )

def update_properties(instance, properties):
    instance.properties = properties
    instance.qualified_reference = properties["qualifiedReference"]
    instance.scoped_reference = properties["scopedReference"]
    instance.name = properties["name"]
    instance.uri = properties["uri"]