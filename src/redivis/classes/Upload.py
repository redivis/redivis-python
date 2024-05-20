import time
import os
import json
import logging
from urllib.parse import quote as quote_uri
import warnings
import io
import pyarrow as pa
from tqdm.auto import tqdm

from ..common.util import get_geography_variable, get_warning
from ..common.list_rows import list_rows

from .Base import Base
from .Variable import Variable
from ..common.api_request import make_request, make_paginated_request
from ..common.retryable_upload import perform_resumable_upload, perform_standard_upload

class Upload(Base):
    def __init__(
        self,
        name,
        *,
        table,
        properties=None,
    ):
        self.table = table
        self.name = name
        self.uri = properties["uri"] if "uri" in (properties or {}) else (
            f"{table.uri}/uploads/{quote_uri(self.name,'')}"
        )
        self.properties = properties

    def create(
        self,
        data=None,
        *,
        type=None,
        transfer_specification=None,
        delimiter=None,
        schema=None,
        metadata=None,
        has_header_row=True,
        skip_bad_records=False,
        has_quoted_newlines=None,
        quote_character=None,
        rename_on_conflict=False,
        replace_on_conflict=False,
        allow_jagged_rows=False,
        if_not_exists=False,
        remove_on_fail=False,
        wait_for_finish=True,
        raise_on_fail=True,
        progress=True
    ):
        temp_upload_id = None

        if data and (
            (hasattr(data, "read") and os.stat(data.name).st_size > 1e7)
            or (hasattr(data, "__len__") and len(data) > 1e7)
        ):
            pbar_bytes = None
            size = os.stat(data.name).st_size if hasattr(data, "read") else len(data)
            if progress:
                pbar_bytes = tqdm(total=size, unit='B', leave=False, unit_scale=True)

            res = make_request(
                method="POST",
                path=f"{self.table.uri}/tempUploads",
                payload={"tempUploads": [{"size": size, "name": self.name, "resumable": size > 1e7}]},
            )
            temp_upload = res["results"][0]
            if temp_upload["resumable"]:
                perform_resumable_upload(data=data, progressbar=pbar_bytes,
                                         temp_upload_url=temp_upload["url"])
            else:
                perform_standard_upload(data=data, temp_upload_url=temp_upload["url"],
                                        progressbar=pbar_bytes)

            if progress:
                pbar_bytes.close()

            temp_upload_id = temp_upload["id"]
            data = None
        elif data and not hasattr(data, "read"):
            data = io.StringIO(data)

        if schema and type != "stream":
            warnings.warn("The schema option is ignored for uploads that aren't of type `stream`")

        exists = self.exists()

        if if_not_exists and exists:
            return self

        if replace_on_conflict is True and rename_on_conflict is True:
            raise Exception("Invalid parameters. replace_on_conflict and rename_on_conflict cannot both be True.")

        if exists:
            if replace_on_conflict is True:
                self.delete()
            elif rename_on_conflict is not True:
                raise Exception(f"An upload with the name {self.name} already exists on this version of the table. If you want to upload this file anyway, set the parameter rename_on_conflict=True or replace_on_conflict=True.")

        files = None
        payload = {
            "name": self.name,
            "type": type,
            "schema": schema,
            "metadata": metadata,
            "hasHeaderRow": has_header_row,
            "skipBadRecords": skip_bad_records,
            "hasQuotedNewlines": has_quoted_newlines,
            "allowJaggedRows": allow_jagged_rows,
            "quoteCharacter": quote_character,
            "delimiter": delimiter,
            "tempUploadId": temp_upload_id,
            "transferSpecification": transfer_specification
        }

        if data is not None:
            files={'metadata': json.dumps(payload), 'data': data}
            payload=None

        response = make_request(
            method="POST",
            path=f"{self.table.uri}/uploads",
            parse_payload=data is None,
            payload=payload,
            files=files
        )
        self.properties = response
        self.uri = self.properties["uri"]

        try:
            if (data or temp_upload_id or transfer_specification) and wait_for_finish:
                while True:
                    time.sleep(2)
                    self.get()
                    if self["status"] == "completed" or self["status"] == "failed":
                        if self["status"] == "failed" and raise_on_fail:
                            raise Exception(self["errorMessage"])
                        break
                    else:
                        logging.debug("Upload is still in progress...")
        except Exception as e:
            if remove_on_fail:
                self.delete()
            raise Exception(str(e))

        return self

    def delete(self):
        make_request(
            method="DELETE",
            path=self.uri,
        )

    def exists(self):
        try:
            make_request(method="HEAD", path=self.uri)
            return True
        except Exception as err:
            if err.args[0]["status"] != 404:
                raise err
            return False

    def get(self):
        self.properties = make_request(
            method="GET",
            path=self.uri,
        )
        self.uri = self.properties["uri"]

    def insert_rows(self, rows, *, update_schema=False):
        response = make_request(
            method="POST",
            path=f"{self.uri}/rows",
            payload={"rows": rows, "updateSchema": update_schema},
        )
        return response

    def list_variables(self, *, max_results=10000):
        variables = make_paginated_request(
            path=f"{self.uri}/variables", page_size=1000, max_results=max_results
        )
        return [
            Variable(variable["name"], properties=variable, upload=self)
            for variable in variables
        ]

    def to_arrow_dataset(self, max_results=None, *, variables=None, progress=True, batch_preprocessor=None):
        mapped_variables = get_mapped_variables(variables, self.uri)

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="arrow_dataset",
            progress=progress,
            coerce_schema=True,
            batch_preprocessor=batch_preprocessor
        )

    def to_arrow_table(self, max_results=None, *, variables=None, progress=True, batch_preprocessor=None):
        mapped_variables = get_mapped_variables(variables, self.uri)

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="arrow_table",
            progress=progress,
            coerce_schema=True,
            batch_preprocessor=batch_preprocessor
        )

    def to_polars_lazyframe(self, max_results=None, *, variables=None, progress=True, batch_preprocessor=None):
        mapped_variables = get_mapped_variables(variables, self.uri)

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="polars_lazyframe",
            progress=progress,
            coerce_schema=True,
            batch_preprocessor=batch_preprocessor
        )

    def to_dask_dataframe(self, max_results=None, *, variables=None, progress=True, batch_preprocessor=None):
        mapped_variables = get_mapped_variables(variables, self.uri)

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="dask_dataframe",
            progress=progress,
            coerce_schema=True,
            batch_preprocessor=batch_preprocessor
        )

    def to_pandas_dataframe(self, max_results=None, *, variables=None, progress=True, dtype_backend='pyarrow',
                            date_as_object=False, batch_preprocessor=None):
        if dtype_backend not in ['numpy', 'numpy_nullable', 'pyarrow']:
            raise Exception(
                f"Unknown dtype_backend. Must be one of 'pyarrow'|'numpy_nullable'|'numpy'. Default is 'pyarrow'")

        mapped_variables = get_mapped_variables(variables, self.uri)
        arrow_table = list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="arrow_table",
            progress=progress,
            coerce_schema=True,
            batch_preprocessor=batch_preprocessor
        )
        import pandas as pd

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

    def to_geopandas_dataframe(self, max_results=None, *, variables=None, geography_variable="", progress=True,
                               dtype_backend='pyarrow', date_as_object=False, batch_preprocessor=None):
        import geopandas

        if dtype_backend not in ['numpy', 'numpy_nullable', 'pyarrow']:
            raise Exception(
                f"Unknown dtype_backend. Must be one of 'pyarrow'|'numpy_nullable'|'numpy'. Default is 'pyarrow'")

        mapped_variables = get_mapped_variables(variables, self.uri)
        arrow_table = list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="arrow_table",
            progress=progress,
            coerce_schema=True,
            batch_preprocessor=batch_preprocessor
        )
        import pandas as pd

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

        mapped_variables = get_mapped_variables(variables, self.uri)
        arrow_table = list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="arrow_table",
            progress=progress,
            coerce_schema=True,
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
        mapped_variables = get_mapped_variables(variables, self.uri)
        return list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="arrow_iterator",
            progress=progress,
            coerce_schema=True,
        )

    def list_rows(self, max_results=None, *, variables=None, progress=True):
        warnings.warn(
            "The list_rows method is deprecated. Please use table.to_arrow_batch_iterator() or table.to_arrow_table().to_pylist() for better performance and memory utilization.",
            FutureWarning, stacklevel=2)

        mapped_variables = get_mapped_variables(variables, self.uri)
        return list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="tuple",
            progress=progress,
            coerce_schema=True,
        )

    def variable(self, name):
        return Variable(name, upload=self)


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
