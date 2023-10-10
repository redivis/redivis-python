import time
import re
import math
import os
import json
import logging
import requests
from urllib.parse import quote as quote_uri
import warnings
import io
import pyarrow as pa
import pandas as pd

from ..common.util import get_geography_variable, get_warning
from ..common.list_rows import list_rows

from .Base import Base
from .Variable import Variable
from ..common.api_request import make_request, make_paginated_request

# 8MB
MAX_CHUNK_SIZE = 2 ** 23


class Upload(Base):
    def __init__(
        self,
        name,
        *,
        table,
        properties={},
    ):
        self.table = table
        self.name = name
        self.uri = f"{table.uri}/uploads/{quote_uri(self.name,'')}"
        self.properties = {
            **{
                "kind": "upload",
                "name": name,
                "uri": self.uri
            },
            **properties
        }

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
    ):
        resumable_upload_id = None

        if data and (
            (hasattr(data, "read") and os.stat(data.name).st_size > 1e7)
            or (hasattr(data, "__len__") and len(data) > 1e7)
        ):
            resumable_upload_id = upload_file(data, self.table.uri)
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
            "resumableUploadId": resumable_upload_id,
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
            if (data or resumable_upload_id or transfer_specification) and wait_for_finish:
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
            make_request(method="GET", path=self.uri)
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
        if not self.properties or not hasattr(self.properties, "numRows"):
            self.get()

        mapped_variables = get_mapped_variables(variables, self.uri)

        return list_rows(
            uri=self.uri,
            max_results=self.properties["numRows"] if max_results is None else min(max_results,
                                                                                   int(self.properties["numRows"])),
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="arrow_dataset",
            progress=progress,
            coerce_schema=hasattr(self.properties, "container") is False or self.properties["container"][
                "kind"] == 'dataset',
            batch_preprocessor=batch_preprocessor
        )

    def to_arrow_table(self, max_results=None, *, variables=None, progress=True, batch_preprocessor=None):
        if not self.properties or not hasattr(self.properties, "numRows"):
            self.get()

        mapped_variables = get_mapped_variables(variables, self.uri)

        return list_rows(
            uri=self.uri,
            max_results=self.properties["numRows"] if max_results is None else min(max_results,
                                                                                   int(self.properties["numRows"])),
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="arrow_table",
            progress=progress,
            coerce_schema=hasattr(self.properties, "container") is False or self.properties["container"][
                "kind"] == 'dataset',
            batch_preprocessor=batch_preprocessor
        )

    def to_polars_lazyframe(self, max_results=None, *, variables=None, progress=True, batch_preprocessor=None):
        if not self.properties or not hasattr(self.properties, "numRows"):
            self.get()

        mapped_variables = get_mapped_variables(variables, self.uri)

        return list_rows(
            uri=self.uri,
            max_results=self.properties["numRows"] if max_results is None else min(max_results,
                                                                                   int(self.properties["numRows"])),
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="polars_lazyframe",
            progress=progress,
            coerce_schema=hasattr(self.properties, "container") is False or self.properties["container"][
                "kind"] == 'dataset',
            batch_preprocessor=batch_preprocessor
        )

    def to_dask_dataframe(self, max_results=None, *, variables=None, progress=True, batch_preprocessor=None):
        if not self.properties or not hasattr(self.properties, "numRows"):
            self.get()

        mapped_variables = get_mapped_variables(variables, self.uri)

        return list_rows(
            uri=self.uri,
            max_results=self.properties["numRows"] if max_results is None else min(max_results,
                                                                                   int(self.properties["numRows"])),
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="dask_dataframe",
            progress=progress,
            coerce_schema=hasattr(self.properties, "container") is False or self.properties["container"][
                "kind"] == 'dataset',
            batch_preprocessor=batch_preprocessor
        )

    def to_pandas_dataframe(self, max_results=None, *, variables=None, progress=True, dtype_backend='pyarrow',
                            date_as_object=False, batch_preprocessor=None):
        if dtype_backend not in ['numpy', 'numpy_nullable', 'pyarrow']:
            raise Exception(
                f"Unknown dtype_backend. Must be one of 'pyarrow'|'numpy_nullable'|'numpy'. Default is 'pyarrow'")

        if not self.properties or not hasattr(self.properties, "numRows"):
            self.get()

        mapped_variables = get_mapped_variables(variables, self.uri)
        arrow_table = list_rows(
            uri=self.uri,
            max_results=self.properties["numRows"] if max_results is None else min(max_results,
                                                                                   int(self.properties["numRows"])),
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="arrow_table",
            progress=progress,
            coerce_schema=hasattr(self.properties, "container") is False or self.properties["container"][
                "kind"] == 'dataset',
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

    def to_geopandas_dataframe(self, max_results=None, *, variables=None, geography_variable="", progress=True,
                               dtype_backend='pyarrow', date_as_object=False, batch_preprocessor=None):
        import geopandas

        if dtype_backend not in ['numpy', 'numpy_nullable', 'pyarrow']:
            raise Exception(
                f"Unknown dtype_backend. Must be one of 'pyarrow'|'numpy_nullable'|'numpy'. Default is 'pyarrow'")

        if not self.properties or not hasattr(self.properties, "numRows"):
            self.get()

        mapped_variables = get_mapped_variables(variables, self.uri)
        arrow_table = list_rows(
            uri=self.uri,
            max_results=self.properties["numRows"] if max_results is None else min(max_results,
                                                                                   int(self.properties["numRows"])),
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="arrow_table",
            progress=progress,
            coerce_schema=hasattr(self.properties, "container") is False or self.properties["container"][
                "kind"] == 'dataset',
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

        if not self.properties or not hasattr(self.properties, "numRows"):
            self.get()

        mapped_variables = get_mapped_variables(variables, self.uri)
        arrow_table = list_rows(
            uri=self.uri,
            max_results=self.properties["numRows"] if max_results is None else min(max_results,
                                                                                   int(self.properties["numRows"])),
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="arrow_table",
            progress=progress,
            coerce_schema=hasattr(self.properties, "container") is False or self.properties["container"][
                "kind"] == 'dataset'
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
        if not self.properties or not hasattr(self.properties, "numRows"):
            self.get()

        mapped_variables = get_mapped_variables(variables, self.uri)
        return list_rows(
            uri=self.uri,
            max_results=self.properties["numRows"] if max_results is None else min(max_results, int(self.properties["numRows"])),
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="arrow_iterator",
            progress=progress,
            coerce_schema=hasattr(self.properties, "container") is False or self.properties["container"][
                "kind"] == 'dataset'
        )

    def list_rows(self, max_results=None, *, variables=None, progress=True):
        warnings.warn(
            "The list_rows method is deprecated. Please use table.to_arrow_batch_iterator() or table.to_arrow_table().to_pylist() for better performance and memory utilization.",
            FutureWarning, stacklevel=2)

        if not self.properties or not hasattr(self.properties, "numRows"):
            self.get()

        mapped_variables = get_mapped_variables(variables, self.uri)
        return list_rows(
            uri=self.uri,
            max_results=self.properties["numRows"] if max_results is None else min(max_results,
                                                                                   int(self.properties["numRows"])),
            selected_variables=variables,
            mapped_variables=mapped_variables,
            output_type="tuple",
            progress=progress,
            coerce_schema=hasattr(self.properties, "container") is False or self.properties["container"][
                "kind"] == 'dataset'
        )

    def variable(self, name):
        return Variable(name, upload=self)


def upload_file(data, table_uri):
    start_byte = 0
    retry_count = 0
    chunk_size = MAX_CHUNK_SIZE
    did_reopen_file = False
    is_file = True if hasattr(data, "read") else False
    file_size = os.stat(data.name).st_size if is_file else len(data)

    if (is_file and hasattr(data, 'mode') and 'b' not in data.mode):
        data = open(data.name, 'rb')
        did_reopen_file = True

    res = make_request(
        path=f"{table_uri}/resumableUpload",
        method="POST",
        payload={"size": file_size},
    )
    resumable_uri = res["url"]
    resumable_upload_id = res["id"]

    while start_byte < file_size:
        end_byte = min(start_byte + chunk_size - 1, file_size - 1)
        if is_file:
            data.seek(start_byte)
            chunk = data.read(end_byte - start_byte + 1)
        else:
            chunk = data[start_byte : end_byte + 1]

        try:
            res = requests.put(
                url=resumable_uri,
                headers={
                    "Content-Length": f"{end_byte - start_byte + 1}",
                    "Content-Range": f"bytes {start_byte}-{end_byte}/{file_size}",
                },
                data=chunk,
            )
            res.raise_for_status()

            start_byte += chunk_size
            retry_count = 0
        except Exception as e:
            if retry_count > 20:
                print("A network error occurred. Upload failed after too many retries.")
                raise e

            retry_count += 1
            time.sleep(retry_count)
            print("A network error occurred. Retrying last chunk of resumable upload.")
            start_byte = retry_partial_upload(
                file_size=file_size, resumable_uri=resumable_uri
            )

    if did_reopen_file:
        data.close()

    return resumable_upload_id

def retry_partial_upload(*, retry_count=0, file_size, resumable_uri):
    logging.debug("Attempting to resume upload")

    try:
        res = requests.put(
            url=resumable_uri,
            headers={"Content-Length": "0", "Content-Range": f"bytes */{file_size}"},
        )

        if res.status_code == 404:
            return 0

        res.raise_for_status()

        if res.status_code == 200 or res.status_code == 201:
            return file_size
        elif res.status_code == 308:
            range_header = res.headers["Range"]

            if range_header:
                match = re.match(r"bytes=0-(\d+)", range_header)
                if match.group(0) and not math.isnan(int(match.group(1))):
                    return int(match.group(1)) + 1
                else:
                    raise Exception("An unknown error occurred. Please try again.")
            # If GCS hasn't received any bytes, the header will be missing
            else:
                return 0
    except Exception as e:
        if retry_count > 10:
            raise e

        time.sleep(retry_count / 10)
        retry_partial_upload(
            retry_count=retry_count + 1,
            file_size=file_size,
            resumable_uri=resumable_uri,
        )


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
