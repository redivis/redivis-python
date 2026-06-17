import os
import tempfile
import atexit
import shutil
import pathlib
import uuid

from ..common import exceptions


def get_warning(kind):
    if kind == "dataframe_deprecation":
        return 'The to_dataframe() method is deprecated, and has been superceded by to_pandas_dataframe().\nBy default, this new method uses the "pyarrow" dtype_backend, which is more performant and will generally work with existing code.\nTo replicate historic behavior, use to_pandas_dataframe(dtype_backend="numpy").'
    elif kind == "geodataframe_deprecation":
        return "Please use the to_geopandas_dataframe() method to ensure future compatability."
    elif kind == "source_tables_deprecation":
        return "The source_tables() method has been renamed to referenced_tables(); please use this instead. This method will be removed in the future."
    else:
        return "WARNING"


created_temp_dir = None


def rm_tempdir():
    global created_temp_dir
    shutil.rmtree(created_temp_dir, ignore_errors=True)


def get_tempdir():
    if os.getenv("REDIVIS_TMPDIR"):
        user_suffix = os.environ.get("USER", os.environ.get("USERNAME")) or os.getuid()
        return str(
            pathlib.Path("/").joinpath(
                os.getenv("REDIVIS_TMPDIR"),
                f"redivis_{user_suffix}",
            )
        )

    global created_temp_dir
    if created_temp_dir is None:
        created_temp_dir = tempfile.mkdtemp()
        atexit.register(rm_tempdir)

    return created_temp_dir

def get_parquet_rows_per_group(data):
    import pandas as pd
    import pyarrow as pa
    import pyarrow.dataset as pa_dataset
    from dask.dataframe import DataFrame as dask_df
    import polars

    TARGET_GROUP_BYTES = 128 * 1024**2   # ~128MB per row group
    MAX_RPG = 1_000_000
    MIN_RPG = 1
    SAMPLE_N = 10_000

    bytes_per_row = None

    # --- Arrow Dataset: metadata-only, sum file sizes / row count ---
    if isinstance(data, pa_dataset.Dataset):
        total_bytes = 0
        for f in data.files:
            try:
                total_bytes += os.path.getsize(f)
            except OSError:
                pass
        n_rows = data.count_rows()          # metadata-only for parquet
        if n_rows > 0 and total_bytes > 0:
            bytes_per_row = total_bytes / n_rows

    # --- Arrow Table / RecordBatch: zero-copy slice, measure that ---
    elif isinstance(data, (pa.Table, pa.RecordBatch)):
        n_rows = data.num_rows
        if n_rows > 0:
            s = data.slice(0, min(SAMPLE_N, n_rows))
            bytes_per_row = s.nbytes / s.num_rows

    # --- Dask DataFrame: measure one partition, scale by row count ---
    elif isinstance(data, dask_df):
        head = data.head(SAMPLE_N, compute=True)
        if len(head) > 0:
            bytes_per_row = head.memory_usage(deep=True).sum() / len(head)

    # --- pandas / polars eager DataFrame ---
    elif isinstance(data, pd.DataFrame):
        n_rows = len(data)
        if n_rows > 0:
            head = data.head(SAMPLE_N)
            bytes_per_row = head.memory_usage(deep=True).sum() / len(head)
    elif isinstance(data, polars.DataFrame):
        n_rows = data.height
        if n_rows > 0:
            tbl = data.head(SAMPLE_N).to_arrow()
            bytes_per_row = tbl.nbytes / tbl.num_rows
        # --- Polars LazyFrame: fetch a small slice, measure as Arrow ---
    elif isinstance(data, polars.LazyFrame):
        sample = data.head(SAMPLE_N).collect()
        if sample.height > 0:
            tbl = sample.to_arrow()
            bytes_per_row = tbl.nbytes / tbl.num_rows

    if bytes_per_row is None or bytes_per_row <= 0:
        return 100_000

    rpg = int(TARGET_GROUP_BYTES / bytes_per_row)
    return max(MIN_RPG, min(MAX_RPG, rpg))

def convert_data_to_parquet(data):
    temp_file_path = f"{get_tempdir()}/parquet/{uuid.uuid4()}"
    pathlib.Path(temp_file_path).parent.mkdir(exist_ok=True, parents=True)

    import geopandas
    import polars
    import pandas as pd
    import pyarrow as pa
    import pyarrow.dataset as pa_dataset
    import pyarrow.parquet as pa_parquet
    from dask.dataframe import DataFrame as dask_df

    rows_per_group = get_parquet_rows_per_group(data)

    # IMPORTANT: we need to coerce all timestamps to us precision, since BQ doesn't support ns, and will then just load the timestamp as int64
    if isinstance(data, geopandas.GeoDataFrame):
        data.to_parquet(
            path=temp_file_path,
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
            index=False,
            row_group_size=rows_per_group,
            write_statistics=False,
            engine="pyarrow"
        )
    elif isinstance(data, pd.DataFrame):
        data.to_parquet(
            path=temp_file_path,
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
            index=False,
            row_group_size=rows_per_group,
            write_statistics=False,
            engine="pyarrow"
        )
    elif isinstance(data, pa_dataset.Dataset):
        pa_dataset.write_dataset(
            data,
            temp_file_path,
            format="parquet",
            basename_template="part-{i}.parquet",
            file_options=pa_dataset.ParquetFileFormat().make_write_options(
                coerce_timestamps="us",
                allow_truncated_timestamps=True,
                write_statistics=False
            ),
            min_rows_per_group=rows_per_group,
            max_rows_per_group=rows_per_group,
            max_partitions=1,
        )
        temp_file_path = f"{temp_file_path}/part-0.parquet"
    elif isinstance(data, (pa.Table, pa.RecordBatch)):
        if isinstance(data, pa.RecordBatch):
             data = pa.Table.from_batches([data])
        pa_parquet.write_table(
            data,
            temp_file_path,
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
            row_group_size=rows_per_group,
            write_statistics=False
        )
    elif isinstance(data, dask_df):
        # TODO: this can be multiple files, we'll want to refactor once we have the new import API
        data.to_parquet(
            temp_file_path,
            write_index=False,
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
            row_group_size=rows_per_group,
            write_statistics=False,
            engine="pyarrow"
        )
        temp_file_path = f"{temp_file_path}/part.0.parquet"
    elif isinstance(data, polars.LazyFrame):
        data.sink_parquet(
            temp_file_path, 
            row_group_size=rows_per_group,
            statistics=False # Note that this arg looks different than write_statistics elsewhere
        )
    elif isinstance(data, polars.DataFrame):
        data.write_parquet(
            temp_file_path,
            use_pyarrow=True,
            pyarrow_options={
                "row_group_size": rows_per_group,
                "write_statistics": False,
                "coerce_timestamps": "us",
                "allow_truncated_timestamps": True,
            },
        )
    else:
        raise exceptions.ValueError(
            "Unknown datatype provided. Must be an instance of pandas.DataFrame, pyarrow.Dataset, pyarrow.Table, dask.DataFrame, polars.LazyFrame, or polars.DataFrame"
        )

    return temp_file_path


def raise_api_error(response_json=None, response_text=None, response=None):
    status_code = response.status_code if response else response_json.get("status")
    error = response_json.get("error") if response_json else "api_error"
    description = (
        response_json.get("error_description") if response_json else response_text
    )
    if status_code == 404:
        raise exceptions.NotFoundError(
            message=error,
            status_code=404,
            error_description=description,
        ) from None
    elif status_code == 403:
        raise exceptions.AuthorizationError(
            message=error,
            status_code=403,
            error_description=description,
        ) from None
    else:
        raise exceptions.APIError(
            message=error,
            status_code=status_code,
            error_description=description,
        ) from None
