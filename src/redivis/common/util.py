import os
import re
import sys
import tempfile
import time
import atexit
import shutil
import pathlib
import uuid
import logging

from ..common import exceptions
from .config import get_config


def get_warning(kind):
    if kind == "dataframe_deprecation":
        return 'The to_dataframe() method is deprecated, and has been superceded by to_pandas_dataframe().\nBy default, this new method uses the "pyarrow" dtype_backend, which is more performant and will generally work with existing code.\nTo replicate historic behavior, use to_pandas_dataframe(dtype_backend="numpy").'
    elif kind == "geodataframe_deprecation":
        return "Please use the to_geopandas_dataframe() method to ensure future compatability."
    elif kind == "source_tables_deprecation":
        return "The source_tables() method has been renamed to referenced_tables(); please use this instead. This method will be removed in the future."
    else:
        return "WARNING"


def split_reference(reference):
    """Splits a reference like `owner.dataset:v1_0.table` into its dot-separated parts.

    A version tag can also be written with a dot (`:v1.0`), so it's first normalized
    to the equivalent underscore form.
    """
    return re.sub(r":v(\d+)\.(\d+)", r":v\1_\2", reference).split(".")


def normalize_version_tag(tag, *, for_reference=False):
    """Normalizes a version tag, e.g. `1.0` or `v1_0`, to the version's tag (`v1.0`).

    Since a dot also separates the parts of a reference, a reference writes the tag as
    the equivalent `v1_0`, which is what `for_reference` returns.
    """
    if not tag:
        return None
    tag = re.sub(
        r"^(v?\d+)[._](\d+)$",
        r"\1_\2" if for_reference else r"\1.\2",
        str(tag),
        flags=re.IGNORECASE,
    )
    if tag != "current" and tag != "next" and not tag.lower().startswith("v"):
        tag = f"v{tag}"
    return tag


def wait_for_run(instance, run_properties):
    """Waits for a transform or notebook run, given the response to starting it.

    Raises a JobError if the run fails or is cancelled.
    """
    job_id = (run_properties.get("currentJob") or {}).get("id")
    while True:
        time.sleep(2)
        instance.get()
        jobs = [
            instance.properties.get("currentJob"),
            instance.properties.get("lastRunJob"),
        ]
        if job_id:
            # Track the job this run started, rather than whichever is most recent
            job = next((j for j in jobs if j and j.get("id") == job_id), None)
        else:
            job = jobs[0] or jobs[1]
        if job and job["status"] in ["completed", "failed", "cancelled"]:
            if job["status"] != "completed":
                raise exceptions.JobError(
                    message=job.get("errorMessage"),
                    status=job.get("status"),
                    kind=job.get("kind"),
                )
            return
        logging.debug("Job is still in progress...")


created_temp_dir = None


def rm_tempdir():
    global created_temp_dir
    shutil.rmtree(created_temp_dir, ignore_errors=True)


def get_tempdir():
    if get_config("tmp_dir"):
        user_suffix = os.environ.get("USER", os.environ.get("USERNAME")) or os.getuid()
        return str(
            pathlib.Path("/").joinpath(
                os.path.expanduser(get_config("tmp_dir")),
                f"redivis_{user_suffix}",
            )
        )

    global created_temp_dir
    if created_temp_dir is None:
        created_temp_dir = tempfile.mkdtemp()
        atexit.register(rm_tempdir)

    return created_temp_dir


def is_instance_of(data, module_name, class_name):
    """Whether data is an instance of module_name.class_name, without importing that module.

    If the module hasn't been imported, data can't be one of its classes; so data
    libraries that aren't used (or installed) are never imported.
    """
    module = sys.modules.get(module_name)
    return module is not None and isinstance(data, getattr(module, class_name))


def get_parquet_rows_per_group(data):
    import pyarrow as pa
    import pyarrow.dataset as pa_dataset

    TARGET_GROUP_BYTES = 128 * 1024**2  # ~128MB per row group
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
        n_rows = data.count_rows()  # metadata-only for parquet
        if n_rows > 0 and total_bytes > 0:
            bytes_per_row = total_bytes / n_rows

    # --- Arrow Table / RecordBatch: zero-copy slice, measure that ---
    elif isinstance(data, (pa.Table, pa.RecordBatch)):
        n_rows = data.num_rows
        if n_rows > 0:
            s = data.slice(0, min(SAMPLE_N, n_rows))
            bytes_per_row = s.nbytes / s.num_rows

    # --- Dask DataFrame: measure one partition, scale by row count ---
    elif is_instance_of(data, "dask.dataframe", "DataFrame"):
        head = data.head(SAMPLE_N, compute=True)
        if len(head) > 0:
            bytes_per_row = head.memory_usage(deep=True).sum() / len(head)

    # --- pandas / polars eager DataFrame ---
    elif is_instance_of(data, "pandas", "DataFrame"):
        n_rows = len(data)
        if n_rows > 0:
            head = data.head(SAMPLE_N)
            bytes_per_row = head.memory_usage(deep=True).sum() / len(head)
    elif is_instance_of(data, "polars", "DataFrame"):
        n_rows = data.height
        if n_rows > 0:
            tbl = data.head(SAMPLE_N).to_arrow()
            bytes_per_row = tbl.nbytes / tbl.num_rows
        # --- Polars LazyFrame: fetch a small slice, measure as Arrow ---
    elif is_instance_of(data, "polars", "LazyFrame"):
        sample = data.head(SAMPLE_N).collect()
        if sample.height > 0:
            tbl = sample.to_arrow()
            bytes_per_row = tbl.nbytes / tbl.num_rows

    if bytes_per_row is None or bytes_per_row <= 0:
        return 100_000

    rpg = int(TARGET_GROUP_BYTES / bytes_per_row)
    return max(MIN_RPG, min(MAX_RPG, rpg))


def get_dask_parquet_schema(data):
    """The schema for dask to write a DataFrame to parquet with.

    dask infers the type of each column from the DataFrame's metadata, which doesn't
    know what an object column holds: earlier versions of dask treat one as a string, so
    writing one that holds anything else (such as dates) fails, and newer versions fill
    it with placeholder objects that pyarrow can't infer a type from at all. Instead,
    give those the type that pyarrow infers from a sample of their values.
    """
    import pyarrow as pa

    object_columns = [name for name, dtype in data.dtypes.items() if dtype == object]
    if not object_columns:
        return "infer"

    sample = data[object_columns].head(10_000, npartitions=-1)
    types = {}
    for name in object_columns:
        # A column with no values in the sample is a string, as dask has inferred
        types[name] = pa.string()
        values = sample[name].dropna()
        if len(values):
            try:
                types[name] = pa.array(values, from_pandas=True).type
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                # e.g., values of mixed types, which dask will report on when it fails
                # to write them as strings
                pass

    # Given the types of only some columns, dask still infers every column's from the
    # metadata, so give it the whole schema, inferring the others' as dask would
    other_columns = pa.Schema.from_pandas(
        data._meta_nonempty.drop(columns=object_columns), preserve_index=False
    )
    return pa.schema(
        [
            (
                pa.field(name, types[name])
                if name in types
                else other_columns.field(name)
            )
            for name in data.columns
        ]
    )


def convert_data_to_parquet(data):
    temp_file_path = f"{get_tempdir()}/parquet/{uuid.uuid4()}"
    pathlib.Path(temp_file_path).parent.mkdir(exist_ok=True, parents=True)

    import pyarrow as pa
    import pyarrow.dataset as pa_dataset
    import pyarrow.parquet as pa_parquet

    if is_instance_of(data, "dask.dataframe", "DataFrame"):
        import dask

        # dask writes a file for each partition, but an upload is a single file. So
        # write them to a directory, then combine them as an arrow Dataset, below.
        partitions_dir = f"{temp_file_path}_partitions"
        # By default, each new dask collection (including those made here, to write the
        # DataFrame) converts its object columns to strings, which would turn e.g. a
        # column of dates into strings. Write the DataFrame as it is instead.
        with dask.config.set({"dataframe.convert-string": False}):
            data.to_parquet(
                partitions_dir,
                write_index=False,
                schema=get_dask_parquet_schema(data),
                coerce_timestamps="us",
                allow_truncated_timestamps=True,
                write_statistics=False,
                engine="pyarrow",
            )
        try:
            return convert_data_to_parquet(
                pa_dataset.dataset(partitions_dir, format="parquet")
            )
        finally:
            shutil.rmtree(partitions_dir, ignore_errors=True)

    rows_per_group = get_parquet_rows_per_group(data)

    # IMPORTANT: we need to coerce all timestamps to us precision, since BQ doesn't support ns, and will then just load the timestamp as int64
    # This includes a geopandas.GeoDataFrame, whose to_parquet() writes its geometry as GeoParquet
    if is_instance_of(data, "pandas", "DataFrame"):
        data.to_parquet(
            path=temp_file_path,
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
            index=False,
            row_group_size=rows_per_group,
            write_statistics=False,
            engine="pyarrow",
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
                write_statistics=False,
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
            write_statistics=False,
        )
    elif is_instance_of(data, "polars", "LazyFrame"):
        data.sink_parquet(
            temp_file_path,
            row_group_size=rows_per_group,
            statistics=False,  # Note that this arg looks different than write_statistics elsewhere
        )
    elif is_instance_of(data, "polars", "DataFrame"):
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
    # NB: a niquests.Response for an error status is falsy, so test for None
    status_code = (
        response.status_code
        if response is not None
        else (response_json.get("status") if response_json else None)
    )
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
