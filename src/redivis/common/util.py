import os
import tempfile
import atexit
import shutil
import pathlib
import uuid


def get_geography_variable(variables, geography_variable_name):
    if geography_variable_name == "":
        for variable in variables:
            if variable["type"] == "geography":
                return variable

        return None
    else:
        for variable in variables:
            if variable["name"] == geography_variable_name:
                return variable

        raise Exception(
            f"The specified geography variable '{geography_variable_name()}' could not be found"
        )


def get_warning(kind):
    if kind == "dataframe_deprecation":
        return 'The to_dataframe() method is deprecated, and has been superceded by to_pandas_dataframe().\nBy default, this new method uses the "pyarrow" dtype_backend, which is more performant and will generally work with existing code.\nTo replicate historic behavior, use to_pandas_dataframe(dtype_backend="numpy").'
    elif kind == "geodataframe_deprecation":
        return "Please use the to_geopandas_dataframe() method to ensure future compatability."
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


def convert_data_to_parquet(data):
    temp_file_path = f"{get_tempdir()}/parquet/{uuid.uuid4()}"
    pathlib.Path(temp_file_path).parent.mkdir(exist_ok=True, parents=True)

    import geopandas
    import pandas as pd
    import pyarrow as pa
    import pyarrow.dataset as pa_dataset
    import pyarrow.parquet as pa_parquet
    from dask.dataframe import DataFrame as dask_df

    if isinstance(data, geopandas.GeoDataFrame):
        data.to_wkt().to_parquet(path=temp_file_path, index=False)
    elif isinstance(data, pd.DataFrame):
        data.to_parquet(path=temp_file_path, index=False)
    elif isinstance(data, pa_dataset.Dataset):
        pa_dataset.write_dataset(
            data,
            temp_file_path,
            format="parquet",
            basename_template="part-{i}.parquet",
            max_partitions=1,
        )
        temp_file_path = f"{temp_file_path}/part-0.parquet"
    elif isinstance(data, pa.Table):
        pa_parquet.write_table(data, temp_file_path)
    elif isinstance(data, dask_df):
        data.to_parquet(temp_file_path, write_index=False)
        temp_file_path = f"{temp_file_path}/part.0.parquet"
    else:
        # importing polars is causing an IllegalInstruction error on ARM + Docker. Import inline to avoid crashes elsewhwere
        # TODO: revert once fixed upstream
        import polars

        if isinstance(data, polars.LazyFrame):
            data.sink_parquet(temp_file_path)
        elif isinstance(data, polars.DataFrame):
            data.write_parquet(temp_file_path)
        else:
            raise Exception(
                "Unknown datatype provided. Must be an instance of pandas.DataFrame, pyarrow.Dataset, pyarrow.Table, dask.DataFrame, polars.LazyFrame, or polars.DataFrame"
            )

    return temp_file_path


def arrow_table_to_pandas(
    arrow_table, dtype_backend, date_as_object, max_parallelization
):
    import pandas as pd
    import pyarrow as pa

    pa.set_cpu_count(max_parallelization)
    pa.set_io_thread_count(max_parallelization)

    if dtype_backend not in ["numpy", "numpy_nullable", "pyarrow"]:
        raise Exception(
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
