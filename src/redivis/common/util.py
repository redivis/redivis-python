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
    if kind == 'dataframe_deprecation':
        return 'The to_dataframe() method is deprecated, and has been superceded by to_pandas_dataframe().\nBy default, this new method uses the "pyarrow" dtype_backend, which is more performant and will generally work with existing code.\nTo replicate historic behavior, use to_pandas_dataframe(dtype_backend="numpy").'
    elif kind == 'geodataframe_deprecation':
        return 'Please use the to_geopandas_dataframe() method to ensure future compatability.'
    else:
        return 'WARNING'


def arrow_table_to_pandas(arrow_table, dtype_backend, date_as_object, max_parallelization):
    import pandas as pd
    import pyarrow as pa

    pa.set_cpu_count(max_parallelization)
    pa.set_io_thread_count(max_parallelization)

    if dtype_backend not in ['numpy', 'numpy_nullable', 'pyarrow']:
        raise Exception(
            f"Unknown dtype_backend. Must be one of 'pyarrow'|'numpy_nullable'|'numpy'. Default is 'pyarrow'")

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

