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