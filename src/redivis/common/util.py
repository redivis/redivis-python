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
    if kind == 'dataframe_dtype':
        return 'No dtype_backend was provided: it is highly recommended to specify dtype_backend=pyarrow to reduce memory usage and improve performance. This may become the default in the future.'
    else:
        return 'WARNING'