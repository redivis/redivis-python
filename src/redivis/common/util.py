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
