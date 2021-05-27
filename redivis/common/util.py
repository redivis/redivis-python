import pandas as pd


def set_dataframe_types(df, variables):
    for variable in variables:
        name = variable["name"]
        type = variable["type"]

        if type == "integer":
            df[name] = pd.to_numeric(df[name])
            # df[name] = df[name].astype("Int64")
        elif type == "float":
            df[name] = df[name].astype("float64")
            # df[name] = df[name].astype("Float64")
        elif type == "date":
            df[name] = pd.to_datetime(df[name], errors="coerce")
        elif type == "dateTime":
            df[name] = pd.to_datetime(df[name], errors="coerce")
        elif type == "time":
            df[name] = pd.to_timedelta(df[name], errors="coerce")
        elif type == "boolean":
            df.replace(to_replace=["true", "false"], value=[True, False], inplace=True)
            df[name] = df[name].astype("boolean")
        else:
            df[name] = df[name].astype("string")

    return df
