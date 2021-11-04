import csv
import gzip
import io
import pandas as pd
from collections import namedtuple

from ..common.api_request import make_request


def list_rows(
    *, uri, type="tuple", max_results=None, selected_variables=None, mapped_variables
):
    res = make_request(
        method="get",
        path=uri,
        parse_response=False,
        stream=type != "tuple",
        query={
            "selectedVariables": ",".join([name for name in selected_variables])
            if selected_variables
            else None,
            "maxResults": max_results,
            "format": "csv",
        },
    )
    if type == "tuple":
        Row = namedtuple(
            "Row",
            [variable["name"] for variable in mapped_variables],
        )

        reader = csv.reader(io.StringIO(res.text))

        return [Row(*row) for row in reader]
    else:
        df = pd.read_csv(
            res.raw,
            dtype="string",
            names=[variable["name"] for variable in mapped_variables],
            compression="gzip"
            if res.headers.get("content-encoding") == "gzip"
            else None,
        )

        return set_dataframe_types(df, mapped_variables)


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
            df[name].replace(
                to_replace=["TRUE", "FALSE"], value=[True, False], inplace=True
            )
            # Pandas seems to throw errors if all the boolean values are NA, in which case we should just ignore and fall back to a string dtype
            df[name] = df[name].astype("boolean", errors="ignore")
        else:
            df[name] = df[name].astype("string")

    return df
