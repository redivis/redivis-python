
import pandas as pd
from collections import namedtuple
import pyarrow
from tqdm.auto import tqdm
from ..common.api_request import make_request

def list_rows(
    *, uri, type="tuple", max_results=None, selected_variables=None, mapped_variables=None
):
    format = 'arrow'
    read_session = make_request(
        method="post",
        path=f'{uri}/readSessions',
        parse_response=True,
        payload={
            "selectedVariables": selected_variables,
            "maxResults": max_results,
            "format": format
        },
    )

    progressbar = tqdm(total=max_results, leave=False)
    stream_results = []
    for stream in read_session["streams"]:
        arrow_response = make_request(
            method="get",
            path=f'/readStreams/{stream["id"]}',
            stream=True,
            parse_response=False,
        )

        reader = pyarrow.ipc.open_stream(arrow_response.raw)
        batches = []
        for batch in reader:
            batches.append(batch)
            progressbar.update(batch.num_rows)

        if type == "tuple":
            stream_results.append(pyarrow.Table.from_batches(batches).to_pydict())
        else:
            stream_results.append(pyarrow.Table.from_batches(batches).to_pandas())

    if type == "tuple":
        Row = namedtuple(
            "Row",
            [variable["name"] for variable in mapped_variables],
        )
        res = []
        for pydict in stream_results:
            keys = list(pydict.keys())
            for i in range(len(pydict[keys[0]])):
                if len(res) == max_results:
                    break
                res.append(Row(*[format_tuple_type(pydict[variable["name"]][i], variable["type"]) if variable["name"] in pydict else None for variable in mapped_variables]))

        progressbar.close()
        return res
    else:
        df = pd.concat(stream_results) if len(stream_results) > 1 else stream_results[0]
        df = set_dataframe_types(df, mapped_variables)
        if len(df.index) > max_results:
            return df.iloc[0:max_results, :]

        progressbar.close()
        return df

    # Old avro parsing, removed in preference of Arrow
    # if type == "tuple":
    #     res = []
    #     for stream in read_session["streams"]:
    #         avro_response = make_request(
    #             method="get",
    #             path=f'/readStreams/{stream["id"]}',
    #             stream=True,
    #             parse_response=False,
    #         )
    #         parsed_schema = fastavro.parse_schema(read_session["schemas"][stream["schemaIndex"]])
    #
    #         Row = namedtuple(
    #             "Row",
    #             [variable["name"] for variable in mapped_variables],
    #         )
    #         while True:
    #             try:
    #                 res.append(Row(**fastavro.schemaless_reader(avro_response.raw, parsed_schema)))
    #             except Exception as err:
    #                 break
    #             progressbar.update()
    #
    #         progressbar.close()
    #
    #     return res
    # else:
    #     res = []
    #     for stream in read_session["streams"]:
    #         avro_response = make_request(
    #             method="get",
    #             path=f'/readStreams/{stream["id"]}',
    #             stream=True,
    #             parse_response=False,
    #         )
    #         parsed_schema = fastavro.parse_schema(read_session["schemas"][stream["schemaIndex"]])
    #
    #         while True:
    #             try:
    #                 res.append(fastavro.schemaless_reader(avro_response.raw, parsed_schema))
    #             except Exception as err:
    #                 break
    #             progressbar.update()
    #
    #         progressbar.close()
    #
    #     df = pd.DataFrame(res, dtype="string")
    #     return set_dataframe_types(df, mapped_variables)


def format_tuple_type(val, type):
    if val is None:
        return val
    elif type == "integer":
        return int(val)
    elif type == "float":
        return float(val)
    elif type == "date":
        return str(val)
    elif type == "dateTime":
        return str(val)
    elif type == "time":
        return str(val)
    elif type == "boolean":
        return bool(val)
    else:
        return str(val)


def set_dataframe_types(df, variables):
    for variable in variables:
        name = variable["name"]
        type = variable["type"]

        # TODO: need to finalize what types we're returning
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
