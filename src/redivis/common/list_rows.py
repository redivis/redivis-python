import concurrent.futures
import multiprocessing as mp
import pyarrow
import pyarrow.dataset as pyarrow_dataset # need to import separately, it's not on the pyarrow import
import tempfile
import uuid
import pathlib
from ..classes.Row import Row
from tqdm.auto import tqdm
import shutil
from ..common.api_request import make_request

def list_rows(
    *, uri, output_type="dataframe", max_results=None, selected_variables=None, mapped_variables=None, target_parallelization=None, progress=True, coerce_schema
):
    read_session = make_request(
        method="post",
        path=f'{uri}/readSessions',
        parse_response=True,
        payload={
            "selectedVariables": selected_variables,
            "maxResults": max_results,
            "format": "arrow",
            "requestedStreamCount": min(mp.cpu_count(), 16) if target_parallelization is None else target_parallelization
        },
    )

    folder = f'/{tempfile.gettempdir()}/redivis/tables/{uuid.uuid4()}'
    pathlib.Path(folder).mkdir(parents=True, exist_ok=True)

    progressbar = None
    if progress:
        progressbar = tqdm(total=max_results, leave=False)

    # Code to use multiprocess... doesn't seem to give us any advantage, and progress doesn't currently work
    # with concurrent.futures.ProcessPoolExecutor(max_workers=len(read_session["streams"]), mp_context=mp.get_context('fork')) as executor:
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(read_session["streams"])) as executor:
        for stream in read_session["streams"]:
            executor.submit(process_stream, stream, folder, mapped_variables, coerce_schema, progressbar)

    if progress:
        progressbar.close()

    arrow_dataset = pyarrow_dataset.dataset(folder, format="feather", schema = pyarrow.schema(map(variable_to_field, mapped_variables)))

    if output_type == 'arrow_dataset':
        return arrow_dataset
    elif output_type == 'polars_lazyframe':
        import polars
        return polars.scan_ipc(f'{folder}/*', memory_map=True)
    elif output_type == 'dask_dataframe':
        import dask.dataframe as dd
        # TODO: simplify once dask supports reading from feather: https://github.com/dask/dask/issues/6865
        parquet_base_dir = f'/{tempfile.gettempdir()}/redivis/tables/{uuid.uuid4()}'
        pyarrow_dataset.write_dataset(arrow_dataset, parquet_base_dir, format='parquet')
        shutil.rmtree(folder)
        return dd.read_parquet(parquet_base_dir, dtype_backend='pyarrow')
    else:
        # TODO: remove head() once BE is sorted, instead use dataset.to_table()
        arrow_table = pyarrow_dataset.Scanner.from_dataset(arrow_dataset).head(max_results)
        shutil.rmtree(folder)
        if output_type == 'arrow_table':
            return arrow_table
        elif output_type == 'tuple':
            variable_name_to_index = {}
            for index, variable in enumerate(mapped_variables):
                variable_name_to_index[variable["name"]] = index

            pydict = arrow_table.to_pydict()
            keys = list(pydict.keys())

            return [
                Row([pydict[variable["name"]][i] for variable in mapped_variables], variable_name_to_index)
                for i in range(len(pydict[keys[0]]))
            ]


def variable_to_field(variable):
    if variable["type"] == 'string' or variable["type"] == 'geography':
        return pyarrow.field(variable["name"], pyarrow.string())
    elif variable["type"] == 'integer':
        return pyarrow.field(variable["name"], pyarrow.int64())
    elif variable["type"] == 'float':
        return pyarrow.field(variable["name"], pyarrow.float64())
    elif variable["type"] == 'date':
        return pyarrow.field(variable["name"], pyarrow.date32())
    elif variable["type"] == 'dateTime':
        return pyarrow.field(variable["name"], pyarrow.timestamp('us'))
    elif variable["type"] == 'time':
        return pyarrow.field(variable["name"], pyarrow.time64('us'))
    elif variable["type"] == 'boolean':
        return pyarrow.field(variable["name"], pyarrow.bool_())


def coerce_string_variable(pyarrow_array, variable):
    if variable["type"] == 'string' or variable["type"] == 'geography':
        return pyarrow_array
    elif variable["type"] == 'integer':
        return pyarrow.compute.cast(pyarrow_array, pyarrow.int64())
    elif variable["type"] == 'float':
        return pyarrow.compute.cast(pyarrow_array, pyarrow.float64())
    elif variable["type"] == 'date':
        return pyarrow.compute.cast(pyarrow.compute.cast(pyarrow_array, pyarrow.timestamp('us')), pyarrow.date32())
    elif variable["type"] == 'dateTime':
        return pyarrow.compute.cast(pyarrow_array, pyarrow.timestamp('us'))
    elif variable["type"] == 'time':
        return pyarrow.compute.cast(
            pyarrow.compute.cast(
                pyarrow.compute.utf8_replace_slice(pyarrow_array,start=0,stop=0,replacement='2020-01-01T'),
                pyarrow.timestamp('us')
            ),
            pyarrow.time64('us')
        )

    elif variable["type"] == 'boolean':
        return pyarrow.compute.cast(pyarrow_array, pyarrow.bool_())


def process_stream(stream,folder, mapped_variables, coerce_schema, progressbar):
    arrow_response = make_request(
        method="get",
        path=f'/readStreams/{stream["id"]}',
        stream=True,
        parse_response=False,
    )

    reader = pyarrow.ipc.RecordBatchStreamReader(arrow_response.raw)

    if coerce_schema:
        variables_in_stream = list(map(lambda field_name: next(x for x in mapped_variables if x["name"] == field_name), reader.schema.names))
        output_schema = pyarrow.schema(map(variable_to_field, variables_in_stream))
    else:
        output_schema = reader.schema

    with open(f"{folder}/{stream['id']}", "wb") as f:
        writer = pyarrow.ipc.RecordBatchFileWriter(f, output_schema)
        for batch in reader:
            if coerce_schema:
                batch = pyarrow.RecordBatch.from_arrays(list(map(coerce_string_variable, batch.columns, variables_in_stream)), schema=output_schema)

            writer.write_batch(batch)

            if progressbar is not None:
                progressbar.update(batch.num_rows)

        writer.close()

    reader.close()


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


