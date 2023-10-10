import concurrent.futures
import multiprocessing as mp
import pyarrow
import pyarrow.dataset as pyarrow_dataset # need to import separately, it's not on the pyarrow import
import tempfile
import uuid
import os
import pathlib
from ..classes.Row import Row
from tqdm.auto import tqdm
import shutil
from ..common.api_request import make_request

class RedivisArrowIterator:
    def __init__(self, streams, mapped_variables, progressbar, coerce_schema):
        self.streams = streams
        self.mapped_variables = mapped_variables
        self.progressbar = progressbar
        self.coerce_schema = coerce_schema
        self.current_stream_index = 0
        self.__get_next_reader__()

    def __get_next_reader__(self):
        arrow_response = make_request(
            method="get",
            path=f'/readStreams/{self.streams[self.current_stream_index]["id"]}',
            stream=True,
            parse_response=False,
        )
        self.current_record_batch_reader = pyarrow.ipc.RecordBatchStreamReader(arrow_response.raw)
        if self.coerce_schema:
            self.variables_in_stream = list(
                map(lambda field_name: next(x for x in self.mapped_variables if x["name"] == field_name),
                    self.current_record_batch_reader.schema.names))
            self.output_schema = pyarrow.schema(map(variable_to_field, self.variables_in_stream))
        else:
            self.output_schema = self.current_record_batch_reader.schema

    def __iter__(self):
        return self

    def __next__(self):
        try:
            batch = self.current_record_batch_reader.read_next_batch()
            if self.coerce_schema:
                batch = pyarrow.RecordBatch.from_arrays(
                    list(map(coerce_string_variable, batch.columns, self.variables_in_stream)), schema=self.output_schema)

            if self.progressbar is not None:
                self.progressbar.update(batch.num_rows)

            return batch
        except StopIteration:
            if self.current_stream_index == len(self.streams) - 1:
                if self.progressbar:
                    self.progressbar.close()
                raise StopIteration
            else:
                self.current_stream_index += 1
                self.__get_next_reader__()

def list_rows(
    *,
    uri,
    output_type="dataframe",
    max_results=None,
    selected_variables=None,
    mapped_variables=None,
    target_parallelization=None,
    progress=True,
    coerce_schema = False,
    batch_preprocessor = None
):
    if target_parallelization is None:
        target_parallelization = mp.cpu_count()

    read_session = make_request(
        method="post",
        path=f'{uri}/readSessions',
        parse_response=True,
        payload={
            "selectedVariables": selected_variables,
            "maxResults": max_results,
            "format": "arrow",
            "requestedStreamCount": min(target_parallelization, 16)
        },
    )

    folder = f'/{tempfile.gettempdir()}/redivis/tables/{uuid.uuid4()}'
    pathlib.Path(folder).mkdir(parents=True, exist_ok=True)

    progressbar = None
    if progress:
        progressbar = tqdm(total=max_results, leave=False)

    if output_type == 'arrow_iterator':
        return RedivisArrowIterator(streams=read_session["streams"], mapped_variables=mapped_variables, progressbar=progressbar, coerce_schema=coerce_schema )


    # Use download_state to notify worker threads when to quit.
    # See: https://stackoverflow.com/a/29237343/101923
    download_state = {"done": False}

    # Code to use multiprocess... would simplify exiting on stop, but progress doesn't currently work
    # with concurrent.futures.ProcessPoolExecutor(max_workers=len(read_session["streams"]), mp_context=mp.get_context('fork')) as executor:

    # See https://github.com/googleapis/python-bigquery/blob/main/google/cloud/bigquery/_pandas_helpers.py#L920
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(read_session["streams"])) as executor:
        futures = [executor.submit(process_stream, stream, folder, mapped_variables, coerce_schema, progressbar, download_state, batch_preprocessor)
                   for stream in read_session["streams"]]

        not_done = futures

        try:
            while not_done:
                # next line 'sleeps' this main thread, letting the thread pool run
                freshly_done, not_done = concurrent.futures.wait(not_done, timeout=0.2)
                for future in freshly_done:
                    # Call result() on any finished threads to raise any exceptions encountered.
                    future.result()
        finally:
            for future in not_done:
                # Only cancels futures that were never started
                future.cancel()
            download_state["done"] = True
            # Shutdown all background threads, now that they should know to exit early.
            executor.shutdown(wait=True, cancel_futures=True)

    if progress:
        progressbar.close()

    arrow_dataset = pyarrow_dataset.dataset(folder, format="feather", schema = pyarrow.schema(map(variable_to_field, mapped_variables)) if batch_preprocessor is None else None)

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


def process_stream(stream, folder, mapped_variables, coerce_schema, progressbar, download_state, batch_preprocessor):
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

    has_content = False
    with open(f"{folder}/{stream['id']}", "wb") as f:
        writer = None
        for batch in reader:
            # exit out of thread
            if download_state["done"]:
                break

            if coerce_schema:
                batch = pyarrow.RecordBatch.from_arrays(list(map(coerce_string_variable, batch.columns, variables_in_stream)), schema=output_schema)

            num_rows = batch.num_rows
            if batch_preprocessor:
                batch = batch_preprocessor(batch)

            if batch is not None:
                has_content = True
                if writer is None:
                    writer = pyarrow.ipc.RecordBatchFileWriter(f, output_schema if batch_preprocessor is None else batch.schema)

                writer.write_batch(batch)

            if progressbar is not None:
                progressbar.update(num_rows)

        if writer is not None:
            writer.close()

    if has_content == False:
        os.remove(f"{folder}/{stream['id']}")

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


