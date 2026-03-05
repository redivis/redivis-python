import concurrent.futures
import uuid
import os
import pathlib
import time
from requests import RequestException
from urllib3.exceptions import HTTPError
from contextlib import closing, nullcontext

from ..common import exceptions
from tqdm.auto import tqdm
import shutil
from .util import get_tempdir
from .api_request import make_request
from threading import Event

MAX_PARALLELIZATION = 8


class RedivisArrowIterator:
    def __init__(self, streams, mapped_variables, progressbar, coerce_schema):
        self.streams = streams
        self.mapped_variables = mapped_variables
        self.progressbar = progressbar
        self.coerce_schema = coerce_schema
        self.current_stream_index = 0
        self.current_offset = 0
        self.retry_count = 0
        self.__get_next_reader__()

    def __get_next_reader__(self, offset=0):
        import pyarrow

        try:
            self.current_offset = offset
            # TODO: this won't get closed properly if the iterator is not fully consumed
            arrow_response = make_request(
                method="get",
                path=f'/readStreams/{self.streams[self.current_stream_index]["id"]}?offset={offset}',
                stream=True,
                parse_response=False,
            )
            self.current_record_batch_reader = pyarrow.ipc.RecordBatchStreamReader(
                arrow_response.raw
            )
            if self.coerce_schema:
                self.variables_in_stream = list(
                    map(
                        lambda field_name: next(
                            x for x in self.mapped_variables if x["name"] == field_name
                        ),
                        self.current_record_batch_reader.schema.names,
                    )
                )
                self.output_schema = pyarrow.schema(
                    map(variable_to_field, self.variables_in_stream)
                )
            else:
                self.output_schema = self.current_record_batch_reader.schema
        except (RequestException, HTTPError) as e:
            self.retry_count = self.retry_count + 1
            if self.retry_count > 10:
                raise exceptions.NetworkError(
                    message=f"Download connection failed after {self.retry_count} retries.",
                    original_exception=e,
                ) from e

            time.sleep(self.retry_count)
            return self.__get_next_reader__(self.current_offset)

    def __iter__(self):
        return self

    def __next__(self):
        import pyarrow

        try:
            batch = self.current_record_batch_reader.read_next_batch()
            if self.coerce_schema:
                batch = pyarrow.RecordBatch.from_arrays(
                    list(
                        map(
                            coerce_arrow_array,
                            batch.columns,
                            self.variables_in_stream,
                        )
                    ),
                    schema=self.output_schema,
                )

            if self.progressbar is not None:
                self.progressbar.update(batch.num_rows)

            self.current_offset += batch.num_rows
            self.retry_count = 0
            return batch
        except StopIteration:
            if self.current_stream_index == len(self.streams) - 1:
                if self.progressbar:
                    self.progressbar.close()
                raise StopIteration
            else:
                self.current_stream_index += 1
                self.__get_next_reader__()
                return self.__next__()
        except (RequestException, HTTPError) as e:
            self.retry_count = self.retry_count + 1
            if self.retry_count > 10:
                raise exceptions.NetworkError(
                    message=f"A network error occurred. Download connection failed after {self.retry_count} retries.",
                    original_exception=e,
                ) from e
            time.sleep(self.retry_count)
            self.__get_next_reader__(self.current_offset)
            return self.__next__()


def list_rows(
    *,
    uri,
    output_type="dataframe",
    max_results=None,
    selected_variables=None,
    mapped_variables=None,
    progress=True,
    coerce_schema=False,
    batch_preprocessor=None,
    instance=None,
    use_export_api=False,
    max_parallelization=os.cpu_count(),
):
    import pyarrow
    import pyarrow.dataset as pyarrow_dataset  # need to import separately, it's not on the pyarrow import
    from ..classes.Table import Table
    from ..classes.ReadStream import ReadStream

    progressbar = None

    if isinstance(instance, ReadStream):
        read_session = {
            "streams": [{"id": instance.id}],
            "numRows": instance.properties.get("estimatedRows", 0),
        }
        max_parallelization = 1
    else:
        if max_parallelization < 1:
            raise exceptions.ValueError("max_parallelization must be greater than 0")

        pyarrow.set_cpu_count(max_parallelization)
        pyarrow.set_io_thread_count(max_parallelization)

        use_export_api = (
            use_export_api
            and isinstance(instance, Table)
            and output_type != "arrow_iterator"
            and selected_variables is None
            and batch_preprocessor is None
            and max_results is None
        )
        payload = {
            "requestedStreamCount": min(MAX_PARALLELIZATION, max_parallelization)
        }

        if max_results is not None:
            payload["maxResults"] = max_results

        if selected_variables is not None:
            payload["selectedVariables"] = selected_variables

        if not use_export_api:
            read_session = make_request(
                method="post",
                path=f"{uri}/readSessions",
                parse_response=True,
                payload=payload,
            )

    if progress:
        progressbar = tqdm(total=read_session["numRows"], leave=False, mininterval=0.1)

    if output_type == "arrow_iterator":
        return RedivisArrowIterator(
            streams=read_session["streams"],
            mapped_variables=mapped_variables,
            progressbar=progressbar,
            coerce_schema=coerce_schema,
        )

    folder = None
    folder_path = None
    # get the absolute folder path, as a string
    # We need to always write to disk if we're doing things in parallel,
    # because we can't efficiently copy results between processes when working in parallel
    if (
        use_export_api
        or output_type in ["arrow_dataset", "dask_dataframe", "polars_lazyframe"]
        or (len(read_session["streams"]) > 1 and max_parallelization > 1)
    ):
        folder = pathlib.Path().joinpath(
            get_tempdir(),
            "tables",
            f"{uuid.uuid4()}",
        )
        folder_path = str(folder.absolute())

    try:
        arrow_dataset = None
        all_batches = []
        if use_export_api:
            instance.download(folder_path + "/", format="parquet", progress=progress)
        else:
            if folder_path is not None:
                # create the folder, if it doesn't exist
                folder.mkdir(parents=True, exist_ok=True)

            # Use download_state to notify worker threads when to quit.
            # See: https://stackoverflow.com/a/29237343/101923
            cancel_event = Event()

            # Code to use multiprocess... would simplify exiting on stop, but progress doesn't currently work
            # with concurrent.futures.ProcessPoolExecutor(max_workers=len(read_session["streams"]), mp_context=mp.get_context('fork')) as executor:

            # See https://github.com/googleapis/python-bigquery/blob/main/google/cloud/bigquery/_pandas_helpers.py#L920
            futures = []
            if len(read_session["streams"]):
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=min(max_parallelization, len(read_session["streams"]))
                ) as executor:
                    futures = [
                        executor.submit(
                            process_stream,
                            stream,
                            folder_path,
                            mapped_variables,
                            coerce_schema,
                            progressbar,
                            batch_preprocessor,
                            cancel_event,
                        )
                        for stream in read_session["streams"]
                    ]

                    not_done = futures

                    try:
                        while not_done and not cancel_event.is_set():
                            # next line 'sleeps' this main thread, letting the thread pool run
                            freshly_done, not_done = concurrent.futures.wait(
                                not_done, timeout=0.2
                            )
                            for future in freshly_done:
                                # Call result() on any finished threads to raise any exceptions encountered.
                                res = future.result()
                                if folder_path is None and res:
                                    all_batches.extend(res)
                    finally:
                        cancel_event.set()
                        # Shutdown all background threads, now that they should know to exit early.
                        executor.shutdown(wait=True, cancel_futures=True)

            if progressbar:
                progressbar.close()

        if folder_path is None:
            if all_batches:
                return pyarrow.Table.from_batches(all_batches)

            # No batches were returned; construct an empty table with the expected schema
            schema = pyarrow.schema(map(variable_to_field, mapped_variables))
            return pyarrow.Table.from_batches([], schema=schema)
        elif use_export_api:
            if output_type == "polars_lazyframe":
                import polars

                return polars.scan_parquet(f"{folder_path}/*")
            elif output_type == "dask_dataframe":
                import dask.dataframe as dd

                return dd.read_parquet(folder_path, dtype_backend="pyarrow")

            arrow_dataset = pyarrow_dataset.dataset(folder_path, format="parquet")
            if output_type == "arrow_dataset":
                return arrow_dataset
            else:
                arrow_table = arrow_dataset.to_table()
                shutil.rmtree(folder_path, ignore_errors=True)
                return arrow_table
        else:
            if output_type == "polars_lazyframe":
                import polars

                return polars.scan_ipc(f"{folder_path}/*", memory_map=True)

            arrow_dataset = pyarrow_dataset.dataset(
                folder_path,
                format="feather",
                schema=(
                    pyarrow.schema(map(variable_to_field, mapped_variables))
                    if batch_preprocessor is None
                    else None
                ),
            )

        if output_type == "arrow_dataset":
            return arrow_dataset
        elif output_type == "dask_dataframe":
            import dask.dataframe as dd

            # TODO: simplify once dask supports reading from feather: https://github.com/dask/dask/issues/6865
            # Make sure we no longer remove the folder in the finally clause after making this change
            # Create the Parquet base directory
            parquet_base_dir = str(
                pathlib.Path()
                .joinpath(
                    get_tempdir(),
                    "tables",
                    f"{uuid.uuid4()}",
                )
                .absolute()
            )
            pyarrow_dataset.write_dataset(
                arrow_dataset, parquet_base_dir, format="parquet"
            )
            shutil.rmtree(folder_path, ignore_errors=True)
            return dd.read_parquet(parquet_base_dir, dtype_backend="pyarrow")
        else:
            arrow_table = arrow_dataset.to_table()
            shutil.rmtree(folder_path, ignore_errors=True)
            return arrow_table
    finally:
        if (
            folder_path
            and output_type != "arrow_dataset"
            and output_type != "polars_lazyframe"
        ):
            shutil.rmtree(folder_path, ignore_errors=True)


def variable_to_field(variable):
    import pyarrow

    if variable["type"] == "string" or variable["type"] == "geography":
        return pyarrow.field(variable["name"], pyarrow.string())
    elif variable["type"] == "integer":
        return pyarrow.field(variable["name"], pyarrow.int64())
    elif variable["type"] == "float":
        return pyarrow.field(variable["name"], pyarrow.float64())
    elif variable["type"] == "date":
        return pyarrow.field(variable["name"], pyarrow.date32())
    elif variable["type"] == "dateTime":
        return pyarrow.field(variable["name"], pyarrow.timestamp("us"))
    elif variable["type"] == "time":
        return pyarrow.field(variable["name"], pyarrow.time64("us"))
    elif variable["type"] == "boolean":
        return pyarrow.field(variable["name"], pyarrow.bool_())


# If streaming from a dataset, data types _may_ be incorrect. We need to check and convert if possible.
def coerce_arrow_array(pyarrow_array, variable):
    import pyarrow

    if variable["type"] == "string" or variable["type"] == "geography":
        return pyarrow_array
    elif variable["type"] == "integer":
        if pyarrow_array.type == "int64":
            return pyarrow_array
        else:
            return pyarrow.compute.cast(pyarrow_array, pyarrow.int64())
    elif variable["type"] == "float":
        if pyarrow_array.type == "double":
            return pyarrow_array
        else:
            return pyarrow.compute.cast(pyarrow_array, pyarrow.float64())
    elif variable["type"] == "date":
        if pyarrow_array.type == "date32[day]":
            return pyarrow_array
        else:
            return pyarrow.compute.cast(pyarrow_array, pyarrow.date32())
    elif variable["type"] == "dateTime":
        if pyarrow_array.type == "timestamp[us]":
            return pyarrow_array
        else:
            return pyarrow.compute.cast(pyarrow_array, pyarrow.timestamp("us"))
    elif variable["type"] == "time":
        if pyarrow_array.type == "time64[us]":
            return pyarrow_array
        else:
            # Hopefully someday this is supported. Until then, need to do the workaround below
            # return pyarrow.compute.cast(pyarrow_array, pyarrow.time64("us"))
            return pyarrow.compute.cast(
                pyarrow.compute.cast(
                    pyarrow.compute.utf8_replace_slice(
                        pyarrow_array, start=0, stop=0, replacement="2020-01-01T"
                    ),
                    pyarrow.timestamp("us"),
                ),
                pyarrow.time64("us"),
            )
    elif variable["type"] == "boolean":
        if pyarrow_array.type == "bool":
            return pyarrow_array
        else:
            return pyarrow.compute.cast(pyarrow_array, pyarrow.bool_())


def process_stream(
    stream,
    folder_path,
    mapped_variables,
    coerce_schema,
    progressbar,
    batch_preprocessor,
    cancel_event,
    offset=0,
    retry_count=0,
):
    try:
        import pyarrow

        with closing(
            make_request(
                method="get",
                path=f'/readStreams/{stream["id"]}?offset={offset}',
                stream=True,
                parse_response=False,
            )
        ) as arrow_response:
            record_batches = [] if folder_path is None else None
            has_content = False
            # create the os_file path
            os_file = (
                pathlib.Path(folder_path).joinpath(f"{stream['id']}.feather").absolute()
                if folder_path is not None
                else None
            )
            file_context = (
                pyarrow.OSFile(str(os_file), mode="wb")
                if folder_path is not None
                else nullcontext()
            )
            with file_context as f, pyarrow.ipc.RecordBatchStreamReader(
                arrow_response.raw
            ) as reader:
                if coerce_schema:
                    variables_in_stream = list(
                        map(
                            lambda field_name: next(
                                x
                                for x in mapped_variables
                                if x["name"].lower() == field_name.lower()
                            ),
                            reader.schema.names,
                        )
                    )

                    output_schema = pyarrow.schema(
                        map(variable_to_field, variables_in_stream)
                    )
                else:
                    output_schema = reader.schema
                writer = None
                for batch in reader:
                    # exit out of thread
                    if cancel_event.is_set():
                        has_content = False
                        break

                    if coerce_schema:
                        batch = pyarrow.RecordBatch.from_arrays(
                            list(
                                map(
                                    coerce_arrow_array,
                                    batch.columns,
                                    variables_in_stream,
                                )
                            ),
                            schema=output_schema,
                        )

                    num_rows = batch.num_rows
                    offset += num_rows
                    if batch_preprocessor:
                        batch = batch_preprocessor(batch)

                    if batch is not None:
                        has_content = True
                        if folder_path is None:
                            record_batches.append(batch)
                        else:
                            if writer is None:
                                writer = pyarrow.ipc.RecordBatchFileWriter(
                                    f,
                                    (
                                        output_schema
                                        if batch_preprocessor is None
                                        else batch.schema
                                    ),
                                )

                            writer.write_batch(batch)

                    if progressbar is not None:
                        progressbar.update(num_rows)

                if writer is not None:
                    writer.close()

            if folder_path is None:
                return record_batches
            elif not has_content:
                os.remove(os_file)
    except (RequestException, HTTPError) as e:
        if retry_count >= 10:
            raise exceptions.NetworkError(
                message=f"A network error occurred. Stream rows connection failed after {retry_count} retries.",
                original_exception=e,
            ) from e
        time.sleep(retry_count + 1)
        return process_stream(
            stream,
            folder_path,
            mapped_variables,
            coerce_schema,
            progressbar,
            batch_preprocessor,
            cancel_event,
            offset=offset,
            retry_count=retry_count + 1,
        )
