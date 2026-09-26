import concurrent.futures
import uuid
import os
import pathlib
import time
from requests import RequestException
from urllib3.exceptions import HTTPError
from contextlib import closing, nullcontext

from . import exceptions
from tqdm.auto import tqdm
import shutil
from .util import get_tempdir
from .api_request import make_request
from threading import Event
import io

MAX_PARALLELIZATION = 8

# Tables at or below this size are read via table.listRows, rather than by
# creating a read session. See should_use_list_rows()
LIST_ROWS_MAX_BYTES = 1e8


class ArrowStreamSource:
    """A single arrow IPC stream that can be read from the API.

    There are two flavors:
      - A readStream belonging to a read session. These can be resumed from a
        row offset, and terminate with an empty end-of-stream sentinel batch,
        so an interrupted read picks up where it left off.
      - A table.listRows request. This supports neither, so an interrupted read
        must restart from the beginning, and completion is verified by comparing
        the number of rows read against the number of rows we expect.
    """

    def __init__(self, *, name, path, query=None, resumable=True, expected_rows=None):
        self.name = name
        self.path = path
        self.query = query or {}
        self.resumable = resumable
        self.expected_rows = expected_rows

    def open(self, offset=0):
        query = {**self.query}
        if self.resumable:
            query["offset"] = offset
            query["eosSentinel"] = "true"

        return make_request(
            method="get",
            path=self.path,
            query=query,
            stream=True,
            parse_response=False,
        )

    def is_finished(self, *, received_sentinel, rows_read):
        """Whether the stream completed, as opposed to being cut off mid-read."""
        if self.resumable:
            return received_sentinel

        # listRows has no end-of-stream sentinel; the expected row count is the
        # only signal that everything arrived.
        return self.expected_rows is None or rows_read >= self.expected_rows


def read_stream_source(stream_id):
    return ArrowStreamSource(name=stream_id, path=f"/readStreams/{stream_id}")


def list_rows_source(*, uri, selected_variables, max_results, expected_rows):
    query = {"format": "arrow"}

    if selected_variables is not None:
        query["selectedVariables"] = ",".join(selected_variables)

    if max_results is not None:
        query["maxResults"] = max_results

    return ArrowStreamSource(
        name="rows",
        path=f"{uri}/rows",
        query=query,
        resumable=False,
        expected_rows=expected_rows,
    )


def should_use_list_rows(instance, max_results=None):
    """Whether to read this instance's rows via table.listRows.

    listRows returns all rows in a single request, avoiding the read session
    round trip. Small reads don't meaningfully benefit from the parallelization
    that read sessions provide, and listRows can additionally be read without
    any credentials when the table is publicly accessible.

    What matters is the size of the read, not of the table, so a capped read of
    a large table is sized by the fraction of its rows that were requested —
    the same approximation the server makes.
    """
    from ..classes.Table import Table

    if not isinstance(instance, Table):
        return False

    if not instance.properties or "numBytes" not in instance.properties:
        instance.get()

    num_bytes = instance.properties.get("numBytes")

    if num_bytes is None:
        return False

    num_bytes = int(num_bytes)
    num_rows = instance.properties.get("numRows")

    if max_results is not None and num_rows:
        num_rows = int(num_rows)
        num_bytes = num_bytes * min(max_results, num_rows) / num_rows

    return num_bytes < LIST_ROWS_MAX_BYTES


def get_expected_list_rows(instance, max_results):
    num_rows = instance.properties.get("numRows")

    if num_rows is None:
        return None

    num_rows = int(num_rows)

    return num_rows if max_results is None else min(num_rows, max_results)


class RedivisArrowIterator:
    def __init__(self, sources, mapped_variables, progressbar, coerce_schema):
        self.sources = sources
        self.mapped_variables = mapped_variables
        self.progressbar = progressbar
        self.coerce_schema = coerce_schema
        self.current_stream_index = 0
        self.current_offset = 0
        self.retry_count = 0
        self.current_record_batch_reader = None
        self.current_arrow_response = None
        self.__get_next_reader__()

    def __close_current_reader__(self):
        # Release the prior stream's HTTP connection before opening a new one.
        # pyarrow reads from response.raw; with stream=True the socket is not
        # returned to the connection pool until the Response is closed, so an
        # abandoned (e.g. mid-stream interrupted) reader would otherwise leak
        # sockets/file descriptors — multiplied by every retry.
        reader = self.current_record_batch_reader
        if reader is not None:
            try:
                reader.close()
            except Exception:
                pass
            self.current_record_batch_reader = None

        response = self.current_arrow_response
        if response is not None:
            try:
                response.close()
            except Exception:
                pass
            self.current_arrow_response = None

    def __get_next_reader__(self, offset=0):
        import pyarrow

        # Close whatever stream is currently open before replacing it.
        self.__close_current_reader__()

        try:
            self.current_offset = offset
            # Tracks whether the current stream delivered the end-of-stream
            # sentinel (a final, empty batch). Until we see it, an exhausted
            # reader means the connection was interrupted, not that the stream
            # completed. Sources without a sentinel verify completion by row
            # count instead; see ArrowStreamSource.is_finished()
            self.received_sentinel = False
            arrow_response = self.sources[self.current_stream_index].open(offset)
            # Track the Response on self so it is always closed on the next
            # __get_next_reader__ / close(), even if reader construction below
            # raises and triggers a retry.
            self.current_arrow_response = arrow_response
            # urllib3 closes the underlying socket the instant the body is fully
            # consumed. pyarrow (via the BufferedReader below) issues one more
            # read() after the last batch to detect EOF, which would then hit the
            # already-closed socket and raise "ValueError: read of closed file"
            # instead of returning b''. Disabling auto_close makes that final read
            # return cleanly.
            arrow_response.raw.auto_close = False
            # IMPORTANT: we need to wrap this in a buffered reader, otherwise we get partial read errors
            #            with chunked transfers over http 1.1 (happens w/ notebooks in-cluster)
            self.current_record_batch_reader = pyarrow.ipc.RecordBatchStreamReader(
                io.BufferedReader(arrow_response.raw, buffer_size=1024 * 1024)
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
                self.stream_schema = pyarrow.schema(
                    map(variable_to_field, self.variables_in_stream)
                )
            else:
                self.stream_schema = self.current_record_batch_reader.schema

            self.should_reorder_fields = False
            self.fields_to_add = []

            if self.mapped_variables is not None:
                reader_names = self.current_record_batch_reader.schema.names

                for i, field_name in enumerate(reader_names):
                    mapped_index = next(
                        (
                            j
                            for j, v in enumerate(self.mapped_variables)
                            if v["name"] == field_name
                        ),
                        None,
                    )
                    if mapped_index is not None and mapped_index != i:
                        self.should_reorder_fields = True
                        break

                self.fields_to_add = [
                    v for v in self.mapped_variables if v["name"] not in reader_names
                ]

                if self.fields_to_add:
                    self.should_reorder_fields = True

            self.output_schema = (
                pyarrow.schema(map(variable_to_field, self.mapped_variables))
                if self.should_reorder_fields
                else self.stream_schema
            )
        except (RequestException, HTTPError) as e:
            self.retry_count = self.retry_count + 1
            if self.retry_count > 10:
                raise exceptions.NetworkError(
                    message=f"Download connection failed after {self.retry_count} retries.",
                    original_exception=e,
                ) from e

            time.sleep(self.retry_count)
            return self.__get_next_reader__(self.__prepare_retry__())

    def __prepare_retry__(self):
        """Return the offset to retry the current stream at.

        Sources that can't be resumed have to be re-read from the beginning,
        which is only safe before any batch has been handed to the caller: the
        rows aren't guaranteed to come back in the same order, so re-reading
        after that could duplicate some rows and drop others. The buffered
        (non-iterator) reads don't have this problem, since they discard
        everything read so far and keep only the complete retried result.
        """
        if self.sources[self.current_stream_index].resumable:
            return self.current_offset

        if self.current_offset:
            raise exceptions.NetworkError(
                message="The connection was interrupted partway through reading rows, and this read cannot be resumed. Please try again.",
            )

        return 0

    def __iter__(self):
        return self

    def __next__(self):
        import pyarrow

        try:
            batch = self.current_record_batch_reader.read_next_batch()

            # An empty batch is the end-of-stream sentinel emitted by the server
            # (eosSentinel=true). It confirms the stream completed cleanly, so
            # advance to the next stream rather than yielding an empty batch.
            if batch.num_rows == 0:
                self.received_sentinel = True
                raise StopIteration

            if self.coerce_schema:
                batch = pyarrow.RecordBatch.from_arrays(
                    list(
                        map(
                            coerce_arrow_array,
                            batch.columns,
                            self.variables_in_stream,
                        )
                    ),
                    schema=self.stream_schema,
                )

            if self.fields_to_add:
                null_arrays = [
                    pyarrow.nulls(batch.num_rows, type=variable_to_field(v).type)
                    for v in self.fields_to_add
                ]
                all_arrays = list(batch.columns) + null_arrays
                all_names = list(batch.schema.names) + [
                    v["name"] for v in self.fields_to_add
                ]
                name_to_array = dict(zip(all_names, all_arrays))
                ordered_arrays = [
                    name_to_array[v["name"]] for v in self.mapped_variables
                ]
                batch = pyarrow.RecordBatch.from_arrays(
                    ordered_arrays, schema=self.output_schema
                )
            elif self.should_reorder_fields:
                name_to_array = dict(zip(batch.schema.names, batch.columns))
                ordered_arrays = [
                    name_to_array[v["name"]] for v in self.mapped_variables
                ]
                batch = pyarrow.RecordBatch.from_arrays(
                    ordered_arrays, schema=self.output_schema
                )

            if self.progressbar is not None:
                self.progressbar.update(batch.num_rows)

            self.current_offset += batch.num_rows
            self.retry_count = 0
            return batch
        except StopIteration:
            # The reader was exhausted without the stream having completed, which
            # means the connection was interrupted at a record batch boundary.
            # Resume the same stream from the current offset (or, if it can't be
            # resumed, re-read it) instead of treating it as complete.
            if not self.sources[self.current_stream_index].is_finished(
                received_sentinel=self.received_sentinel,
                rows_read=self.current_offset,
            ):
                self.retry_count += 1
                if self.retry_count > 10:
                    raise exceptions.NetworkError(
                        message=f"Download connection failed after {self.retry_count} retries.",
                    )
                time.sleep(self.retry_count)
                self.__get_next_reader__(self.__prepare_retry__())
                return self.__next__()

            if self.current_stream_index == len(self.sources) - 1:
                self.__close_current_reader__()
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
            self.__get_next_reader__(self.__prepare_retry__())
            return self.__next__()

    def close(self):
        # Release the underlying HTTP connection. Safe to call multiple times,
        # and important when the iterator is not fully consumed (e.g. the caller
        # breaks out of the loop), since consuming to completion is otherwise the
        # only thing that closes the final stream.
        self.__close_current_reader__()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        # Last-resort cleanup if the caller neither fully consumed the iterator
        # nor closed it explicitly.
        try:
            self.__close_current_reader__()
        except Exception:
            pass


def make_rows_request(
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

    sources = []
    num_rows = 0

    if isinstance(instance, ReadStream):
        sources = [read_stream_source(instance.id)]
        num_rows = instance.properties.get("estimatedRows", 0)
        max_parallelization = 1
    else:
        if max_parallelization < 1:
            raise exceptions.ValueError("max_parallelization must be greater than 0")

        pyarrow.set_cpu_count(max_parallelization)
        pyarrow.set_io_thread_count(max_parallelization)

        use_export_api = (
            use_export_api
            and output_type != "arrow_iterator"
            and selected_variables is None
            and batch_preprocessor is None
            and max_results is None
        )

        if not use_export_api and should_use_list_rows(instance, max_results):
            num_rows = get_expected_list_rows(instance, max_results)
            sources = [
                list_rows_source(
                    uri=uri,
                    selected_variables=selected_variables,
                    max_results=max_results,
                    expected_rows=num_rows,
                )
            ]
        elif not use_export_api:
            payload = {
                "requestedStreamCount": min(MAX_PARALLELIZATION, max_parallelization)
            }

            if max_results is not None:
                payload["maxResults"] = max_results

            if selected_variables is not None:
                payload["selectedVariables"] = selected_variables

            read_session = make_request(
                method="post",
                path=f"{uri}/readSessions",
                parse_response=True,
                payload=payload,
            )
            sources = [
                read_stream_source(stream["id"]) for stream in read_session["streams"]
            ]
            num_rows = read_session["numRows"]

    if progress and not use_export_api:
        progressbar = tqdm(total=num_rows, leave=False, mininterval=0.2)

    if output_type == "arrow_iterator":
        return RedivisArrowIterator(
            sources=sources,
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
        or (len(sources) > 1 and max_parallelization > 1)
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
            if len(sources):
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=min(max_parallelization, len(sources))
                ) as executor:
                    futures = [
                        executor.submit(
                            process_stream,
                            source,
                            folder_path,
                            mapped_variables,
                            coerce_schema,
                            progressbar,
                            batch_preprocessor,
                            cancel_event,
                        )
                        for source in sources
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

        schema = (
            pyarrow.schema(map(variable_to_field, mapped_variables))
            if batch_preprocessor is None and use_export_api == False
            else None
        )

        if folder_path is None:
            if all_batches:
                return pyarrow.Table.from_batches(all_batches, schema=schema)

            # No batches were returned; construct an empty table with the expected schema
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
                schema=schema,
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
    source,
    folder_path,
    mapped_variables,
    coerce_schema,
    progressbar,
    batch_preprocessor,
    cancel_event,
    offset=0,
    retry_count=0,
):
    writer = None
    # Initialized here (not inside the try) so the network-retry handler can
    # concatenate batches read before a mid-stream failure, even if the failure
    # happens in the request before the reader loop begins.
    record_batches = [] if folder_path is None else None

    def retry_stream(next_retry_count):
        """Continue reading the source after an interruption.

        Resumable sources pick up at the current offset, keeping the batches
        already read; other sources re-read from the beginning and discard them.
        On the on-disk path, a resumed read lands in its own "-retry_offset-"
        file, while a restarted read overwrites the partial file.
        """
        if not source.resumable and progressbar is not None and offset:
            # The rows read so far are about to be read again; don't double count.
            progressbar.update(-offset)

        time.sleep(next_retry_count)
        retry_result = process_stream(
            source,
            folder_path,
            mapped_variables,
            coerce_schema,
            progressbar,
            batch_preprocessor,
            cancel_event,
            offset=offset if source.resumable else 0,
            retry_count=next_retry_count,
        )

        if folder_path is None:
            return (record_batches if source.resumable else []) + (retry_result or [])

        return retry_result

    try:
        import pyarrow

        with closing(source.open(offset)) as arrow_response:
            # urllib3 closes the underlying socket the instant the body is fully
            # consumed. pyarrow (via the BufferedReader below) issues one more
            # read() after the last batch to detect EOF, which would then hit the
            # already-closed socket and raise "ValueError: read of closed file"
            # instead of returning b''. Disabling auto_close makes that final read
            # return cleanly. (A single raw.read() is unaffected because it reaches
            # EOF within one call; only the chunked read does the extra read.)
            arrow_response.raw.auto_close = False
            has_content = False
            # Set once the server's end-of-stream sentinel (an empty batch) is
            # received, confirming the stream completed rather than being cut off.
            # Sources that don't emit a sentinel verify the row count instead;
            # see ArrowStreamSource.is_finished()
            received_sentinel = False
            retry_suffix = f"-retry_offset-{offset}" if offset > 0 else ""
            # create the os_file path
            os_file = (
                pathlib.Path(folder_path)
                .joinpath(f"{source.name}{retry_suffix}.feather")
                .absolute()
                if folder_path is not None
                else None
            )
            file_context = (
                pyarrow.OSFile(str(os_file), mode="wb")
                if folder_path is not None
                else nullcontext()
            )
            # IMPORTANT: we need to wrap this in a buffered reader, otherwise we get partial read errors
            #            with chunked transfers over http 1.1 (happens w/ notebooks in-cluster)
            with file_context as f, pyarrow.ipc.RecordBatchStreamReader(
                io.BufferedReader(arrow_response.raw, buffer_size=1024 * 1024)
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

                    stream_schema = pyarrow.schema(
                        map(variable_to_field, variables_in_stream)
                    )
                else:
                    stream_schema = reader.schema

                should_reorder_fields = False
                fields_to_add = []

                if mapped_variables is not None:
                    reader_names_lower = [n.lower() for n in reader.schema.names]

                    for i, field_name in enumerate(reader.schema.names):
                        mapped_index = next(
                            (
                                j
                                for j, v in enumerate(mapped_variables)
                                if v["name"].lower() == field_name.lower()
                            ),
                            None,
                        )
                        if mapped_index is not None and mapped_index != i:
                            should_reorder_fields = True
                            break

                    fields_to_add = [
                        v
                        for v in mapped_variables
                        if v["name"].lower() not in reader_names_lower
                    ]

                    if fields_to_add:
                        should_reorder_fields = True

                output_schema = (
                    pyarrow.schema(map(variable_to_field, mapped_variables))
                    if should_reorder_fields
                    else stream_schema
                )

                for batch in reader:
                    # exit out of thread
                    if cancel_event.is_set():
                        has_content = False
                        break

                    # The empty end-of-stream sentinel confirms the stream
                    # completed cleanly; it carries no data to write.
                    if batch.num_rows == 0:
                        received_sentinel = True
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
                            schema=stream_schema,
                        )

                    if fields_to_add:
                        null_arrays = [
                            pyarrow.nulls(
                                batch.num_rows, type=variable_to_field(v).type
                            )
                            for v in fields_to_add
                        ]
                        all_arrays = list(batch.columns) + null_arrays
                        all_names = [n.lower() for n in batch.schema.names] + [
                            v["name"].lower() for v in fields_to_add
                        ]
                        name_to_array = dict(zip(all_names, all_arrays))
                        ordered_arrays = [
                            name_to_array[v["name"].lower()] for v in mapped_variables
                        ]
                        batch = pyarrow.RecordBatch.from_arrays(
                            ordered_arrays, schema=output_schema
                        )
                    elif should_reorder_fields:
                        name_to_array = {
                            n.lower(): a
                            for n, a in zip(batch.schema.names, batch.columns)
                        }
                        ordered_arrays = [
                            name_to_array[v["name"].lower()] for v in mapped_variables
                        ]
                        batch = pyarrow.RecordBatch.from_arrays(
                            ordered_arrays, schema=output_schema
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

        if folder_path is not None and not has_content:
            os.remove(os_file)

        # The reader ended before the stream completed and we weren't cancelled:
        # the connection dropped at a record batch boundary. Read on rather than
        # treating the stream as complete.
        if (
            not source.is_finished(
                received_sentinel=received_sentinel, rows_read=offset
            )
            and not cancel_event.is_set()
        ):
            if retry_count >= 10:
                raise exceptions.NetworkError(
                    message=f"A network error occurred. Stream rows connection failed after {retry_count} retries.",
                )
            return retry_stream(retry_count + 1)

        if folder_path is None:
            return record_batches
    except (RequestException, HTTPError) as e:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass

        if retry_count >= 10:
            raise exceptions.NetworkError(
                message=f"A network error occurred. Stream rows connection failed after {retry_count} retries.",
                original_exception=e,
            ) from e

        return retry_stream(retry_count + 1)
