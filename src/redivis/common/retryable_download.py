from tqdm.auto import tqdm
import os
import pathlib
from base64 import b64decode
import time
import hashlib
import re
import asyncio
import math
import concurrent.futures
import threading
from ..common import exceptions
from ..common.api_request import __get_api_endpoint, __get_user_agent, make_request
from ..common.auth import get_auth_token
from contextlib import closing
from requests import RequestException
from urllib3.exceptions import HTTPError
import httpx


md5_regexp = re.compile(r"(?:^|,)\s*md5\s*=\s*([^,\s]+)\s*(?=,|$)", re.IGNORECASE)

_DEFAULT_MAX_CONCURRENCY = 48


def perform_retryable_download(
    #     IMPORTANT: if updating params, make sure to update retry call signature below
    path,
    *,
    filename=None,
    progress=True,
    pbar=None,
    cancel_event=None,
    query=None,
    overwrite=False,
    size=None,
    md5_hash=None,
    start_byte=0,
    on_progress=None,
    retry_count=0,
):
    if size is not None and md5_hash is not None:
        exact_file_exists = check_filename(
            filename, overwrite, retry_count, size, md5_hash
        )
        if exact_file_exists:
            if on_progress:
                on_progress(size)
            return filename

    try:
        with closing(
            make_request(
                method="GET",
                path=path,
                stream=True,
                query=query,
                parse_response=False,
                headers={"Range": f"bytes={start_byte}-"} if start_byte else None,
            )
        ) as r:
            should_check_filename = size is None or md5_hash is None
            content_range = r.headers.get("Content-Range")
            if content_range:
                size = content_range.split("/")[-1]
            else:
                size = r.headers.get("Content-Length")

            content_digest = r.headers.get("Content-Digest")
            if content_digest:
                # Looks like "crc32c=:U26yZA==:, md5=:uE0r1xmbDXTJAGiWL6xlHw==:, ..."
                md5_match = md5_regexp.search(content_digest)
                md5_hash = md5_match.group(1).strip().strip(":") if md5_match else None
            else:
                # Looks like "crc32c=U26yZA==, md5=uE0r1xmbDXTJAGiWL6xlHw==, ..."
                md5_match = md5_regexp.search(r.headers.get("x-goog-hash", ""))
                md5_hash = md5_match.group(1).strip() if md5_match else None

            if should_check_filename:
                exact_file_exists = check_filename(
                    filename, overwrite, retry_count, size, md5_hash
                )
                if exact_file_exists:
                    if on_progress:
                        on_progress(size)
                    return filename

            # Make sure output directory exists
            pathlib.Path(filename).parent.mkdir(exist_ok=True, parents=True)

            with open(filename, "wb" if retry_count == 0 else "ab") as f:
                if progress and not pbar:
                    pbar = tqdm(
                        total=int(r.headers["content-length"]),
                        leave=False,
                        unit="iB",
                        unit_scale=True,
                        mininterval=0.1,
                    )

                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    retry_count = 0
                    if cancel_event and cancel_event.is_set():
                        os.remove(filename)
                        return None
                    if pbar:
                        pbar.update(len(chunk))
                    f.write(chunk)
                    if on_progress:
                        on_progress(len(chunk))
    except (RequestException, HTTPError) as e:
        if retry_count < 10:
            time.sleep(retry_count)
            return perform_retryable_download(
                path=path,
                filename=filename,
                progress=progress,
                pbar=pbar,
                query=query,
                size=size,
                md5_hash=md5_hash,
                overwrite=overwrite,
                cancel_event=cancel_event,
                start_byte=(
                    os.path.getsize(filename) if os.path.exists(filename) else 0
                ),
                on_progress=on_progress,
                retry_count=retry_count + 1,
            )
        else:
            # TODO: remove partially downloaded files
            raise exceptions.NetworkError(
                message=f"A network error occurred. Download failed after {retry_count} retries.",
                original_exception=e,
            ) from e

    if progress:
        pbar.close()

    return filename


def perform_parallel_download(
    uris,
    download_paths,
    *,
    sizes=None,
    md5_hashes=None,
    overwrite=False,
    max_parallelization=None,
    total_bytes=None,
    max_concurrency=None,
    progress=True,
):
    """Download a list of files in parallel using async HTTP with HTTP/2 multiplexing.

    For large file counts the list is partitioned across multiple threads, each
    running its own asyncio event loop, mirroring the multi-worker approach used
    in the R client.

    Parameters
    ----------
    uris : list[str]
        API path components (e.g. ``/rawFiles/{id}``) for each file.
    download_paths : list[str]
        Local filesystem paths to write each file to.
    sizes : list[int] | None
        Known byte sizes corresponding to each URI (used for pre-network checks
        and the progress bar).
    md5_hashes : list[str | bytes] | None
        Base64-encoded MD5 hashes (or raw bytes) used to skip already-correct
        files.
    overwrite : bool
        When ``False`` raise an error if a file already exists at the target
        path and does not match the expected hash.
    max_parallelization : int | None
        Upper bound on the number of worker threads spawned.
    total_bytes : int | None
        Aggregate byte count used to render the progress bar.
    max_concurrency : int | None
        Maximum number of simultaneous HTTP connections across all workers.
        Defaults to 48 per worker.
    progress : bool
        Show a ``tqdm`` progress bar when ``total_bytes`` is provided.
    """
    if not uris:
        return

    if max_concurrency is not None and max_concurrency < 1:
        raise exceptions.ValueError("max_concurrency must be >= 1")

    n = len(uris)
    cpu_count = os.cpu_count() or 1
    effective_max_par = (
        max_parallelization if max_parallelization is not None else cpu_count
    )
    worker_count = max(1, min(8, math.ceil(n / 1000), cpu_count, effective_max_par))

    pbar = None
    on_progress = None
    if progress and total_bytes:
        file_count = 0
        pbar = tqdm(
            total=total_bytes,
            leave=False,
            desc=f"{file_count}/{len(uris)} files",
            unit="B",
            unit_scale=True,
            mininterval=0.2,
        )
        pbar_lock = threading.Lock()

        cached_file_count = 0
        last_updated_description = time.time()

        def on_progress(byte_count, file_count=0):
            with pbar_lock:
                pbar.update(byte_count)
                if file_count:
                    nonlocal cached_file_count
                    nonlocal last_updated_description
                    cached_file_count += file_count
                    # If more than 0.5s, update the description
                    if time.time() - last_updated_description >= 0.5:
                        last_updated_description = time.time()
                        pbar.set_description(f"{cached_file_count}/{len(uris)} files")

    cancel_event = threading.Event()
    executor = None

    # Always run workers in dedicated threads so that asyncio.run() is never
    # called on the calling thread. This matters when the caller is already
    # inside a running event loop (e.g. Jupyter / IPython), where asyncio.run()
    # would raise RuntimeError("This event loop is already running").
    chunk_size = math.ceil(n / worker_count)
    slices = [slice(i, i + chunk_size) for i in range(0, n, chunk_size)]
    per_worker_concurrency = (
        math.ceil(max_concurrency / len(slices))
        if max_concurrency is not None
        else None
    )

    def run_worker(s):
        asyncio.run(
            _parallel_download_worker(
                uris=uris[s],
                download_paths=download_paths[s],
                sizes=sizes[s] if sizes is not None else None,
                md5_hashes=md5_hashes[s] if md5_hashes is not None else None,
                overwrite=overwrite,
                on_progress=on_progress,
                max_concurrency=per_worker_concurrency,
                cancel_event=cancel_event,
            )
        )

    try:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(slices))
        futures = [executor.submit(run_worker, s) for s in slices]
        not_done = list(futures)
        try:
            while not_done and not cancel_event.is_set():
                freshly_done, not_done = concurrent.futures.wait(not_done, timeout=0.2)
                for future in freshly_done:
                    future.result()
        except KeyboardInterrupt:
            pass
        finally:
            cancel_event.set()
            for future in not_done:
                future.cancel()
            executor.shutdown(wait=True, cancel_futures=True)
    finally:
        cancel_event.set()
        if pbar:
            pbar.close()


async def _parallel_download_worker(
    uris,
    download_paths,
    *,
    sizes=None,
    md5_hashes=None,
    overwrite=False,
    max_concurrency=None,
    on_progress=None,
    cancel_event=None,
):
    if not uris:
        return

    if max_concurrency is None:
        max_concurrency = _DEFAULT_MAX_CONCURRENCY

    max_concurrency = max(1, min(100, max_concurrency, len(uris)))

    sem = asyncio.Semaphore(max_concurrency)

    async with httpx.AsyncClient(
        headers={
            "Authorization": f"Bearer {get_auth_token()}",
            "User-Agent": __get_user_agent(),
        },
        timeout=httpx.Timeout(60.0, read=None),
        http2=True,
        follow_redirects=True,
        limits=httpx.Limits(
            max_connections=max_concurrency,
            max_keepalive_connections=max_concurrency,
        ),
    ) as client:
        tasks = [
            asyncio.create_task(
                _download_single_file(
                    client=client,
                    sem=sem,
                    url=f"{__get_api_endpoint()}{uri}",
                    download_path=download_paths[i],
                    size=sizes[i] if sizes is not None else None,
                    md5_hash=md5_hashes[i] if md5_hashes is not None else None,
                    overwrite=overwrite,
                    on_progress=on_progress,
                    cancel_event=cancel_event,
                )
            )
            for i, uri in enumerate(uris)
        ]

        if cancel_event is not None:

            async def _cancel_watcher():
                while not cancel_event.is_set():
                    await asyncio.sleep(0.05)
                for t in tasks:
                    t.cancel()

            watcher = asyncio.create_task(_cancel_watcher())
        else:
            watcher = None

        try:
            # gather with return_exceptions=False raises the first exception
            # immediately; the finally block then cancels all remaining tasks
            # before the AsyncClient context exits.
            await asyncio.gather(*tasks)
        except (Exception, asyncio.CancelledError):
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        finally:
            if watcher is not None:
                watcher.cancel()
                try:
                    await watcher
                except asyncio.CancelledError:
                    pass


async def _download_single_file(
    client,
    sem,
    url,
    download_path,
    *,
    size=None,
    md5_hash=None,
    overwrite=False,
    on_progress=None,
    cancel_event=None,
):
    MAX_RETRIES = 10
    retry_count = 0
    start_byte = 0
    supports_range_requests = False
    current_size = size
    current_md5 = md5_hash
    did_pre_check = False

    while True:
        # Pre-network check: skip the connection entirely when we already have
        # enough information to verify the local file.
        if (
            not did_pre_check
            and retry_count == 0
            and start_byte == 0
            and current_size is not None
            and current_md5 is not None
        ):
            did_pre_check = True
            if check_filename(download_path, overwrite, 0, current_size, current_md5):
                if on_progress:
                    on_progress(current_size, 1)
                return

        if retry_count > 0:
            await asyncio.sleep(retry_count)

        if cancel_event and cancel_event.is_set():
            return

        request_headers = {}
        if start_byte > 0:
            request_headers["Range"] = f"bytes={start_byte}-"

        should_retry = False
        completed = False

        async with sem:
            try:
                async with client.stream(
                    "GET", url, headers=request_headers
                ) as response:
                    status = response.status_code

                    if status == 503:
                        if retry_count < MAX_RETRIES:
                            should_retry = True
                        else:
                            raise exceptions.NetworkError(
                                message=(
                                    f"HTTP 503. Download failed after"
                                    f" {MAX_RETRIES} retries: {url}"
                                )
                            )

                    elif status >= 400:
                        body = (await response.aread()).decode(
                            "utf-8", errors="replace"
                        )
                        raise exceptions.APIError(
                            message=f"HTTP {status}",
                            status_code=status,
                            error_description=body,
                        )

                    else:
                        # Determine whether the server supports byte-range requests
                        # (needed to resume interrupted downloads).
                        accept_ranges = response.headers.get("accept-ranges", "")
                        if accept_ranges and accept_ranges.lower() != "none":
                            supports_range_requests = True

                        # When resuming (start_byte > 0), ensure the server actually
                        # honored the Range request. If not, fall back to a full
                        # download starting at byte 0 to avoid corrupting the file.
                        if start_byte > 0:
                            if status == 206:
                                content_range = response.headers.get("Content-Range")
                                # Expect "bytes <start_byte>-..." at the beginning.
                                if not content_range:
                                    supports_range_requests = False
                                    start_byte = 0
                                else:
                                    match = re.match(r"bytes\s+(\d+)-", content_range)
                                    if not match or int(match.group(1)) != start_byte:
                                        supports_range_requests = False
                                        start_byte = 0
                            elif status == 200:
                                # Server ignored the Range header and is sending the
                                # entire file. Treat this as a fresh download and
                                # overwrite any existing partial file.
                                supports_range_requests = False
                                start_byte = 0

                        # Prefer total-size from Content-Range over Content-Length
                        # so that current_size always reflects the complete file size.
                        content_range = response.headers.get("content-range")
                        if content_range:
                            total_str = content_range.split("/")[-1]
                            if total_str.isdigit():
                                current_size = int(total_str)
                        elif current_size is None:
                            cl = response.headers.get("content-length")
                            if cl and cl.isdigit():
                                current_size = int(cl)

                        # Parse MD5 from Content-Digest (colons around value) or
                        # x-goog-hash (no colons).
                        content_digest = response.headers.get("content-digest")
                        if content_digest:
                            md5_match = md5_regexp.search(content_digest)
                            current_md5 = (
                                md5_match.group(1).strip().strip(":")
                                if md5_match
                                else None
                            )
                        else:
                            md5_match = md5_regexp.search(
                                response.headers.get("x-goog-hash", "")
                            )
                            current_md5 = (
                                md5_match.group(1).strip() if md5_match else None
                            )

                        # Post-network check: we now have hash/size from headers.
                        if not did_pre_check:
                            did_pre_check = True
                            if check_filename(
                                download_path,
                                overwrite,
                                retry_count,
                                current_size,
                                current_md5,
                            ):
                                if on_progress:
                                    on_progress(current_size, 1)
                                return

                        # Reset retry counter after a successful connection so
                        # that mid-stream errors get the same 10 fresh retries.
                        retry_count = 0

                        pathlib.Path(download_path).parent.mkdir(
                            exist_ok=True, parents=True
                        )

                        pending_progress_bytes = 0
                        last_updated_progress = time.time()
                        loop = asyncio.get_running_loop()

                        with open(
                            download_path, "wb" if start_byte == 0 else "ab"
                        ) as f:
                            try:
                                async for chunk in response.aiter_bytes(
                                    chunk_size=256 * 1024
                                ):
                                    if cancel_event and cancel_event.is_set():
                                        try:
                                            os.remove(download_path)
                                        except OSError:
                                            pass
                                        return
                                    # Offload the write to a thread so the GIL is
                                    # released and other download coroutines can
                                    # make progress while this chunk hits the disk.
                                    await loop.run_in_executor(None, f.write, chunk)
                                    if on_progress:
                                        pending_progress_bytes += len(chunk)
                                        if time.time() - last_updated_progress >= 0.2:
                                            on_progress(pending_progress_bytes)
                                            pending_progress_bytes = 0
                                            last_updated_progress = time.time()
                            except Exception as e:
                                try:
                                    os.remove(download_path)
                                except OSError:
                                    pass
                                raise

                        if on_progress:
                            on_progress(pending_progress_bytes, 1)
                        completed = True

            except (httpx.RequestError,) as e:
                if retry_count < MAX_RETRIES:
                    should_retry = True
                else:
                    raise exceptions.NetworkError(
                        message=(
                            f"A network error occurred. Download failed after"
                            f" {MAX_RETRIES} retries: {url}"
                        ),
                        original_exception=e,
                    ) from e

        if completed:
            return

        if should_retry:
            retry_count += 1
            start_byte = (
                os.path.getsize(download_path)
                if supports_range_requests and os.path.exists(download_path)
                else 0
            )


def check_filename(filename, overwrite, retry_count, size, md5_hash):
    if retry_count == 0 and os.path.exists(filename):
        if (
            size is not None
            and md5_hash is not None
            and os.path.getsize(filename) == int(size)
        ):
            if isinstance(md5_hash, str):
                md5_hash = b64decode(md5_hash)

            file_hash = hashlib.md5()
            with open(filename, "rb") as f:
                for byte_block in iter(lambda: f.read(8192), b""):
                    file_hash.update(byte_block)

            if file_hash.digest() == md5_hash:
                return filename
            elif not overwrite:

                raise exceptions.ValueError(
                    f"File already exists at '{filename}'. Set parameter overwrite=True to overwrite existing files."
                )
        elif not overwrite:
            raise exceptions.ValueError(
                f"File already exists at '{filename}'. Set parameter overwrite=True to overwrite existing files."
            )
    return None
