from tqdm.auto import tqdm
import os
import pathlib
from base64 import b64decode
import time
import hashlib
import json
import re
import asyncio
import math
import concurrent.futures
import threading
from ..common import exceptions
from ..common.config import resolve_config
from ..common.api_request import __get_api_endpoint, __get_user_agent, make_request
from ..common.auth import get_auth_token, has_credentials, refresh_credentials
from contextlib import closing
from requests import RequestException
from urllib3.exceptions import HTTPError
import niquests


md5_regexp = re.compile(r"(?:^|,)\s*md5\s*=\s*([^,\s]+)\s*(?=,|$)", re.IGNORECASE)

_DEFAULT_MAX_CONCURRENCY = 48
_TARGET_ACTIVE_BYTES = 1_000_000_000  # 1 GB
_STREAM_THRESHOLD = (
    4 * 1024 * 1024
)  # 4 MB — below this, use aread() instead of streaming
# The most times a download re-authenticates after its credentials are rejected
_MAX_AUTH_ATTEMPTS = 3

# Serializes getting and refreshing credentials across download workers, so that many
# concurrent downloads rejected at once only prompt (at most) one login
_auth_lock = threading.Lock()


def _get_authorization_headers():
    """The Authorization header for a download, if credentials are available.

    Public files can be downloaded without any.
    """
    with _auth_lock:
        if has_credentials():
            return {"Authorization": f"Bearer {get_auth_token()}"}
        return {}


def _reauthorize(rejected_headers, response_json):
    """Authenticates after a download's credentials were rejected.

    Returns whether to retry the download.
    """
    if (
        os.getenv("REDIVIS_API_TOKEN") is not None
        or os.getenv("REDIVIS_DEFAULT_NOTEBOOK") is not None
    ):
        # The token was provided to us, so logging in wouldn't change anything
        return False
    with _auth_lock:
        current_headers = (
            {"Authorization": f"Bearer {get_auth_token()}"} if has_credentials() else {}
        )
        if current_headers and current_headers != rejected_headers:
            # Another download already re-authenticated
            return True
        refresh_credentials(
            scope=(
                response_json["scope"].split(" ") if "scope" in response_json else None
            ),
            amr_values=response_json.get("amr_values"),
        )
        return True


class _SimpleSemaphore:
    """Thin wrapper around asyncio.Semaphore with the same acquire/release
    signature as _ByteBudgetSemaphore (accepting but ignoring estimated_size).
    """

    def __init__(self, max_concurrency):
        self._sem = asyncio.Semaphore(max_concurrency)

    async def acquire(self, estimated_size):
        await self._sem.acquire()

    async def release(self, estimated_size):
        self._sem.release()


class _ByteBudgetSemaphore:
    """Semaphore that gates on both a concurrent-download count and a total
    bytes-in-flight budget. A slot is available when active_count is below
    max_concurrency AND active_bytes is below _TARGET_ACTIVE_BYTES, with the
    guarantee that at least one download can always proceed.
    """

    def __init__(self, max_concurrency):
        self._max_concurrency = max_concurrency
        self._active_count = 0
        self._active_bytes = 0
        self._condition = asyncio.Condition()

    def _can_proceed(self):
        return self._active_count == 0 or (
            self._active_count < self._max_concurrency
            and self._active_bytes < _TARGET_ACTIVE_BYTES
        )

    async def acquire(self, estimated_size):
        async with self._condition:
            await self._condition.wait_for(self._can_proceed)
            self._active_count += 1
            self._active_bytes += estimated_size

    async def release(self, estimated_size):
        async with self._condition:
            self._active_count -= 1
            self._active_bytes -= estimated_size
            self._condition.notify_all()


def perform_retryable_download(
    path,
    *,
    filename=None,
    progress=None,
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
    progress = resolve_config("progress", progress)
    if size is not None and md5_hash is not None:
        exact_file_exists = check_filename(
            filename, overwrite, retry_count, size, md5_hash
        )
        if exact_file_exists:
            if on_progress:
                on_progress(size)
            return filename

    # Download to a temporary file alongside the destination, which only replaces it
    # once complete and verified, so that a failed download never leaves a partial file
    # at the destination
    partial_filename = get_partial_filename(filename)
    did_check_filename = size is not None and md5_hash is not None
    did_restart_for_hash = False
    owns_pbar = False

    def report_progress(byte_count):
        if pbar is not None:
            pbar.update(byte_count)
        if on_progress:
            on_progress(byte_count)

    try:
        while True:
            try:
                with closing(
                    make_request(
                        method="GET",
                        path=path,
                        stream=True,
                        query=query,
                        parse_response=False,
                        headers=(
                            {"Range": f"bytes={start_byte}-"} if start_byte else None
                        ),
                    )
                ) as r:
                    # A response that doesn't resume where we left off starts the
                    # download over
                    resumed_at = (
                        get_content_range_start(r) if r.status_code == 206 else 0
                    )
                    if resumed_at != start_byte:
                        report_progress(-start_byte)
                        start_byte = 0
                        if resumed_at:
                            continue

                    total_size = get_total_size(r)
                    if size is None:
                        size = total_size
                    if md5_hash is None:
                        md5_hash = get_md5_header(r)
                    # A response that's decompressed in transit can't be checked against
                    # the stored file
                    is_encoded = (
                        r.headers.get("Content-Encoding", "identity") != "identity"
                    )

                    if not did_check_filename:
                        did_check_filename = True
                        if check_filename(filename, overwrite, 0, size, md5_hash):
                            if on_progress:
                                on_progress(int(size))
                            return filename

                    pathlib.Path(filename).parent.mkdir(exist_ok=True, parents=True)
                    if progress and pbar is None:
                        pbar = tqdm(
                            total=int(size) if size is not None else None,
                            initial=start_byte,
                            leave=False,
                            unit="iB",
                            unit_scale=True,
                            mininterval=0.1,
                        )
                        owns_pbar = True

                    with open(partial_filename, "ab" if start_byte else "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            retry_count = 0
                            if cancel_event and cancel_event.is_set():
                                f.close()
                                os.remove(partial_filename)
                                return None
                            f.write(chunk)
                            start_byte += len(chunk)
                            report_progress(len(chunk))

                if not is_encoded and size is not None and start_byte != int(size):
                    # The connection ended early; resume from what we have
                    raise RequestException(
                        f"The download ended after {start_byte} of {size} bytes"
                    )
                if not is_encoded and md5_hash is not None:
                    if get_file_md5(partial_filename) != decode_md5(md5_hash):
                        if did_restart_for_hash:
                            raise exceptions.RedivisError(
                                f"The downloaded file's MD5 hash doesn't match the expected hash: {filename}"
                            )
                        # Try once more from scratch, in case the data was corrupted in
                        # transit
                        did_restart_for_hash = True
                        report_progress(-start_byte)
                        start_byte = 0
                        continue

                os.replace(partial_filename, filename)
                return filename
            except (RequestException, HTTPError) as e:
                if retry_count >= 10:
                    raise exceptions.NetworkError(
                        message=f"A network error occurred. Download failed after {retry_count} retries.",
                        original_exception=e,
                    ) from e
                time.sleep(retry_count)
                retry_count += 1
                start_byte = (
                    os.path.getsize(partial_filename)
                    if os.path.exists(partial_filename)
                    else 0
                )
    except BaseException:
        if os.path.exists(partial_filename):
            os.remove(partial_filename)
        raise
    finally:
        if owns_pbar:
            pbar.close()


def get_partial_filename(filename):
    return f"{filename}.redivis-partial"


def get_content_range_start(response):
    match = re.match(r"bytes\s+(\d+)-", response.headers.get("Content-Range") or "")
    return int(match.group(1)) if match else 0


def get_total_size(response):
    """The size of the whole file, from the Content-Range header (or Content-Length,
    for a complete response)."""
    total = (response.headers.get("Content-Range") or "").split("/")[-1]
    if total.isdigit():
        return int(total)
    length = response.headers.get("Content-Length")
    if response.status_code == 200 and length and length.isdigit():
        return int(length)
    return None


def get_md5_header(response):
    """The file's base64 MD5 hash, from its Content-Digest or x-goog-hash header."""
    content_digest = response.headers.get("Content-Digest")
    if content_digest:
        # Looks like "crc32c=:U26yZA==:, md5=:uE0r1xmbDXTJAGiWL6xlHw==:, ..."
        md5_match = md5_regexp.search(content_digest)
        return md5_match.group(1).strip().strip(":") if md5_match else None
    # Looks like "crc32c=U26yZA==, md5=uE0r1xmbDXTJAGiWL6xlHw==, ..."
    md5_match = md5_regexp.search(response.headers.get("x-goog-hash", ""))
    return md5_match.group(1).strip() if md5_match else None


def decode_md5(md5_hash):
    return b64decode(md5_hash) if isinstance(md5_hash, str) else md5_hash


def get_file_md5(filename):
    file_hash = hashlib.md5()
    with open(filename, "rb") as f:
        for byte_block in iter(lambda: f.read(1024 * 1024), b""):
            file_hash.update(byte_block)
    return file_hash.digest()


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
    progress=None,
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
    avg_file_size = total_bytes / n if total_bytes else 0
    cpu_count = os.cpu_count() or 1
    effective_max_par = resolve_config(
        "max_parallelization", max_parallelization, cpu_count
    )
    worker_count = max(1, min(8, math.ceil(n / 1000), cpu_count, effective_max_par))

    pbar = None
    on_progress = None
    progress = resolve_config("progress", progress)
    if progress and total_bytes:
        file_count = 0
        pbar = tqdm(
            total=total_bytes,
            leave=False,
            desc=f"{file_count}/{len(uris)} files",
            unit="B",
            unit_scale=True,
            mininterval=0.1,
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
                    # If more than 0.1s, update the description
                    if time.time() - last_updated_description >= 0.1:
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
                avg_file_size=avg_file_size,
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
    avg_file_size=0,
    on_progress=None,
    cancel_event=None,
):
    if not uris:
        return

    if max_concurrency is None:
        max_concurrency = _DEFAULT_MAX_CONCURRENCY

    max_concurrency = max(1, min(500, max_concurrency, len(uris)))

    # Only use the byte-budget semaphore when it could actually constrain
    # concurrency. For small files max_concurrency * avg_file_size is well
    # below _TARGET_ACTIVE_BYTES, so the budget check never fires — but the
    # asyncio.Condition machinery still serializes every acquire/release and
    # calls notify_all() on every completion, causing a thundering-herd of
    # coroutine wakeups. Plain asyncio.Semaphore avoids all of that overhead.
    if avg_file_size > 0 and avg_file_size * max_concurrency > _TARGET_ACTIVE_BYTES:
        sem = _ByteBudgetSemaphore(max_concurrency)
    else:
        sem = _SimpleSemaphore(max_concurrency)

    created_dirs = set()

    async with niquests.AsyncSession(
        # NB: the Authorization header is added per request, since credentials can
        # change (or be obtained) partway
        headers={"User-Agent": __get_user_agent()},
        # A read timeout ensures a stalled stream raises and is retried
        # (resuming via Range when supported) rather than hanging forever.
        timeout=(60, 120),
        # Downloads may 302 to a second host (e.g. a GCS signed URL). With a
        # single pool slot, concurrent requests to the two hosts deadlock
        # waiting on each other's pool to go idle.
        pool_connections=10,
        pool_maxsize=max_concurrency,
    ) as client:
        tasks = [
            asyncio.create_task(
                _download_single_file(
                    client=client,
                    sem=sem,
                    url=f"{__get_api_endpoint()}{uri}",
                    download_path=download_paths[i],
                    size=sizes[i] if sizes is not None else None,
                    estimated_size=(
                        sizes[i]
                        if sizes is not None and sizes[i] is not None
                        else avg_file_size
                    ),
                    md5_hash=md5_hashes[i] if md5_hashes is not None else None,
                    overwrite=overwrite,
                    on_progress=on_progress,
                    cancel_event=cancel_event,
                    created_dirs=created_dirs,
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
    estimated_size=0,
    md5_hash=None,
    overwrite=False,
    on_progress=None,
    cancel_event=None,
    created_dirs=None,
):
    MAX_RETRIES = 10
    retry_count = 0
    auth_attempts = 0
    did_restart_for_hash = False
    start_byte = 0
    # Download to a temporary file alongside the destination, which only replaces it
    # once complete and verified
    partial_path = get_partial_filename(download_path)
    supports_range_requests = False
    current_size = size
    current_md5 = md5_hash
    did_pre_check = False
    is_encoded = False
    loop = asyncio.get_running_loop()

    try:
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
                if await loop.run_in_executor(
                    None,
                    check_filename,
                    download_path,
                    overwrite,
                    0,
                    current_size,
                    current_md5,
                ):
                    if on_progress:
                        on_progress(current_size, 1)
                    return

            if retry_count > 0:
                await asyncio.sleep(retry_count)

            if cancel_event and cancel_event.is_set():
                return

            auth_headers = await loop.run_in_executor(None, _get_authorization_headers)
            request_headers = {**auth_headers}
            if start_byte > 0:
                request_headers["Range"] = f"bytes={start_byte}-"

            should_retry = False
            completed = False

            await sem.acquire(estimated_size)
            response = None
            try:
                try:
                    response = await client.get(
                        url, stream=True, headers=request_headers
                    )
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
                        body = (await response.content).decode(
                            "utf-8", errors="replace"
                        )
                        try:
                            response_json = json.loads(body)
                        except ValueError:
                            response_json = {}
                        is_auth_failure = status == 401 or (
                            status == 403
                            and response_json.get("error") == "insufficient_scope"
                        )
                        if (
                            is_auth_failure
                            and auth_attempts < _MAX_AUTH_ATTEMPTS
                            and await loop.run_in_executor(
                                None, _reauthorize, auth_headers, response_json
                            )
                        ):
                            auth_attempts += 1
                            continue
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

                        # Prefer the MD5 hash the server reports, but keep the one we
                        # were given if it doesn't report one
                        current_md5 = get_md5_header(response) or current_md5

                        # Post-network check: we now have hash/size from headers.
                        if not did_pre_check:
                            did_pre_check = True
                            if await loop.run_in_executor(
                                None,
                                check_filename,
                                download_path,
                                overwrite,
                                retry_count,
                                current_size,
                                current_md5,
                            ):
                                if on_progress:
                                    on_progress(current_size, 1)
                                return

                        # A response that's decompressed in transit can't be checked
                        # against the stored file's size and hash
                        is_encoded = (
                            response.headers.get("content-encoding", "identity")
                            != "identity"
                        )

                        # Reset retry counter after a successful connection so
                        # that mid-stream errors get the same 10 fresh retries.
                        retry_count = 0

                        parent_dir = str(pathlib.Path(download_path).parent)
                        if created_dirs is None or parent_dir not in created_dirs:
                            pathlib.Path(parent_dir).mkdir(exist_ok=True, parents=True)
                            if created_dirs is not None:
                                created_dirs.add(parent_dir)

                        if 0 < estimated_size <= _STREAM_THRESHOLD:
                            # Small-file fast path: read the entire body in
                            # one call and write to disk in a single write.
                            body = await response.content
                            if cancel_event and cancel_event.is_set():
                                return
                            with open(
                                partial_path,
                                "wb" if start_byte == 0 else "ab",
                            ) as f:
                                f.write(body)
                            if on_progress:
                                on_progress(len(body), 1)
                        else:
                            pending_progress_bytes = 0
                            last_updated_progress = time.time()

                            with open(
                                partial_path,
                                "wb" if start_byte == 0 else "ab",
                            ) as f:
                                try:
                                    async for chunk in await response.iter_content(
                                        chunk_size=256 * 1024
                                    ):
                                        if cancel_event and cancel_event.is_set():
                                            try:
                                                os.remove(partial_path)
                                            except OSError:
                                                pass
                                            return
                                        await loop.run_in_executor(None, f.write, chunk)
                                        if on_progress:
                                            pending_progress_bytes += len(chunk)
                                            if (
                                                time.time() - last_updated_progress
                                                >= 0.2
                                            ):
                                                on_progress(pending_progress_bytes)
                                                pending_progress_bytes = 0
                                                last_updated_progress = time.time()
                                except Exception as e:
                                    if not supports_range_requests:
                                        try:
                                            os.remove(partial_path)
                                        except OSError:
                                            pass
                                    raise

                            if on_progress:
                                on_progress(pending_progress_bytes, 1)
                        completed = True

                except (niquests.exceptions.RequestException,) as e:
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
            finally:
                if response is not None:
                    await response.close()
                await sem.release(estimated_size)

            if completed:
                downloaded_size = os.path.getsize(partial_path)
                if (
                    not is_encoded
                    and current_size is not None
                    and downloaded_size != current_size
                ):
                    # The connection ended early; resume (or restart) the download
                    if retry_count >= MAX_RETRIES:
                        raise exceptions.NetworkError(
                            message=f"The download ended after {downloaded_size} of {current_size} bytes: {url}"
                        )
                    should_retry = True
                elif (
                    not is_encoded
                    and current_md5 is not None
                    and await loop.run_in_executor(None, get_file_md5, partial_path)
                    != decode_md5(current_md5)
                ):
                    if did_restart_for_hash:
                        raise exceptions.RedivisError(
                            f"The downloaded file's MD5 hash doesn't match the expected hash: {download_path}"
                        )
                    # Try once more from scratch, in case the data was corrupted in
                    # transit
                    did_restart_for_hash = True
                    if on_progress:
                        on_progress(-downloaded_size, -1)
                    start_byte = 0
                    continue
                else:
                    await loop.run_in_executor(
                        None, os.replace, partial_path, download_path
                    )
                    return

            if should_retry:
                retry_count += 1
                start_byte = (
                    os.path.getsize(partial_path)
                    if supports_range_requests and os.path.exists(partial_path)
                    else 0
                )
    finally:
        # A download that failed for good (or was cancelled) leaves nothing behind; a
        # completed one was already moved
        if os.path.exists(partial_path):
            os.remove(partial_path)


def check_filename(filename, overwrite, retry_count, size, md5_hash):
    if retry_count == 0 and os.path.exists(filename):
        if (
            size is not None
            and md5_hash is not None
            and os.path.getsize(filename) == int(size)
        ):
            if get_file_md5(filename) == decode_md5(md5_hash):
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
