import os
import concurrent.futures
import pathlib
from tqdm.auto import tqdm
from threading import Event


def download_files(
    self,
    path=None,
    *,
    overwrite=False,
    max_results=None,
    file_id_variable=None,
    progress=True,
    max_parallelization=os.cpu_count() * 5,
):
    files = self.list_files(max_results, file_id_variable=file_id_variable)

    if path is None:
        path = os.getcwd()
    else:
        path = os.path.expanduser(path)

    if progress:
        pbar_count = tqdm(total=len(files), leave=False, unit=" files")
        pbar_bytes = tqdm(
            unit="B",
            leave=False,
            unit_scale=True,
            bar_format="{n_fmt} ({rate_fmt})",
        )

    if not os.path.exists(path):
        pathlib.Path(path).mkdir(exist_ok=True, parents=True)

    def on_progress(bytes):
        nonlocal pbar_bytes
        pbar_bytes.update(bytes)

    def download(file, cancel_event):
        nonlocal progress
        nonlocal pbar_count
        # TODO: if overwriting file, first check if file is the same size, and if so check md5 hash, and skip if identical (need md5 in header)
        file.download(
            path,
            overwrite=overwrite,
            progress=False,
            on_progress=on_progress if progress else None,
            cancel_event=cancel_event,
        )
        if progress and not cancel_event.is_set():
            pbar_count.update()

    cancel_event = Event()
    # TODO: this should use async, not threads
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(100, max_parallelization, len(files))
    ) as executor:
        futures = [executor.submit(download, file, cancel_event) for file in files]

        not_done = futures
        try:
            while not_done and not cancel_event.is_set():
                # next line 'sleeps' this main thread, letting the thread pool run
                freshly_done, not_done = concurrent.futures.wait(not_done, timeout=0.5)
                for future in freshly_done:
                    # Call result() on any finished threads to raise any exceptions encountered.
                    future.result()
        except KeyboardInterrupt:
            print("KeyboardInterrupt")
        finally:
            cancel_event.set()

            for future in not_done:
                # Only cancels futures that were never started
                future.cancel()

            # Shutdown all background threads, now that they should know to exit early.
            executor.shutdown(wait=True, cancel_futures=True)

    if progress:
        pbar_count.close()
        pbar_bytes.close()
