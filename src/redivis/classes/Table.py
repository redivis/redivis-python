from urllib.parse import quote as quote_uri
import os
import time
import glob
import concurrent.futures
from tqdm.auto import tqdm


from .Upload import Upload
from .Export import Export
from .Variable import Variable
from ..common.TabularReader import TabularReader
from ..common.api_request import make_request, make_paginated_request
from ..common.retryable_upload import perform_resumable_upload, perform_standard_upload


class Table(TabularReader):
    def __init__(
        self,
        name,
        *,
        dataset=None,
        workflow=None,
        properties=None,
    ):
        super().__init__(is_table=True)
        if len(name.split(".")) != 3:
            dataset, workflow = get_table_parents(dataset, workflow)

        if dataset:
            reference_scope = f"{dataset.qualified_reference}."
        elif workflow:
            reference_scope = f"{workflow.qualified_reference}."
        elif len(name.split(".")) == 3:
            from .Dataset import Dataset
            from .Workflow import Workflow

            reference_scope = ".".join(name.split(".")[0:2])
            name = name.split(".")[-1]
            dataset = Dataset(reference_scope)
            workflow = Workflow(reference_scope)
            reference_scope += "."
        else:
            if not os.getenv("REDIVIS_DEFAULT_NOTEBOOK"):
                raise Exception(
                    "Invalid table specifier, must be the fully qualified reference if no dataset or workflow is specified"
                )
            reference_scope = ""

        self.name = name
        self.dataset = dataset
        self.workflow = workflow
        self.directory = None

        self.qualified_reference = (
            properties["qualifiedReference"]
            if "qualifiedReference" in (properties or {})
            else (f"{reference_scope}{self.name}")
        )
        self.scoped_reference = (
            properties["scopedReference"]
            if "scopedReference" in (properties or {})
            else self.name
        )
        self.uri = f"/tables/{quote_uri(self.qualified_reference, '')}"
        self.properties = properties

    def create(
        self, *, description=None, upload_merge_strategy="append", is_file_index=False
    ):
        payload = {
            "name": self.name,
            "uploadMergeStrategy": upload_merge_strategy,
            "isFileIndex": is_file_index,
        }
        if description is not None:
            payload["description"] = description

        response = make_request(
            method="POST",
            path=f"{self.dataset.uri}/tables",
            payload=payload,
        )
        update_properties(self, response)
        return self

    def delete(self):
        make_request(
            method="DELETE",
            path=self.uri,
        )
        return

    def get(self):
        properties = make_request(method="GET", path=self.uri)
        update_properties(self, properties)
        return self

    def exists(self):
        try:
            make_request(method="HEAD", path=self.uri)
            return True
        except Exception as err:
            if err.args[0]["status"] != 404:
                raise err
            return False

    def list_uploads(self, *, max_results=None):
        uploads = make_paginated_request(
            path=f"{self.uri}/uploads", max_results=max_results
        )
        return [
            Upload(upload["name"], table=self, properties=upload) for upload in uploads
        ]

    def list_variables(self, *, max_results=None):
        variables = make_paginated_request(
            path=f"{self.uri}/variables", page_size=1000, max_results=max_results
        )
        return [
            Variable(variable["name"], table=self, properties=variable)
            for variable in variables
        ]

    def add_files(
        self,
        *,
        files=None,
        directory=None,
        progress=True,
        max_parallelization=os.cpu_count() * 5,
    ):
        if (files is None) == (directory is None):
            raise Exception("Either files or directory must be specified")

        total_size = 0

        if directory:
            files = []
            if not directory.endswith("/"):
                directory += "/"
            for filename in glob.iglob(directory + "**/*", recursive=True):
                if os.path.isfile(filename):
                    size = os.stat(filename).st_size
                    total_size += size
                    files.append(
                        {
                            "path": filename,
                            "name": os.path.relpath(filename, directory),
                            "size": size,
                        }
                    )
        else:
            files = list(map(map_file, files))
            for file in files:
                total_size += file["size"]

        pbar_bytes = None
        pbar_count = None
        if progress:
            pbar_count = tqdm(
                total=len(files), leave=False, unit=" files", mininterval=0.1
            )
            pbar_bytes = tqdm(
                total=total_size,
                leave=False,
                unit="B",
                unit_scale=True,
                mininterval=0.1,
            )

        try:
            current_batch_timestamp = time.time()
            current_batch_files = []
            current_temp_uploads_batch = []
            target_batch_size = 1e8
            current_batch_size = 0

            for i, file in enumerate(files):
                if i % 1000 == 0:
                    res = make_request(
                        method="POST",
                        path=f"{self.uri}/tempUploads",
                        payload={
                            "tempUploads": [
                                {
                                    "size": file["size"],
                                    "name": file["name"],
                                    "resumable": file["size"] > 5e7,
                                }
                                for file in files[i : i + 1000]
                            ]
                        },
                    )
                    current_temp_uploads_batch = res["results"]

                current_batch_files.append(
                    {"file": file, "temp_upload": current_temp_uploads_batch[i % 1000]}
                )
                current_batch_size += file["size"]

                if (
                    len(current_batch_files) >= 1000
                    or i == len(files) - 1
                    or current_batch_size > target_batch_size
                ):

                    def upload(batch_file):
                        nonlocal pbar_bytes
                        file = batch_file["file"]
                        temp_upload = batch_file["temp_upload"]

                        if temp_upload["resumable"]:
                            data = (
                                open(file["path"], "rb")
                                if "path" in file
                                else file["data"]
                            )
                            perform_resumable_upload(
                                data=data,
                                progressbar=pbar_bytes,
                                temp_upload_url=temp_upload["url"],
                            )
                        else:
                            data = (
                                open(file["path"], "rb")
                                if "path" in file
                                else file["data"]
                            )
                            perform_standard_upload(
                                data=data,
                                temp_upload_url=temp_upload["url"],
                                progressbar=pbar_bytes,
                            )

                        if "path" in file:
                            data.close()

                    with concurrent.futures.ThreadPoolExecutor(
                        max_workers=min(100, max_parallelization)
                    ) as executor:
                        futures = [
                            executor.submit(upload, batch_file)
                            for batch_file in current_batch_files
                        ]

                        not_done = futures
                        try:
                            while not_done:
                                # next line 'sleeps' this main thread, letting the thread pool run
                                freshly_done, not_done = concurrent.futures.wait(
                                    not_done, timeout=0.5
                                )
                                for future in freshly_done:
                                    # Call result() on any finished threads to raise any exceptions encountered.
                                    future.result()
                        finally:
                            for future in not_done:
                                # Only cancels futures that were never started
                                future.cancel()
                            # Shutdown all background threads, now that they should know to exit early.
                            executor.shutdown(wait=True, cancel_futures=True)

                    if time.time() - current_batch_timestamp > 60:
                        target_batch_size = target_batch_size / 2
                    elif time.time() - current_batch_timestamp < 15:
                        target_batch_size = target_batch_size * 2

                    response = make_request(
                        method="POST",
                        path=f"{self.uri}/rawFiles",
                        payload={
                            "files": [
                                {
                                    "name": batch_file["file"]["name"],
                                    "tempUploadId": batch_file["temp_upload"]["id"],
                                }
                                for batch_file in current_batch_files
                            ]
                        },
                    )

                    if progress:
                        pbar_count.update(len(current_batch_files))

                    current_batch_timestamp = time.time()
                    current_batch_files = []
                    current_batch_size = 0

            if progress:
                pbar_bytes.close()
                pbar_count.close()
        except Exception as e:
            if progress:
                pbar_bytes.close()
                pbar_count.close()

            raise e

    def download(
        self,
        path=None,
        *,
        format="csv",
        overwrite=False,
        progress=True,
    ):
        res = make_request(
            method="POST",
            path=f"{self.uri}/exports",
            payload={"format": format},
        )
        export_job = Export(res["id"], table=self, properties=res)
        return export_job.download_files(
            path=path,
            overwrite=overwrite,
            progress=progress,
        )

    def update(self, *, name=None, description=None, upload_merge_strategy=None):
        payload = {}
        if name:
            payload["name"] = name
        if upload_merge_strategy:
            payload["uploadMergeStrategy"] = upload_merge_strategy
        if description is not None:
            payload["description"] = description

        response = make_request(
            method="PATCH",
            path=f"{self.uri}",
            payload=payload,
        )
        update_properties(self, response)
        return self

    def update_variables(self, variables):
        make_request(
            method="PATCH",
            path=f"{self.uri}/variables",
            payload={"variables": variables},
        )

    def upload(self, name=""):
        return Upload(name=name, table=self)

    def variable(self, name):
        return Variable(name, table=self)


def get_table_parents(dataset, workflow):
    from .User import User

    if dataset or workflow:
        return dataset, workflow
    elif os.getenv("REDIVIS_DEFAULT_NOTEBOOK") is not None:
        return None, None
    elif os.getenv("REDIVIS_DEFAULT_WORKFLOW") is not None:
        return None, User(os.getenv("REDIVIS_DEFAULT_WORKFLOW").split(".")[0]).workflow(
            os.getenv("REDIVIS_DEFAULT_WORKFLOW").split(".")[1]
        )
    elif os.getenv("REDIVIS_DEFAULT_DATASET") is not None:
        return (
            User(os.getenv("REDIVIS_DEFAULT_DATASET").split(".")[0]).dataset(
                os.getenv("REDIVIS_DEFAULT_DATASET").split(".")[1]
            )
        ), None
    return None, None


def update_properties(instance, properties):
    instance.properties = properties
    instance.qualified_reference = properties["qualifiedReference"]
    instance.scoped_reference = properties["scopedReference"]
    instance.name = properties["name"]
    instance.uri = properties["uri"]
    if instance.dataset and instance.workflow:
        if instance.properties.get("container"):
            if instance.properties.get("container")["kind"] == "dataset":
                instance.workflow = None
            else:
                instance.dataset = None
        elif instance.dataset.exists():
            instance.workflow = None
        else:
            instance.dataset = None


def map_file(file):
    if isinstance(file, str):
        file = {"path": file}
    else:
        file = {**file}

    if "name" not in file:
        if "data" in file:
            raise Exception(
                'All file specifications with a "data" key must specify a name'
            )

        file["name"] = os.path.basename(file["path"])

    if "data" in file:
        if isinstance(file["data"], str):
            file["data"] = bytes(file["data"], "utf-8")
        file["size"] = len(file["data"])
    else:
        file["size"] = os.stat(file["path"]).st_size

    return file
