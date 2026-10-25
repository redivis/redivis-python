from urllib.parse import quote as quote_uri
import os
import re
import time
import glob
import concurrent.futures
from tqdm.auto import tqdm


from .Upload import Upload
from .Variable import Variable
from ..common import exceptions
from ..common.TabularReader import TabularReader
from ..common.api_request import make_request, make_paginated_request
from ..common.retryable_upload import perform_resumable_upload, perform_standard_upload
from ..common.util import split_reference


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
        if len(split_reference(name)) != 3:
            dataset, workflow = get_table_parents(dataset, workflow)

        is_workflow_table = False
        if dataset:
            reference_scope = f"{dataset.qualified_reference}."
        elif workflow:
            reference_scope = f"{workflow.qualified_reference}."
            is_workflow_table = True
        elif len(split_reference(name)) == 3:
            from .Dataset import Dataset
            from .Workflow import Workflow

            reference_scope = ".".join(split_reference(name)[0:2])
            name = split_reference(name)[-1]
            dataset = Dataset(reference_scope)
            workflow = Workflow(reference_scope)
            reference_scope += "."
        else:
            if not os.getenv("REDIVIS_DEFAULT_NOTEBOOK"):
                raise exceptions.ValueError(
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
        if is_workflow_table:
            # Tables are resolved within their workflow, which also finds tables
            # inherited through the workflow's datasources
            self.uri = f"{workflow.uri}/tables/{quote_uri(self.scoped_reference, '')}"
        else:
            self.uri = f"/tables/{quote_uri(self.qualified_reference, '')}"
        self.properties = properties

    def __repr__(self):
        return f"<Table {self.qualified_reference}>"

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
        except exceptions.NotFoundError:
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
            raise exceptions.ValueError("Either files or directory must be specified")

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


    def update(self, *, name=None, description=None, upload_merge_strategy=None):
        resolve_table_for_write(self)
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
        resolve_table_for_write(self)
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
    from .Dataset import Dataset
    from .Workflow import Workflow

    if dataset or workflow:
        return dataset, workflow
    elif os.getenv("REDIVIS_DEFAULT_NOTEBOOK") is not None:
        return None, None
    elif os.getenv("REDIVIS_DEFAULT_WORKFLOW") is not None:
        # Parsing the qualified reference resolves the owner, whether a user or an
        # organization
        return None, Workflow(os.getenv("REDIVIS_DEFAULT_WORKFLOW"))
    elif os.getenv("REDIVIS_DEFAULT_DATASET") is not None:
        return Dataset(os.getenv("REDIVIS_DEFAULT_DATASET")), None
    return None, None


def is_workflow_scoped_uri(uri):
    return re.match(r"^/workflows/[^/]+/tables/", uri) is not None


# A workflow-scoped table URI (/workflows/:workflow/tables/:table) can also
# resolve tables inherited through the workflow's datasources. Writes must never
# go through it, since that would make it easy to unintentionally modify a table
# outside of the workflow, so tables are fetched to get their canonical URI first.
def resolve_table_for_write(table):
    if is_workflow_scoped_uri(table.uri):
        table.get()


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
            raise exceptions.ValueError(
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
