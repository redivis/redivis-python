import json
from urllib.parse import quote as quote_uri
import warnings
import os

from .Upload import Upload
from .Variable import Variable
from ..common.list_rows import list_rows
from ..common.api_request import make_request, make_paginated_request


class Table:
    def __init__(
        self,
        name,
        *,
        sample=False,
        dataset=None,
        project=None,
        properties=None,
    ):
        dataset, project = get_table_parents(dataset, project)
        parent = dataset or project
        owner = parent.user or parent.organization
        sample_string = ":sample" if sample else ""
        version_string = f":{dataset.version}" if dataset else ""
        self.name = name
        self.dataset = dataset
        self.project = project
        self.identifier = (
            f"{owner.name}.{parent.name}{version_string}.{self.name}{sample_string}"
        )
        self.uri = f"/tables/{quote_uri(self.identifier)}"
        self.properties = properties

    def __getitem__(self, key):
        return (
            self.properties[key] if self.properties and key in self.properties else None
        )

    def __str__(self):
        return json.dumps(self.properties, indent=2)

    def create(self, *, description=None, upload_merge_strategy="append"):
        response = make_request(
            method="POST",
            path=f"{self.dataset.uri}/tables",
            payload={
                "name": self.name,
                "description": description,
                "uploadMergeStrategy": upload_merge_strategy,
            },
        )
        self.properties = response
        self.uri = self.properties["uri"]
        return self

    def delete(self):
        make_request(
            method="DELETE",
            path=self.uri,
        )
        return

    def get(self):
        self.properties = make_request(method="GET", path=self.uri)
        self.uri = self.properties["uri"]
        return self

    def exists(self):
        try:
            make_request(method="GET", path=self.uri)
            return True
        except Exception as err:
            if err.args[0]["status"] != 404:
                raise err
            return False

    def list_rows(self, max_results=None, *, limit=None, variables=None):
        if limit and max_results is None:
            warnings.warn(
                "The limit parameter has been renamed to max_results, and will be removed in a future version of this library",
                DeprecationWarning,
            )
            max_results = limit

        if not self.properties or not hasattr(self.properties, "numRows"):
            self.get()

        max_results = (
            min(max_results, int(self.properties["numRows"]))
            if max_results is not None
            else self.properties["numRows"]
        )

        mapped_variables = get_mapped_variables(variables, self.uri)

        return list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            type="tuple",
        )

    def list_uploads(self, *, max_results=None):
        uploads = make_paginated_request(
            path=f"{self.uri}/uploads", max_results=max_results
        )
        return [Upload(upload) for upload in uploads]

    def list_variables(self, *, max_results=None):
        variables = make_paginated_request(
            path=f"{self.uri}/variables", page_size=1000, max_results=max_results
        )
        return [
            Variable(variable["name"], table=self, properties=variable)
            for variable in variables
        ]

    def to_dataframe(self, max_results=None, *, limit=None, variables=None):
        if limit and max_results is None:
            warnings.warn(
                "The limit parameter has been renamed to max_results, and will be removed in a future version of this library",
                DeprecationWarning,
            )
            max_results = limit

        if not self.properties or not hasattr(self.properties, "numRows"):
            self.get()

        max_results = (
            min(max_results, int(self.properties["numRows"]))
            if max_results is not None
            else self.properties["numRows"]
        )

        mapped_variables = get_mapped_variables(variables, self.uri)
        return list_rows(
            uri=self.uri,
            max_results=max_results,
            selected_variables=variables,
            mapped_variables=mapped_variables,
            type="dataframe",
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
        self.properties = response
        return

    def upload(
        self,
        name,
        *,
        type="delimited",
        schema=None,
        has_header_row=True,
        skip_bad_records=False,
        allow_quoted_newlines=False,
        quote_character='"',
        delimiter=None,
        #     This is deprecated
        data=None,
        remove_on_fail=True,
    ):
        upload = Upload(
            table=self,
            name=name,
        )
        if data is not None:
            warnings.warn(
                "Passing data directly to the upload constructor is deprecated. Please call table.upload().upload_file(file) instead.",
                DeprecationWarning,
            )
            upload.create(
                schema=schema,
                type=type,
                has_header_row=has_header_row,
                skip_bad_records=skip_bad_records,
                allow_quoted_newlines=allow_quoted_newlines,
                quote_character=quote_character,
                delimiter=delimiter,
            )
            upload.upload_file(
                data,
                create_if_needed=True,
                wait_for_finish=True,
                raise_on_fail=True,
                remove_on_fail=remove_on_fail,
            )

        return upload

    def variable(self, name):
        return Variable(name, table=self)


def get_mapped_variables(variables, uri):
    all_variables = make_paginated_request(path=f"{uri}/variables", page_size=1000)

    if variables is None:
        return all_variables
    else:
        lower_variable_names = [variable.lower() for variable in variables]
        variables_list = list(
            filter(
                lambda variable: variable["name"].lower() in lower_variable_names,
                all_variables,
            )
        )
        variables_list.sort(
            key=lambda variable: lower_variable_names.index(variable["name"].lower())
        )
        return variables_list


def get_table_parents(dataset, project):
    from .User import User

    if dataset or project:
        return dataset, project
    elif os.getenv("REDIVIS_DEFAULT_PROJECT") is not None:
        return None, User(os.getenv("REDIVIS_DEFAULT_PROJECT").split(".")[0]).project(
            os.getenv("REDIVIS_DEFAULT_PROJECT").split(".")[1]
        )
    elif os.getenv("REDIVIS_DEFAULT_DATASET") is not None:
        return (
            User(os.getenv("REDIVIS_DEFAULT_DATASET").split(".")[0]).dataset(
                os.getenv("REDIVIS_DEFAULT_DATASET").split(".")[1]
            )
        ), None
    else:
        raise Exception(
            "Cannot reference an unqualified table if the neither the REDIVIS_DEFAULT_PROJECT or REDIVIS_DEFAULT_DATASET environment variables are set."
        )
