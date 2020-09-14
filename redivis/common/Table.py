import logging, time

from .Upload import *
from .Variable import *
from .api_request import make_request, make_paginated_request


class Table:
    def __init__(self, identifier, properties=None):
        self.uri = f"/tables/{identifier}"
        self.properties = properties

    def __getitem__(self, key):
        return self.properties[key]

    def __str__(self):
        return json.dumps(self.properties, indent=2)

    def list_uploads(self):
        uploads = make_paginated_request(path=f"{self.uri}/uploads")
        # TODO: for each upload, Upload(res)
        return

    def list_variables(self):
        variables = make_paginated_request(path=f"{self.uri}/variables")
        # TODO: for each variable, Variable(res)
        return

    def update(self, *, name, description, merge_strategy):
        response = make_request(
            method="PATCH",
            path=f"{self.uri}",
            payload={
                "name": name,
                "description": description,
                "mergeStrategy": merge_strategy,
            },
        )
        self.properties = response
        return

    def create_upload(self, *, name, data, type, remove_on_fail=True):
        response = make_request(
            method="POST",
            path=f"{self.uri}/uploads",
            payload={"name": name, "type": type},
        )
        upload = Upload(uri=response["uri"])
        try:
            upload.upload_file(data)
            while True:
                time.sleep(2)
                upload.get()
                if upload["status"] == "completed":
                    break
                elif upload["status"] == "failed":
                    raise Exception(upload["errorMessage"])
                else:
                    logging.debug("Upload is still in progress...")
        except Exception as e:
            if remove_on_fail:
                print("An error occurred. Deleting upload.")
                upload.delete()
            raise e

        return upload

    def download(self):
        # TODO
        return

    def delete(self):
        make_request(
            method="DELETE",
            path=self.uri,
        )
        return
