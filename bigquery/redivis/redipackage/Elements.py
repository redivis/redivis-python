import requests
import os
import csv
import json
import sys
import time

#make the defs function classes reference ian's message

access_token = os.environ["REDIVIS_ACCESS_TOKEN"]
api_base_path = "https://redivis.com/api/v1"


def checkForAPIError(r):
    if r.status_code >= 400:
        res_json = r.json()
        sys.exit(
            "An API error occurred at {} {} with status {}:\n\t{} ".format(r.request.method, r.request.path_url,
                                                                           r.status_code,
                                                                           res_json['error']['message']))
def create_next_version_if_not_exists():

    r = get_next_version()

    if r.status_code == 200:
        print("Next version already exists. Continuing...")
        return

    url = "{}/datasets/{}/versions".format(api_base_path, dataset_identifier)
    headers = {"Authorization": "Bearer {}".format(access_token)}

    r = requests.post(url, headers=headers)
    checkForAPIError(r)
    return r


def get_next_version():

    url = "{}/datasets/{}/versions".format(api_base_path, dataset_identifier)
    headers = {"Authorization": "Bearer {}".format(access_token)}

    r = requests.get("{}/next".format(url), headers=headers)

    # Don't exit on 404 errors
    if r.status_code != 404:
        checkForAPIError(r)

    return r

def upload_data(epa_data, upload_uri):
    url = api_base_path + upload_uri
    headers = { "Authorization": "Bearer {}".format(access_token) }

    r = requests.put(url, data=epa_data, headers=headers)
    checkForAPIError(r)

    return r.json()

def get_upload( upload_uri ):
    url = api_base_path + upload_uri
    headers = {
        "Authorization": "Bearer {}".format(access_token),
    }
    r = requests.get( url,  headers=headers)
    checkForAPIError(r)

    res_json = r.json()

    return res_json



class Upload:
    """ The upload class gives the user streamlined access to the Redivis API, simplifying the process of uploading data """
    api_base_path = "https://redivis.com/api/v1"

    def __init__(self, user, dataset, table):
        self.user = user
        self.dataset = dataset
        self.table = table
        self.dataset_identifier = "{}.{}".format(self.user, self.dataset)
        self.table_identifier = "{}.{}".format(self.user, self.dataset, self.table)


    def create_upload(self,filename, endpoint_dataset):

        url = "{}/datasets/{}/versions".format(api_base_path, self.dataset_identifier)
        headers = {"Authorization": "Bearer {}".format(access_token)}

        r = requests.get("{}/next".format(url), headers=headers)

        if r.status_code == 200:
            print("Next version already exists. Continuing...")
        else:
            url = "{}/datasets/{}/versions".format(api_base_path, self.dataset_identifier)
            headers = {"Authorization": "Bearer {}".format(access_token)}

            r = requests.post(url, headers=headers)
            checkForAPIError(r)


        url = "{}/tables/{}/uploads".format(api_base_path, self.table_identifier)
        data = {"name": filename, "mergeStrategy": "append", "type": "delimited"}
        headers = {"Authorization": "Bearer {}".format(access_token)}

        r = requests.post(url, data=json.dumps(data), headers=headers)

        checkForAPIError(r)

        res_json = r.json()

        upload_data(endpoint_dataset, res_json['uri'])

        # Wait for upload to finish importing
        while True:
            time.sleep(2)
            upload = get_upload(res_json['uri'])
            if upload['status'] == 'failed':
                sys.exit(
                    "Issue with importing uploaded file, abandoning process...: \n\t{}".format(upload['errorMessage']))
            elif upload['status'] == 'completed':
                break
            else:
                print("Import is still in progress.")

        print("Import completed.")

    def release_dataset(self):
        url = "{}/datasets/{}/versions/next/release".format(api_base_path, self.dataset_identifier)

        data = {"releaseNotes": "Initial Release", "label": "Test Release"}
        headers = {"Authorization": "Bearer {}".format(access_token)}

        r = requests.post(url, data=json.dumps(data), headers=headers)

        checkForAPIError(r)

        return r


class User:
    """ The User class contains information about  users"""

    def __init__(self, username):
        self.username = username

    def list_datasets(self, max_results=10):
        # Returns a list of dataset instances
        headers = {"Authorization": "Bearer {}".format(os.environ["REDIVIS_ACCESS_TOKEN"])}
        r = requests.get("https://redivis.com/api/v1/users/{}/datasets".format(self.username), headers=headers)
        json_dict = r.json()

        dataset_list = []
        presentation_list = []
        result = json_dict["results"]

        i = 0
        for i in range(len(result)):

            dataset_list.append(Dataset(self.username, result[i]["name"]))
            presentation_list.append(result[i]["name"])
            presentation_list.append(result[i]["uri"])

        return dataset_list, presentation_list


    def dataset(self, dataset):
        # headers = {"Authorization": "Bearer {}".format(os.environ["REDIVIS_ACCESS_TOKEN"])}
        # r = requests.get("https://redivis.com/api/v1/users/{}/datasets/{}".format(self.username, dataset), headers=headers)
        # res_json = r.json()

        return Dataset("kevin", dataset)



class Dataset:
    """ The Dataset class encapsulates information about a particular user's datasets"""

    def __init__(self, user, dataset_name):
        self.user = user
        self.dataset_name = dataset_name



    def exists(self, data_set):
        """returns boolean value response about the existence of a dataset on Redivis"""

        bool = True
        headers = {"Authorization": "Bearer {}".format(os.environ["REDIVIS_ACCESS_TOKEN"])}
        r = requests.get("https://redivis.com/api/v1/users/{}/datasets/{}".format(self.user, data_set), headers=headers)
        res_json = r.json()

        if ("error" in res_json and res_json["error"]["status"] == 404):
            bool = False
            print("Dataset does not exist")

        elif ("error" in res_json):
            return r
            # informs the user of any other type of error that may have occurred

        return bool



    def get(self):

        headers = {"Authorization": "Bearer {}".format(os.environ["REDIVIS_API_TOKEN"])}
        r = requests.get(
            "https://redivis.com/api/v1/datasets/users/{}/{}".format(self.short_name, self.dataset_name), headers=headers)
        res_json = r.json()

        return res_json
    # Populates properties on the dataset. Throws error if not exists


    def list_tables(self):
        # Returns a list of tables

        headers = {"Authorization": "Bearer {}".format(os.environ["REDIVIS_ACCESS_TOKEN"])}
        r = requests.get("https://redivis.com/api/v1/datasets/{}.{}/tables".format(self.user, self.dataset_name), headers=headers)
        json_dict = r.json()

        table_list = []
        result = json_dict["results"]

        i = 0
        # print(result)
        for i in range(len(result)):
            # Nested tuple in a list?
            table_list.append(Table(self.user, result[i]["name"]))
            print()

        return table_list



    def Table(self, table_name):
        headers = {"Authorization": "Bearer {}".format(os.environ["REDIVIS_API_TOKEN"])}
        r = requests.get("https://redivis.com/api/v1/tables/users/{}/datasets/{}:{}".format(self.short_name, self.dataset_name,table_name), headers=headers)
        res_json = r.json()

        return res_json


class Table:
    """ The Table class encapsulates information """


    def __init__(self, user, dataset_name):
        self.user = user
        self.dataset_name = dataset_name


    def exists(self,table):
        bool = True
        headers = {"Authorization": "Bearer {}".format(os.environ["REDIVIS_ACCESS_TOKEN"])}
        r = requests.get("https://redivis.com/api/v1/tables/users/{}/datasets/{}:{}".format(self.username, self.dataset_name, table), headers=headers)
        res_json = r.json()

        if ("error" in res_json and res_json["error"]["status"] == 404):
            bool = False
            print("Dataset does not exist")

        elif ("error" in res_json):
            return r
            # informs the user of any other type of error that may have occurred

        return bool

    def get(self, table_name):

        headers = {"Authorization": "Bearer {}".format(os.environ["REDIVIS_API_TOKEN"])}
        r = requests.get("https://redivis.com/api/v1/tables/users/{}/datasets/{}:{}".format(self.user, self.dataset_name, table_name), headers=headers)
        res_json = r.json()

        return res_json

    # Populates properties on the table. Throws error if not exists

    def list_variables(self, table_name, max_results=100):

        headers = {"Authorization": "Bearer {}".format(os.environ["REDIVIS_ACCESS_TOKEN"])}
        r = requests.get("https://redivis.com/api/v1/tables/{}.{}.{}/variables".format(self.user, self.dataset_name, table_name),headers=headers)
        json_dict = r.json()

        variable_list = []
        result = json_dict["results"]
        num_var = len(result)
        if (num_var > max_results):
            num_var = max_results

        i = 0
        for i in range(len(result)):
            variable_list.append(result[0])

        return variable_list

    # Returns a list of variable instances

    def listRows(self, variables, max_results=100, as_data_frame=False): #force the variable input to be a comma-separated

        headers = {"Authorization": "Bearer {}".format(os.environ["REDIVIS_ACCESS_TOKEN"])}
        r = requests.get(
            "https://redivis.com/api/v1/tables/{}.{}.{}/rows?selectedVariables={}".format(self.user, self.dataset_name, table_name, variables),
            headers=headers)
        json_dict = r.json()

        return json_dict
    # Returns an iterator for table rows






