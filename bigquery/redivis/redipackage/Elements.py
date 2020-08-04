import requests
import os
import csv
import json
import time
import pandas
import sys




# # Prints all tables that Ian has
# user = User(short_name="imathews")
# ians_datasets = user.list_datasets()
#
# for dataset in ians_datasets:
#     tables = dataset.list_tables()
#     for table in tables:
#         print(table.name)
#
# # Get rows from a particular table
# rows = User(short_name="redivis")
#     .Dataset("epa_test")
#     .Table("pm2_5")
#     .getRows(max_results=100)

access_token = os.environ["REDIVIS_ACCESS_TOKEN"]

# See https://apidocs.redivis.com/referencing-resources
user_name = "kevin"
dataset_name = "a_dataset"
table_name = "a_table"

dataset_identifier = "{}.{}".format(user_name, dataset_name)
table_identifier = "{}.{}:next.{}".format(user_name, dataset_name, table_name)
api_base_path = "https://redivis.com/api/v1"
filename = "test.csv"
file_path = os.path.join("./", filename)

def checkForAPIError(r):
    if r.status_code >= 400:
        res_json = r.json()
        sys.exit("An API error occurred at {} {} with status {}:\n\t{} ".format(r.request.method, r.request.path_url, r.status_code, res_json['error']['message']))

def get_next_version():
    url = "{}/datasets/{}/versions".format(api_base_path, dataset_identifier)
    headers = {"Authorization": "Bearer {}".format(access_token)}

    r = requests.get("{}/next".format(url), headers=headers)

    # Don't exit on 404 errors
    if r.status_code != 404:
        checkForAPIError(r)
        return r



class User:
    """ The User class contains information about particular users"""

    def __init__(self, short_name, identifier):
        self.short_name = short_name
        self.identifier = identifier

    def list_datasets(maxResults=10):
        # Returns a list of dataset instances
        headers = {"Authorization": "Bearer {}".format(os.environ["REDIVIS_API_TOKEN"])}
        r = requests.get("https://redivis.com/api/v1/users/{}/datasets".format(self.short_name), headers=headers)
        res_json = r.json()
        # We now have a JSON response for all datasets on this user
        return res_json

    def Dataset(dataset_name):
        headers = {"Authorization": "Bearer {}".format(os.environ["REDIVIS_API_TOKEN"])}
        r = requests.get("https://redivis.com/api/v1/users/{}/datasets/{}".format(self.short_name), headers=headers, dataset_name)
        res_json = r.json()
        # We now have a JSON response for all datasets on this user
        return res_json


class Dataset:
    """ The Dataset class encapsulates information about a particular user's datasets"""

    def __init__(self, user, dataset_identifier, version):
        self.user = user
        self.dataset_identifier = dataset_identifier
        self.version = version #will be made into a class at some point


    def exists(dataset_name):

    # returns a boolean for whether the dataset exists
        if (Dataset.get(dataset_name).status_code == 404):
            raise ValueError



    def get(self):
        # Populates properties on the dataset. Throws error if not exists

        # try:
        #     exists(dataset)
        # except:
        #     raise ValueError("The dataset does not exist")

    def list_tables(self):

    # Returns a list of table instances

        headers = {"Authorization": "Bearer {}".format(os.environ["REDIVIS_API_TOKEN"])}
        r = requests.get("https://redivis.com/api/v1/users/{}/datasets/tables".format(self.short_name), headers=headers)
        res_json = r.json()
        # We now have a JSON response for all datasets on this user
        return res_json
        #fix the api endpoint




    def create_version(self):

    # Creates the next version
    #     r = get_next_version()
    #
    #     if r.status_code == 200:
    #         print("Next version already exists. Continuing...")
    #         return
    #
    #     url = "{}/datasets/{}/versions".format(api_base_path, dataset_identifier)
    #     headers = {"Authorization": "Bearer {}".format(access_token)}
    #
    #     r = requests.post(url, headers=headers)
    #     checkForAPIError(r)
    #     return r

    def get_next_version(autocreate=false):

    # Gets the next version of the dataset

    def Table(table_name):
        headers = {"Authorization": "Bearer {}".format(os.environ["REDIVIS_API_TOKEN"])}
        r = requests.get("https://redivis.com/api/v1/tables/users/{}/datasets/{}:{}".format(self.short_name), headers=headers,dataset_identifier,table_name)
        res_json = r.json()
        # We now have a JSON response for all datasets on this user
        return res_json


class Table:
    """ The Table class encapsulates information"""

    tableCount = 0

    def __init__(self, dataset, identifier):
        self.dataset = dataset
        self.identifier = identifier
        Table.tableCount += 1

def exists():
    # returns a boolean for whether the table exists

    def get():

    # Populates properties on the table. Throws error if not exists

    def list_variables(maxResults=100)

    # Returns a list of variable instances

    def getRows(max_results=100, as_data_frame=false):

    # Returns an iterator for table rows

    def upload_file(file, type='csv', merge_strategy=nil, autocreate_next_version=false):


# Uploads a file to the table


class Variable:
# TODO
