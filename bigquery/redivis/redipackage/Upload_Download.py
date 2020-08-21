import sys
import csv
import os
import requests
import json
import time

# When calling this script, provide the API Token as an environment variables
# E.g.: REDIVIS_API_TOKEN=your_api_token pipenv run python upload_script.py

# This script assumes you have an API access token with date.edit scope. See https://apidocs.redivis.com/authorization
access_token = os.environ["REDIVIS_API_TOKEN"]

# See https://apidocs.redivis.com/referencing-resources
user_name = "[user_name]"
dataset_name = "[dataset_name]"
table_name = "[table_name]"

dataset_identifier = "{}.{}".format(user_name, dataset_name)
table_identifier = "{}.{}:next.{}".format(user_name, dataset_name, table_name)
api_base_path = "https://redivis.com/api/v1"
filename = "test.csv"
file_path = os.path.join("./", filename)

def pull_from_api(): # api request

    #insert place holder as url
    url = "http://www.airnowapi.org/aq/observation/latLong/current/?format=text/csv&latitude=37.7555&longitude=-122.4423&distance=25&API_KEY=A6A59B4B-07D4-4843-A7A2-72D4CF1B1AA5"

    payload = {}
    headers= {}

    response = requests.request("GET", url, headers=headers, data = payload)

    dataset_name = response

    return response.text.encode('utf8')

def convert_to_csv():


    with open('data.json') as json_file:
        data = json.load(json_file)


    data_file = open('data_file.csv', 'w')

    csv_writer = csv.writer(data_file)


    count = 0

    for data_chunk in data:
        if count == 0:


            header = data.keys()
            csv_writer.writerow(header)
            count += 1

        csv_writer.writerow(data.values())

    data_file.close()

    return csv_writer

def get_next_version():
    url = "{}/datasets/{}/versions".format( api_base_path, dataset_identifier )
    headers = {"Authorization": "Bearer {}".format(access_token)}

    r = requests.get( "{}/next".format(url), headers=headers )

    # Don't exit on 404 errors
    if r.status_code != 404:
        checkForAPIError(r)
    return r

def create_next_version_if_not_exists( ):
    r = get_next_version()

    if r.status_code == 200:
        print("Next version already exists. Continuing...")
        return

    url = "{}/datasets/{}/versions".format( api_base_path, dataset_identifier )
    headers = {"Authorization": "Bearer {}".format(access_token)}

    r = requests.post( url, headers=headers )
    checkForAPIError(r)
    return r


def create_upload( filename ):
    url = "{}/tables/{}/uploads".format(api_base_path, table_identifier)
    data = {"name": filename, "mergeStrategy": "append", "type": "delimited"}
    headers = {"Authorization": "Bearer {}".format(access_token)}

    r = requests.post(url, data=json.dumps(data), headers=headers)

    checkForAPIError(r)

    res_json = r.json()

    return res_json


def upload_file(path_to_file, upload_uri):
    url = api_base_path + upload_uri
    files = {"upload_file": open(path_to_file, "rb")}
    headers = { "Authorization": "Bearer {}".format(access_token) }

    with open( path_to_file, 'rb' ) as f:
        r = requests.put(url, data=f, headers=headers)
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

def release_dataset( ):
    url = "{}/datasets/{}/versions/next/release".format( api_base_path, dataset_identifier )

    data = { "releaseNotes": "Initial Release", "label": "Test Release" }
    headers = { "Authorization": "Bearer {}".format(access_token) }

    r = requests.post( url, data=json.dumps(data), headers=headers )

    checkForAPIError(r)

    return r

def checkForAPIError(r):
    if r.status_code >= 400:
        res_json = r.json()
        sys.exit( "An API error occurred at {} {} with status {}:\n\t{} ".format( r.request.method, r.request.path_url, r.status_code, res_json['error']['message'] ) )

def main():

    pull_from_api

    create_next_version_if_not_exists()

    upload = create_upload(convert_to_csv)

    upload_file(file_path, upload['uri'])

    # Wait for upload to finish importing
    while True:
        time.sleep(2)
        upload = get_upload(upload['uri'])
        if upload['status'] == 'failed':
            sys.exit( "Issue with importing uploaded file, abandoning process...: \n\t{}".format( upload['errorMessage'] ) )
        elif upload['status'] == 'completed':
            break
        else:
            print("Import is still in progress.")

    print("Import completed. Releasing version...")

    release_dataset()



main()


