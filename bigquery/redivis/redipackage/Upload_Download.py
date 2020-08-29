import sys
import csv
import os
import requests
import json
import time
from io import StringIO
from redivis import bigquery

# When calling this script, provide the API Token as an environment variable
# E.g.: REDIVIS_API_TOKEN=your_api_token pipenv run python upload_script.py

# This script assumes you have an API access token with date.edit scope. See https://apidocs.redivis.com/authorization
access_token = os.environ["REDIVIS_API_TOKEN"]

# See https://apidocs.redivis.com/referencing-resources
user_name = "[kevin]"
dataset_name = "[library-test]"
table_name = "[addresses]"

dataset_identifier = "{}.{}".format(user_name, dataset_name)
table_identifier = "{}.{}:next.{}".format(user_name, dataset_name, table_name)
api_base_path = "https://redivis.com/api/v1"
filename = "test.csv"
file_path = os.path.join("./", filename)


def main():

    start_time = get_start_time()

    temp = get_start_time()

    starting_day = int(temp[9])

    end_day = starting_day + 2

    temp.replace(temp[9], str(end_day))
    print("Fetching EPA data")

    epa_data = pull_from_epa_api(start_time=start_time, end_time = end_day)

    print("Creating next version if needed...")

    create_next_version_if_not_exists()

    print("Creating upload...")

    upload = create_upload()

    print("Uploading file...")

    upload_data(epa_data, upload['uri'])

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

def get_start_time():

    client = bigquery.Client()

    start_time = """SELECT DATETIME_ADD(MAX(PARSE_DATETIME('%Y-%m-%dT%H:%M', UTC)), INTERVAL 1 hour) as next_dt 
                    FROM redivis.epa_test.pm2_5:1"""

    query_job = client.query(start_time)

    print("Fetching EPA data")
    for row in query_job:
        print("EPA Data Retrieved")


    time = row.next_dt.strftime('%Y-%m-%dT%H:%M')
    return time

def pull_from_epa_api(start_time, end_time):
    # See https://docs.airnowapi.org/Data/query
    # BBox is for California
    url = "http://www.airnowapi.org/aq/data/?startDate={start_time}&endDate={end_time}&parameters=PM25&BBOX={bbox}&dataType=A&format=application/json&verbose=0&nowcastonly=0&includerawconcentrations=0&API_KEY={epa_api_key}".format(start_time = start_time, end_time = end_time, bbox="-125.087280,32.200885,-113.837280,42.250626", epa_api_key="A6A59B4B-07D4-4843-A7A2-72D4CF1B1AA5")
    payload = {}
    headers= {}

    response = requests.request("GET", url, headers=headers, data = payload)

    dataset_name = response
    return convert_to_csv(response.json())


def convert_to_csv(input_json):
    # See https://www.idkrtm.com/converting-json-to-csv-and-back-again-using-python/
    # and https://stackoverflow.com/questions/9157314/how-do-i-write-data-into-csv-format-as-string-not-file
    si = StringIO()
    keylist = []
    for key in input_json[0]:
        keylist.append(key)
        f = csv.writer(si)

    f.writerow(keylist)

    for record in input_json:
        currentrecord = []
        for key in keylist:
            currentrecord.append(record[key])
        f.writerow(currentrecord)

    return si.getvalue()

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


def create_upload( ):
    url = "{}/tables/{}/uploads".format(api_base_path, table_identifier)
    data = {"name": filename, "mergeStrategy": "append", "type": "delimited"}
    headers = {"Authorization": "Bearer {}".format(access_token)}

    r = requests.post(url, data=json.dumps(data), headers=headers)

    checkForAPIError(r)

    res_json = r.json()

    return res_json


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

main()

