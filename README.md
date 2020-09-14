# redipy
Redivis client libraries for python.

## Alpha disclaimer
This library is currently in alpha testing. We encourage early adopters to test it out and give feedback, though be aware that some interfaces are incomplete and/or may change in future releases.

## Installation
```
pip install redivis
```

## Authentication
In order to authenticate with your Redivis account, you must first [generate an API Token](https://apidocs.redivis.com/authorization).

This API token must then be set as the `REDIVIS_API_TOKEN` environment variable before running your script. You can set the variable for your current session by running the following in your terminal:
```
export REDIVIS_API_TOKEN=YOUR_TOKEN
```

## Interface
When referencing datasets, projects, and tables on Redivis, you should be familiar with the [resource reference syntax](https://apidocs.redivis.com/referencing-resources).

For example:
```
username.dataset_name:v1_0.table_name     # All non-ascii characters can be escaped with an underscore
organization_name.my_dataset.table_name   # version tag is optional

username.project_name:3.table_name:4      # projects, datasets, and tables have persistent numeric identifiers 
                                          # to ensure your script works after a rename. 
```

### Querying data
The redivis.bigquery subpackage is a thin wrapper around the google-cloud-bigquery python client, allowing you to leverage its functionality to interface with tables stored on Redivis. All authentication is managed via your Redivis API credentials.

Please note that the only supported methods are those that involve querying tables. Interfaces involved in listing BigQuery resource, referencing BigQuery datasets, or any calls to create, modify, or delete BigQuery resources are not supported.

Consult the [google-cloud-bigquery](https://googleapis.dev/python/bigquery/latest/index.html) python library for full documentation.

#### Examples:
Simple queries
```py
from redivis import bigquery

client = bigquery.Client()

# Perform a query.
# Table at https://redivis.com/projects/1008/tables/9443
QUERY = ('SELECT * FROM `ianmathews91.medicare_public_example.high_cost_in_providers_in_CA_output` LIMIT 10')

query_job = client.query(QUERY)  # API request

for row in query_job:
	print(row)
```
Working with data frames
```py
from redivis import bigquery

client = bigquery.Client()

# Perform a query.
# Table at https://redivis.com/StanfordPHS/datasets/1411/tables
QUERY = ("SELECT * FROM `stanfordphs.commuting_zone:v1_0.life_expectancy_trends` LIMIT 10")

df = client.query(QUERY).to_dataframe()  # API request

print(df)
```
### Uploading data
Create a new dataset
```py
from redivis import create_dataset


user_name = "your_user_name"
dataset_name = "dataset_name"
table_name = "table_name"


# Can also provide an organization parameter to create a dataset in an organization

dataset = create_dataset(
    user=user_name, name=dataset_name, public_access_level="none", description="A description"
)


# Create a table on the dataset. Datasets may have multiple tables
# Merge strategy (append or replace) will affect whether future updates
#    are appended to or replace the existing table. This can also be modified at a later point.

table = dataset.create_table(
    name=table_name, description="Some description", upload_merge_strategy="replace"
)


# Upload a file to the table. You can upload multiple files to any table, and they'll be unioned into one

with open("path/to/file", "rb") as f:
    table.create_upload(name="tiny.csv", type="delimited", data=f)

dataset.release()
```
Update an exiting dataset
```py
from redivis import Table

owner_name = "your_user_or_organization_name"
dataset_name = "dataset_name"
table_name = "table_name"

table = Table(f"{owner_name}.{dataset_name}.{table_name}")

with open("path/to/file", "rb") as f:
    table.create_upload(name="tiny.csv", type="delimited", data=f)

dataset.release()
```

## Contributing
Please mark any bugs or feature requests by opening an issue on this repo — your feedback is much appreciated!

For local development, clone this repository and then run
```py
python setup.py develop
```
You can then run the tests, e.g.: 
```
REDIVIS_API_TOKEN=YOUR_TOKEN python tests 
```