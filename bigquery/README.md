# redivis-bigquery
Redivis authorization wrapper around google-cloud-bigquery

# usage
```
pipenv install -e "git+https://github.com/redivis/redipy.git#egg=redivis-bigquery&subdirectory=bigquery"

REDIVIS_API_TOKEN=<your-api-token> pipenv run python
```
Then
```py
from redivis import bigquery

client = bigquery.Client()

# Perform a query.
QUERY = ('SELECT * FROM `dataset_160.237`')

query_job = client.query(QUERY)  # API request

for row in query_job:
	print(row)
```
Or to retrieve results as a **pandas.DataFrame**
```py
from redivis import bigquery

client = bigquery.Client()

# Perform a query.
QUERY = ('SELECT * FROM `dataset_160.237`')

df = client.query(QUERY).to_dataframe()  # API request

print(df)
```
Consult the [google-cloud-bigquery](https://googleapis.dev/python/bigquery/latest/index.html) documentation for further information. Please note the following changes from the google-cloud-bigquery library:
- redivis-bigquery is **read only**, meaning that various write methods are not supported
- You do not need to provide a project_id to any calls; it will be ignored
- As long as the REDIVIS_API_TOKEN environment variable has been set, you do not need to worry about any additional authentication
