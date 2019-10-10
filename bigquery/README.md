# redivis-bigquery
Redivis authorization wrapper around google-cloud-bigquery

# usage
```
pipenv install redivis-bigquery
```
Then
```
from redivis import bigquery

client = bigquery.Client(os.getenv('REDIVIS_API_TOKEN'))

# Perform a query.
QUERY = ('SELECT * FROM `dataset_165.271`')

query_job = client.query(QUERY)  # API request

for row in query_job:
	print(row)
```
