# redivis-bigquery
Redivis authorization wrapper around google-cloud-bigquery

### Alpha disclaimer
This library is currently an alpha. It may break in unexpected ways, and is not intended for production use.

Known issues:
- Error handling / messaging is limited. Make sure that you have access to tables referenced by your query, and that the tables are authorized for export to your compute environment.
- Queries are currently restricted to referencing 100GB of data or less

# usage
### Installation
```
pipenv install -e "git+https://github.com/redivis/redipy.git#egg=redivis-bigquery&subdirectory=bigquery"

REDIVIS_API_TOKEN=<your-api-token> pipenv run python
```
### Simple queries
```py
from redivis import bigquery

client = bigquery.Client()

# Perform a query.
# Table at
QUERY = ('SELECT * FROM `ianmathews91.medicare_public_example.high_cost_in_providers_in_CA_output` LIMIT 10')

query_job = client.query(QUERY)  # API request

for row in query_job:
	print(row)
```
### Working with data frames
```py
from redivis import bigquery

client = bigquery.Client()

# Perform a query.
# Table at https://redivis.com/StanfordPHS/datasets/165
QUERY = ('SELECT * FROM `stanfordphs.commuting_zone_life_expectancy_trends.v1_0` LIMIT 10')

df = client.query(QUERY).to_dataframe()  # API request

print(df)
```
### Referencing tables
If a table belongs to a dataset, it should be referenced as: `dataset_owner.dataset_name.version`. The decimal in the version should be replaced by an underscore; e.g. `v1.0` -> `v1_0`. If a table belongs to a project, it should be referenced as: `project_owner.project_name.table_name`.

Any non-alphanumeric characters in dataset, project, and table names can be replaced by an underscore (`_`), and multiple non-alphanumeric characters can be replaced by a single underscore. For example, the dataset `Yay!!! A dataset` could be represented at `yay_a_dataset`.

# Further reference
Consult the [google-cloud-bigquery](https://googleapis.dev/python/bigquery/latest/index.html) documentation for further information. Please note the following changes from the google-cloud-bigquery library:
- redivis-bigquery is **read only**, meaning that various write methods are not supported
- You do not need to provide a project_id to any calls; it will be ignored
- As long as the REDIVIS_API_TOKEN environment variable has been set, you do not need to worry about any additional authentication
