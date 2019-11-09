# redivis-bigquery
Redivis authorization wrapper around google-cloud-bigquery

### Alpha disclaimer
This library is currently an alpha. It may break in unexpected ways, and is not intended for production use.

Known issues:
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
# Table at https://redivis.com/projects/1008/tables/9443
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
## Referencing tables
All tables belong to either a dataset or project. Table references use the following pattern:
```
// Datasets
// Note: table_name for datasets should always be "main". 
// An upcoming release of Redivis will allow for named tables within datasets.
owner_name.dataset_name[:version][:sample][:dataset_id].table_name[:table_id]

// Projects
owner_name.project_name[:project_id].table_name[:table_id]
```
Note that datasets (and their tables) that are added to a project should still be referenced via their dataset identifier; the tables are not considered to be within the project. (`TODO: reconsider?`)

All non alpha-numeric and underscore characters in names and version tags can be escaped by an underscore (`_`) character. Colons (`:`), periods (`.`), and backticks (`` ` ``) must be escaped. Multiple underscores can be collapsed into a single underscore, and leading and trailing underscores can be ignored.

References can take several optional arguments, denoted by a `:`. 

### Examples

# Further reference
Consult the [google-cloud-bigquery](https://googleapis.dev/python/bigquery/latest/index.html) documentation for further information. Please note the following changes from the google-cloud-bigquery library:
- redivis-bigquery is **read only**, meaning that various write methods are not supported
- You do not need to provide a project_id to any calls; it will be ignored
- As long as the REDIVIS_API_TOKEN environment variable has been set, you do not need to worry about any additional authentication
