# redivis-bigquery
Redivis authorization wrapper around google-cloud-bigquery

### Alpha disclaimer
This library is currently an alpha. It may change in backwards-incompatible ways, and is not intended for production use.

Known issues:
- Queries are currently restricted to referencing 100GB of data or less

# usage
### Installation
```
pipenv install -e "git+https://github.com/redivis/redipy.git#egg=redivis-bigquery&subdirectory=bigquery"

REDIVIS_API_TOKEN=<your-api-token> pipenv run python
```
### authorization
In order to make calls to the Redivis API, you must set the REDIVIS_API_TOKEN environment variable:

REDIVIS_API_TOKEN=<your_api_token> python script.py
In order to generate a new authorization token, navigate to you https://redivis.com/workspace/settings and scroll to the API tokens section. Your token must be authorized have the data.data scope.

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
```sql
# Datasets
# Note: table_name for datasets should always be "main". 
# An upcoming release of Redivis will allow for named tables within datasets.
owner_name.dataset_name[:v{version}][:sample][:{dataset_id}].table_name[:{table_id}]

# Projects
owner_name.project_name[:{project_id}].table_name[:{table_id}]
```
Note that datasets (and their tables) that are added to a project should still be referenced via their dataset identifier; the tables are not considered to be within the project. (`TODO: reconsider?`)

All non alpha-numeric and underscore characters in names and version tags can be replaced by an underscore (`_`) character. Colons (`:`), periods (`.`), and backticks (`` ` ``) must be replaced. Multiple underscores can be collapsed into a single underscore, and leading and trailing underscores can be ignored.

References can include several optional additions, denoted by a `:`. The `:sample` suffix on a dataset will reference that dataset's sample. The `:v{version}` suffix references a particular version of a dataset; if no version is specified, the latest version will be used. The `:{datasetId}`, `:{tableId}`, and `:{projectId}` suffixes allow you to specify the relevant persistent identifier, which will be used in place of the name and avoid future errors if table or dataset names are modified. 

If the `:tableId` is specified, no owner or project|dataset identifier is required. If the `:datasetId|:projectId` is specified, no owner is required.

### Examples

We can reference the 1940 households table from the [IPUMS](https://redivis.com/StanfordPHS/datasets/152) dataset as:

``` sql
SELECT [] FROM `stanfordphs.IPUMS.1940 Households` 
```

We can reference without backticks if we escape non-word characters.
Note that references are case-insensitive.

``` sql
SELECT [] FROM stanfordphs.ipums.1940_households
```

By default this uses the latest (2.0) version of the dataset. If we want
to work with version 1.0:

``` sql
SELECT [] FROM stanfordphs.ipums:v1_0.1940_households
```

If we want to work with the 1% sample:

``` sql
# The order of the suffixes does not matter
SELECT [] FROM stanfordphs.ipums:v1_0:sample.1940_households
```

Finally, we can provide persistent ids to prevent our reference from
breaking if a dataset or table is renamed:

``` sql
SELECT [] FROM stanfordphs.ipums:152:v1_0:sample.1940_households

# We don't actually need the owner if an id is provided
SELECT [] FROM ipums:152:v1_0:sample.1940_households

# Can also provide a table id
SELECT [] FROM ipums:152.1940_households:178

# Don't need the dataset name if tableId is provided
SELECT [] FROM 1940_households:178

# Or even the table name
SELECT [] FROM :178
```

Referencing tables in [a
project](https://redivis.com/projects/1008/tables/9443) is quite
similar, though projects don’t have versions or
samples:

``` sql
SELECT [] FROM ianmathews91.medicare_public_example.high_cost_in_providers_in_CA_output

# We don't need the project owner if an projectId is provided
SELECT [] FROM medicare_public_example:1008.high_cost_in_providers_in_CA_output

# We don't need the project name a tableId is provided
SELECT [] FROM high_cost_in_providers_in_CA_output:9443
```

# Further reference
Consult the [google-cloud-bigquery](https://googleapis.dev/python/bigquery/latest/index.html) documentation for further information. Please note the following changes from the google-cloud-bigquery library:
- redivis-bigquery is **read only**, meaning that various write methods are not supported
- You do not need to provide a project_id to any calls; it will be ignored
- As long as the REDIVIS_API_TOKEN environment variable has been set, you do not need to worry about any additional authentication
