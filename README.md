![Redivis Logo](https://github.com/redivis/redivis-python/raw/master/assets/logo_small.png)
# redivis-python
[Redivis](redivis.com) client library for python! 

Connect data on Redivis to the Python scientific stack, and script your data management tasks.

## Beta disclaimer
This library is currently in beta testing. Many of the interfaces are nearing stability, but there may still be backwards-incompatible changes in the future. 

Please report any issues or feature requests in the [issue tracker](https://github.com/redivis/redivis-python/issues).

Comprehensive documentation for this package is available at https://apidocs.redivis.com/client-libraries/redivis-python

## Installation
```
pip install redivis
```

## Authentication
> If you are using this library within a Redivis notebook, you'll automatically be authenticated; you can ignore this section.

In order to authenticate with your Redivis account, you must first [generate an API Token](https://apidocs.redivis.com/authorization).

This API token must then be set as the `REDIVIS_API_TOKEN` environment variable before running your script. You can set the variable for your current session by running the following in your terminal:
```
export REDIVIS_API_TOKEN=YOUR_TOKEN
```

## Interface
```py
import redivis

query = redivis.query("""
    SELECT * FROM demo.cms_2014_medicare_data.home_health_agencies 
    LIMIT 10
""")

df = query.to_dataframe();
```
When referencing datasets, projects, and tables on Redivis, you should be familiar with the [resource reference syntax](https://apidocs.redivis.com/referencing-resources).

Full reference for this package is available at https://apidocs.redivis.com/client-libraries/redivis-python/reference

## Examples

### Querying data
#### Execute a query
```py
import redivis

# Perform a query on the Demo CMS Medicare data. Table at https://redivis.com/demo/datasets/1754/tables
query = redivis.query("""
    SELECT * FROM demo.cms_2014_medicare_data.home_health_agencies 
    WHERE state = 'CA'
""")

for row in query.list_rows():
    print(row['agency_name'])

# We can also use data frames
df = query.to_dataframe();
print(df)
```
#### Execute a scoped query
```py
import redivis

# Perform a query on the Demo CMS Medicare data. Table at https://redivis.com/demo/datasets/1754/tablesd

# We don't need to include fully-qualified table names if we scope our query to the appropriate dataset or project

query = (
    redivis
    .organization("Demo")
    .dataset("CMS 2014 Medicare Data")
    .query("""
        SELECT provider_name, average_total_payments 
        FROM nursing_facilities
        INNER JOIN outpatient_charges USING (provider_id)
        WHERE state = 'CA'
    """)
)

for row in query.list_rows():
    print(row.agency_name)

# We can also use data frames
df = query.to_dataframe();
print(df)
```

### Reading table data
```py
table = (
    redivis
    .organization("Demo")
    .dataset("CMS 2014 Medicare Data")
    .table("Hospice providers")

# We can specify an optional limit argument to only load some of the records
for row in table.list_rows(limit=100):
    print(row)

df = table.to_dataframe(limit=100)
print(df)

```

### Uploading data
#### Create a new dataset
```py
import redivis

dataset = redivis.user("your-username").dataset("some dataset")

# Could also create a dataset under an organization:
# dataset = redivis.organization("Demo organization").dataset("some dataset")


# public_access_level can be one of ('none', 'overview', 'metadata', 'sample', 'data')
dataset.create(public_access_level="overview")
```
#### Create a table and upload data
```py
import redivis

dataset = redivis.user("your-username").dataset("some dataset")

# Create a table on the dataset. Datasets may have multiple tables
# Merge strategy (append or replace) will affect whether future updates
#    are appended to or replace the existing table. This can also be modified at a later point.

table = (
    dataset
    .table("Table name")
    .create(description="Some description", upload_merge_strategy="replace")
)

# Upload a file to the table. You can upload multiple files to any table, and they'll be appended together

with open("path/to/file", "rb") as f:
    table.create_upload(name="tiny.csv", type="delimited", data=f)
```
#### Release a new version
```py
import redivis

dataset = redivis.organization("Demo").dataset("some dataset")
dataset.release()
```
#### Update an existing dataset
```py
import redivis

dataset = redivis.user("your-username").dataset("some dataset")

# dataset.create_next_version will throw an error if a "next" version already exists,
# unless the ignore_if_exists argument is provided
dataset = dataset.create_next_version(ignore_if_exists=True)

# Upload new data to your table
with open("tiny.csv", "rb") as f:
    dataset.table("Table name").upload(name="tiny.csv", type="delimited", data=f)

dataset.release()
```

## Contributing
Please report any issues or feature requests in the [issue tracker](https://github.com/redivis/redivis-python/issues)
 — your feedback is much appreciated!
