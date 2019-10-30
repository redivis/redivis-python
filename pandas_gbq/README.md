# redivis-bigquery
Redivis authorization wrapper around google-cloud-bigquery

# usage
```
pipenv install -e "git+https://github.com/redivis/redipy.git#egg=redivis-pandas-gbq&subdirectory=pandas-gbq"

REDIVIS_API_TOKEN=<your-api-token> pipenv run python
```
Then
```
from redivis import pandas_gbq

sql = """
SELECT * FROM `dataset_165.271`
LIMIT 100
"""
df = pandas_gbq.read_gbq(sql)
```

Consult the [pandas-gbq](https://pandas-gbq.readthedocs.io/en/latest/reading.html) for further information. Please note the following changes from the pandas-gbq library:
- redivis-pandas-gbq is **read only**, meaning that various write methods are not supported
- You do not need to provide a project_id to any calls; it will be ignored
- As long as the REDIVIS_API_TOKEN environment variable has been set, you do not need to worry about any additional authentication
