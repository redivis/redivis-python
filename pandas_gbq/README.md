# redivis-pandas-gbq
Redivis authorization wrapper around google-cloud-bigquery

# usage
```
pipenv install --skip-lock -e "git+https://github.com/redivis/redipy.git#egg=redivis-pandas-gbq&subdirectory=pandas_gbq"

REDIVIS_API_TOKEN=<your-api-token> pipenv run python
```
Then
```
from redivis import pandas_gbq

sql = """
SELECT * FROM `dataset_160.237`
"""
df = pandas_gbq.read_gbq(sql)
```

Consult the [pandas-gbq](https://pandas-gbq.readthedocs.io/en/latest/reading.html) for further information. Please note the following changes from the pandas-gbq library:
- redivis-pandas-gbq is **read only**, meaning that various write methods are not supported
- You do not need to provide a project_id to any calls; it will be ignored
- As long as the REDIVIS_API_TOKEN environment variable has been set, you do not need to worry about any additional authentication
