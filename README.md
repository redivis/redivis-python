# redipy
Redivis client libraries for python. These libraries are currently in an **alpha** state and may change in backwards incompatible ways.

### authorization
In order to make calls to the Redivis API, you must set the `REDIVIS_API_TOKEN` environment variable:
```
REDIVIS_API_TOKEN=<your_api_token> python script.py
```
In order to generate a new authorization token, navigate to you [workspace/settings](https://redivis.com/workspace/settings) on Redivis and scroll to the API tokens section. Your token should have the `data.data` scope.

### [redivis-bigquery](https://github.com/redivis/redipy/tree/master/bigquery)
Provides an authorization wrapper around the [Google BigQuery client library](https://github.com/googleapis/google-cloud-python/tree/master/bigquery)
