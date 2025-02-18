![Redivis Logo](https://github.com/redivis/redivis-python/raw/main/assets/logo_small.png)

# redivis-python

[Redivis](https://redivis.com) client library for python!

Connect data on Redivis to the Python scientific stack, upload new data to Redivis, and script your data management
tasks!

## Getting started

The easiest way to get started is
to [create a new python notebook on Redivis](https://docs.redivis.com/reference/workflows/notebooks/python-notebooks).
This package, alongside other common data science python packages, are all preinstalled.

You can also install the latest version of this package in any other python environment:

```shell
pip install redivis
```

And then you're ready to go!

```py
import redivis

iris_table = redivis.organization("Demo").dataset("Iris species").table("Iris")
df = iris_table.to_pandas_dataframe()
```

## Documentation

**[View the comprehensive documentation here >](https://apidocs.redivis.com/client-libraries/redivis-python)**

This package contains a number of methods for processing data on Redivis and interfacing with the API through python.
Consult
the documentation for a detailed list of all methods and accompanying examples.

## Issue reporting

Please report any issues or feature requests in the [issue tracker](https://github.com/redivis/redivis-python/issues).

## Contributing

Please report any issues or feature requests in the [issue tracker](https://github.com/redivis/redivis-python/issues)
— your feedback is much appreciated!
