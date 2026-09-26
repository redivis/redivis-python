## Contributing

For local development, clone this repository and then run

```sh
source ./venv/bin/activate && python setup.py develop --user
# OR
source ./venv/bin/activate && python3 -m pip install . && REDIVIS_API_ENDPOINT=https://local.host:8443/api/v1 python3 -W ignore
```

You can then run the tests, e.g.:

```
REDIVIS_API_ENDPOINT=https://localhost:8443/api/v1 pytest -s --disable-warnings [-k test_prefix]
```

The tests in `tests/offline` run against an in-process mock of the API, so they need no server or credentials. To run them against the working tree rather than the installed package:

```
PYTHONPATH=src pytest tests/offline
```

#### To upload to PyPi:

First, update the version in setup.py.
Also, make sure `twine` is installed.
Then:

```
python3 setup.py upload
```
