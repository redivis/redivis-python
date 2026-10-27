## Contributing

### Development setup

Clone this repository, then create a virtual environment and install the package in
editable mode, along with its test dependencies:

```sh
python3 -m venv venv
source ./venv/bin/activate
pip install -e ".[test]"
```

To try the client against the local development environment:

```sh
REDIVIS_API_ENDPOINT=https://local.host:8443/api/v1 python3
```

### Running the tests

The tests live in two suites:

| Suite               | What it covers                                                                                                     | Needs                                     |
|---------------------|--------------------------------------------------------------------------------------------------------------------|-------------------------------------------|
| `tests/unit`        | The client's internals (retries, auth, reading, uploads, downloads, mounting…), against an in-process mock of the API | Nothing; runs offline in seconds          |
| `tests/integration` | End-to-end behavior against a live Redivis API, by uploading data and verifying what's read back                  | A running environment, and a login for it |

```sh
pytest                             # Everything, with the integration tests run against dev
pytest tests/unit                  # Just the unit tests (or: pytest -m "not integration")
pytest tests/integration           # Just the integration tests
pytest tests/integration/test_uploads.py -k dataframe   # A subset
pytest -n auto                     # In parallel, using pytest-xdist
```

The unit tests take a few seconds. Against dev, the integration tests take about 15 minutes,
or about 11 with `-n 4`, since most of that time goes to waiting for uploads to be processed.

Tests marked `xfail(strict=True)` document known problems with the API; once one is fixed,
the test will "unexpectedly pass" and fail the run, as a reminder to remove the marker.

pytest always imports the package from `src/` (see `pythonpath` in `pyproject.toml`), so the
tests run against your working tree, whichever version of `redivis` is installed.

### Integration tests

#### Choosing an environment

By default, the integration tests run against the dev environment, at
`https://local.host:8443`. Choose another with `--redivis-env` (or the `REDIVIS_TEST_ENV`
environment variable), which takes either the name of an environment or the URL of an API:

```sh
pytest tests/integration --redivis-env prod
pytest tests/integration --redivis-env https://staging.example.com/api/v1
```

The named environments, and their defaults for the options below, are defined by
`ENVIRONMENTS` in [`tests/conftest.py`](tests/conftest.py); add an entry there to name a new
one. If an environment's API can't be reached, its integration tests are skipped (and listed
as such in pytest's summary).

#### Authenticating

The first time you run the integration tests against an environment, the client opens a
browser to log in to it, as it would for any user. The client keeps each environment's
credentials apart, in `~/.redivis/environments/<host>/`, so the test environment's never
touch your credentials for redivis.com. Set `REDIVIS_TEST_CONFIG_DIR` to use another config
directory.

For a non-interactive run (e.g., in CI, where `CI` is set and the tests fail rather than wait
for a login), provide an API token for the environment instead:

```sh
REDIVIS_API_TOKEN=... pytest tests/integration --redivis-env prod
```

#### Test data

A fresh environment (like dev) has no data in it, so the tests upload their own. At the start
of a run, the `sample_dataset` fixture creates a dataset of tables with known contents, which
the reading, query, and file tests verify their results against:

| Fixture       | Contents                                                                    | Uploaded as        |
|---------------|-----------------------------------------------------------------------------|--------------------|
| `tiny_table`  | [`tests/data/tiny.csv`](tests/data/tiny.csv)                                | CSV                |
| `typed_table` | A column of each scalar type, with nulls and awkward strings                | An arrow table     |
| `geo_table`   | A point, a line, and a polygon                                              | GeoJSON            |
| `files_table` | A few files, including nested directories and a 3MB binary file             | Raw files          |

Their expected contents are defined in
[`tests/integration/helpers.py`](tests/integration/helpers.py). Tests that modify data create
their own datasets and tables, with the `new_dataset` and `new_table` fixtures, rather than
touching these.

#### Cleanup

Everything the tests create lives in datasets and workflows named `pytest_<timestamp>_<worker>_…`, owned by
the test owner (below). They're deleted as the tests that created them finish, and the rest at
the end of the run. Pass `--keep-resources` to leave them in place to inspect. Datasets and
workflows left behind by an interrupted run are deleted by a later run once they're 12 hours
old — though only those the API lists, which excludes datasets that were never released.

#### Options

| Option                    | Environment variable        | Purpose                                                                                                                                          |
|---------------------------|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| `--redivis-env`           | `REDIVIS_TEST_ENV`          | The environment to test: a name from `ENVIRONMENTS`, or an API URL. Defaults to `dev`.                                                            |
| `--redivis-owner`         | `REDIVIS_TEST_OWNER`        | The user or organization that owns the datasets the tests create. Defaults to the environment's default (`demo` for dev, whose user accounts are limited to 50 datasets), then the authenticated user. |
| `--redivis-organization`  | `REDIVIS_TEST_ORGANIZATION` | An organization the test user belongs to, for the organization tests. Defaults to the environment's default (`demo` for dev); the tests are skipped without one. |
| `--redivis-workflow`      | `REDIVIS_TEST_WORKFLOW`     | A scratch workflow (`owner.workflow_name`) for the tests that create transforms, parameters, and datasources. Defaults to a workflow created for (and deleted after) the run. |
| `--keep-resources`        |                             | Don't delete the datasets the tests create.                                                                                                      |
| `--run-slow`              |                             | Also run the tests marked `slow`, such as a multi-GB resumable upload.                                                                            |

The tests also unset any `REDIVIS_DEFAULT_*` variables in your environment, so that they run
the same everywhere.

### Writing tests

- Prefer a unit test, against the mock API in
  [`tests/unit/mock_redivis_api.py`](tests/unit/mock_redivis_api.py), for anything about the
  client's own logic: how it builds requests, retries, authenticates, or handles a particular
  response. They're fast and deterministic, and can simulate failures that a live API can't.
- Use an integration test to verify that the client and the API work together: that data
  survives a round trip, that a workflow behaves end to end, and so on. Assert on the data that
  comes back, compared with what was uploaded, rather than just that a call didn't raise.
- Integration tests may run against any environment, and in parallel, so they can't depend on
  existing data or on each other. Create what you need with the fixtures in
  [`tests/integration/conftest.py`](tests/integration/conftest.py), and name anything else you
  create with `live.unique_name()`.
- Rows come back in no particular order, so sort them before comparing (see
  `helpers.sorted_by()`). Pass `progress=False` to keep progress bars out of the output.
- Mark a test `slow` if it takes more than a minute or so.

### To upload to PyPI

First, update the version in `src/redivis/_version.py`.
Also, make sure `twine` is installed.
Then:

```sh
python3 setup.py upload
```
