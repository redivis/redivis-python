# AGENTS.md

The Python client for the [Redivis](https://redivis.com) API, published to PyPI as `redivis`. Many of
its users are researchers who are new to Python, so `pip install redivis` should just work, and errors
should say what to do about them.

[CONTRIBUTING.md](CONTRIBUTING.md) is the guide for people, and covers setup, the test suites, and
releasing in detail. This file adds what's useful to know when changing the code.

## Commands

```sh
python3 -m venv venv && source venv/bin/activate && pip install -e ".[test]"
pytest tests/unit -n auto    # Offline, in seconds. Run before finishing any change
black .                      # Formats the code. Also run before finishing any change
python -m build              # Builds the sdist and wheel into dist/
```

The integration tests (`tests/integration`) need a running Redivis environment and a login for it,
which opens a browser. They take about 15 minutes and create data, so only run them when asked.

## Layout

- `src/redivis/__init__.py`: the public API. `redivis.table`, `redivis.dataset`, etc. are the classes
  themselves, exported under lowercase names, alongside functions like `redivis.config()` and
  `redivis.authenticate()`.
- `src/redivis/classes/`: a class for each kind of API resource (`Dataset`, `Table`, `Workflow`, ...),
  each a subclass of `Base`; those with rows to read (`Table`, `Query`, `Upload`, `ReadStream`) via
  `TabularReader`. A resource's `uri` is its API path, and its `properties` are what the API returned
  for it, which are `None` until `get()` is called.
- `src/redivis/common/`:
  - `api_request.py`: `make_request()` and `make_paginated_request()`, which handle retries,
    authentication, and API errors. All API calls go through these.
  - `auth.py`: OAuth login, and the credentials cached for each environment.
  - `config.py`: the library-wide settings behind `redivis.config()`.
  - `fetch_rows.py`, `TabularReader.py`: reading tables and queries into Arrow, pandas, polars, dask,
    and geopandas.
  - `retryable_upload.py`, `retryable_download.py`: resumable, verified transfers.
  - `mount_directory.py` (FUSE mounts), `fsspec.py` (the `redivis://` filesystem), `exceptions.py`,
    `util.py`.
- `tests/unit/`: tests against an in-process mock of the API, `mock_redivis_api.py`. The `api` fixture
  resets it and returns its `STATE`, for injecting failures and inspecting requests.
- `tests/integration/`: tests against a live API; see CONTRIBUTING.md.

## Conventions

- **Requests**: make HTTP requests with `niquests`, not `requests`, which isn't a dependency.
- **Errors**: raise the exceptions in `redivis.common.exceptions`, not builtins. For example,
  `exceptions.ValueError` is also a builtin `ValueError`, so users can catch either.
- **Settings**: read them when they're used, with `get_config()`, or `resolve_config("progress", progress)`
  for an argument that defaults to a setting. Never read them with `os.getenv()`, or when the package
  is imported. To add a setting, add it to `_settings` in `config.py`, and to the signature and
  docstring of `config()`; its environment variable is `REDIVIS_<NAME>`. The exceptions are
  `REDIVIS_API_TOKEN` and `REDIVIS_DEFAULT_NOTEBOOK`, which aren't settings: Redivis notebooks set
  them, and the client reads them directly to detect that it's running in one.
- **Data libraries**: pyarrow is the core format. Import pandas, polars, dask, and geopandas inside the
  functions that use them, so that `import redivis` stays fast. To check the type of a user's data,
  use `util.is_instance_of(data, "polars", "DataFrame")`, which never imports a library the user
  hasn't already imported.
- **Python versions**: Python 3.9 is the oldest supported version (CI tests 3.9 to 3.14). So no
  `match` statements, no `X | Y` type unions outside of strings, and no `tomllib`.
- **Platforms**: many users are on Windows and macOS. Build paths with `pathlib` or `os.path`, don't
  assume `/` separators or POSIX-only APIs (`os.getuid()`, `fcntl`), and remember that Windows can't
  replace or delete a file that's still open. CI runs the unit tests on Windows and macOS (once each)
  as well as Linux; the mount tests are skipped where libfuse isn't installed.
- **Other clients**: the [JS](https://github.com/redivis/redivis-js) and
  [R](https://github.com/redivis/redivis-r) clients have the same API, e.g., `redivis.config()`
  matches JS's `config()`. When changing public behavior here, mention whether they need the same
  change.
- **Public API**: keep changes backwards compatible. Deprecate rather than remove, using the patterns
  already there (`util.get_warning()`, `exceptions.DeprecationError`).
- **Progress bars**: use `tqdm.auto`, and respect the `progress` argument.
- **Formatting**: all Python code is formatted with black, which CI checks. Run `black .` before
  finishing any change, with the version pinned in `pyproject.toml` (installed by `.[test]`); other
  versions refuse to run.

## Dependencies

Every dependency, including the data libraries, is required (see `pyproject.toml`), so that users
never have to install anything else. Discuss before moving any to an optional extra, or adding a large
one: folium and mapclassify were dropped because they pulled in about 135MB for a method the client
never calls. Give dependencies lower bounds only, without upper caps. The oldest supported Python
version is set by the oldest that dask, geopandas, and mfusepy support.

## Tests

- Prefer a unit test for the client's own logic, adding whatever endpoints it needs to
  `mock_redivis_api.py`. Use an integration test only to check that the client and the API work
  together.
- Unit tests must never touch real credentials. The `api` fixture points `REDIVIS_CONFIG_DIR` at a
  temporary directory; keep it that way in any new fixture.

## Changes, commits, and releases

- Describe every user-facing change under `## Unreleased` in [CHANGELOG.md](CHANGELOG.md), in the
  pull request that makes it, written for users, in the style of the existing notes. Its sections
  become the GitHub release notes.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org) (`feat:`, `fix:`,
  `refactor:`, `test:`, `chore:`), with a body saying what changed and why for anything non-trivial.
- Merging a change to the version in `src/redivis/_version.py` into `main` publishes that version to
  PyPI. So don't change the version, create tags, or create GitHub releases unless asked.
- `.github/workflows/test.yml` runs the unit tests and the black check on pull requests, and `publish.yml` releases on
  pushes to `main`. Don't rename `publish.yml`: PyPI's trusted publishing names it. Check changes to
  the workflows with `actionlint`.
