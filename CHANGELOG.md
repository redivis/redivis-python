# Changelog

Each release's section here becomes its notes on [GitHub](https://github.com/redivis/redivis-python/releases);
see [CONTRIBUTING.md](CONTRIBUTING.md#to-upload-to-pypi). Notes for releases before this file was added are on GitHub.

## Unreleased

## 0.22.0

****This release adds `redivis.config()` for library-wide settings, keeps each environment's login separate, and
fixes many bugs in uploads, downloads, and authentication****

### What's new

- New `redivis.config()` sets library-wide defaults, and returns the current configuration. Each setting falls back to
  its `REDIVIS_*` environment variable (e.g., `REDIVIS_DEFAULT_WORKFLOW`) when it isn't set.
  ```python
  redivis.config(default_workflow="username.workflow_name", progress=False)
  table = redivis.table("my_table")  # A table in username.workflow_name
  ```
  The settings are `default_workflow`, `default_dataset`, `default_user`, `default_organization`, `api_endpoint`,
  `config_dir`, `tmp_dir`, `max_parallelization`, and `progress`. Settings are now read when they're used, rather than
  when `redivis` is imported, so changing an environment variable after importing the package takes effect.
- Credentials are cached separately for each environment, so logging in to another one (via `api_endpoint`) no longer
  replaces your redivis.com login. Those for redivis.com stay in `~/.redivis/python_credentials`, shared with earlier
  versions of the client.
- The directory where credentials are cached can be changed with `config_dir` (or `REDIVIS_CONFIG_DIR`), e.g., where
  the home directory isn't writable.

### Improvements and fixes

Authentication

- A temporary server error while refreshing your login no longer logs you out; you're only asked to log in again if
  the login was revoked or has expired.
- Cached credentials are written in a single step, and are only readable by you, so another process using the client
  at the same time (e.g., a second notebook kernel) never reads a partially written file.

Uploads

- Fixed `table.add_files()` failing when given bytes, and uploads of bytes now show a progress bar.
- `upload.create()` checks for an upload with the same name before uploading the data, rather than after, and cleans
  up the files it creates. `remove_on_fail=True` now works without `raise_on_fail=True`.
- Interrupted uploads retry correctly: resumable uploads restart when their session has expired, and other uploads
  resend their data from the start.
- Temporary uploads no longer send your Redivis credentials to the storage service, or ask you to log in.
- `upload.get()` returns the upload, and `upload.list_variables()` lists every variable by default.
- Uploading a pandas DataFrame or Arrow table no longer imports polars, dask, and geopandas.
- Fixed uploading a dask DataFrame with recent versions of dask, which failed for any DataFrame with a column of
  Python objects (e.g., strings or dates).

Downloads

- Downloads are written to a temporary file, which replaces the destination only once it's complete and its MD5 hash
  has been verified, so an interrupted download never leaves a partial file in its place.
- A download that can't be resumed starts over, rather than failing, and downloads no longer require the server to
  report their size.
- Parallel downloads log in again (once) when their credentials are rejected.
- `file.read()` handles `end_byte=0`, and `file.stream()` returns bytes again.

Reading tables and queries

- Fixed `to_polars_lazyframe()` with polars 2, which removed an argument the client passed to it.
- Tables whose size isn't known yet can be read.
- Read streams raise an error when given `variables` or `max_results`, rather than ignoring them.
- Variables, including geography variables, are matched case-insensitively when reading.
- Requests that only read data are retried after connection failures and timeouts.
- Reading a query's results is faster for queries that finish quickly: the API returns their rows in the same response
  that runs the query, rather than the client waiting for the query and then reading its rows separately. Reads of
  large results, and other uses of a query (e.g., `query.get()` or `query.download()`), are unchanged.
- A query used from several threads at once is run only once.

Datasets, workflows, and other resources

- References with a dotted version tag, like `"owner.dataset:v1.0"`, are parsed correctly.
- `REDIVIS_DEFAULT_WORKFLOW` and `REDIVIS_DEFAULT_DATASET` may be owned by an organization, and are validated.
- Fixed `version.dataset()`, `dataset.version()` after `dataset.get()`, and `previous_version()` / `next_version()`
  for the first and last versions.
- `redivis.workflow(name, organization=...)` uses the organization, and names with special characters work in member,
  variable, secret, and parameter references.
- `datasource.update()` works for a datasource without a source dataset, keeps its existing sample, and only updates
  what it's given.
- `transform.run()` and `notebook.run()` wait for the run they started, and raise an error if it's cancelled.
- A transform created without a name is given one, rather than being named "None".
- `version.delete()`, `version.undelete()`, and `transform.cancel()` return the object they were called on.
- Removed a duplicate `DataSource.py` module, which collided with `Datasource.py` on case-insensitive filesystems
  (e.g., macOS and Windows).

Installation

- Requires Python 3.9 or later. On earlier versions, pip installs the previous release.
- No longer installs folium and mapclassify, which geopandas only uses for `GeoDataFrame.explore()`. This makes
  installing the client about 135MB smaller; install them yourself to use `explore()`.
- No longer depends on requests.
