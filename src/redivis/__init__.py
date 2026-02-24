from .classes.Workflow import Workflow as workflow
from .classes.Dataset import Dataset as dataset
from .classes.Datasource import Datasource as datasource
from .classes.User import User as user
from .classes.Organization import Organization as organization
from .classes.Parameter import Parameter as parameter
from .classes.Query import Query as query
from .classes.Table import Table as table
from .classes.Notebook import Notebook as notebook
from .classes.Transform import Transform as transform
from .common import exceptions
from .common.api_request import make_request as make_api_request
import warnings
import sys
import traceback
import os

from ._version import __version__


def file(*args, **kwargs):

    raise exceptions.DeprecationError(
        'Calling redivis.file() is no longer supported. Please use redivis.table("table_reference").file("filename") instead.'
    )


def _warning(message, category, filename, lineno, file=None, line=None):
    print(f"Warning: {message}")


warnings.showwarning = _warning


def authenticate(scope=None, force_reauthentication=False):
    from .common.auth import clear_cached_credentials, get_auth_token

    if force_reauthentication:
        clear_cached_credentials()

    if isinstance(scope, str):
        scope = [scope]

    get_auth_token(scope=scope)


def current_notebook():
    import os

    if os.getenv("REDIVIS_DEFAULT_NOTEBOOK") is not None:
        return notebook(os.getenv("REDIVIS_DEFAULT_NOTEBOOK"))

    return None


def current_user():
    res = make_api_request(method="GET", path="/users/me")
    return user(res["name"], properties=res)


def current_workflow():
    import os

    if os.getenv("REDIVIS_DEFAULT_WORKFLOW") is not None:
        return workflow(os.getenv("REDIVIS_DEFAULT_WORKFLOW"))

    return None


def _install_excepthook():
    _package_dir = os.path.dirname(os.path.abspath(__file__))
    _original_hook = sys.excepthook

    def _format_filtered_tb(exc_type, exc_value, exc_tb):

        entries = traceback.extract_tb(exc_tb)

        # Extract all frames, keep only those outside the redivis package
        # Also exclude ipython noise
        # Directories to filter out of tracebacks
        _filter_dirs = [
            _package_dir,
            os.path.join("IPython", ""),
            os.path.join("ipykernel", ""),
        ]

        user_entries = [
            e
            for e in entries
            if not any(d in os.path.abspath(e.filename) for d in _filter_dirs)
        ]

        lines = []
        lines.append("Traceback (most recent call last):\n")
        if user_entries:
            lines.extend(traceback.format_list(user_entries))
        else:
            # No user frames; show the last frame before filtering so there's some context
            last_entry = entries[-1] if entries else None
            if last_entry:
                lines.extend(traceback.format_list([last_entry]))
            else:
                lines.append("  (full traceback omitted from redivis internals)\n")

        lines.append(f"{exc_type.__name__}: {exc_value}\n")
        return "".join(lines)

    def _custom_excepthook(exc_type, exc_value, exc_tb):
        if isinstance(exc_value, exceptions.RedivisError):
            print(
                _format_filtered_tb(exc_type, exc_value, exc_tb),
                file=sys.stderr,
                end="",
            )
        else:
            _original_hook(exc_type, exc_value, exc_tb)

    sys.excepthook = _custom_excepthook

    try:
        from IPython import get_ipython

        ipython = get_ipython()
        if ipython is not None:

            def _ipython_custom_exc(shell, exc_type, exc_value, exc_tb, tb_offset=None):
                print(
                    _format_filtered_tb(exc_type, exc_value, exc_tb),
                    file=sys.stderr,
                    end="",
                )

            ipython.set_custom_exc((exceptions.RedivisError,), _ipython_custom_exc)
    except ImportError:
        pass


_install_excepthook()
