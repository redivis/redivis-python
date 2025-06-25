from .classes.Workflow import Workflow as workflow
from .classes.Dataset import Dataset as dataset
from .classes.User import User as user
from .classes.Organization import Organization as organization
from .classes.Query import Query as query
from .classes.Table import Table as table
from .classes.Notebook import Notebook as notebook
from .classes.Transform import Transform as transform
from .classes.File import File as file
from .common.api_request import make_request as make_api_request
import warnings

from ._version import __version__


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
        from .classes.Notebook import Notebook

        return Notebook(os.getenv("REDIVIS_DEFAULT_NOTEBOOK"))

    return None


def current_workflow():
    import os

    if os.getenv("REDIVIS_DEFAULT_WORKFLOW") is not None:
        return Workflow(os.getenv("REDIVIS_DEFAULT_WORKFLOW"))

    return None


import pkg_resources  # part of setuptools
