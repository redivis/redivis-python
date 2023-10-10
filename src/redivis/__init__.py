from .classes.User import User as user
from .classes.Organization import Organization as organization
from .classes.Query import Query as query
from .classes.Table import Table as table
from .classes.File import File as file

from ._version import __version__


def current_notebook():
    import os
    if os.getenv("REDIVIS_NOTEBOOK_JOB_ID") is not None:
        from .classes.Notebook import Notebook
        return Notebook(os.getenv("REDIVIS_NOTEBOOK_JOB_ID"))

import pkg_resources  # part of setuptools
