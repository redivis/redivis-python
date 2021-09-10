import os

try:
    import pkg_resources

    pkg_resources.declare_namespace(__name__)
except ImportError:
    import pkgutil

    __path__ = pkgutil.extend_path(__path__, __name__)

from .classes.Query import Query as _Query
from .classes.User import User as _User
from .classes.Organization import Organization as _Organization


def organization(name):
    return _Organization(name)


def user(name):
    return _User(name)


def query(query):
    return _Query(query)


def table(name):
    if os.getenv("REDIVIS_DEFAULT_PROJECT") is not None:
        return (
            _User(os.getenv("REDIVIS_DEFAULT_PROJECT").split(".")[0])
            .project(os.getenv("REDIVIS_DEFAULT_PROJECT").split(".")[1])
            .table(name)
        )
    elif os.getenv("REDIVIS_DEFAULT_DATASET") is not None:
        return (
            _User(os.getenv("REDIVIS_DEFAULT_DATASET").split(".")[0])
            .dataset(os.getenv("REDIVIS_DEFAULT_DATASET").split(".")[1])
            .table(name)
        )
    else:
        raise Exception(
            "Cannot reference an unqualified table if the neither the REDIVIS_DEFAULT_PROJECT or REDIVIS_DEFAULT_DATASET environment variables are set."
        )


__all__ = ["organization", "user", "query"]
