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


__all__ = ["organization", "user", "query"]
