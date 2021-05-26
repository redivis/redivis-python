from .Dataset import Dataset
from .Project import Project


class User:
    def __init__(self, name):
        self.name = name

    def dataset(self, name, *, version="current"):
        return Dataset(name, user=self, version=version)

    def project(self, name):
        return Project(name, user=self)
