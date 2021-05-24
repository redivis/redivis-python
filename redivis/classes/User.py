from .Dataset import Dataset


class User:
    def __init__(self, name):
        self.name = name

    def dataset(self, name, *, version="current"):
        return Dataset(name, user=self, version=version)
