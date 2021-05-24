from .Dataset import Dataset


class Organization:
    def __init__(self, name):
        self.name = name

    def dataset(self, name, *, version="current"):
        return Dataset(name, organization=self, version=version)
