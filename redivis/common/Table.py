from .methods import list_uploads, list_variables


class Table:
    def __init__(self, identifier):
        self.identifier = identifier

    def __getitem__(self, key):
        return self.properties[key]

    def list_uploads(self):
        return list_uploads(table=self.identifer)

    def list_variables(self):
        return list_variables(table=self.identifier)

    def update(self, *, name, merge_strategy):
        return

    def upload_file(self, *, name, file_descriptor, type):
        return

    def download(self):
        return

    def delete(self):
        return


class Upload:
    def __init__(self, identifier):
        self.identifier = identifier
