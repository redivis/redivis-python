import json


class Variable:
    def __init__(self, properties):
        self.properties = properties
        self.uri = self.properties.uri

    def __getitem__(self, key):
        return self.properties[key]

    def __str__(self):
        return json.dumps(self.properties, indent=2)
