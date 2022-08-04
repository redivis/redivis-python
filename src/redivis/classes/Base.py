import json


class Base:
    def __getitem__(self, key):
        return (
            self.properties[key] if hasattr(self, 'properties') and self.properties is not None and key in self.properties else None
        )

    def __str__(self):
        properties = self.properties if hasattr(self, 'properties') and self.properties is not None else {}
        return json.dumps(properties, indent=2)

    def __repr__(self) -> str:
        field_strings = []
        for key, field in vars(self).items():
            field_strings.append(f'{key}={field!r}')

        return f"<{self.__class__.__name__}({','.join(field_strings)})>"