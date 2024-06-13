class Base:
    def __getitem__(self, key):
        return (
            self.properties[key]
            if hasattr(self, "properties")
            and self.properties is not None
            and key in self.properties
            else None
        )

    def __repr__(self) -> str:
        field_strings = []
        for key, field in vars(self).items():
            if (
                key != "properties"
                and key != "scoped_reference"
                and field is not None
                and not isinstance(field, Base)
            ):
                field_strings.append(f"{key}:{field!r}")

        return f"<{self.__class__.__name__} {' '.join(field_strings)}>"
