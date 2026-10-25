from .Base import Base
from ..common.api_request import make_request
from urllib.parse import quote as quote_uri


class Secret(Base):
    def __init__(self, name, *, user=None, organization=None):
        self.user = user
        self.organization = organization
        self.name = name

        base_path = user.uri if user else organization.uri

        self.uri = f"{base_path}/secrets/{quote_uri(name, '')}"
        self.properties = None

    def __repr__(self):
        return f"<Secret {self.user.name if self.user else self.organization.name}.{self.name}>"

    def get_value(self):
        secret = make_request(path=self.uri)
        return secret["value"]
