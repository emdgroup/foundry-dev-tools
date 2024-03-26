"""Types that are needed for the Configuration classes."""
from __future__ import annotations

from enum import Enum

from foundry_dev_tools.utils.misc import EnumContainsMeta

"""Token is not a different class, it is exactly the same as a str, this is only for code clarity."""
Token = str

DEFAULT_SCHEME = "https"


class Host:
    """Provides the domain and url for the domain and scheme provided."""

    def __init__(self, domain: str, scheme: str | None = None) -> None:
        self.domain = domain
        self.scheme = scheme or DEFAULT_SCHEME
        self.url = (self.scheme + "://" + self.domain).rstrip("/")

    def __repr__(self) -> str:
        return "<" + self.__class__.__name__ + "(" + self.url + ")>"

    def __eq__(self, o: Host | object):
        if isinstance(o, Host):
            return o.domain == self.domain and o.scheme == self.scheme
        return object.__eq__(self, o)


class FoundryOAuthGrantType(str, Enum, metaclass=EnumContainsMeta):
    """The available grant types for the Foundry OAuth API."""

    client_credentials = "client_credentials"
    authorization_code = "authorization_code"
