from __future__ import annotations

from typing import TYPE_CHECKING

import requests_mock

from foundry_dev_tools.config.config import Config
from foundry_dev_tools.config.config_types import FoundryOAuthGrantType, Host, Token
from foundry_dev_tools.config.context import FoundryContext
from foundry_dev_tools.config.token_provider import JWTTokenProvider, OAuthTokenProvider

"""The domain for unit tests."""
TEST_DOMAIN = "foundry-dev-tools.test"
"""The url scheme, useful for :py:class:`requests_mock.Adapter`"""
TEST_SCHEME = "http+mock"
"""The default host for all tests."""
TEST_HOST = Host(TEST_DOMAIN, TEST_SCHEME)
"""Default token if no token was provided."""
DEFAULT_TOKEN = "default_mock_token"  # noqa: S105
"""The mock adapter for :py:class:`FoundryMockContext`."""
MOCK_ADAPTER = requests_mock.Adapter()
if TYPE_CHECKING:
    from collections.abc import Callable


class MockTokenProvider(JWTTokenProvider):
    """The mock token provider, sets the host to :py:attr:`TEST_HOST`."""

    def __init__(self, jwt: Token):
        self._jwt = jwt
        self.host = TEST_HOST


class MockOAuthTokenProvider(OAuthTokenProvider):
    """The mock OAuth token provider, set the host to :py:attr:`TEST_HOST`"""

    def __init__(
        self,
        client_id: str,
        client_secret: str | None = None,
        grant_type: FoundryOAuthGrantType | None = None,
        scopes: list[str] | str | None = None,
        mock_request_token: Callable[[], tuple[str, float]] | None = None,
    ):
        super().__init__(
            TEST_HOST,
            client_id=client_id,
            client_secret=client_secret,
            grant_type=grant_type,
            scopes=scopes,
        )
        if mock_request_token is not None:
            self._request_token = mock_request_token


class FoundryMockContext(FoundryContext):
    """The foundry mock context uses a mock token provider and mounts a requests mock adapter."""

    mock_adapter: requests_mock.Adapter | None

    def __init__(
        self,
        config: Config | None = None,
        token_provider: MockTokenProvider | None = None,
        profile: str | None = None,
        mock_adapter: requests_mock.Adapter | None = None,
    ):
        super().__init__(
            config=config or Config(),
            token_provider=token_provider or MockTokenProvider(jwt=DEFAULT_TOKEN),
            profile=profile,
        )
        if mock_adapter:
            self.mock_adapter = mock_adapter
        else:
            self.mock_adapter = MOCK_ADAPTER
        self.client.mount("http+mock://", self.mock_adapter)
