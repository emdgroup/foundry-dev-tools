"""Tests for foundry-platform-sdk auth integration."""

from __future__ import annotations

from foundry_dev_tools.public_sdk.auth import FoundryDevToolsAuth, FoundryDevToolsToken


def test_foundry_dev_tools_token_access_token():
    """Verify FoundryDevToolsToken.access_token returns the correct token."""
    test_token = "my-test-token-123"  # noqa: S105

    token = FoundryDevToolsToken(test_token)

    assert token.access_token == test_token


def test_foundry_dev_tools_auth_get_token_bridges_context_token(test_context_mock):
    """Verify FoundryDevToolsAuth.get_token() retrieves token from FoundryContext."""
    expected_token = "context-token-456"  # noqa: S105
    test_context_mock.token_provider._jwt = expected_token

    auth = FoundryDevToolsAuth(test_context_mock)
    token = auth.get_token()

    assert isinstance(token, FoundryDevToolsToken)
    assert token.access_token == expected_token


def test_public_client_v2_uses_foundry_dev_tools_auth(test_context_mock):
    """Verify FoundryContext.public_client_v2 creates FoundryClient with FoundryDevToolsAuth."""
    from foundry_sdk.v2 import FoundryClient

    client = test_context_mock.public_client_v2

    assert isinstance(client, FoundryClient)

    assert test_context_mock.host.domain is not None
