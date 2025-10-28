"""Tests for foundry-platform-sdk auth integration."""

from __future__ import annotations

import pytest

from foundry_dev_tools._optional import FakeModule
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


def test_public_client_v2_raises_helpful_error_when_sdk_not_installed(test_context_mock):
    """Verify helpful error message when foundry-platform-sdk is not installed."""
    import foundry_dev_tools.config.context as context_module

    orig_foundry_sdk = context_module.foundry_sdk
    context_module.foundry_sdk = FakeModule("foundry-platform-sdk")

    try:
        if "public_client_v2" in test_context_mock.__dict__:
            del test_context_mock.__dict__["public_client_v2"]

        with pytest.raises(ImportError, match="Missing optional dependency 'foundry-platform-sdk'"):
            _ = test_context_mock.public_client_v2
    finally:
        context_module.foundry_sdk = orig_foundry_sdk
