from __future__ import annotations

import os
import warnings
from typing import TYPE_CHECKING
from unittest import mock

import pytest
import requests_mock

from foundry_dev_tools import Config, FoundryContext
from foundry_dev_tools.errors.config import FoundryConfigError
from foundry_dev_tools.errors.multipass import ClientAuthenticationFailedError
from tests.unit.mocks import TEST_DOMAIN, FoundryMockContext, MockOAuthTokenProvider

if TYPE_CHECKING:
    from pathlib import Path


def test_app_service(mock_config_location: dict[Path, None]):
    with (  # noqa: PT012
        mock.patch.dict(os.environ, {"APP_SERVICE_TS": "1", "FDT_CREDENTIALS__DOMAIN": TEST_DOMAIN}, clear=True),
        pytest.raises(
            FoundryConfigError,
            match="Could not get Foundry token from flask/dash/streamlit headers.",
        ),
        warnings.catch_warnings(),
    ):
        warnings.simplefilter("error")  # raise errors on warnings
        _ = FoundryContext()


def test_client_credentials_error_handling(foundry_token):
    ctx = FoundryMockContext(
        config=Config(),
        token_provider=MockOAuthTokenProvider(
            client_id="test_client_id",
            client_secret="test",  # noqa: S106
            grant_type="client_credentials",
        ),
    )
    with requests_mock.Mocker() as m:
        m.post(
            f"{ctx.token_provider.host.url}/multipass/api/oauth2/token",
            json={"error": "invalid_client", "error_description": "Client authentication failed"},
            status_code=401,
        )
        m.get(
            f"{ctx.token_provider.host.url}/multipass/api/me",
            json={"id": "id", "attributes": "a", "username": "u"},
        )
        with pytest.raises(ClientAuthenticationFailedError) as catched:
            ctx.get_user_info()
    assert catched.value.client_id == "test_client_id"
