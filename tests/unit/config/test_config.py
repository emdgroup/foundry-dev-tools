from __future__ import annotations

import os
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from foundry_dev_tools.config import config
from foundry_dev_tools.config.config_types import Host
from foundry_dev_tools.config.token_provider import JWTTokenProvider, OAuthTokenProvider
from foundry_dev_tools.errors.config import (
    FoundryConfigError,
    MissingCredentialsConfigError,
    MissingFoundryHostError,
    TokenProviderConfigError,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_get_config_dict(mock_config_location: dict[Path, None]):
    """Tests that the files are read in the correct order and that the merge happens correctly."""
    path_list = list(mock_config_location)
    site_config = path_list[0]
    user_config = path_list[1]
    site_config.write_text(
        """
[config]
rich_traceback = true

[credentials.token_provider]
config = { client_id = "will_be_overriden_by_site_config", client_secret = "get_config_dict_client_secret" }
""",
    )
    user_config.write_text(
        """
[credentials]
domain = "example.com"
scheme = "mock"
token_provider = {name = "oauth", config = { client_id = "get_config_dict_client_id" }}
""",
    )
    assert {
        "config": {
            "rich_traceback": True,
        },
        "credentials": {
            "domain": "example.com",
            "scheme": "mock",
            "token_provider": {
                "name": "oauth",
                "config": {"client_id": "get_config_dict_client_id", "client_secret": "get_config_dict_client_secret"},
            },
        },
    } == config.get_config_dict()
    with pytest.raises(AttributeError, match="Profile name can't be credentials"):
        config.get_config_dict("credentials")

    with pytest.raises(AttributeError, match="Profile name can't be config"):
        config.get_config_dict("config")

    with mock.patch.dict(os.environ, FDT_CONFIG__RICH_TRACEBACK="false"):
        config.get_config_dict()["config"]["rich_traceback"] = False


def test_parse_credentials_config(mock_config_location):
    with pytest.raises(MissingCredentialsConfigError):
        config.parse_credentials_config({})

    with pytest.raises(MissingCredentialsConfigError):
        config.parse_credentials_config({"credentials": {}})

    with pytest.raises(MissingFoundryHostError):
        config.parse_credentials_config({"credentials": {"token_provider": {"config": {}}}})

    with pytest.raises(TokenProviderConfigError, match="To authenticate with Foundry you need a TokenProvider"):
        config.parse_credentials_config({"credentials": {"domain": "example.com"}})
    with pytest.raises(
        TokenProviderConfigError,
        match="The token provider implementation test_token_provider_does_not_exist does not exist",
    ):
        config.parse_credentials_config(
            {
                "credentials": {
                    "domain": "example.com",
                    "token_provider": {"name": "test_token_provider_does_not_exist"},
                },
            },
        )

    # check_init gets imported in foundry_dev_tools.config.config, we need to mock it there
    with mock.patch("foundry_dev_tools.config.config.check_init") as check_init_mock:
        # return the dict 'kwargs'
        check_init_mock.side_effect = lambda *args, **kwargs: args[2]  # noqa: ARG005
        config.parse_credentials_config(
            {"credentials": {"domain": "example.com", "token_provider": {"config": {"jwt": "test"}}}},
        )
        check_init_mock.assert_called_with(
            JWTTokenProvider,
            "credentials",
            {"host": Host("example.com"), "jwt": "test"},
        )
        config.parse_credentials_config(
            {
                "credentials": {
                    "domain": "example.com",
                    "token_provider": {"name": "oauth", "config": {"client_id": "test"}},
                },
            },
        )
        check_init_mock.assert_called_with(
            OAuthTokenProvider,
            "credentials",
            {"host": Host("example.com"), "client_id": "test"},
        )
    with mock.patch.dict(os.environ, {"APP_SERVICE_TS": "1"}), pytest.raises(
        FoundryConfigError,
        match="Could not get Foundry token from flask/dash/streamlit headers.",
    ):
        config.parse_credentials_config({"credentials": {"domain": "example.com"}})
