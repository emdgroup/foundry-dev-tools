from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from foundry_dev_tools.config.token_provider import JWTTokenProvider, OAuthTokenProvider
from foundry_dev_tools.utils.compat import get_v1_environment_variables, v1_to_v2_config

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


def test_v1_to_v2_config(mocker: MockerFixture):
    get_config_dict = mocker.patch("foundry_dev_tools.utils.compat.get_config_dict")
    get_config_dict.return_value = {}
    tp, v2c = v1_to_v2_config({"jwt": "123", "foundry_url": "https://test"})
    assert isinstance(tp, JWTTokenProvider)
    assert tp.token == "123"  # noqa: S105
    assert tp.host.url == "https://test"
    get_config_dict.return_value = {
        "credentials": {
            "domain": "test",
            "oauth": {"client_id": "cid", "client_secret": "csec"},
        },
    }
    tp, v2c = v1_to_v2_config({"jwt": "123"})
    assert isinstance(tp, JWTTokenProvider)
    assert tp.token == "123"  # noqa: S105
    assert tp.host.url == "https://test"
    get_config_dict.return_value = {
        "credentials": {
            "domain": "test",
            "oauth": {"client_id": "cid2", "client_secret": "csec2"},
        },
    }

    tp, v2c = v1_to_v2_config({"foundry_url": "https://test2"})
    assert isinstance(tp, OAuthTokenProvider)
    assert tp._client_id == "cid2"
    assert tp._client_secret == "csec2"  # noqa: S105
    assert tp.host.url == "https://test2"
    assert tp.grant_type == "authorization_code"
    get_config_dict.return_value = {
        "credentials": {
            "domain": "test",
            "oauth": {"client_id": "cid3", "client_secret": "csec3"},
        },
    }

    tp, v2c = v1_to_v2_config({"foundry_url": "https://test2", "grant_type": "client_credentials"})
    assert isinstance(tp, OAuthTokenProvider)
    assert tp._client_id == "cid3"
    assert tp._client_secret == "csec3"  # noqa: S105
    assert tp.host.url == "https://test2"
    assert tp.grant_type == "client_credentials"
    get_config_dict.return_value = {
        "credentials": {
            "domain": "test",
            "jwt": "jwt3",
        },
    }

    tp, v2c = v1_to_v2_config({"client_id": "cid5", "client_secret": "csec5"})
    assert isinstance(tp, OAuthTokenProvider)
    assert tp._client_id == "cid5"
    assert tp._client_secret == "csec5"  # noqa: S105
    assert tp.host.url == "https://test"
    assert tp.grant_type == "authorization_code"


def test_get_v1_environment_variables(mocker: MockerFixture):
    # Set up mock environment variables
    mocker.patch.dict(
        os.environ,
        {
            "FOUNDRY_DEV_TOOLS_JWT": "jwt",
            "FOUNDRY_DEV_TOOLS_FOUNDRY_URL": "https://example.com",
        },
    )

    with pytest.warns(
        DeprecationWarning,
        match="The v1 environment variables are deprecated, please use the v2 environment variables instead",
    ):
        result = get_v1_environment_variables()

    # Check the result
    assert result == {
        "credentials": {
            "jwt": "jwt",
            "domain": "example.com",
        },
    }
