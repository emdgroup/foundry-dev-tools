"""Tests for the tokenproviders."""
import sys
from unittest import mock

import pytest

from foundry_dev_tools.utils.token_provider import APP_SERVICE_ACCESS_TOKEN_HEADER
from tests.conftest import PatchConfig


def test_token_provider_streamlit(mocker):
    """Tests the streamlit token provider."""
    mocker.patch(
        "foundry_dev_tools.utils.token_provider.AppServiceStreamlitTokenProvider.get_streamlit_request_headers",
        return_value={APP_SERVICE_ACCESS_TOKEN_HEADER: "secret-token"},
    )
    with PatchConfig(config_overwrite={"jwt": None}):
        from foundry_dev_tools.foundry_api_client import FoundryRestClient

        client = FoundryRestClient()
        print(client._config)
        assert client._config["jwt"] == "secret-token"


def test_token_provider_streamlit_114_case_insensitive():
    """Tests the case insenstivity of the headers."""
    mock_get_websocket_headers = mock.MagicMock()
    sys.modules["streamlit.web.server.websocket_headers"] = mock_get_websocket_headers
    mock_get_websocket_headers._get_websocket_headers.return_value = {
        APP_SERVICE_ACCESS_TOKEN_HEADER.lower(): "secret-token2"
    }
    from foundry_dev_tools.foundry_api_client import FoundryRestClient

    with PatchConfig(config_overwrite={"jwt": None}):
        client = FoundryRestClient()
        assert client._config["jwt"] == "secret-token2"

    del sys.modules["streamlit.web.server.websocket_headers"]


def test_token_provider_streamlit_arg_higher_preference(mocker):
    """Tests that a configured jwt takes precedence."""
    mocker.patch(
        "foundry_dev_tools.utils.token_provider.AppServiceStreamlitTokenProvider.get_streamlit_request_headers",
        return_value={APP_SERVICE_ACCESS_TOKEN_HEADER: "secret-token"},
    )
    with PatchConfig(config_overwrite={"jwt": None}):
        from foundry_dev_tools.foundry_api_client import FoundryRestClient

        client = FoundryRestClient({"jwt": "shouldtakePrecedence"})
        assert client._config["jwt"] == "shouldtakePrecedence"


@pytest.mark.no_patch_conf()
def test_token_provider_streamlit_no_cache_on_config_class(mocker):
    """We test that a new instantiation of FoundryRestClient grabs from the Configuration class."""
    mocker.patch(
        "foundry_dev_tools.utils.token_provider.AppServiceStreamlitTokenProvider.get_streamlit_request_headers",
        side_effect=[
            {APP_SERVICE_ACCESS_TOKEN_HEADER: "secret-token-ONE"},
            {APP_SERVICE_ACCESS_TOKEN_HEADER: "secret-token-TWO"},
        ],
    )
    with PatchConfig(config_overwrite={"jwt": None}):
        from foundry_dev_tools.foundry_api_client import FoundryRestClient

        client = FoundryRestClient(
            {"foundry_url": "https://loremipsum.palantirfoundry.com"}
        )
        assert client._config["jwt"] == "secret-token-ONE"

        client2 = FoundryRestClient(
            {"foundry_url": "https://loremipsum.palantirfoundry.com"}
        )

        assert client2._config["jwt"] == "secret-token-TWO"
        assert client._config["jwt"] == "secret-token-ONE"


def test_token_provider_dash(mocker):
    """Tests the flask/dash token provider."""
    mocker.patch(
        "foundry_dev_tools.utils.token_provider.AppServiceDashTokenProvider.get_flask_request_headers",
        return_value={APP_SERVICE_ACCESS_TOKEN_HEADER: "secret-token-dash"},
    )
    with PatchConfig(config_overwrite={"jwt": None}):
        from foundry_dev_tools.foundry_api_client import FoundryRestClient

        client = FoundryRestClient()
        assert client._config["jwt"] == "secret-token-dash"


def test_token_provider_dash_arg_higher_preference(mocker):
    """Tests that a configured jwt takes precedence."""
    mocker.patch(
        "foundry_dev_tools.utils.token_provider.AppServiceDashTokenProvider.get_flask_request_headers",
        return_value={APP_SERVICE_ACCESS_TOKEN_HEADER: "secret-token"},
    )
    with PatchConfig(config_overwrite={"jwt": None}):
        from foundry_dev_tools.foundry_api_client import FoundryRestClient

        client = FoundryRestClient({"jwt": "shouldtakePrecedence"})
        assert client._config["jwt"] == "shouldtakePrecedence"
