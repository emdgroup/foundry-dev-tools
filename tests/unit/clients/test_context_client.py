import time
from unittest import mock

import requests
from requests_mock import ANY

from foundry_dev_tools.__about__ import __version__
from foundry_dev_tools.clients.context_client import DEFAULT_TIMEOUT
from tests.unit.mocks import MockOAuthTokenProvider


def test_context_http_client(test_context_mock, foundry_client_id):
    # check if args are passed
    with mock.patch("requests.Session.request") as m:
        test_context_mock.client.request("GET", "test_call_args")
        assert m.call_args[0] == ("GET", "test_call_args")
        assert m.call_args[1]["timeout"] == DEFAULT_TIMEOUT
        test_context_mock.client.request(
            "GET",
            "test_kwargs_passed",
            headers={"a": "b"},
        )
        assert m.call_args[0] == ("GET", "test_kwargs_passed")
        assert m.call_args[1]["headers"] == {"a": "b"}

    test_context_mock.mock_adapter.register_uri(ANY, ANY)

    req = test_context_mock.client.request("POST", "http+mock://test_authorization").request
    assert req.headers["Authorization"] == f"Bearer {test_context_mock.token}"
    from requests import __version__ as requests_version

    assert req.headers["User-Agent"] == f"foundry-dev-tools/{__version__}/python-requests/{requests_version}"

    tokens = ["token_from_oauth", "second_token_from_oauth"]

    def side_effects():
        return tokens.pop(0), time.time()

    test_context_mock.token_provider = MockOAuthTokenProvider(
        client_id=foundry_client_id,
        mock_request_token=side_effects,
    )
    for token in tokens:
        req = test_context_mock.client.request("GET", "http+mock://test_oauth_token").request
        assert req.headers["Authorization"] == f"Bearer {token}"


def test_retry_on_connection_error(test_context_mock):
    response_mock = mock.Mock()
    with mock.patch("requests.Session.request") as m:
        m.side_effect = [requests.exceptions.ConnectionError(), response_mock]
        response = test_context_mock.client.request("GET", "test_call_args")
        assert response is response_mock
