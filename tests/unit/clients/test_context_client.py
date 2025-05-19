import os
import time
from pathlib import Path
from unittest import mock

import pytest
import requests
from requests_mock import ANY

from foundry_dev_tools import Config
from foundry_dev_tools.__about__ import __version__
from foundry_dev_tools.clients.context_client import DEFAULT_TIMEOUT, ContextHTTPClient
from tests.unit.mocks import FoundryMockContext, MockOAuthTokenProvider, MockTokenProvider


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


def test_requests_ca_bundle(request, tmp_path_factory):
    cert_dir = tmp_path_factory.mktemp(f"foundry_dev_tools_test__{request.node.name}").absolute()
    with Path.open(cert_dir / "ca-bundle.pem", "w") as f:
        f.write("test")
    client1 = FoundryMockContext(
        Config(
            requests_ca_bundle=os.fspath(cert_dir / "ca-bundle.pem"),
        ),
        MockTokenProvider(jwt=request.node.name + "_token"),
    ).client
    assert client1.verify == os.fspath(cert_dir / "ca-bundle.pem")
    client_with_pathlib = FoundryMockContext(
        Config(
            requests_ca_bundle=cert_dir / "ca-bundle.pem",
        ),
        MockTokenProvider(jwt=request.node.name + "_token"),
    ).client
    assert client_with_pathlib.verify == os.fspath(cert_dir / "ca-bundle.pem")

    client = ContextHTTPClient(debug=False, requests_ca_bundle=None)
    assert client.verify is True

    with pytest.raises(TypeError):
        _ = ContextHTTPClient(debug=False, requests_ca_bundle=False)

    session = requests.Session()
    assert session.verify is True


def test_verify_is_passed(test_context_mock):
    client1 = test_context_mock.client
    assert client1.verify is True
    test_context_mock.mock_adapter.register_uri(ANY, ANY)

    req = test_context_mock.client.request("POST", "http+mock://test_authorization", verify=False).request
    assert req.verify is False
