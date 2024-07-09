from __future__ import annotations

import sys
import time
from typing import TYPE_CHECKING
from unittest import mock

import pytest
import requests_mock
from freezegun import freeze_time

from foundry_dev_tools.config.token_provider import DEFAULT_OAUTH_SCOPES, AppServiceTokenProvider, CachedTokenProvider
from foundry_dev_tools.errors.config import FoundryConfigError, TokenProviderConfigError
from tests.unit.mocks import TEST_HOST, FoundryMockContext, MockOAuthTokenProvider

if TYPE_CHECKING:
    from collections.abc import Generator

    from pytest_mock import MockerFixture

    from foundry_dev_tools.config.config_types import Token


def generate_tokens_with_expiry(expiries: list[int]) -> Generator[tuple[Token, float], None, None]:
    for i in range(len(expiries)):
        yield (str(i), time.time() + (expiries[i] or 0 if expiries and len(expiries) > i else 0))


@mock.patch(
    "foundry_dev_tools.config.token_provider.CachedTokenProvider._request_token",
    side_effect=generate_tokens_with_expiry([0, 5, 1000, 0]),
)
def test_cached_token_provider(foundry_client_id, foundry_client_secret):
    ctp = CachedTokenProvider(TEST_HOST)
    assert ctp.token == str(0)
    assert ctp.token == str(1)
    # expiry of 5 seconds, but clock_skew is 10 seconds, token should be requested
    assert ctp.token == str(2)
    # expiry of 1000 seconds, token shouldn't be requested
    assert ctp.token == str(2)
    # now invalidate cache and get the next token
    ctp.invalidate_cache()
    assert ctp.token == str(3)


def test_oauth_token_provider(mocker: MockerFixture):
    with pytest.raises(TokenProviderConfigError) as e:
        tp = MockOAuthTokenProvider("client_id", grant_type="client_credentials")
    assert e.value.args[0] == "You need to provide a client secret for the client credentials grant type."

    tp = MockOAuthTokenProvider("client_id", grant_type="authorization_code")
    assert tp.scopes == DEFAULT_OAUTH_SCOPES
    tp_ac = MockOAuthTokenProvider("client_id", grant_type="authorization_code", scopes=["extra scope"])
    assert sorted(tp_ac.scopes) == sorted(
        [
            "extra scope",
            *DEFAULT_OAUTH_SCOPES,
        ]
    )
    tp = MockOAuthTokenProvider("client_id", "client_secret", grant_type="client_credentials")
    assert tp.scopes == []
    tp_cc = MockOAuthTokenProvider("client_id", "client_secret", grant_type="client_credentials", scopes=["scope"])
    assert tp_cc.scopes == ["scope"]

    guc_mock = mocker.patch("foundry_dev_tools.config.token_provider.palantir_oauth_client.get_user_credentials")
    guc_mock.return_value.token = "token"  # noqa: S105
    ac_tok = tp_ac.token

    guc_mock.assert_called_once()
    assert ac_tok == "token"

    request_mock = mocker.patch("foundry_dev_tools.config.token_provider.requests.request")
    request_mock.return_value.json = lambda: {"access_token": "token", "expires_in": 0}

    cc_tok = tp_cc.token

    request_mock.assert_called_once()
    assert cc_tok == "token"


def test_foundry_client_credentials_provider(foundry_token, foundry_client_id, foundry_client_secret):
    ctx = FoundryMockContext(
        token_provider=MockOAuthTokenProvider(
            client_id=foundry_client_id,
            client_secret=foundry_client_secret,
            grant_type="client_credentials",
        ),
    )
    with requests_mock.Mocker() as m:
        m.post(
            f"{ctx.token_provider.host.url}/multipass/api/oauth2/token",
            json={"access_token": foundry_token, "expires_in": 0},
        )
        assert ctx.token == foundry_token


def test_app_service_token_provider():
    with (
        mock.patch.dict(sys.modules, {"streamlit.web.server.websocket_headers": None, "flask": None}),
        pytest.raises(
            FoundryConfigError,
            match="Could not get Foundry token from flask/dash/streamlit headers.",
        ),
    ):
        tp = AppServiceTokenProvider(TEST_HOST)

    from argparse import Namespace

    # flask
    with mock.patch.dict(
        sys.modules,
        {
            "streamlit.web.server.websocket_headers": None,
            "flask": Namespace(request=Namespace(headers={"X-Foundry-AccessToken": "bla"})),
        },
    ):
        tp = AppServiceTokenProvider(TEST_HOST)
        assert tp.token == "bla"  # noqa: S105

    # streamlit takes precedence
    with (
        mock.patch.dict(
            sys.modules,
            {
                "streamlit.web.server.websocket_headers": Namespace(
                    _get_websocket_headers=lambda: {"X-Foundry-AccessToken": "bla2"},
                ),
                "flask": Namespace(request=Namespace(headers={"X-Foundry-AccessToken": "bla"})),
            },
        ),
        freeze_time("0s"),
    ):
        tp = AppServiceTokenProvider(TEST_HOST)
        assert tp.token == "bla2"  # noqa: S105:

    with freeze_time("1h"), pytest.raises(FoundryConfigError, match="Token is expired. Please refresh the web page."):
        str(tp.token)
