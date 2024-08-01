"""Enable enable_third_party_application
Rotate Secret rotate_third_party_application_secret.

"""

import pytest
import requests

from foundry_dev_tools.clients.multipass import DEFAULT_TOKEN_LIFETIME_IN_SECONDS
from foundry_dev_tools.errors.meta import FoundryAPIError
from foundry_dev_tools.foundry_api_client import FoundryRestClient
from tests.integration.conftest import TEST_SINGLETON


def test_crud(mocker):
    try:
        _test_crud_inner(mocker)
    except FoundryAPIError as err:
        if err.response.status_code == requests.codes.forbidden:
            pytest.skip(
                "To test integration for multipass tpa, you need permissions to manage third party applications!",
            )
        else:
            raise


def _test_crud_inner(mocker):
    client = FoundryRestClient()
    user_info = client.get_user_info()
    organization_rid = user_info["attributes"]["multipass:organization-rid"][0]
    app = client.create_third_party_application(
        client_type="CONFIDENTIAL",
        display_name="foundry_dev_tools_integration_test",
        description=None,
        grant_types=["REFRESH_TOKEN", "AUTHORIZATION_CODE"],
        redirect_uris=None,
        logo_uri=None,
        organization_rid=organization_rid,
        allowed_organization_rids=None,
    )
    assert "clientSecret" in app
    assert app["clientSecret"] is not None

    client_id = app["clientId"]

    updated_app = client.update_third_party_application(
        client_id=client_id,
        client_type="CONFIDENTIAL",
        display_name="foundry_dev_tools_integration_test",
        description="Please contact M276304 for questions",
        grant_types=["CLIENT_CREDENTIALS", "REFRESH_TOKEN"],
        redirect_uris=None,
        logo_uri=None,
        organization_rid=organization_rid,
        allowed_organization_rids=None,
    )
    assert updated_app["clientSecret"] is None

    updated_app2 = client.rotate_third_party_application_secret(
        client_id=client_id,
    )
    assert "clientSecret" in updated_app2
    assert updated_app2["clientSecret"] is not None

    client_secret = updated_app2["clientSecret"]

    enabled_app = client.enable_third_party_application(
        client_id=client_id,
        operations=[],
        resources=[],
        require_consent=False,
    )
    assert enabled_app["installation"]["resources"] == []
    assert enabled_app["installation"]["operations"] == []
    assert enabled_app["installation"]["requireConsent"] is True

    enabled_app = client.enable_third_party_application(
        client_id=client_id,
        operations=["api:read-data"],
        resources=[],
        require_consent=True,
    )
    assert enabled_app["installation"]["resources"] == []
    assert enabled_app["installation"]["operations"] == ["api:read-data"]
    assert enabled_app["installation"]["requireConsent"] is True

    tpa_client = FoundryRestClient(
        config={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
            "scopes": ["api:read-data"],
            "jwt": None,
        },
    )

    tpa_user_info = tpa_client.get_user_info()
    assert tpa_user_info["username"] == client_id
    assert tpa_user_info["attributes"]["multipass:realm"][0] == "oauth2-client-realm"

    tpa_client_no_scopes = FoundryRestClient(
        config={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
            "scopes": None,
            "jwt": None,
        },
    )

    tpa_user_info = tpa_client_no_scopes.get_user_info()
    assert tpa_user_info["username"] == client_id
    assert tpa_user_info["attributes"]["multipass:realm"][0] == "oauth2-client-realm"

    client.delete_third_party_application(client_id=client_id)


def test_token_endpoints():
    try:
        _test_token_endpoints_inner()
    except FoundryAPIError as err:
        if err.response.status_code == requests.codes.forbidden:
            pytest.skip("To test integration for multipass token endpoints, you need permissions to manage tokens!")
        else:
            raise


def _test_token_endpoints_inner():
    # Create a new token
    name = "test-token"
    description = "test-description"

    response = TEST_SINGLETON.ctx.multipass.api_create_token(name, description, DEFAULT_TOKEN_LIFETIME_IN_SECONDS)

    assert response.status_code == 200

    token_info = response.json()["tokenInfo"]
    user_info = TEST_SINGLETON.ctx.multipass.get_user_info()

    assert token_info["userId"] == user_info["id"]
    assert token_info["expires_in"] == DEFAULT_TOKEN_LIFETIME_IN_SECONDS - 1
    assert token_info["name"] == name
    assert token_info["description"] == description
    assert token_info["state"] == "ENABLED"

    token_id = token_info["tokenId"]
    token_expires_in = token_info["expires_in"]

    # Get tokens and check whether it contains the newly generated token
    tokens = list(TEST_SINGLETON.ctx.multipass.get_tokens("USER_GENERATED", limit=1))

    token = next((t for t in tokens if t["tokenId"] == token_id), None)

    assert token is not None
    assert token["tokenId"] == token_id
    assert token["userId"] == user_info["id"]
    assert token["name"] == name
    assert token["description"] == description
    assert token["expires_in"] <= token_expires_in

    # Revoke the token created previously
    response = TEST_SINGLETON.ctx.multipass.api_revoke_token(token_id)

    is_revoked = response.json()

    assert response.status_code == 200
    assert is_revoked
