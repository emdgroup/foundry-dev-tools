"""Enable enable_third_party_application
Rotate Secret rotate_third_party_application_secret.

"""

import pytest
import requests

from foundry_dev_tools.errors.meta import FoundryAPIError
from foundry_dev_tools.foundry_api_client import FoundryRestClient


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
    assert enabled_app["installation"]["requireConsent"] is False

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
    # Delete JWT that comes from local config file
    if "jwt" in tpa_client._config:
        del tpa_client._config["jwt"]

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
