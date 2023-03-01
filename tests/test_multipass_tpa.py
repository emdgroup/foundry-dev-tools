"""
Enable enable_third_party_application
Rotate Secret rotate_third_party_application_secret

"""
import json

import pytest
from requests.exceptions import HTTPError
from requests_mock.adapter import ANY
from requests_mock.mocker import Mocker

from foundry_dev_tools import FoundryRestClient

USER_INFO_RESPONSE = {
    "id": "3c8fbda5-686e-4fcb-ad52-d95e4281d99f",
    "username": "M12345@eu.orggroup.com",
    "attributes": {
        "multipass:organization": ["Org1"],
        "multipass:email:primary": ["firstname.lastname@orggroup.com"],
        "multipass:given-name": ["FirstName"],
        "multipass:organization-rid": [
            "ri.multipass..organization.fakefe28-4333-474c-a268-3c3cc5caedc2"
        ],
        "multipass:family-name": ["LastName"],
        "multipass:upn": ["M12345@eu.orggroup.com"],
        "multipass:realm": ["org1"],
        "multipass:realm-name": ["Org1"],
    },
}

CREATE_RESPONSE = {
    "clientId": "20b49e8a40cc1257bb00e69eab17074d",
    "clientSecret": "5de6361c5120a3fc2a247cd13f03bcc0",
    "clientType": "CONFIDENTIAL",
    "organizationRid": "ri.multipass..organization.fakefe28-4333-474c-a268-3c3cc5caedc2",
    "displayName": "foundry_dev_tools_integration_test",
    "description": None,
    "logoUri": None,
    "grantTypes": ["AUTHORIZATION_CODE", "REFRESH_TOKEN"],
    "redirectUris": [],
    "allowedOrganizationRids": [],
}
UPDATE_RESPONSE = {
    "clientId": "20b49e8a40cc1257bb00e69eab17074d",
    "clientSecret": None,
    "clientType": "CONFIDENTIAL",
    "organizationRid": "ri.multipass..organization.fakefe28-4333-474c-a268-3c3cc5caedc2",
    "displayName": "foundry_dev_tools_integration_test",
    "description": "Please contact M276304 for questions",
    "logoUri": None,
    "grantTypes": ["CLIENT_CREDENTIALS", "REFRESH_TOKEN"],
    "redirectUris": [],
    "allowedOrganizationRids": [],
}

ROTATE_RESPONSE = {
    "clientId": "20b49e8a40cc1257bb00e69eab17074d",
    "clientSecret": "5bc7d0eff2a8cc0e4e3ce239cabca123",
    "clientType": "CONFIDENTIAL",
    "organizationRid": "ri.multipass..organization.fakefe28-4333-474c-a268-3c3cc5caedc2",
    "displayName": "foundry_dev_tools_integration_test",
    "description": None,
    "logoUri": None,
    "grantTypes": ["CLIENT_CREDENTIALS", "REFRESH_TOKEN"],
    "redirectUris": [],
    "allowedOrganizationRids": [],
}
ENABLE_RESPONSE_1 = {
    "client": {
        "clientId": "20b49e8a40cc1257bb00e69eab17074d",
        "organizationRid": "ri.multipass..organization.fakefe28-4333-474c-a268-3c3cc5caedc2",
        "displayName": "foundry_dev_tools_integration_test",
        "description": None,
        "logoUri": None,
    },
    "installation": {"resources": [], "operations": [], "markingIds": None},
}

ENABLE_RESPONSE_2 = {
    "client": {
        "clientId": "20b49e8a40cc1257bb00e69eab17074d",
        "organizationRid": "ri.multipass..organization.fakefe28-4333-474c-a268-3c3cc5caedc2",
        "displayName": "foundry_dev_tools_integration_test",
        "description": None,
        "logoUri": None,
    },
    "installation": {
        "resources": [],
        "operations": ["api:read-data"],
        "markingIds": None,
    },
}

OAUTH2_TOKEN_RESPONSE = {
    "access_token": "eyJwbG50...",
    "refresh_token": None,
    "scope": None,
    "expires_in": 3600,
    "token_type": "bearer",
}

TPA_USER_INFO_RESPONSE = {
    "id": "multipass-id",
    "username": "20b49e8a40cc1257bb00e69eab17074d",
    "attributes": {
        "multipass:organization-rid": [
            "ri.multipass..organization.fakefe28-4333-474c-a268-3c3cc5caedc2"
        ],
        "multipass:organization": ["Org1"],
        "multipass:given-name": ["foundry_dev_tools_integration_test"],
        "multipass:realm": ["oauth2-client-realm"],
    },
}


def test_crud(is_integration_test, requests_mock: Mocker, mocker):
    if not is_integration_test:
        # UserInfo
        requests_mock.get(
            ANY,
            [
                {"text": json.dumps(USER_INFO_RESPONSE), "status_code": 200},
                {"text": json.dumps(TPA_USER_INFO_RESPONSE), "status_code": 200},
            ],
        )
        requests_mock.post(
            ANY,
            [
                # Create
                {"text": json.dumps(CREATE_RESPONSE), "status_code": 200},
                # Oauth2 Token
                {"text": json.dumps(OAUTH2_TOKEN_RESPONSE), "status_code": 200},
            ],
        )
        requests_mock.put(
            ANY,
            [
                {"text": json.dumps(UPDATE_RESPONSE), "status_code": 200},
                {"text": json.dumps(ROTATE_RESPONSE), "status_code": 200},
                {"text": json.dumps(ENABLE_RESPONSE_1), "status_code": 200},
                {"text": json.dumps(ENABLE_RESPONSE_2), "status_code": 200},
            ],
        )
        requests_mock.delete(
            ANY,
            [
                {"text": None, "status_code": 200},
            ],
        )
        _test_crud_inner(mocker)


@pytest.mark.integration
def test_crud_integration(mocker):
    try:
        _test_crud_inner(mocker)
    except HTTPError as err:
        if err.response.status_code == 403:
            pytest.skip(
                "To test integration for multipass tpa, you need permissions to manage third party applications!"
            )
        else:
            raise err


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
        client_id=client_id, operations=[], resources=[]
    )
    assert enabled_app["installation"]["resources"] == []
    assert enabled_app["installation"]["operations"] == []

    enabled_app = client.enable_third_party_application(
        client_id=client_id, operations=["api:read-data"], resources=[]
    )
    assert enabled_app["installation"]["resources"] == []
    assert enabled_app["installation"]["operations"] == ["api:read-data"]

    tpa_client = FoundryRestClient(
        config={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
            "scopes": ["api:read-data"],
            "jwt": None,
        }
    )
    # Delete JWT that comes from local config file
    if "jwt" in tpa_client._config:
        del tpa_client._config["jwt"]

    tpa_user_info = tpa_client.get_user_info()
    assert tpa_user_info["username"] == client_id
    assert tpa_user_info["attributes"]["multipass:realm"][0] == "oauth2-client-realm"

    client.delete_third_party_application(client_id=client_id)
