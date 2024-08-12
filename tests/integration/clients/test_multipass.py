"""Enable enable_third_party_application
Rotate Secret rotate_third_party_application_secret.

"""

import functools
from datetime import datetime, timedelta, timezone

import pytest
import requests
from integration.utils import backoff

from foundry_dev_tools.clients.multipass import (
    DEFAULT_MAX_DURATION_IN_SECONDS,
    DEFAULT_TOKEN_LIFETIME_IN_SECONDS,
)
from foundry_dev_tools.errors.meta import FoundryAPIError
from foundry_dev_tools.foundry_api_client import FoundryRestClient
from foundry_dev_tools.utils import api_types
from tests.integration.conftest import TEST_SINGLETON

TEST_GROUP_ID = "7bf29909-cb64-4353-a192-e5970e443909"

DEV_WORKSPACE_OWNER_GROUP_ID = "e9a4a5e7-fbff-4856-ac6c-c657e97a73c2"
DEV_WORKSPACE_VIEWER_GROUP_ID = "2ba614c8-65bb-4d1f-afa1-323610b755ec"


def handle_forbidden_error(skip_message):
    """Decorator function to handle 403 forbidden error when user lacks permission to skip the test.

    Args:
        skip_message: The message to pass on to :py:meth:`pytest.skip` when skipping a function
    """

    def decorator(test_function):
        @functools.wraps(test_function)
        def wrapper(*args, **kwargs):
            try:
                test_function(*args, **kwargs)
            except FoundryAPIError as err:
                if err.response.status_code == requests.codes.forbidden:
                    pytest.skip(skip_message)
                else:
                    raise

        return wrapper

    return decorator


def _get_group_name(group_id: api_types.GroupId) -> str:
    """Helper function to retrieve the name of the specified group identifier.

    Args:
        group_id: The identifier of the group for which to retrieve the name

    Returns:
        str:
            the name of the group
    """
    return TEST_SINGLETON.ctx.multipass.api_get_group(group_id).json()["name"]


@handle_forbidden_error(
    skip_message="To test integration for multipass tpa, you need permissions to manage third party applications!"
)
def test_crud_inner():
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


@handle_forbidden_error(
    skip_message="To test integration for multipass token endpoints, you need permissions to manage tokens!"
)
def test_token_endpoints():
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


@handle_forbidden_error(
    skip_message=(
        "To test integration for multipass group administration endpoints, you need to be member of group "
        f"`{_get_group_name(DEV_WORKSPACE_OWNER_GROUP_ID)}` ({DEV_WORKSPACE_OWNER_GROUP_ID})!"
    )
)
def test_group_administrations():
    user_id = TEST_SINGLETON.ctx.multipass.get_user_info()["id"]

    sleep_time = 0.2

    # Add user as `manager manager` to test group
    resp = TEST_SINGLETON.ctx.multipass.add_group_manager_managers(TEST_GROUP_ID, {user_id})

    assert resp.status_code == 204

    successful, resp, _ = backoff(
        lambda: TEST_SINGLETON.ctx.multipass.api_get_group_manager_managers(TEST_GROUP_ID),
        lambda r: any(manager_manager["id"] == user_id for manager_manager in r.json()),
        sleep_time,
    )

    assert successful
    assert resp.status_code == 200

    assert any(manager_manager["id"] == user_id for manager_manager in resp.json())

    # Withdraw the role of `manager manager` from user for the test group
    resp = TEST_SINGLETON.ctx.multipass.remove_group_manager_managers(TEST_GROUP_ID, {user_id})

    assert resp.status_code == 204

    successful, resp, _ = backoff(
        lambda: TEST_SINGLETON.ctx.multipass.api_get_group_manager_managers(TEST_GROUP_ID),
        lambda r: not any(manager_manager["id"] == user_id for manager_manager in r.json()),
        sleep_time,
    )

    assert successful
    assert resp.status_code == 200

    assert not any(manager_manager["id"] == user_id for manager_manager in resp.json())

    # Add user as `member manager` to test group
    resp = TEST_SINGLETON.ctx.multipass.add_group_member_managers(TEST_GROUP_ID, {user_id})

    assert resp.status_code == 204

    successful, resp, _ = backoff(
        lambda: TEST_SINGLETON.ctx.multipass.api_get_group_member_managers(TEST_GROUP_ID),
        lambda r: any(member_manager["id"] == user_id for member_manager in r.json()),
        sleep_time,
    )

    assert successful
    assert resp.status_code == 200

    assert any(member_manager["id"] == user_id for member_manager in resp.json())

    # Withdraw the role of `member manager` from user for the test group
    resp = TEST_SINGLETON.ctx.multipass.remove_group_member_managers(TEST_GROUP_ID, {user_id})

    assert resp.status_code == 204

    successful, resp, _ = backoff(
        lambda: TEST_SINGLETON.ctx.multipass.api_get_group_member_managers(TEST_GROUP_ID),
        lambda r: not any(member_manager["id"] == user_id for member_manager in r.json()),
        sleep_time,
    )

    assert successful
    assert resp.status_code == 200

    assert not any(member_manager["id"] == user_id for member_manager in resp.json())


@handle_forbidden_error(
    skip_message=(
        "To test integration for multipass group member administration endpoints, you need to be member of group "
        f"`{_get_group_name(DEV_WORKSPACE_OWNER_GROUP_ID)}` ({DEV_WORKSPACE_OWNER_GROUP_ID})!"
    )
)
def test_group_member_administration():
    user_id = TEST_SINGLETON.ctx.multipass.get_user_info()["id"]

    # Add user as member to test group
    resp = TEST_SINGLETON.ctx.multipass.api_add_group_members({TEST_GROUP_ID}, {user_id})

    assert resp.status_code == 204

    # Additionally make the test group a member of the dev workspace viewer group
    resp = TEST_SINGLETON.ctx.multipass.api_add_group_members({DEV_WORKSPACE_VIEWER_GROUP_ID}, {TEST_GROUP_ID})

    assert resp.status_code == 204

    # When retrieving immediate group members for dev workspace viewer group assert it does not contain the user
    resp = TEST_SINGLETON.ctx.multipass.api_get_immediate_group_members(DEV_WORKSPACE_VIEWER_GROUP_ID)

    assert resp.status_code == 200

    group_members = resp.json()

    assert any(member["id"] == TEST_GROUP_ID for member in group_members)
    assert not any(member["id"] == user_id for member in group_members)

    # Assert that the user has been added as member to test group and is indirect member of dev workspace viewer group
    resp = TEST_SINGLETON.ctx.multipass.api_get_all_group_members({TEST_GROUP_ID, DEV_WORKSPACE_VIEWER_GROUP_ID})

    assert resp.status_code == 200

    group_id_members_mapping = resp.json()["membersByGroupId"]

    assert TEST_GROUP_ID in group_id_members_mapping
    assert any(member["principalId"] == user_id for member in group_id_members_mapping[TEST_GROUP_ID])

    assert DEV_WORKSPACE_VIEWER_GROUP_ID in group_id_members_mapping
    assert any(
        member["principalId"] == TEST_GROUP_ID for member in group_id_members_mapping[DEV_WORKSPACE_VIEWER_GROUP_ID]
    )
    assert any(member["principalId"] == user_id for member in group_id_members_mapping[DEV_WORKSPACE_VIEWER_GROUP_ID])

    # Assert that users of dev workspace viewer group contain at least all known user members of the dev workspace group
    # but none of group members
    resp = TEST_SINGLETON.ctx.multipass.api_get_all_group_users(DEV_WORKSPACE_VIEWER_GROUP_ID)

    assert resp.status_code == 200

    users = [user["id"] for user in resp.json()]
    user_members = [
        member["principalId"]
        for member in group_id_members_mapping[DEV_WORKSPACE_VIEWER_GROUP_ID]
        if member["principalType"] == "USER"
    ]
    group_members = [
        member["principalId"]
        for member in group_id_members_mapping[DEV_WORKSPACE_VIEWER_GROUP_ID]
        if member["principalType"] == "GROUP"
    ]

    assert len(users) >= len(group_members)
    assert all(user in users for user in user_members)
    assert not any(group in users for group in group_members)

    # Remove test group as member from dev workspace viewer group
    resp = TEST_SINGLETON.ctx.multipass.api_remove_group_members(DEV_WORKSPACE_VIEWER_GROUP_ID, {TEST_GROUP_ID})

    assert resp.status_code == 204

    # Verify that test group has been removed from the dev workspace viewer group
    resp = TEST_SINGLETON.ctx.multipass.api_get_immediate_group_members(DEV_WORKSPACE_VIEWER_GROUP_ID)

    assert resp.status_code == 200
    assert not any(member["id"] == TEST_GROUP_ID for member in resp.json())

    # Assert that user is not indirect member of dev workspace viewer group anymore
    resp = TEST_SINGLETON.ctx.multipass.api_get_all_group_members({DEV_WORKSPACE_VIEWER_GROUP_ID})

    assert resp.status_code == 200

    group_id_members_mapping = resp.json()["membersByGroupId"]

    assert DEV_WORKSPACE_VIEWER_GROUP_ID in group_id_members_mapping
    assert not any(
        member["principalId"] == user_id for member in group_id_members_mapping[DEV_WORKSPACE_VIEWER_GROUP_ID]
    )

    # Remove user from test group again
    resp = TEST_SINGLETON.ctx.multipass.api_remove_group_members(TEST_GROUP_ID, {user_id})

    assert resp.status_code == 204

    # Verify that user has been removed from test group
    resp = TEST_SINGLETON.ctx.multipass.api_get_immediate_group_members(TEST_GROUP_ID)

    assert resp.status_code == 200
    assert not any(member["id"] == user_id for member in resp.json())


@handle_forbidden_error(
    skip_message=(
        "To test integration for multipass group member expiration endpoints, you need to be member of group "
        f"`{_get_group_name(DEV_WORKSPACE_OWNER_GROUP_ID)}` ({DEV_WORKSPACE_OWNER_GROUP_ID})!"
    )
)
def test_group_member_expiration():
    user_id = TEST_SINGLETON.ctx.multipass.get_user_info()["id"]

    # Add user as member to test group
    expiration_date = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(days=7)
    expirations = {TEST_GROUP_ID: {user_id: expiration_date}}

    resp = TEST_SINGLETON.ctx.multipass.api_add_group_members({TEST_GROUP_ID}, {user_id}, expirations)

    assert resp.status_code == 204

    # Check that the expiration has been applied for the membership in test group
    resp = TEST_SINGLETON.ctx.multipass.api_get_group_member_expirations({TEST_GROUP_ID})

    assert resp.status_code == 200

    expirations = resp.json()["expirationsByGroupId"]

    assert TEST_GROUP_ID in expirations
    assert "expirationsByPrincipalId" in expirations[TEST_GROUP_ID]

    principal_expirations = expirations[TEST_GROUP_ID]["expirationsByPrincipalId"]

    assert user_id in principal_expirations
    assert datetime.fromisoformat(principal_expirations[user_id]["expiration"]) == expiration_date

    # Remove user as member from test group
    resp = TEST_SINGLETON.ctx.multipass.api_remove_group_members(TEST_GROUP_ID, {user_id})

    assert resp.status_code == 204


@handle_forbidden_error(
    skip_message=(
        "To test integration for multipass group member expiration settings endpoints, you need to be member of group "
        f"`{_get_group_name(DEV_WORKSPACE_OWNER_GROUP_ID)}` ({DEV_WORKSPACE_OWNER_GROUP_ID})!"
    )
)
def test_group_member_expiration_settings():
    # Update expiration settings for test group
    max_expiration = datetime.now(timezone.utc) + timedelta(seconds=DEFAULT_MAX_DURATION_IN_SECONDS)
    max_duration_in_seconds = DEFAULT_MAX_DURATION_IN_SECONDS

    resp = TEST_SINGLETON.ctx.multipass.api_update_group_member_expiration_settings(
        TEST_GROUP_ID, max_expiration, max_duration_in_seconds
    )

    assert resp.status_code == 200

    expiration_settings = resp.json()

    assert expiration_settings["maxExpiration"] == max_expiration.strftime("%Y-%m-%dT%H:%M:%SZ")
    assert expiration_settings["maxDurationInSeconds"] == max_duration_in_seconds

    # Reset group member expiration settings to initial state
    expiration_settings = TEST_SINGLETON.ctx.multipass.reset_group_member_expiration_settings(TEST_GROUP_ID)

    assert expiration_settings["maxExpiration"] is None
    assert expiration_settings["maxDurationInSeconds"] is None
