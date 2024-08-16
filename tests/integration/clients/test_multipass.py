"""Enable enable_third_party_application
Rotate Secret rotate_third_party_application_secret.

"""

from datetime import datetime, timedelta, timezone
from random import choice
from string import ascii_uppercase

import pytest
import requests
from integration.utils import (
    PROJECT_GROUP_ROLE_EDITOR,
    PROJECT_GROUP_ROLE_OWNER,
    PROJECT_GROUP_ROLE_VIEWER,
    backoff,
    skip_test_on_error,
)

from foundry_dev_tools.clients.multipass import (
    DEFAULT_MAX_DURATION_IN_SECONDS,
    DEFAULT_TOKEN_LIFETIME_IN_SECONDS,
)
from foundry_dev_tools.errors.meta import FoundryAPIError
from foundry_dev_tools.errors.multipass import DuplicateGroupNameError
from foundry_dev_tools.foundry_api_client import FoundryRestClient
from tests.integration.conftest import TEST_SINGLETON


@pytest.fixture(scope="module", autouse=True)
def _tear_down():
    yield

    try:
        # Remove simple group
        TEST_SINGLETON.simple_group.delete()

        # Remove project groups
        project_groups = TEST_SINGLETON.project_groups
        project_groups[PROJECT_GROUP_ROLE_VIEWER].delete()
        project_groups[PROJECT_GROUP_ROLE_EDITOR].delete()
        project_groups[PROJECT_GROUP_ROLE_OWNER].delete()
    except FoundryAPIError as exc:
        if exc.response.status_code != requests.codes.forbidden:
            raise


def test_organizations():
    user_info = TEST_SINGLETON.ctx.multipass.get_user_info()

    organization_rid = user_info["attributes"]["multipass:organization-rid"][0]
    organization_name = user_info["attributes"]["multipass:organization"][0]

    resp = TEST_SINGLETON.ctx.multipass.api_get_all_organizations()

    assert resp.status_code == 200

    organization = next((organization for organization in resp.json() if organization["rid"] == organization_rid), None)

    assert organization is not None
    assert organization["rid"] == organization_rid
    assert organization["displayName"] == organization_name


@skip_test_on_error(
    status_code_to_skip=requests.codes.forbidden,
    skip_message="To test integration for multipass groups, you need permissions to manage groups in organizations!",
)
def test_create_and_delete_group():
    # Create a new group
    rnd = "".join(choice(ascii_uppercase) for _ in range(5))
    name = "test-group_" + rnd
    description = "Description of test group"

    user_info = TEST_SINGLETON.ctx.multipass.get_user_info()
    organization_rid = user_info["attributes"]["multipass:organization-rid"][0]

    resp = TEST_SINGLETON.ctx.multipass.api_create_group(name, [organization_rid], description)

    assert resp.status_code == 200

    group = resp.json()

    assert "id" in group
    assert group["name"] == name
    assert len(group["attributes"]["multipass:description"]) == 1
    assert description in group["attributes"]["multipass:description"]
    assert len(group["attributes"]["multipass:organization-rid"]) == 1
    assert organization_rid in group["attributes"]["multipass:organization-rid"]

    group_id = group["id"]

    # Creating a new group with existing group name should raise error
    with pytest.raises(DuplicateGroupNameError):
        TEST_SINGLETON.ctx.multipass.api_create_group(name, [organization_rid], description)

    # Retrieve the group
    resp = TEST_SINGLETON.ctx.multipass.api_get_group(group_id)

    assert resp.status_code == 200

    group = resp.json()

    assert group["id"] == group_id
    assert group["name"] == name
    assert description in group["attributes"]["multipass:description"]
    assert organization_rid in group["attributes"]["multipass:organization-rid"]

    # Delete the group
    resp = TEST_SINGLETON.ctx.multipass.api_delete_group(group_id)

    assert resp.status_code == 204


@skip_test_on_error(
    status_code_to_skip=requests.codes.forbidden,
    skip_message="To test integration for multipass groups, you need permissions to manage groups in organizations!",
)
def test_update_group():
    # Update group description
    updated_description = "Description update"
    resp = TEST_SINGLETON.ctx.multipass.api_update_group(TEST_SINGLETON.simple_group.id, updated_description)

    assert resp.status_code == 200

    group = resp.json()
    assert group["id"] == TEST_SINGLETON.simple_group.id
    assert len(group["attributes"]["multipass:description"]) == 1
    assert group["attributes"]["multipass:description"][0] == updated_description

    # Rename group
    original_name = TEST_SINGLETON.simple_group.name
    new_group_name = original_name + "_RENAMED"

    resp = TEST_SINGLETON.ctx.multipass.api_rename_group(TEST_SINGLETON.simple_group.id, new_group_name)

    assert resp.status_code == 200

    resp_body = resp.json()

    renamed_group = resp_body["renamedGroup"]
    alias_group = resp_body["aliasGroup"]

    assert renamed_group["id"] == TEST_SINGLETON.simple_group.id
    assert renamed_group["name"] == new_group_name

    assert "id" in alias_group
    assert alias_group["name"] == original_name

    alias_group_id = alias_group["id"]

    # Renaming a group to the name of the group itself should not change anything and not create alias group
    resp = TEST_SINGLETON.ctx.multipass.api_rename_group(TEST_SINGLETON.simple_group.id, new_group_name)

    assert resp.status_code == 200

    resp_body = resp.json()

    renamed_group = resp_body["renamedGroup"]
    alias_group = resp_body["aliasGroup"]

    assert renamed_group["id"] == TEST_SINGLETON.simple_group.id
    assert renamed_group["name"] == new_group_name

    assert alias_group is None

    # Renaming a group to a name that already exists, should fail
    with pytest.raises(DuplicateGroupNameError):
        TEST_SINGLETON.ctx.multipass.api_rename_group(TEST_SINGLETON.simple_group.id, original_name)

    # Delete the alias group
    resp = TEST_SINGLETON.ctx.multipass.api_delete_group(alias_group_id)

    assert resp.status_code == 204


@skip_test_on_error(
    status_code_to_skip=requests.codes.forbidden,
    skip_message="To test integration for multipass tpa, you need permissions to manage third party applications!",
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


def test_user_information_and_principal_endpoints():
    # Retrieve user information
    user_info = TEST_SINGLETON.ctx.multipass.get_user_info()

    user_id = user_info["id"]

    # Get multiple principals at once
    resp = TEST_SINGLETON.ctx.multipass.api_get_principals({user_id})

    assert resp.status_code == 200

    user = next((principal for principal in resp.json() if principal["id"] == user_id), None)

    assert user
    assert user_info == user


@skip_test_on_error(
    status_code_to_skip=requests.codes.forbidden,
    skip_message="To test integration for multipass token endpoints, you need permissions to manage tokens!",
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


@skip_test_on_error(
    status_code_to_skip=requests.codes.forbidden,
    skip_message=(
        "To test integration for multipass group administration endpoints, "
        "you need permissions to manage groups in organizations!"
    ),
)
def test_group_administrations():
    sleep_time = 0.2

    # Retrieve the viewer group from TEST_SINGLETON project groups
    viewer_group = TEST_SINGLETON.project_groups[PROJECT_GROUP_ROLE_VIEWER]

    # Add user as `manager manager` to viewer group
    resp = TEST_SINGLETON.ctx.multipass.add_group_manager_managers(viewer_group.id, {TEST_SINGLETON.user.id})

    assert resp.status_code == 204

    successful, resp, _ = backoff(
        lambda: TEST_SINGLETON.ctx.multipass.api_get_group_manager_managers(viewer_group.id),
        lambda r: any(manager_manager["id"] == TEST_SINGLETON.user.id for manager_manager in r.json()),
        sleep_time,
    )

    assert successful
    assert resp.status_code == 200

    assert any(manager_manager["id"] == TEST_SINGLETON.user.id for manager_manager in resp.json())

    # Withdraw the role of `manager manager` from user viewer group
    resp = TEST_SINGLETON.ctx.multipass.remove_group_manager_managers(viewer_group.id, {TEST_SINGLETON.user.id})

    assert resp.status_code == 204

    successful, resp, _ = backoff(
        lambda: TEST_SINGLETON.ctx.multipass.api_get_group_manager_managers(viewer_group.id),
        lambda r: not any(manager_manager["id"] == TEST_SINGLETON.user.id for manager_manager in r.json()),
        sleep_time,
    )

    assert successful
    assert resp.status_code == 200

    assert not any(manager_manager["id"] == TEST_SINGLETON.user.id for manager_manager in resp.json())

    # Add user as `member manager` to viewer group
    resp = TEST_SINGLETON.ctx.multipass.add_group_member_managers(viewer_group.id, {TEST_SINGLETON.user.id})

    assert resp.status_code == 204

    successful, resp, _ = backoff(
        lambda: TEST_SINGLETON.ctx.multipass.api_get_group_member_managers(viewer_group.id),
        lambda r: any(member_manager["id"] == TEST_SINGLETON.user.id for member_manager in r.json()),
        sleep_time,
    )

    assert successful
    assert resp.status_code == 200

    assert any(member_manager["id"] == TEST_SINGLETON.user.id for member_manager in resp.json())

    # Withdraw the role of `member manager` from user for viewer group
    resp = TEST_SINGLETON.ctx.multipass.remove_group_member_managers(viewer_group.id, {TEST_SINGLETON.user.id})

    assert resp.status_code == 204

    successful, resp, _ = backoff(
        lambda: TEST_SINGLETON.ctx.multipass.api_get_group_member_managers(viewer_group.id),
        lambda r: not any(member_manager["id"] == TEST_SINGLETON.user.id for member_manager in r.json()),
        sleep_time,
    )

    assert successful
    assert resp.status_code == 200

    assert not any(member_manager["id"] == TEST_SINGLETON.user.id for member_manager in resp.json())


@skip_test_on_error(
    status_code_to_skip=requests.codes.forbidden,
    skip_message=(
        "To test integration for multipass group member administration endpoints, "
        "you need permissions to manage groups in organizations!"
    ),
)
def test_group_member_administration():
    # Retrieve the viewer and editor group from TEST_SINGLETON project groups
    viewer_group = TEST_SINGLETON.project_groups[PROJECT_GROUP_ROLE_VIEWER]
    editor_group = TEST_SINGLETON.project_groups[PROJECT_GROUP_ROLE_EDITOR]

    # Add user as member to test group
    resp = TEST_SINGLETON.ctx.multipass.api_add_group_members({editor_group.id}, {TEST_SINGLETON.user.id})

    assert resp.status_code == 204

    # Additionally make the editor group a member of the viewer group
    resp = TEST_SINGLETON.ctx.multipass.api_add_group_members({viewer_group.id}, {editor_group.id})

    assert resp.status_code == 204

    # When retrieving immediate group members for viewer group assert it does not contain the user
    resp = TEST_SINGLETON.ctx.multipass.api_get_immediate_group_members(viewer_group.id)

    assert resp.status_code == 200

    group_members = resp.json()

    assert any(member["id"] == editor_group.id for member in group_members)
    assert not any(member["id"] == TEST_SINGLETON.user.id for member in group_members)

    # Assert that the user has been added as member to editor group and is indirect member of viewer group
    resp = TEST_SINGLETON.ctx.multipass.api_get_all_group_members({editor_group.id, viewer_group.id})

    assert resp.status_code == 200

    group_id_members_mapping = resp.json()["membersByGroupId"]

    assert editor_group.id in group_id_members_mapping
    assert any(member["principalId"] == TEST_SINGLETON.user.id for member in group_id_members_mapping[editor_group.id])

    assert viewer_group.id in group_id_members_mapping
    assert any(member["principalId"] == editor_group.id for member in group_id_members_mapping[viewer_group.id])
    assert any(member["principalId"] == TEST_SINGLETON.user.id for member in group_id_members_mapping[viewer_group.id])

    # Assert that users of viewer group contain at least all known users of the viewer group but no groups
    resp = TEST_SINGLETON.ctx.multipass.api_get_all_group_users(viewer_group.id)

    assert resp.status_code == 200

    users = [user["id"] for user in resp.json()]

    members_of_viewer_group = group_id_members_mapping[viewer_group.id]
    user_members = [member["principalId"] for member in members_of_viewer_group if member["principalType"] == "USER"]
    group_members = [member["principalId"] for member in members_of_viewer_group if member["principalType"] == "GROUP"]

    assert all(user in users for user in user_members)
    assert not any(group in users for group in group_members)

    # Remove editor group as member from viewer group
    resp = TEST_SINGLETON.ctx.multipass.api_remove_group_members(viewer_group.id, {editor_group.id})

    assert resp.status_code == 204

    # Verify that editor group has been removed from viewer group
    resp = TEST_SINGLETON.ctx.multipass.api_get_immediate_group_members(viewer_group.id)

    assert resp.status_code == 200
    assert not any(member["id"] == editor_group.id for member in resp.json())

    # Assert that user is not indirect member of viewer group anymore
    resp = TEST_SINGLETON.ctx.multipass.api_get_all_group_members({viewer_group.id})

    assert resp.status_code == 200

    group_id_members_mapping = resp.json()["membersByGroupId"]

    assert viewer_group.id in group_id_members_mapping
    assert not any(
        member["principalId"] == TEST_SINGLETON.user.id for member in group_id_members_mapping[viewer_group.id]
    )

    # Remove user from editor group again
    resp = TEST_SINGLETON.ctx.multipass.api_remove_group_members(editor_group.id, {TEST_SINGLETON.user.id})

    assert resp.status_code == 204

    # Verify that user has been removed from editor group
    resp = TEST_SINGLETON.ctx.multipass.api_get_immediate_group_members(editor_group.id)

    assert resp.status_code == 200
    assert not any(member["id"] == TEST_SINGLETON.user.id for member in resp.json())


@skip_test_on_error(
    status_code_to_skip=requests.codes.forbidden,
    skip_message=(
        "To test integration for multipass group member expiration endpoints, "
        "you need permissions to manage groups in organizations!"
    ),
)
def test_group_member_expiration():
    # Add user as member to the test group
    expiration_date = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(days=7)
    expirations = {TEST_SINGLETON.simple_group.id: {TEST_SINGLETON.user.id: expiration_date}}

    resp = TEST_SINGLETON.ctx.multipass.api_add_group_members(
        {TEST_SINGLETON.simple_group.id}, {TEST_SINGLETON.user.id}, expirations
    )

    assert resp.status_code == 204

    # Check that the expiration has been applied for the membership in the test group
    resp = TEST_SINGLETON.ctx.multipass.api_get_group_member_expirations({TEST_SINGLETON.simple_group.id})

    assert resp.status_code == 200

    expirations = resp.json()["expirationsByGroupId"]

    assert TEST_SINGLETON.simple_group.id in expirations
    assert "expirationsByPrincipalId" in expirations[TEST_SINGLETON.simple_group.id]

    principal_expirations = expirations[TEST_SINGLETON.simple_group.id]["expirationsByPrincipalId"]

    assert TEST_SINGLETON.user.id in principal_expirations
    assert datetime.fromisoformat(principal_expirations[TEST_SINGLETON.user.id]["expiration"]) == expiration_date

    # Reset expiration for user
    resp = TEST_SINGLETON.ctx.multipass.api_add_group_members(
        {TEST_SINGLETON.simple_group.id}, {TEST_SINGLETON.user.id}, None
    )

    assert resp.status_code == 204

    # Check that the expiration has been removed for the membership in the test group
    resp = TEST_SINGLETON.ctx.multipass.api_get_group_member_expirations({TEST_SINGLETON.simple_group.id})

    assert resp.status_code == 200
    assert TEST_SINGLETON.simple_group.id not in resp.json()["expirationsByGroupId"]


@skip_test_on_error(
    status_code_to_skip=requests.codes.forbidden,
    skip_message=(
        "To test integration for multipass group member expiration settings endpoints, "
        "you need permissions to manage groups in organizations!"
    ),
)
def test_group_member_expiration_settings():
    # Update expiration settings for test group
    max_expiration = datetime.now(timezone.utc) + timedelta(seconds=DEFAULT_MAX_DURATION_IN_SECONDS)
    max_duration_in_seconds = DEFAULT_MAX_DURATION_IN_SECONDS

    resp = TEST_SINGLETON.ctx.multipass.api_update_group_member_expiration_settings(
        TEST_SINGLETON.simple_group.id, max_expiration, max_duration_in_seconds
    )

    assert resp.status_code == 200

    expiration_settings = resp.json()

    assert expiration_settings["maxExpiration"] == max_expiration.strftime("%Y-%m-%dT%H:%M:%SZ")
    assert expiration_settings["maxDurationInSeconds"] == max_duration_in_seconds

    # Reset group member expiration settings to initial state
    expiration_settings = TEST_SINGLETON.ctx.multipass.reset_group_member_expiration_settings(
        TEST_SINGLETON.simple_group.id
    )

    assert expiration_settings["maxExpiration"] is None
    assert expiration_settings["maxDurationInSeconds"] is None
