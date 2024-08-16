from random import choice
from string import ascii_uppercase

import requests
from integration.conftest import TEST_SINGLETON
from integration.utils import skip_test_on_error

from foundry_dev_tools.resources import Group, User


@skip_test_on_error(
    status_code_to_skip=requests.codes.forbidden,
    skip_message="To test integration for multipass groups, you need permissions to manage groups in organizations!",
)
def test_crud_group():
    # Create a new group
    rnd = "".join(choice(ascii_uppercase) for _ in range(5))
    name = "test-group_" + rnd
    description = "Description of test group"

    user_info = User.me(TEST_SINGLETON.ctx)
    organization_rid = user_info.attributes["multipass:organization-rid"][0]
    organization_name = user_info.attributes["multipass:organization"][0]

    test_group = Group.create(TEST_SINGLETON.ctx, name, {organization_rid}, description=description)

    assert test_group.id is not None
    assert test_group.name == name
    assert len(test_group.attributes["multipass:organization-rid"]) == 1
    assert test_group.attributes["multipass:organization-rid"][0] == organization_rid
    assert len(test_group.attributes["multipass:description"]) == 1
    assert test_group.attributes["multipass:description"][0] == description
    assert len(test_group.attributes["multipass:organization"]) == 1
    assert test_group.attributes["multipass:organization"][0] == organization_name

    # Update group description
    updated_description = description + " - UPDATED"
    test_group.update(updated_description)

    assert len(test_group.attributes["multipass:description"]) == 1
    assert test_group.attributes["multipass:description"][0] == updated_description

    # Rename group
    new_name = name + "_RENAMED"
    rename_result = test_group.rename(new_name)

    assert test_group.name == new_name

    renamed_group = rename_result["renamedGroup"]
    alias_group = rename_result["aliasGroup"]

    assert renamed_group is not None
    assert renamed_group["id"] == test_group.id
    assert renamed_group["name"] == new_name

    assert alias_group is not None
    assert "id" in alias_group
    assert alias_group["name"] == name

    # Fetch alias group
    test_group_alias = Group.from_id(TEST_SINGLETON.ctx, alias_group["id"])

    assert test_group_alias.id == alias_group["id"]
    assert test_group_alias.name == alias_group["name"]

    # Delete the test and test alias group
    test_group_alias.delete()
    test_group.delete()
