import requests

from foundry_dev_tools.helpers.multipass import Group, Principal, User
from tests.integration.conftest import TEST_SINGLETON
from tests.integration.utils import skip_test_on_error


@skip_test_on_error(
    status_code_to_skip=requests.codes.forbidden,
    skip_message="To test integration for multipass groups, you need permissions to manage groups in organizations!",
)
def test_crud_principal():
    # Retrieve principal for group id and compare against Group type
    group_principal = Principal.from_id(TEST_SINGLETON.ctx, TEST_SINGLETON.simple_group.id)

    assert isinstance(group_principal, Group)

    group_principal.delete()

    # Retrieve principal for user id and compare against User type
    user_principal = Principal.from_id(TEST_SINGLETON.ctx, TEST_SINGLETON.user.id)

    assert isinstance(user_principal, User)
