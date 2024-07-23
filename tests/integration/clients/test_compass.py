import time
from random import choice
from string import ascii_uppercase, hexdigits

import pytest
import requests

from foundry_dev_tools.errors.compass import DuplicateNameError, ResourceNotTrashedError
from foundry_dev_tools.errors.meta import FoundryAPIError
from foundry_dev_tools.utils import api_types
from tests.integration.conftest import TEST_SINGLETON
from tests.integration.utils import (
    INTEGRATION_TEST_COMPASS_ROOT_PATH,
    INTEGRATION_TEST_COMPASS_ROOT_RID,
    INTEGRATION_TEST_PROJECT_RID,
    MARKING_ID,
)


def create_compass_folder() -> tuple[api_types.FolderRid, str]:
    """Create a new folder and return its resource identifier."""

    rnd = "".join(choice(ascii_uppercase) for i in range(5))
    compass_folder_name = f"compass_folder_{rnd}"

    response = TEST_SINGLETON.ctx.compass.api_create_folder(compass_folder_name, INTEGRATION_TEST_COMPASS_ROOT_RID)

    assert response.status_code == 200

    response_data = response.json()

    rid = response_data["rid"]
    name = response_data["name"]

    return rid, name


def delete_compass_folder(folder_rid: api_types.FolderRid) -> None:
    """Tear-Down logic: Permanently Delete the compass folder with the associated `folder_rid`

    Args:
        folder_rid: The resource identifier of the folder to be deleted.

    """

    # Delete permanently with additional delete_options
    response = TEST_SINGLETON.ctx.compass.api_delete_permanently(
        {folder_rid}, delete_options={"DO_NOT_REQUIRE_TRASHED"}
    )

    assert response.status_code == 200


@pytest.fixture()
def compass_folder_setup_fixture():
    # Create test folder
    compass_folder_rid, compass_folder_name = create_compass_folder()

    yield compass_folder_rid, compass_folder_name

    # Delete test folder
    delete_compass_folder(compass_folder_rid)


def test_create_and_delete_compass_folder():
    """Create a new folder and return its resource identifier."""

    rnd = "".join(choice(ascii_uppercase) for i in range(5))
    compass_folder_name = f"compass_folder_{rnd}"

    response = TEST_SINGLETON.ctx.compass.api_create_folder(compass_folder_name, INTEGRATION_TEST_COMPASS_ROOT_RID)

    response_data = response.json()

    assert response.status_code == 200
    assert "rid" in response_data
    assert response_data["name"] == compass_folder_name

    compass_folder_rid = response_data["rid"]

    # Move folder to trash
    response = TEST_SINGLETON.ctx.compass.api_add_to_trash({compass_folder_rid})

    assert response.status_code == 204

    # Restore folder from trash
    response = TEST_SINGLETON.ctx.compass.api_restore({compass_folder_rid})

    assert response.status_code == 204

    # Trying to delete resource that is not trashed should fail
    with pytest.raises(ResourceNotTrashedError):
        TEST_SINGLETON.ctx.compass.api_delete_permanently({compass_folder_rid})

    # Delete permanently with additional delete_options
    response = TEST_SINGLETON.ctx.compass.api_delete_permanently(
        {compass_folder_rid}, delete_options={"DO_NOT_REQUIRE_TRASHED"}
    )

    response_data = response.json()

    assert response.status_code == 200
    assert len(response_data) == 1

    deleted_resource = response_data[0]

    assert deleted_resource["rid"] == compass_folder_rid
    assert deleted_resource["type"] == "SUCCESS"

    # delete a second time to ensure that type is NOT_FOUND
    response = TEST_SINGLETON.ctx.compass.api_delete_permanently({compass_folder_rid})

    response_data = response.json()

    assert response.status_code == 200
    assert len(response_data) == 1

    deleted_resource = response_data[0]

    assert deleted_resource["rid"] == compass_folder_rid
    assert deleted_resource["type"] == "NOT_FOUND"

    # Check if resource exists
    exists = TEST_SINGLETON.ctx.compass.resource_exists(compass_folder_rid)

    assert exists is False


def test_resource_and_path_endpoints(compass_folder_setup_fixture):
    compass_folder_rid, compass_folder_name = compass_folder_setup_fixture

    # Fetch resource with `path` decoration
    response = TEST_SINGLETON.ctx.compass.api_get_resource(compass_folder_rid, decoration={"path"})
    response_data = response.json()

    assert response.status_code == 200
    assert response_data["rid"] == compass_folder_rid
    assert response_data["name"] == compass_folder_name
    assert response_data["path"] is not None

    compass_folder_path = response_data["path"]

    # Get path for single rid and validate
    result_path = TEST_SINGLETON.ctx.compass.get_path(compass_folder_rid)

    assert result_path == compass_folder_path

    absolute_path = f"{INTEGRATION_TEST_COMPASS_ROOT_PATH}/{compass_folder_name}"

    assert absolute_path == result_path

    # Try to receive invalid path
    rnd = "".join(choice(ascii_uppercase) for _ in range(5))
    invalid_path = compass_folder_rid + rnd

    result_path = TEST_SINGLETON.ctx.compass.get_path(invalid_path)

    assert result_path is None

    # Get paths for rids
    rid_path_mapping = TEST_SINGLETON.ctx.compass.get_paths([INTEGRATION_TEST_COMPASS_ROOT_RID, compass_folder_rid])

    assert rid_path_mapping[INTEGRATION_TEST_COMPASS_ROOT_RID] == INTEGRATION_TEST_COMPASS_ROOT_PATH
    assert rid_path_mapping[compass_folder_rid] == compass_folder_path

    # Request rid by providing the folder path
    response = TEST_SINGLETON.ctx.compass.api_get_resource_by_path(compass_folder_path, decoration=["path"])

    response_data = response.json()

    assert response.status_code == 200
    assert response_data["rid"] == compass_folder_rid
    assert response_data["name"] == compass_folder_name
    assert response_data["path"] == compass_folder_path


def test_rename_of_folder(compass_folder_setup_fixture):
    compass_folder_rid, compass_folder_name = compass_folder_setup_fixture

    # Check that name of previously created compass folder is no longer available in compass root folder
    response = TEST_SINGLETON.ctx.compass.api_check_name(INTEGRATION_TEST_COMPASS_ROOT_RID, compass_folder_name)

    available = response.json()

    assert response.status_code == 200
    assert not available

    # Ensure creation of folder with same name raises DuplicateNameError
    with pytest.raises(DuplicateNameError):
        TEST_SINGLETON.ctx.compass.api_create_folder(compass_folder_name, INTEGRATION_TEST_COMPASS_ROOT_RID)

    # Rename compass folder
    rnd = "".join(choice(ascii_uppercase) for i in range(5))
    new_compass_folder_name = f"new_compass_folder_{rnd}"
    response = TEST_SINGLETON.ctx.compass.api_set_name(compass_folder_rid, new_compass_folder_name)

    assert response.status_code == 204

    response = TEST_SINGLETON.ctx.compass.api_get_resource(compass_folder_rid)

    response_data = response.json()

    assert response_data["name"] == new_compass_folder_name


def test_compass_child_objects(compass_folder_setup_fixture):
    compass_folder_rid, compass_folder_name = compass_folder_setup_fixture

    # Create another compass folder and a dataset inside compass folder
    datasets = {}
    for _ in range(5):
        rnd = "".join(choice(ascii_uppercase) for _ in range(5))
        dataset_name = f"test_dataset_{rnd}"

        dataset_path = f"{INTEGRATION_TEST_COMPASS_ROOT_PATH}/{compass_folder_name}/{dataset_name}"
        response = TEST_SINGLETON.ctx.catalog.api_create_dataset(dataset_path)

        assert response.status_code == 200

        dataset_rid = response.json()["rid"]

        datasets[dataset_rid] = dataset_name

    child_objects = list(TEST_SINGLETON.ctx.compass.get_child_objects_of_folder(compass_folder_rid))

    children = {resource["rid"]: resource["name"] for resource in child_objects}

    assert sorted(datasets) == sorted(children)


def test_marking_endpoints(compass_folder_setup_fixture):
    try:
        _test_marking_endpoints_inner(compass_folder_setup_fixture)
    except FoundryAPIError as err:
        if err.response.status_code == requests.codes.forbidden:
            msg = (
                "To test integration for compass marking, "
                f"you need to have access to the marking with id '{MARKING_ID}'"
            )
            pytest.skip(msg)
        else:
            raise


def _test_marking_endpoints_inner(compass_folder_setup_fixture):
    compass_folder_rid, compass_folder_name = compass_folder_setup_fixture

    # Add marking
    response = TEST_SINGLETON.ctx.compass.add_marking(compass_folder_rid, MARKING_ID)

    assert response.status_code == 204

    response = TEST_SINGLETON.ctx.compass.api_get_resource(compass_folder_rid, decoration=["markings"])

    markings = response.json()["markings"]
    markings_len_after_add = len(markings)

    assert response.status_code == 200
    assert any(marking["markingId"] == MARKING_ID for marking in markings)

    # Remove marking
    response = TEST_SINGLETON.ctx.compass.remove_marking(compass_folder_rid, MARKING_ID)

    assert response.status_code == 204

    response = TEST_SINGLETON.ctx.compass.api_get_resource(compass_folder_rid, decoration=["markings"])

    markings = response.json()["markings"]
    markings_len_after_remove = len(markings)

    assert response.status_code == 200
    assert not any(marking["markingId"] == MARKING_ID for marking in markings)

    # Assert that only the provided marking hasbeen removed
    expected_number_of_removed_markings = 1

    assert markings_len_after_add == markings_len_after_remove + expected_number_of_removed_markings


def test_get_and_search_projects():
    # Fetch ls-use-case-foundry-devtools-dev-workspace project
    project = TEST_SINGLETON.ctx.compass.get_project_by_rid(INTEGRATION_TEST_PROJECT_RID)

    if project is None:
        msg = (
            "To run this integration test "
            f"you need to have access to the project with rid '{INTEGRATION_TEST_PROJECT_RID}'"
        )
        pytest.skip(msg)

    assert project["type"] == "PRIVATE"
    assert project["resource"]["rid"] == INTEGRATION_TEST_PROJECT_RID

    test_project_name = project["resource"]["name"]

    # Assert that project for invalid project rid is `None`

    rnd = "".join(choice(hexdigits) for _ in range(5)).lower()
    invalid_project_rid = INTEGRATION_TEST_PROJECT_RID + rnd
    project = TEST_SINGLETON.ctx.compass.get_project_by_rid(invalid_project_rid)

    assert project is None

    # Search project by the project name of the previously fetched project
    page_size = 1
    response = TEST_SINGLETON.ctx.compass.api_search_projects(test_project_name, page_size=page_size)

    assert response.status_code == 200

    response_data = response.json()

    assert int(response_data["nextPageToken"]) == page_size
    assert len(response_data["values"]) == page_size

    project_search_result = response_data["values"][0]
    project_resource = project_search_result["resource"]

    assert project_resource["rid"] == INTEGRATION_TEST_PROJECT_RID
    assert project_resource["name"] == test_project_name


def test_imports():
    project = TEST_SINGLETON.ctx.compass.get_project_by_rid(INTEGRATION_TEST_PROJECT_RID)

    if project is None:
        msg = (
            "To run this integration test "
            f"you need to have access to the project with rid '{INTEGRATION_TEST_PROJECT_RID}'"
        )
        pytest.skip(msg)

    rnd = "".join(choice(ascii_uppercase) for _ in range(5))
    path = TEST_SINGLETON.ctx.compass.get_path(INTEGRATION_TEST_PROJECT_RID)

    dataset_path = f"{path}/new_data/dataset_{rnd}"

    response = TEST_SINGLETON.ctx.catalog.api_create_dataset(dataset_path)

    assert response.status_code == 200

    dataset_rid = response.json()["rid"]

    response = TEST_SINGLETON.ctx.compass.api_get_home_folder()

    assert response.status_code == 200

    home_project_folder_rid = response.json()["rid"]

    response = TEST_SINGLETON.ctx.compass.api_add_imports(home_project_folder_rid, {dataset_rid})

    assert response.status_code == 204

    response = TEST_SINGLETON.ctx.compass.api_remove_imports(home_project_folder_rid, {dataset_rid})

    assert response.status_code == 204

    response = TEST_SINGLETON.ctx.compass.api_delete_permanently({dataset_rid}, {"DO_NOT_REQUIRE_TRASHED"})

    response_data = response.json()[0]

    assert response.status_code == 200
    assert response_data["rid"] == dataset_rid


def test_resolve_path():
    # Get all parents of INTEGRATION_TEST_COMPASS_ROOT_PATH including itself and fetch the resource for the path
    split = INTEGRATION_TEST_COMPASS_ROOT_PATH.split("/")
    paths = ["/".join(split[:i]) for i in range(1, len(split) + 1)]
    rids = [TEST_SINGLETON.ctx.compass.api_get_resource_by_path(path).json()["rid"] for path in paths]

    response = TEST_SINGLETON.ctx.compass.api_resolve_path(INTEGRATION_TEST_COMPASS_ROOT_PATH)

    assert response.status_code == 200

    # Assert that rids of components match the rids of resolved path components
    response_data = response.json()
    for i, component in enumerate(response_data):
        assert component["rid"] == rids[i]


def test_get_and_update_resource_roles():
    project = TEST_SINGLETON.ctx.compass.get_project_by_rid(INTEGRATION_TEST_PROJECT_RID)

    if project is None:
        msg = (
            "To run this integration test "
            f"you need to have access to the project with rid '{INTEGRATION_TEST_PROJECT_RID}'"
        )
        pytest.skip(msg)

    if not project["resourceLevelRoleGrantsAllowed"]:
        msg = (
            "The option 'Allow resource level role grants' "
            f"must be enabled in project with rid '{INTEGRATION_TEST_PROJECT_RID}'"
            "in order to apply roles on resource level"
        )
        pytest.skip(msg)

    rid = "ri.compass.main.folder.9967a4f0-5f7d-4019-b168-0fc8b79d91b8"

    result = TEST_SINGLETON.ctx.compass.get_resource_roles({rid})
    grants = result["resourceRolesResultMap"][rid]["grants"]

    assert len(grants) == 0

    # Set 'ls-use-case-foundry-dev-tools-dev-workspace-viewer' as explicit viewer on the resource
    # and grant discover role to everyone
    grant_patches = [
        {
            "roleGrant": {
                "role": "compass:view",
                "principal": {"id": "2ba614c8-65bb-4d1f-afa1-323610b755ec", "type": "GROUP"},
            },
            "patchOperation": "ADD",
        },
        {
            "roleGrant": {"role": "compass:discover", "principal": {"id": "", "type": "EVERYONE"}},
            "patchOperation": "ADD",
        },
    ]

    expected_grant_size = 2
    grants = _update_resource_roles(rid, grant_patches, expected_grant_size)

    assert len(grants) == expected_grant_size

    view_grant = grants[0]
    discover_grant = grants[1]

    assert view_grant["role"] == "compass:view"
    assert view_grant["principal"]["id"] == "2ba614c8-65bb-4d1f-afa1-323610b755ec"
    assert view_grant["principal"]["type"] == "GROUP"

    assert discover_grant["role"] == "compass:discover"
    assert discover_grant["principal"]["type"] == "EVERYONE"

    grant_patches = [
        {
            "roleGrant": {
                "role": "compass:view",
                "principal": {"id": "2ba614c8-65bb-4d1f-afa1-323610b755ec", "type": "GROUP"},
            },
            "patchOperation": "REMOVE",
        },
        {
            "roleGrant": {"role": "compass:discover", "principal": {"id": "", "type": "EVERYONE"}},
            "patchOperation": "REMOVE",
        },
    ]

    expected_grant_size = 0
    grants = _update_resource_roles(rid, grant_patches, expected_grant_size)

    assert len(grants) == expected_grant_size


def _update_resource_roles(
    rid: api_types.Rid, grant_patches: set[api_types.RoleGrantPatch], expected_grant_size: int
) -> list[api_types.RoleGrant]:
    """Method to patch grants for the given resource and returning the updated grants."""

    response = TEST_SINGLETON.ctx.compass.api_update_resource_roles(rid, grant_patches=grant_patches)

    assert response.status_code == 204

    result = TEST_SINGLETON.ctx.compass.get_resource_roles({rid})
    grants = result["resourceRolesResultMap"][rid]["grants"]

    while len(grants) != expected_grant_size:
        time.sleep(0.1)

        result = TEST_SINGLETON.ctx.compass.get_resource_roles({rid})
        grants = result["resourceRolesResultMap"][rid]["grants"]

    return grants
