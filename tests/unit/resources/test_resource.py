import re

import pytest

from foundry_dev_tools.errors.compass import WrongResourceTypeError
from foundry_dev_tools.utils.clients import build_api_url


def test_wrong_resource_type(test_context_mock):
    test_context_mock.mock_adapter.register_uri(
        "GET",
        re.compile(re.escape(build_api_url(test_context_mock.token_provider.host.url, "compass", "")) + "resources/*"),
        json={
            "rid": "ri.compass.main.folder.1234",
            "name": "test-folder",
            "created": {"time": "2000-11-13T15:22:37.673282746Z", "userId": "73156d57-dbdb-475d-8b4d-aa4742a5eda7"},
            "modified": {"time": "2000-11-13T15:24:45.827467605Z", "userId": "73156d57-dbdb-475d-8b4d-aa4742a5eda7"},
            "lastModified": 974129085827.0,
            "description": None,
            "operations": [],
            "urlVariables": {},
            "favorite": None,
            "branches": None,
            "defaultBranch": None,
            "defaultBranchWithMarkings": None,
            "branchesCount": None,
            "hasBranches": None,
            "hasMultipleBranches": None,
            "backedObjectTypes": None,
            "path": None,
            "longDescription": None,
            "directlyTrashed": False,
            "inTrash": None,
            "isAutosave": False,
            "isHidden": False,
            "deprecation": None,
            "collections": None,
            "namedCollections": None,
            "tags": None,
            "namedTags": None,
            "alias": None,
            "collaborators": None,
            "namedAncestors": None,
            "markings": None,
            "projectAccessMarkings": None,
            "linkedItems": None,
            "contactInformation": None,
            "classification": None,
            "disableInheritedPermissions": None,
            "propagatePermissions": None,
            "resourceLevelRoleGrantsAllowed": None,
            "portfolioRid": None,
        },
    )
    with pytest.raises(WrongResourceTypeError):
        test_context_mock.get_dataset("ri.foundry.main.folder.1234")


def test_additional_property_in_resource_json(test_context_mock):
    test_context_mock.mock_adapter.register_uri(
        "GET",
        re.compile(re.escape(build_api_url(test_context_mock.token_provider.host.url, "compass", "")) + "resources/*"),
        json={
            "rid": "ri.foundry.main.folder.1234",
            "name": "just_a_name",
            "created": {"time": "2024-08-28T06:54:49.569854929Z", "userId": "1234"},
            "modified": {"time": "2024-08-28T06:57:20.413795158Z", "userId": "1234"},
            "lastModified": 1724828240413.0,
            "description": None,
            "operations": [
                "compass:edit-project",
            ],
            "urlVariables": {"compass:isProject": "false"},
            "favorite": None,
            "branches": None,
            "defaultBranch": None,
            "defaultBranchWithMarkings": None,
            "branchesCount": None,
            "hasBranches": None,
            "hasMultipleBranches": None,
            "backedObjectTypes": None,
            "path": "/some/path",
            "longDescription": None,
            "directlyTrashed": False,
            "inTrash": False,
            "isAutosave": False,
            "isHidden": False,
            "deprecation": None,
            "collections": None,
            "namedCollections": None,
            "tags": None,
            "namedTags": None,
            "alias": None,
            "collaborators": None,
            "namedAncestors": None,
            "markings": None,
            "projectAccessMarkings": None,
            "linkedItems": None,
            "contactInformation": None,
            "classification": None,
            "disableInheritedPermissions": None,
            "propagatePermissions": None,
            "resourceLevelRoleGrantsAllowed": None,
            "portfolioRid": None,
        },
    )
    # fixes TypeError: Resource._from_json() got an unexpected keyword argument 'portfolioRid'
    test_context_mock.get_resource("ri.foundry.main.folder.1234")
