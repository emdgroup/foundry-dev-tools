from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from foundry_dev_tools.errors.dataset import (
    DatasetNoReadAccessError,
)


@patch(
    "foundry_dev_tools.foundry_api_client.FoundryRestClient.get_dataset_details",
    MagicMock(
        return_value={
            "rid": "ri.foundry.main.dataset.1234",
            "name": "data1",
            "created": {
                "time": "2022-04-19T14:03:24.072061923Z",
                "userId": "1234",
            },
            "modified": {
                "time": "2022-04-19T14:03:27.656427209Z",
                "userId": "1234",
            },
            "lastModified": 1650377007656.0,
            "description": None,
            "operations": [
                "compass:read-resource",
                "compass:view-contact-information",
                "compass:discover",
            ],
            "urlVariables": {"compass:isProject": "false"},
            "favorite": None,
            "branches": None,
            "defaultBranch": None,
            "defaultBranchWithMarkings": None,
            "branchesCount": None,
            "hasBranches": None,
            "hasMultipleBranches": None,
            "path": "/path/data1",
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
        },
    ),
)
def test_get_dataset_details_throws_on_no_read_permissions(test_context_mock):
    with pytest.raises(DatasetNoReadAccessError):
        _ = test_context_mock.foundry_rest_client.get_dataset_identity(
            dataset_path_or_rid="ri.foundry.main.dataset.1234",
        )
