from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from foundry_dev_tools.config.config_types import FoundryOAuthGrantType
from foundry_dev_tools.config.token_provider import JWTTokenProvider, OAuthTokenProvider
from foundry_dev_tools.errors.dataset import (
    DatasetNoReadAccessError,
)
from foundry_dev_tools.foundry_api_client import v1_to_v2_config

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


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


def test_v1_to_v2_config(mocker: MockerFixture):
    get_config_dict = mocker.patch("foundry_dev_tools.foundry_api_client.get_config_dict")
    get_config_dict.return_value = {}
    tp, v2c = v1_to_v2_config({"jwt": "123", "foundry_url": "https://test"})
    assert isinstance(tp, JWTTokenProvider)
    assert tp.token == "123"  # noqa: S105
    assert tp.host.url == "https://test"
    get_config_dict.return_value = {
        "credentials": {
            "domain": "test",
            "token_provider": {"name": "oauth", "config": {"client_id": "cid", "client_secret": "csec"}},
        },
    }
    tp, v2c = v1_to_v2_config({"jwt": "123"})
    assert isinstance(tp, JWTTokenProvider)
    assert tp.token == "123"  # noqa: S105
    assert tp.host.url == "https://test"
    get_config_dict.return_value = {
        "credentials": {
            "domain": "test",
            "token_provider": {"name": "oauth", "config": {"client_id": "cid2", "client_secret": "csec2"}},
        },
    }

    tp, v2c = v1_to_v2_config({"foundry_url": "https://test2"})
    assert isinstance(tp, OAuthTokenProvider)
    assert tp._client_id == "cid2"
    assert tp._client_secret == "csec2"  # noqa: S105
    assert tp.host.url == "https://test2"
    assert tp.grant_type == FoundryOAuthGrantType.authorization_code
    get_config_dict.return_value = {
        "credentials": {
            "domain": "test",
            "token_provider": {"name": "oauth", "config": {"client_id": "cid3", "client_secret": "csec3"}},
        },
    }

    tp, v2c = v1_to_v2_config({"foundry_url": "https://test2", "grant_type": "client_credentials"})
    assert isinstance(tp, OAuthTokenProvider)
    assert tp._client_id == "cid3"
    assert tp._client_secret == "csec3"  # noqa: S105
    assert tp.host.url == "https://test2"
    assert tp.grant_type == FoundryOAuthGrantType.client_credentials
    get_config_dict.return_value = {
        "credentials": {
            "domain": "test",
            "token_provider": {
                "name": "jwt",
                "config": {
                    "jwt": "jwt3",
                },
            },
        },
    }

    tp, v2c = v1_to_v2_config({"client_id": "cid5", "client_secret": "csec5"})
    assert isinstance(tp, OAuthTokenProvider)
    assert tp._client_id == "cid5"
    assert tp._client_secret == "csec5"  # noqa: S105
    assert tp.host.url == "https://test"
    assert tp.grant_type == FoundryOAuthGrantType.authorization_code
