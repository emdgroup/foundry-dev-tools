from random import choice
from string import ascii_letters
from typing import Any
from unittest.mock import patch

import pytest

from foundry_dev_tools.clients.compass import (
    MAXIMUM_PROJECTS_PAGE_SIZE,
    MAXIMUM_PROJECTS_SEARCH_OFFSET,
    MINIMUM_PROJECTS_PAGE_SIZE,
    MINIMUM_PROJECTS_SEARCH_OFFSET,
)
from foundry_dev_tools.errors.meta import FoundryAPIError
from foundry_dev_tools.utils.clients import build_api_url
from tests.unit.mocks import TEST_HOST

COMPASS_FOLDER_RID = "ri.compass.main.folder.01234567-89ab-cdef-a618-819292bc3a10"


def test_api_restore(test_context_mock):
    test_context_mock.mock_adapter.register_uri(
        "POST",
        build_api_url(TEST_HOST.url, "compass", "batch/trash/restore"),
        response_list=[{"status_code": 204}, {"status_code": 400}],
    )

    resp = test_context_mock.compass.api_restore({COMPASS_FOLDER_RID})

    assert resp.status_code == 204

    with pytest.raises(FoundryAPIError):
        test_context_mock.compass.api_restore({COMPASS_FOLDER_RID})


def test_get_path(test_context_mock):
    test_context_mock.mock_adapter.register_uri(
        "GET",
        build_api_url(TEST_HOST.url, "compass", f"resources/{COMPASS_FOLDER_RID}/path-json"),
        response_list=[{"json": "path/to/resource", "status_code": 200}, {"status_code": 204}],
    )

    path = test_context_mock.compass.get_path(COMPASS_FOLDER_RID)

    assert path == "path/to/resource"

    path = test_context_mock.compass.get_path(COMPASS_FOLDER_RID)

    assert path is None


@patch("foundry_dev_tools.clients.api_client.APIClient.api_request")
def test_api_search_projects(api_request, test_context_mock):
    test_context_mock.mock_adapter.register_uri("POST", build_api_url(TEST_HOST.url, "compass", "search/projects"))

    # Test invalid page sizes and assert that they are reset to the next boundary value
    outside_minimum_projects_page_size = MINIMUM_PROJECTS_PAGE_SIZE - 1
    with pytest.warns():
        test_context_mock.compass.api_search_projects(page_size=outside_minimum_projects_page_size)

    request_body = api_request.call_args.kwargs["json"]
    assert request_body["pageSize"] == MINIMUM_PROJECTS_PAGE_SIZE

    outside_maximum_projects_page_size = MAXIMUM_PROJECTS_PAGE_SIZE + 1
    with pytest.warns():
        test_context_mock.compass.api_search_projects(page_size=outside_maximum_projects_page_size)

    request_body = api_request.call_args.kwargs["json"]
    assert request_body["pageSize"] == MAXIMUM_PROJECTS_PAGE_SIZE

    # Check that random page size in range remains the same
    rnd = choice(range(MAXIMUM_PROJECTS_PAGE_SIZE)) + 1
    test_context_mock.compass.api_search_projects(page_size=rnd)

    request_body = api_request.call_args.kwargs["json"]
    assert request_body["pageSize"] == rnd

    # Test invalid search offset values for `page_token`
    rnd = "".join(choice(ascii_letters) for _ in range(4))
    with pytest.raises(ValueError):  # noqa: PT011
        test_context_mock.compass.api_search_projects(page_token=rnd)

    outside_minimum_projects_search_offset = MINIMUM_PROJECTS_SEARCH_OFFSET - 1
    with pytest.raises(ValueError):  # noqa: PT011
        test_context_mock.compass.api_search_projects(page_token=str(outside_minimum_projects_search_offset))

    outside_maximum_projects_search_offset = MAXIMUM_PROJECTS_SEARCH_OFFSET + 1
    with pytest.raises(ValueError):  # noqa: PT011
        test_context_mock.compass.api_search_projects(page_token=str(outside_maximum_projects_search_offset))

    # Test valid search offsets for `page_token`
    rnd = str(choice(range(MAXIMUM_PROJECTS_SEARCH_OFFSET + 1)))
    test_context_mock.compass.api_search_projects(page_token=rnd)

    request_body = api_request.call_args.kwargs["json"]
    assert request_body["pageToken"] == rnd

    test_context_mock.compass.api_search_projects(page_token=str(MINIMUM_PROJECTS_SEARCH_OFFSET))

    request_body = api_request.call_args.kwargs["json"]
    assert request_body["pageToken"] == str(MINIMUM_PROJECTS_SEARCH_OFFSET)

    test_context_mock.compass.api_search_projects(page_token=str(MAXIMUM_PROJECTS_SEARCH_OFFSET))

    request_body = api_request.call_args.kwargs["json"]
    assert request_body["pageToken"] == str(MAXIMUM_PROJECTS_SEARCH_OFFSET)


def generate_projects(request, context) -> dict[str, Any]:
    request_body = request.json()

    page_size = request_body["pageSize"]
    page_offset = int(request_body["pageToken"] or 0)

    return {
        "nextPageToken": str(page_offset + page_size),
        "values": [{"id": i} for i in range(page_offset, page_offset + page_size)],
    }


def test_search_projects(test_context_mock):
    test_context_mock.mock_adapter.register_uri(
        "POST", build_api_url(TEST_HOST.url, "compass", "search/projects"), json=generate_projects
    )

    # Assert that for
    rnd = choice(range(MAXIMUM_PROJECTS_PAGE_SIZE)) + 1
    projects = list(test_context_mock.compass.search_projects(page_size=rnd))

    expected_page_size = (int(MAXIMUM_PROJECTS_PAGE_SIZE / rnd) + 1) * rnd
    assert len(projects) == expected_page_size
