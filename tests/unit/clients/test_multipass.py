from datetime import datetime, timezone
from random import choice
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from foundry_dev_tools.clients.multipass import (
    MAXIMUM_TOKEN_PAGE_SIZE,
    MINIMUM_TOKEN_PAGE_SIZE,
)
from foundry_dev_tools.utils.clients import build_api_url
from tests.unit.mocks import TEST_HOST


@patch("foundry_dev_tools.clients.api_client.APIClient.api_request")
def test_api_get_tokens(api_request, test_context_mock):
    test_context_mock.mock_adapter.register_uri("GET", build_api_url(TEST_HOST.url, "multipass", "tokens"))

    # Test invalid page sizes and assert that they are reset to the next boundary value
    outside_minimum_token_page_size = MINIMUM_TOKEN_PAGE_SIZE - 1
    with pytest.warns():
        test_context_mock.multipass.api_get_tokens(limit=outside_minimum_token_page_size)

    params = api_request.call_args[1]["params"]
    assert params["limit"] == MINIMUM_TOKEN_PAGE_SIZE

    outside_maximum_token_page_size = MAXIMUM_TOKEN_PAGE_SIZE + 1
    with pytest.warns():
        test_context_mock.multipass.api_get_tokens(limit=outside_maximum_token_page_size)

    params = api_request.call_args[1]["params"]
    assert params["limit"] == MAXIMUM_TOKEN_PAGE_SIZE

    # Check that random page size in range remains the same
    rnd = choice(range(MAXIMUM_TOKEN_PAGE_SIZE + 1))
    test_context_mock.multipass.api_get_tokens(limit=rnd)

    params = api_request.call_args[1]["params"]
    assert params["limit"] == rnd


def test_get_tokens(test_context_mock):
    test_context_mock.mock_adapter.register_uri(
        "GET",
        build_api_url(TEST_HOST.url, "multipass", "tokens"),
        response_list=[
            {"json": {"values": ["token_1", "token_2"], "nextPageToken": 189232545}},
            {"json": {"values": ["token_3"], "nextPageToken": None}},
            {"json": {"values": ["other_token"], "nextPageToken": 14232231}},
        ],
    )

    # Should only fetch the first two responses from the response list and not the third one
    tokens = list(test_context_mock.multipass.get_tokens())

    assert len(tokens) == 3
    assert not any(token == "other_token" for token in tokens)  # noqa: S105
    assert test_context_mock.mock_adapter.call_count == 2


def mock_generate_ttl(request, context, expiration_date) -> float:
    t_now = datetime.now(tz=timezone.utc)

    return (expiration_date - t_now).total_seconds()


def test_api_get_ttl(test_context_mock, foundry_token_expiration_date):
    with freeze_time("0s"):
        test_context_mock.mock_adapter.register_uri(
            "GET",
            build_api_url(TEST_HOST.url, "multipass", "token/ttl"),
            json=lambda request, ctx: mock_generate_ttl(request, ctx, foundry_token_expiration_date),
        )

        first_measure = test_context_mock.multipass.api_get_ttl().json()

    with freeze_time("1s"):
        test_context_mock.mock_adapter.register_uri(
            "GET",
            build_api_url(TEST_HOST.url, "multipass", "token/ttl"),
            json=lambda request, ctx: mock_generate_ttl(request, ctx, foundry_token_expiration_date),
        )

        second_measure = test_context_mock.multipass.api_get_ttl().json()

    time_delta_in_seconds = int(first_measure - second_measure)

    assert first_measure > second_measure
    assert time_delta_in_seconds == 1
