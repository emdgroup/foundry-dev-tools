import re
from datetime import datetime, timedelta, timezone
from random import choice
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from foundry_dev_tools.clients.multipass import (
    DEFAULT_MAX_DURATION_IN_SECONDS,
    MAXIMUM_TOKEN_PAGE_SIZE,
    MINIMUM_MAX_DURATION_IN_SECONDS,
    MINIMUM_TOKEN_PAGE_SIZE,
)
from foundry_dev_tools.utils.clients import build_api_url
from tests.unit.mocks import TEST_HOST

TEST_GROUP_ID = "abcdef01-2345-6789-abcd-ef0123456789"
TEST_USER_ID = "a9b8c7d6-e5f4-3210-0f1e-2d3c4b5a6789"


@freeze_time("0s")
@patch("foundry_dev_tools.clients.api_client.APIClient.api_request")
def test_api_add_group_members_expirations(api_request, test_context_mock):
    test_context_mock.mock_adapter.register_uri(
        "POST", build_api_url(TEST_HOST.url, "multipass", "administration/groups/bulk/members")
    )

    # Assert that expirations config for expiration in the past raises an exception
    expirations = {TEST_GROUP_ID: {TEST_USER_ID: datetime.now(timezone.utc)}}

    with pytest.raises(ValueError):  # noqa: PT011
        test_context_mock.multipass.api_add_group_members({TEST_GROUP_ID}, {TEST_USER_ID}, expirations)

    api_request.assert_not_called()

    # Check that expirations without timezones raise a warning and are updated with utc timezone
    max_expiration = datetime.now().replace(microsecond=0) + timedelta(seconds=DEFAULT_MAX_DURATION_IN_SECONDS)  # noqa: DTZ005
    expirations = {TEST_GROUP_ID: {TEST_USER_ID: max_expiration}}

    with pytest.warns():
        test_context_mock.multipass.api_add_group_members({TEST_GROUP_ID}, {TEST_USER_ID}, expirations)

    max_expiration = datetime.fromisoformat(
        api_request.call_args.kwargs["json"]["expirations"][TEST_GROUP_ID][TEST_USER_ID]
    )
    max_expiration_expected = max_expiration.replace(tzinfo=timezone.utc)

    assert max_expiration == max_expiration_expected


@pytest.fixture()
def _register_update_member_expiration_settings(test_context_mock):
    test_context_mock.mock_adapter.register_uri(
        "POST",
        re.compile(
            re.escape(build_api_url(TEST_HOST.url, "multipass", "groups/member-expiration-settings/groups/")) + "/.*"
        ),
    )


@freeze_time("0s")
@pytest.mark.usefixtures(_register_update_member_expiration_settings.__name__)
@patch("foundry_dev_tools.clients.api_client.APIClient.api_request")
def test_api_update_group_member_expiration_settings_max_expiration_in_past(api_request, test_context_mock):
    now_utc = datetime.now(timezone.utc)

    # Assert that expiration in the past is reset to the datetime of the beginning of the next day
    max_expiration = now_utc - timedelta(seconds=DEFAULT_MAX_DURATION_IN_SECONDS)
    expected_max_expiration = (now_utc + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    with pytest.warns():
        test_context_mock.multipass.api_update_group_member_expiration_settings(TEST_GROUP_ID, max_expiration)

    request_body = api_request.call_args.kwargs["json"]

    assert datetime.fromisoformat(request_body["maxExpiration"]) == expected_max_expiration

    # Assert that expiration date in the moment of time is also reset to the datetime of the beginning of the next day
    max_expiration = now_utc
    expected_max_expiration = (now_utc + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    with pytest.warns():
        test_context_mock.multipass.api_update_group_member_expiration_settings(TEST_GROUP_ID, max_expiration)

    request_body = api_request.call_args.kwargs["json"]

    assert datetime.fromisoformat(request_body["maxExpiration"]) == expected_max_expiration

    # Check that max_expiration without timezone throws warning for missing timezone
    max_expiration = datetime.now() + timedelta(seconds=DEFAULT_MAX_DURATION_IN_SECONDS)  # noqa: DTZ005

    with pytest.warns():
        test_context_mock.multipass.api_update_group_member_expiration_settings(TEST_GROUP_ID, max_expiration)


@pytest.mark.parametrize(
    "max_expiration",
    [
        datetime.now().astimezone() + timedelta(seconds=DEFAULT_MAX_DURATION_IN_SECONDS),
        datetime.now(timezone.utc) + timedelta(seconds=DEFAULT_MAX_DURATION_IN_SECONDS),
    ],
)
@pytest.mark.usefixtures(_register_update_member_expiration_settings.__name__)
@patch("foundry_dev_tools.clients.api_client.APIClient.api_request")
def test_api_update_group_member_expiration_settings_different_timezones(
    api_request, max_expiration, test_context_mock
):
    # Check whether different time zones are handled correctly
    test_context_mock.mock_adapter.register_uri(
        "POST",
        re.compile(
            re.escape(build_api_url(TEST_HOST.url, "multipass", "groups/member-expiration-settings/groups/")) + "/.*"
        ),
    )

    # Assert that time zone of max_expiration gets converted into UTC time zone before being sent
    expected_max_expiration = max_expiration.astimezone(timezone.utc).replace(microsecond=0)

    test_context_mock.multipass.api_update_group_member_expiration_settings(TEST_GROUP_ID, max_expiration)

    request_body = api_request.call_args.kwargs["json"]

    assert datetime.fromisoformat(request_body["maxExpiration"]) == expected_max_expiration


@pytest.mark.usefixtures(_register_update_member_expiration_settings.__name__)
@patch("foundry_dev_tools.clients.api_client.APIClient.api_request")
def test_api_update_group_member_expiration_settings_invalid_max_durations(api_request, test_context_mock):
    # Choose invalid max duration that will be reset to default value
    invalid_max_duration = MINIMUM_MAX_DURATION_IN_SECONDS - 1
    with pytest.warns():
        test_context_mock.multipass.api_update_group_member_expiration_settings(
            TEST_GROUP_ID, max_duration_in_seconds=invalid_max_duration
        )

    request_body = api_request.call_args.kwargs["json"]

    assert request_body["maxExpiration"] is None
    assert request_body["maxDurationInSeconds"] == DEFAULT_MAX_DURATION_IN_SECONDS


@pytest.mark.parametrize(
    "max_duration_in_seconds", [MINIMUM_MAX_DURATION_IN_SECONDS, choice(range(DEFAULT_MAX_DURATION_IN_SECONDS))]
)
@pytest.mark.usefixtures(_register_update_member_expiration_settings.__name__)
@patch("foundry_dev_tools.clients.api_client.APIClient.api_request")
def test_api_update_group_member_expiration_settings_valid_max_durations(
    api_request, max_duration_in_seconds, test_context_mock
):
    test_context_mock.multipass.api_update_group_member_expiration_settings(
        TEST_GROUP_ID, max_duration_in_seconds=max_duration_in_seconds
    )

    request_body = api_request.call_args.kwargs["json"]

    assert request_body["maxExpiration"] is None
    assert request_body["maxDurationInSeconds"] == max_duration_in_seconds


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


def mock_generate_ttl(expiration_date) -> float:
    t_now = datetime.now(tz=timezone.utc)

    return (expiration_date - t_now).total_seconds()


def test_api_get_ttl(test_context_mock, foundry_token_expiration_date):
    with freeze_time("0s"):
        test_context_mock.mock_adapter.register_uri(
            "GET",
            build_api_url(TEST_HOST.url, "multipass", "token/ttl"),
            json=mock_generate_ttl(foundry_token_expiration_date),
        )

        first_measure = test_context_mock.multipass.api_get_ttl().json()

    with freeze_time("1s"):
        test_context_mock.mock_adapter.register_uri(
            "GET",
            build_api_url(TEST_HOST.url, "multipass", "token/ttl"),
            json=mock_generate_ttl(foundry_token_expiration_date),
        )

        second_measure = test_context_mock.multipass.api_get_ttl().json()

    time_delta_in_seconds = int(first_measure - second_measure)

    assert first_measure > second_measure
    assert time_delta_in_seconds == 1
