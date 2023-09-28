from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Iterator

# needed to offset the mocked expiry times
from botocore.credentials import (
    _DEFAULT_ADVISORY_REFRESH_TIMEOUT,
)
from freezegun import freeze_time

from foundry_dev_tools import FoundryRestClient

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


def mock_generate_credentials() -> Iterator[dict]:
    tnow = datetime.now(tz=timezone.utc)
    yield {
        "access_key": "key1",
        "secret_key": "skey1",
        "token": "token1",
        "expiry_time": (
            tnow + timedelta(seconds=2 + _DEFAULT_ADVISORY_REFRESH_TIMEOUT)
        ).isoformat(),
    }
    yield {
        "access_key": "key2",
        "secret_key": "skey2",
        "token": "token2",
        "expiry_time": (
            tnow + timedelta(seconds=4 + _DEFAULT_ADVISORY_REFRESH_TIMEOUT)
        ).isoformat(),
    }
    yield {
        "access_key": "key3",
        "secret_key": "skey3",
        "token": "token3",
        "expiry_time": (
            tnow + timedelta(seconds=6 + _DEFAULT_ADVISORY_REFRESH_TIMEOUT)
        ).isoformat(),
    }


def test_s3_credentials_provider(mocker: MockerFixture):
    mocker.patch(
        "foundry_dev_tools.FoundryRestClient.get_s3_credentials",
        side_effect=mock_generate_credentials(),
    )
    fc = FoundryRestClient()
    with freeze_time("0s"):
        assert fc.get_boto3_session().get_credentials().access_key == "key1"
    with freeze_time("1s"):
        assert fc.get_boto3_session().get_credentials().access_key == "key1"
    # botocore checks with >= we need to be a bit above the expiry time
    with freeze_time("2.1s"):
        assert fc.get_boto3_session().get_credentials().access_key == "key2"
    with freeze_time("3s"):
        assert fc.get_boto3_session().get_credentials().access_key == "key2"
    with freeze_time("4.1s"):
        assert fc.get_boto3_session().get_credentials().access_key == "key3"
    with freeze_time("5s"):
        assert fc.get_boto3_session().get_credentials().access_key == "key3"


async def test_async_s3_credentials_provider(mocker: MockerFixture):
    mocker.patch(
        "foundry_dev_tools.FoundryRestClient.get_s3_credentials",
        side_effect=mock_generate_credentials(),
    )
    fc = FoundryRestClient()
    with freeze_time("0s"):
        assert (
            await (
                await fc.get_aiobotocore_session().get_credentials()
            ).get_frozen_credentials()
        ).access_key == "key1"
    with freeze_time("1s"):
        assert (
            await (
                await fc.get_aiobotocore_session().get_credentials()
            ).get_frozen_credentials()
        ).access_key == "key1"
    # botocore checks with >= we need to be a bit above the expiry time
    with freeze_time("2.1s"):
        assert (
            await (
                await fc.get_aiobotocore_session().get_credentials()
            ).get_frozen_credentials()
        ).access_key == "key2"
    with freeze_time("3s"):
        assert (
            await (
                await fc.get_aiobotocore_session().get_credentials()
            ).get_frozen_credentials()
        ).access_key == "key2"
    with freeze_time("4.1s"):
        assert (
            await (
                await fc.get_aiobotocore_session().get_credentials()
            ).get_frozen_credentials()
        ).access_key == "key3"
    with freeze_time("5s"):
        assert (
            await (
                await fc.get_aiobotocore_session().get_credentials()
            ).get_frozen_credentials()
        ).access_key == "key3"
