from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Iterator

# needed to offset the mocked expiry times
from botocore.credentials import (
    _DEFAULT_ADVISORY_REFRESH_TIMEOUT,
)
from freezegun import freeze_time

from foundry_dev_tools import FoundryRestClient, foundry_api_client

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
        assert fc._get_boto3_session().get_credentials().access_key == "key1"
    with freeze_time("1s"):
        assert fc._get_boto3_session().get_credentials().access_key == "key1"
    # botocore checks with >= we need to be a bit above the expiry time
    with freeze_time("2.1s"):
        assert fc._get_boto3_session().get_credentials().access_key == "key2"
    with freeze_time("3s"):
        assert fc._get_boto3_session().get_credentials().access_key == "key2"
    with freeze_time("4.1s"):
        assert fc._get_boto3_session().get_credentials().access_key == "key3"
    with freeze_time("5s"):
        assert fc._get_boto3_session().get_credentials().access_key == "key3"


async def test_async_s3_credentials_provider(mocker: MockerFixture):
    mocker.patch(
        "foundry_dev_tools.FoundryRestClient.get_s3_credentials",
        side_effect=mock_generate_credentials(),
    )
    fc = FoundryRestClient()
    with freeze_time("0s"):
        assert (
            await (
                await fc._get_aiobotocore_session().get_credentials()
            ).get_frozen_credentials()
        ).access_key == "key1"
    with freeze_time("1s"):
        assert (
            await (
                await fc._get_aiobotocore_session().get_credentials()
            ).get_frozen_credentials()
        ).access_key == "key1"
    # botocore checks with >= we need to be a bit above the expiry time
    with freeze_time("2.1s"):
        assert (
            await (
                await fc._get_aiobotocore_session().get_credentials()
            ).get_frozen_credentials()
        ).access_key == "key2"
    with freeze_time("3s"):
        assert (
            await (
                await fc._get_aiobotocore_session().get_credentials()
            ).get_frozen_credentials()
        ).access_key == "key2"
    with freeze_time("4.1s"):
        assert (
            await (
                await fc._get_aiobotocore_session().get_credentials()
            ).get_frozen_credentials()
        ).access_key == "key3"
    with freeze_time("5s"):
        assert (
            await (
                await fc._get_aiobotocore_session().get_credentials()
            ).get_frozen_credentials()
        ).access_key == "key3"


def test_parse_s3_credentials_response():
    # taken from https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html
    example_response = """<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
      <AssumeRoleWithWebIdentityResult>
        <SubjectFromWebIdentityToken>amzn1.account.AF6RHO7KZU5XRVQJGXK6HB56KR2A</SubjectFromWebIdentityToken>
        <Audience>client.5498841531868486423.1548@apps.example.com</Audience>
        <AssumedRoleUser>
          <Arn>arn:aws:sts::123456789012:assumed-role/FederatedWebIdentityRole/app1</Arn>
          <AssumedRoleId>AROACLKWSDQRAOEXAMPLE:app1</AssumedRoleId>
        </AssumedRoleUser>
        <Credentials>
          <SessionToken>AQoDYXdzEE0a8ANXXXXXXXXNO1ewxE5TijQyp+IEXAMPLE</SessionToken>
          <SecretAccessKey>wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY</SecretAccessKey>
          <Expiration>2014-10-24T23:00:23Z</Expiration>
          <AccessKeyId>ASgeIAIOSFODNN7EXAMPLE</AccessKeyId>
        </Credentials>
        <SourceIdentity>SourceIdentityValue</SourceIdentity>
        <Provider>www.amazon.com</Provider>
      </AssumeRoleWithWebIdentityResult>
      <ResponseMetadata>
        <RequestId>ad4156e9-bce1-11e2-82e6-6b6efEXAMPLE</RequestId>
      </ResponseMetadata>
    </AssumeRoleWithWebIdentityResponse>"""
    parsed = foundry_api_client.parse_s3_credentials_response(example_response)
    assert parsed["access_key"] == "ASgeIAIOSFODNN7EXAMPLE"
    assert (
        parsed["token"]
        == "AQoDYXdzEE0a8ANXXXXXXXXNO1ewxE5TijQyp+IEXAMPLE"  # noqa: S105
    )
    assert parsed["expiry_time"] == "2014-10-24T23:00:23Z"
    assert (
        parsed["secret_key"]
        == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY"  # noqa: S105
    )
