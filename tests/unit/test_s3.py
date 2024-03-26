from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

# needed to offset the mocked expiry times
from botocore.credentials import (
    _DEFAULT_ADVISORY_REFRESH_TIMEOUT,
)
from freezegun import freeze_time

from foundry_dev_tools.utils.s3 import parse_s3_credentials_response
from tests.unit.mocks import TEST_HOST

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


def s3_assume_role_response(session_token: str, secret_access_key: str, expiration: str, access_key_id: str) -> str:
    return f"""<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
      <AssumeRoleWithWebIdentityResult>
        <SubjectFromWebIdentityToken>amzn1.account.AF6RHO7KZU5XRVQJGXK6HB56KR2A</SubjectFromWebIdentityToken>
        <Audience>client.5498841531868486423.1548@apps.example.com</Audience>
        <AssumedRoleUser>
          <Arn>arn:aws:sts::123456789012:assumed-role/FederatedWebIdentityRole/app1</Arn>
          <AssumedRoleId>AROACLKWSDQRAOEXAMPLE:app1</AssumedRoleId>
        </AssumedRoleUser>
        <Credentials>
          <SessionToken>{session_token}</SessionToken>
          <SecretAccessKey>{secret_access_key}</SecretAccessKey>
          <Expiration>{expiration}</Expiration>
          <AccessKeyId>{access_key_id}</AccessKeyId>
        </Credentials>
        <SourceIdentity>SourceIdentityValue</SourceIdentity>
        <Provider>www.amazon.com</Provider>
      </AssumeRoleWithWebIdentityResult>
      <ResponseMetadata>
        <RequestId>ad4156e9-bce1-11e2-82e6-6b6efEXAMPLE</RequestId>
      </ResponseMetadata>
    </AssumeRoleWithWebIdentityResponse>"""


def mock_generate_credentials() -> list[dict]:
    tnow = datetime.now(tz=timezone.utc)
    return [
        {
            "text": s3_assume_role_response(
                "token1",
                "skey1",
                (tnow + timedelta(seconds=2 + _DEFAULT_ADVISORY_REFRESH_TIMEOUT)).isoformat(),
                "key1",
            ),
            "status_code": 200,
        },
        {
            "text": s3_assume_role_response(
                "token2",
                "skey2",
                (tnow + timedelta(seconds=4 + _DEFAULT_ADVISORY_REFRESH_TIMEOUT)).isoformat(),
                "key2",
            ),
            "status_code": 200,
        },
        {
            "text": s3_assume_role_response(
                "token3",
                "skey3",
                (tnow + timedelta(seconds=6 + _DEFAULT_ADVISORY_REFRESH_TIMEOUT)).isoformat(),
                "key3",
            ),
            "status_code": 200,
        },
    ]


def test_s3_credentials_provider(test_context_mock, mocker: MockerFixture):
    with freeze_time("0s"):
        test_context_mock.mock_adapter.register_uri(
            "POST",
            TEST_HOST.url + "/io/s3",
            response_list=mock_generate_credentials(),
        )
        assert test_context_mock.s3._get_boto3_session().get_credentials().access_key == "key1"
    with freeze_time("1s"):
        assert test_context_mock.s3._get_boto3_session().get_credentials().access_key == "key1"
    # botocore checks with >= we need to be a bit above the expiry time
    with freeze_time("2.1s"):
        assert test_context_mock.s3._get_boto3_session().get_credentials().access_key == "key2"
    with freeze_time("3s"):
        assert test_context_mock.s3._get_boto3_session().get_credentials().access_key == "key2"
    with freeze_time("4.1s"):
        assert test_context_mock.s3._get_boto3_session().get_credentials().access_key == "key3"
    with freeze_time("5s"):
        assert test_context_mock.s3._get_boto3_session().get_credentials().access_key == "key3"


async def test_async_s3_credentials_provider(test_context_mock, mocker: MockerFixture):
    with freeze_time("0s"):
        test_context_mock.mock_adapter.register_uri(
            "POST",
            TEST_HOST.url + "/io/s3",
            response_list=mock_generate_credentials(),
        )
        assert (
            await (await test_context_mock.s3._get_aiobotocore_session().get_credentials()).get_frozen_credentials()
        ).access_key == "key1"
    with freeze_time("1s"):
        assert (
            await (await test_context_mock.s3._get_aiobotocore_session().get_credentials()).get_frozen_credentials()
        ).access_key == "key1"
    # botocore checks with >= we need to be a bit above the expiry time
    with freeze_time("2.1s"):
        assert (
            await (await test_context_mock.s3._get_aiobotocore_session().get_credentials()).get_frozen_credentials()
        ).access_key == "key2"
    with freeze_time("3s"):
        assert (
            await (await test_context_mock.s3._get_aiobotocore_session().get_credentials()).get_frozen_credentials()
        ).access_key == "key2"
    with freeze_time("4.1s"):
        assert (
            await (await test_context_mock.s3._get_aiobotocore_session().get_credentials()).get_frozen_credentials()
        ).access_key == "key3"
    with freeze_time("5s"):
        assert (
            await (await test_context_mock.s3._get_aiobotocore_session().get_credentials()).get_frozen_credentials()
        ).access_key == "key3"


def test_parse_s3_credentials_response():
    # taken from https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html
    access_key = "ASgeIAIOSFODNN7EXAMPLE"
    token = "AQoDYXdzEE0a8ANXXXXXXXXNO1ewxE5TijQyp+IEXAMPLE"  # noqa: S105
    expiry = "2014-10-24T23:00:23Z"
    secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY"  # noqa: S105
    example_response = s3_assume_role_response(token, secret_key, expiry, access_key)
    parsed = parse_s3_credentials_response(example_response)
    assert parsed["access_key"] == access_key
    assert parsed["token"] == token
    assert parsed["expiry_time"] == expiry
    assert parsed["secret_key"] == secret_key
