from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import requests_mock

# needed to offset the mocked expiry times
from botocore.credentials import (
    _DEFAULT_ADVISORY_REFRESH_TIMEOUT,
)
from freezegun import freeze_time

from foundry_dev_tools.config.token_provider import CachedTokenProvider
from foundry_dev_tools.utils.s3 import parse_s3_credentials_response
from tests.unit.mocks import TEST_HOST, FoundryMockContext, MockTokenProvider, make_jwt

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


def _jwt_context(test_context_mock: FoundryMockContext) -> FoundryMockContext:
    """Return a context whose token is a JWT: the S3 client needs its expiry."""
    return FoundryMockContext(
        config=test_context_mock.config,
        token_provider=MockTokenProvider(jwt=make_jwt(time.time() + 3600)),
        mock_adapter=test_context_mock.mock_adapter,
    )


def test_s3_credentials_provider(test_context_mock):
    with freeze_time("0s"):
        context = _jwt_context(test_context_mock)
        context.mock_adapter.register_uri(
            "POST",
            TEST_HOST.url + "/io/s3",
            response_list=mock_generate_credentials(),
        )
        assert context.s3._get_boto3_session().get_credentials().access_key == "key1"
    with freeze_time("1s"):
        assert context.s3._get_boto3_session().get_credentials().access_key == "key1"
    # botocore checks with >= we need to be a bit above the expiry time
    with freeze_time("2.1s"):
        assert context.s3._get_boto3_session().get_credentials().access_key == "key2"
    with freeze_time("3s"):
        assert context.s3._get_boto3_session().get_credentials().access_key == "key2"
    with freeze_time("4.1s"):
        assert context.s3._get_boto3_session().get_credentials().access_key == "key3"
    with freeze_time("5s"):
        assert context.s3._get_boto3_session().get_credentials().access_key == "key3"


async def test_async_s3_credentials_provider(test_context_mock):
    with freeze_time("0s"):
        context = _jwt_context(test_context_mock)
        context.mock_adapter.register_uri(
            "POST",
            TEST_HOST.url + "/io/s3",
            response_list=mock_generate_credentials(),
        )
        assert (
            await (await context.s3._get_aiobotocore_session().get_credentials()).get_frozen_credentials()
        ).access_key == "key1"
    with freeze_time("1s"):
        assert (
            await (await context.s3._get_aiobotocore_session().get_credentials()).get_frozen_credentials()
        ).access_key == "key1"
    # botocore checks with >= we need to be a bit above the expiry time
    with freeze_time("2.1s"):
        assert (
            await (await context.s3._get_aiobotocore_session().get_credentials()).get_frozen_credentials()
        ).access_key == "key2"
    with freeze_time("3s"):
        assert (
            await (await context.s3._get_aiobotocore_session().get_credentials()).get_frozen_credentials()
        ).access_key == "key2"
    with freeze_time("4.1s"):
        assert (
            await (await context.s3._get_aiobotocore_session().get_credentials()).get_frozen_credentials()
        ).access_key == "key3"
    with freeze_time("5s"):
        assert (
            await (await context.s3._get_aiobotocore_session().get_credentials()).get_frozen_credentials()
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


STS_TOKEN = "sts-token"  # noqa: S105
STS_SECRET = "sts-secret"  # noqa: S105


class _CachedProvider(CachedTokenProvider):
    """A cached token provider whose expiry and refresh count the test controls."""

    def __init__(self, token: str, expires_at: float) -> None:
        super().__init__(TEST_HOST)
        self._cached = token
        self._valid_until = expires_at
        self.refreshes = 0

    def _request_token(self) -> tuple[str, float]:
        self.refreshes += 1
        self._cached = make_jwt(time.time() + 3600)
        self._valid_until = time.time() + 3600
        return self._cached, self._valid_until


def _s3_context(provider: CachedTokenProvider) -> FoundryMockContext:
    context = FoundryMockContext(token_provider=provider, mock_adapter=requests_mock.Adapter())
    assert context.mock_adapter is not None
    context.mock_adapter.register_uri(
        "POST",
        TEST_HOST.url + "/io/s3",
        text=s3_assume_role_response(
            STS_TOKEN,
            STS_SECRET,
            (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
            "key",
        ),
    )
    context.s3._credentials = {"access_key": "stale"}
    context.s3._credentials_expiration = time.time() + 60
    return context


def test_get_credentials_renews_a_token_near_expiry(mocker: MockerFixture) -> None:
    with freeze_time("0s"):
        now = time.time()
        provider = _CachedProvider(make_jwt(now + 600), now + 600)
        context = _s3_context(provider)
        refresh = mocker.spy(provider, "refresh_if_possible")

        credentials = context.s3.get_credentials()

        assert refresh.call_count == 1
        assert provider.refreshes == 1
        assert credentials["access_key"] == "key"


def test_get_credentials_keeps_a_token_with_enough_lifetime(mocker: MockerFixture) -> None:
    with freeze_time("0s"):
        now = time.time()
        provider = _CachedProvider(make_jwt(now + 3600), now + 3600)
        context = _s3_context(provider)
        refresh = mocker.spy(provider, "refresh_if_possible")

        credentials = context.s3.get_credentials()

        assert refresh.call_count == 0
        assert provider.refreshes == 0
        assert credentials["access_key"] == "key"
