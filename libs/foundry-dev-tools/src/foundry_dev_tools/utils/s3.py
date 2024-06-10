"""Custom foundry boto3 credential provider used by the FoundryRestClient."""

from __future__ import annotations

from typing import TYPE_CHECKING

import botocore.client
import botocore.credentials
import botocore.session

if TYPE_CHECKING:
    from foundry_dev_tools.clients.s3_client import S3Client


class CustomFoundryCredentialProvider(botocore.credentials.CredentialProvider):
    """Boto3 credential provider for s3 credentials."""

    METHOD = "foundry"
    CANONICAL_NAME = "foundry"

    def __init__(
        self,
        s3_client: S3Client,
        session: botocore.session.Session | None = None,
    ):
        self.s3_client = s3_client
        super().__init__(session)

    def load(self) -> botocore.credentials.DeferredRefreshableCredentials:
        """Return the credentials from FoundryRestClient."""
        return botocore.credentials.DeferredRefreshableCredentials(
            self.s3_client.get_credentials,
            method="sts-assume-role",
        )


def parse_s3_credentials_response(requests_response_text: str) -> dict:
    """Parses the AssumeRoleWithWebIdentity XML response."""
    return {
        "access_key": requests_response_text[
            requests_response_text.find("<AccessKeyId>") + len("<AccessKeyId>") : requests_response_text.rfind(
                "</AccessKeyId>",
            )
        ],
        "secret_key": requests_response_text[
            requests_response_text.find("<SecretAccessKey>") + len("<SecretAccessKey>") : requests_response_text.rfind(
                "</SecretAccessKey>",
            )
        ],
        "token": requests_response_text[
            requests_response_text.find("<SessionToken>") + len("<SessionToken>") : requests_response_text.rfind(
                "</SessionToken>",
            )
        ],
        "expiry_time": requests_response_text[
            requests_response_text.find("<Expiration>") + len("<Expiration>") : requests_response_text.rfind(
                "</Expiration>",
            )
        ],
    }
