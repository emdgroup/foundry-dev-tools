"""Async custom foundry credential provider for aiobotocore.

Basically the same as :py:mod:`foundry_dev_tools.utils.s3`, except that it's async
this was needed for S3Fs to work, which is used by pandas.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import aiobotocore.credentials
import aiobotocore.session
import botocore.client
import botocore.credentials
import botocore.session

if TYPE_CHECKING:
    from foundry_dev_tools.clients.s3_client import S3Client


class CustomAsyncFoundryCredentialProvider(botocore.credentials.SharedCredentialProvider):
    """Boto3 credential provider for s3 credentials."""

    METHOD = "foundry"
    CANONICAL_NAME = "foundry"

    def __init__(
        self,
        s3_client: S3Client,
        session: aiobotocore.session.Session | None = None,
    ):
        self.s3_client = s3_client
        super().__init__(session)

    async def load(self) -> aiobotocore.credentials.AioDeferredRefreshableCredentials:
        """Return the credentials from FoundryRestClient."""
        return aiobotocore.credentials.AioDeferredRefreshableCredentials(
            self.s3_client.get_credentials,
            method="sts-assume-role",
        )
