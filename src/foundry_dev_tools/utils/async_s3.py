from __future__ import annotations

from typing import TYPE_CHECKING

import aiobotocore.credentials
import aiobotocore.session
import botocore.client
import botocore.credentials
import botocore.session

if TYPE_CHECKING:
    from foundry_dev_tools import FoundryRestClient


class CustomAsyncFoundryCredentialProvider(
    botocore.credentials.SharedCredentialProvider
):
    """Boto3 credential provider for s3 credentials."""

    METHOD = "foundry"
    CANONICAL_NAME = "foundry"

    def __init__(
        self,
        foundry_rest_client: FoundryRestClient,
        session: aiobotocore.session.Session | None = None,
    ):
        self.foundry_rest_client = foundry_rest_client
        super().__init__(session)

    async def load(self):
        """Return the credentials from FoundryRestClient."""
        # Here you should implement your logic to get the credentials.
        # This is just an example.
        return aiobotocore.credentials.AioDeferredRefreshableCredentials(
            self._refresh, method="sts-assume-role"
        )

    async def _refresh(self):
        return self.foundry_rest_client.get_s3_credentials()
