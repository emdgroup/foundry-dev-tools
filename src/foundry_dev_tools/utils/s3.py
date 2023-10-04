"""Custom foundry boto3 credential provider used by the FoundryRestClient."""
from __future__ import annotations

from typing import TYPE_CHECKING

import botocore.client
import botocore.credentials
import botocore.session

if TYPE_CHECKING:
    from foundry_dev_tools import FoundryRestClient


class CustomFoundryCredentialProvider(botocore.credentials.CredentialProvider):
    """Boto3 credential provider for s3 credentials."""

    METHOD = "foundry"
    CANONICAL_NAME = "foundry"

    def __init__(
        self,
        foundry_rest_client: FoundryRestClient,
        session: botocore.session.Session | None = None,
    ):
        self.foundry_rest_client = foundry_rest_client
        super().__init__(session)

    def load(self):
        """Return the credentials from FoundryRestClient."""
        return botocore.credentials.DeferredRefreshableCredentials(
            self.foundry_rest_client.get_s3_credentials, method="sts-assume-role"
        )
