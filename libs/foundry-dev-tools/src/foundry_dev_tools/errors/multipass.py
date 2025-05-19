"""Multipass specific errors."""

import requests

from foundry_dev_tools.errors.meta import FoundryAPIError


class DuplicateGroupNameError(FoundryAPIError):
    """Exception is thrown when the group name already exists."""

    message = "The group name already exists!"


class ClientAuthenticationFailedError(FoundryAPIError):
    """Exception is thrown when client credentials grant failed."""

    def __init__(
        self, response: requests.Response | None = None, info: str | None = None, client_id: str | None = None
    ):
        self.client_id = client_id
        self.message = "Client authentication failed (invalid_client)."

        super().__init__(response=response, info=info, client_id=client_id)
