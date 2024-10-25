"""Foundry SQL specific custom exceptions."""

from __future__ import annotations

from typing import TYPE_CHECKING

from foundry_dev_tools.errors.meta import FoundryAPIError

if TYPE_CHECKING:
    import requests


class FoundrySqlQueryFailedError(FoundryAPIError):
    """Exception is thrown when SQL Query execution failed."""

    message = "Foundry SQL Query Failed."

    def __init__(self, response: requests.Response):
        self.error_message = response.json().get("status", {}).get("failed", {}).get("errorMessage", "")
        super().__init__(response=response, info=self.error_message)


class FoundrySqlQueryClientTimedOutError(FoundryAPIError):
    """Exception is thrown when the Query execution time exceeded the client timeout value."""

    message = "The client timeout has been reached"


class FoundrySqlSerializationFormatNotImplementedError(FoundryAPIError):
    """Exception is thrown when the Query results are not sent in arrow ipc format."""

    message = "Serialization formats other than arrow ipc not implemented in Foundry DevTools."
