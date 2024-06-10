"""Foundry SQL specific custom exceptions."""

from __future__ import annotations

from foundry_dev_tools.errors.meta import FoundryAPIError


class FoundrySqlQueryFailedError(FoundryAPIError):
    """Exception is thrown when SQL Query execution failed."""

    message = "Foundry SQL Query Failed."


class FoundrySqlQueryClientTimedOutError(FoundryAPIError):
    """Exception is thrown when the Query execution time exceeded the client timeout value."""

    message = "The client timeout has been reached"


class FoundrySqlSerializationFormatNotImplementedError(FoundryAPIError):
    """Exception is thrown when the Query results are not sent in arrow ipc format."""

    message = "Serialization formats other than arrow ipc not implemented in Foundry DevTools."
