"""Foundry SQL specific custom exceptions."""

from __future__ import annotations

from typing import TYPE_CHECKING

from foundry_dev_tools.errors.meta import FoundryAPIError
from foundry_dev_tools.utils.misc import decamelize

if TYPE_CHECKING:
    import requests


class FoundrySqlQueryFailedError(FoundryAPIError):
    """Exception is thrown when SQL Query execution failed."""

    message = "Foundry SQL Query Failed."

    def __init__(self, response: requests.Response, **context_kwargs):
        kwargs = {}
        info = ""

        try:
            response_json = response.json()
            failed_data = response_json.get("status", {}).get("failed", {})

            # Try to extract V2 error structure with rich parameters
            if error_code := failed_data.get("errorCode"):
                kwargs["error_code"] = error_code
            if error_name := failed_data.get("errorName"):
                kwargs["error_name"] = error_name
            if error_instance_id := failed_data.get("errorInstanceId"):
                kwargs["error_instance_id"] = error_instance_id

            # Extract all parameters and convert camelCase to snake_case
            if parameters := failed_data.get("parameters"):
                for key, value in parameters.items():
                    kwargs[decamelize(key)] = value

                # Prefer userFriendlyMessage as the info text
                info = parameters.get("userFriendlyMessage", "")

            # Fall back to V1 errorMessage if userFriendlyMessage not available
            if not info:
                info = failed_data.get("errorMessage", "")

            # Store legacy error_message attribute for backward compatibility
            self.error_message = info

        except Exception:  # noqa: BLE001
            # If any error occurs during extraction, fall back to empty
            self.error_message = ""

        # Merge context kwargs (e.g., query, branch) with extracted error parameters
        kwargs.update(context_kwargs)

        super().__init__(response=response, info=info, **kwargs)


class FurnaceSqlSqlParseError(FoundryAPIError):
    """Exception is thrown when SQL Query is not valid."""

    message = "Foundry SQL Query Parsing Failed."


class FoundrySqlQueryClientTimedOutError(FoundryAPIError):
    """Exception is thrown when the Query execution time exceeded the client timeout value."""

    message = "The client timeout has been reached"


class FoundrySqlSerializationFormatNotImplementedError(FoundryAPIError):
    """Exception is thrown when the Query results are not sent in arrow ipc format."""

    message = "Serialization formats other than arrow ipc not implemented in Foundry DevTools."
