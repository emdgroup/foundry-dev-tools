from foundry_dev_tools.Exceptions.FoundryDevToolsError import FoundryAPIError
from typing import Optional
import requests


class FoundrySqlQueryFailedError(FoundryAPIError):
    """Exception is thrown when SQL Query execution failed."""

    def __init__(self, response: Optional[requests.Response] = None):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             response (Optional[requests.Response]): requests response if available
        """
        super().__init__(
            "Foundry SQL Query Failed\n"
            + (response.text if response is not None else "")
        )
        self.response = response


class FoundrySqlQueryClientTimedOutError(FoundryAPIError):
    """Exception is thrown when the Query execution time exceeded the client timeout value."""

    def __init__(self, timeout: int):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             timeout (int): value of the reached timeout
        """
        super().__init__(f"The client timeout value of {timeout} has " f"been reached")
        self.timeout = timeout


class FoundrySqlSerializationFormatNotImplementedError(FoundryAPIError):
    """Exception is thrown when the Query results are not sent in arrow ipc format."""

    def __init__(self):
        super().__init__(
            "Serialization formats "
            "other than arrow ipc "
            "not implemented in "
            "Foundry DevTools."
        )
