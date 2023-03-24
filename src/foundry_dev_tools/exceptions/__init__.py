"""
General modules for foundry_dev_tools exceptions.
"""

from typing import Optional

import requests

from .generic import FoundryAPIError


class BranchNotFoundError(FoundryAPIError):
    """Exception is thrown when no transaction exists for a dataset in a specific branch."""

    def __init__(
        self,
        dataset_rid: str,
        branch: str,
        transaction_rid: Optional[str] = None,
        response: Optional[requests.Response] = None,
    ):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset_rid for the branch that does not exist
             transaction_rid (str): transaction_rid if available
             branch (Optional[str]): dataset branch
             response (Optional[requests.Response]): requests response if available
        """
        super().__init__(
            f"Dataset {dataset_rid} "
            + (
                f"on transaction {transaction_rid}"
                if transaction_rid is not None
                else ""
            )
            + " has no branch {branch}.\n"
            + (response.text if response is not None else "")
        )
        self.dataset_rid = dataset_rid
        self.branch = branch
        self.transaction_rid = transaction_rid
        self.response = response


class BranchesAlreadyExistError(FoundryAPIError):
    """Exception is thrown when branch exists already."""

    def __init__(
        self,
        dataset_rid: str,
        branch_rid: str,
        response: Optional[requests.Response] = None,
    ):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset_rid for the branch that already exists
             branch_rid (str): dataset branch which already exists
             response (Optional[requests.Response]): requests response if available
        """
        super().__init__(
            f"Branch {branch_rid} already exists in {dataset_rid}.\n"
            + (response.text if response is not None else "")
        )
        self.dataset_rid = dataset_rid
        self.branch_rid = branch_rid
        self.response = response


class FolderNotFoundError(FoundryAPIError):
    """Exception is thrown when compass folder does not exist."""

    def __init__(self, folder_rid: str, response: Optional[requests.Response] = None):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             folder_rid (str): folder_rid which can't be found
             response (Optional[requests.Response]): requests response if available
        """
        super().__init__(
            f"Compass folder {folder_rid} not found; "
            f"If you are sure your folder_rid is correct, "
            f"and you have access, check if your jwt token "
            f"is still valid!\n" + (response.text if response is not None else "")
        )
        self.folder_rid = folder_rid
        self.response = response


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
