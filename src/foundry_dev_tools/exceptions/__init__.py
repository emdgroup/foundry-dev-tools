"""
General modules for foundry_dev_tools exceptions.
"""

from typing import AnyStr, IO, Iterator, List, Optional, Tuple, Union

import requests


class FoundryDevToolsError(Exception):
    """Metaclass for :class:`FoundryAPIError` and :class:`foundry_dev_tools.fsspec_impl.FoundryFileSystemError`.

    Catch all foundry_dev_tools errors:

    >>> try:
    >>>     fun() # raise DatasetNotFoundError or any other
    >>> except FoundryDevToolsError:
    >>>     print("Some foundry_dev_tools error")

    """


class FoundryAPIError(FoundryDevToolsError):
    """Parent class for all foundry api errors.

    Some children of this Error can take arguments
    which can later be used in an except block e.g.:

    >>> try:
    >>>     abcd() # raises DatasetNotFoundError
    >>> except DatasetNotFoundError as e:
    >>>     print(e.dataset_rid)

    Also, all "child" errors can be catched with this parent class e.g.:

    >>> try:
    >>>     abcd() # could raise DatasetHasNoSchemaError or DatasetNotFoundError
    >>> except FoundryAPIError as e:
    >>>     print(e.dataset_rid)

    """


class DatasetHasNoSchemaError(FoundryAPIError):
    """Exception is thrown when dataset has no associated schema."""

    def __init__(
        self,
        dataset_rid: str,
        transaction_rid: Optional[str] = None,
        branch: Optional[str] = None,
        response: Optional[requests.Response] = None,
    ):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset which has no schema
             transaction_rid (Optional[str]): transaction_rid if available
             branch (Optional[str]): dataset branch if available
             response (Optional[requests.Response]): requests response if available
        """
        super().__init__(
            f"Dataset {dataset_rid} "
            + (
                f"on transaction {transaction_rid} "
                if transaction_rid is not None
                else ""
            )
            + (f"on branch {branch} " if branch is not None else "")
            + "has no schema.\n"
            + (response.text if response is not None else "")
        )
        self.dataset_rid = dataset_rid
        self.transaction_rid = transaction_rid
        self.branch = branch
        self.response = response


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


class DatasetNotFoundError(FoundryAPIError):
    """Exception is thrown when dataset does not exist."""

    def __init__(self, dataset_rid: str, response: Optional[requests.Response] = None):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset which can't be found
             response (Optional[requests.Response]): requests response if available
        """
        super().__init__(
            f"Dataset {dataset_rid} not found.\n"
            + (response.text if response is not None else "")
        )
        self.dataset_rid = dataset_rid
        self.response = response


class DatasetAlreadyExistsError(FoundryAPIError):
    """Exception is thrown when dataset already exists."""

    def __init__(self, dataset_rid: str):
        super().__init__(f"Dataset {dataset_rid} already exists.")


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


class DatasetHasNoTransactionsError(FoundryAPIError):
    """Exception is thrown when dataset has no transactions."""

    def __init__(self, dataset_rid: str, response: Optional[requests.Response] = None):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset which has no transactions
             response (Optional[requests.Response]): requests response if available
        """
        super().__init__(
            f"Dataset {dataset_rid} has no transactions.\n"
            + (response.text if response is not None else "")
        )
        self.dataset_rid = dataset_rid
        self.response = response


class DatasetNoReadAccessError(FoundryAPIError):
    """Exception is thrown when user is missing 'gatekeeper:view-resource' on the dataset,
    which normally comes with the Viewer role."""

    def __init__(self, dataset_rid: str, response: Optional[requests.Response] = None):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset which can't be read
             response (Optional[requests.Response]): requests response if available
        """
        super().__init__(
            f"No read access to dataset {dataset_rid}.\n"
            + (response.text if response is not None else "")
        )
        self.dataset_rid = dataset_rid
        self.response = response


class DatasetHasOpenTransactionError(FoundryAPIError):
    """Exception is thrown when dataset has an open transaction already."""

    def __init__(
        self,
        dataset_rid: str,
        open_transaction_rid: str,
        response: Optional[requests.Response] = None,
    ):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset which has an open transaction
             open_transaction_rid (str): transaction_rid which is open
             response (Optional[requests.Response]): requests response if available
        """
        super().__init__(
            f"Dataset {dataset_rid} already has open transaction {open_transaction_rid}.\n"
            + (response.text if response is not None else "")
        )
        self.dataset_rid = dataset_rid
        self.open_transaction_rid = open_transaction_rid
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
