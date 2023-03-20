from foundry_dev_tools.Exceptions.FoundryDevToolsError import FoundryAPIError
from typing import Optional
import requests


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
    """Exception is thrown when user is missing 'compass:view' on the dataset."""

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
