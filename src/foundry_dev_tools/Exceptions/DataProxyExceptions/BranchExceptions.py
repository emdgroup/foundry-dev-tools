from foundry_dev_tools.Exceptions.FoundryDevToolsError import FoundryAPIError
from typing import Optional
import requests


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
