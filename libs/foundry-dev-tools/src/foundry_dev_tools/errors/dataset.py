"""Foundry error classes for Dataset related errors."""

from __future__ import annotations

from foundry_dev_tools.errors.compass import ResourceNotFoundError
from foundry_dev_tools.errors.meta import FoundryAPIError, FoundryDevToolsError


class DatasetHasNoOpenTransactionError(FoundryDevToolsError):
    """Raised when trying to access transaction property without an open transaction."""

    def __init__(self, dataset: str):
        super().__init__(f"Dataset ({dataset}) has no open transactions.")


class DatasetHasNoSchemaError(FoundryAPIError):
    """Exception is thrown when dataset has no associated schema."""

    message = "Dataset has no schema."


class BranchNotFoundError(FoundryAPIError):
    """Exception is thrown when no transaction exists for a dataset in a specific branch."""

    message = "Branch not found in dataset."


class BranchesAlreadyExistError(FoundryAPIError):
    """Exception is thrown when branch exists already."""

    message = "Branch already exists."


class DatasetNotFoundError(ResourceNotFoundError):
    """Exception is thrown when dataset does not exist."""

    message = "Dataset not found."


class DatasetAlreadyExistsError(FoundryAPIError):
    """Exception is thrown when dataset already exists."""

    message = "Dataset already exists."


class DatasetHasNoTransactionsError(FoundryAPIError):
    """Exception is thrown when dataset has no transactions."""

    message = "Dataset has no transactions."


class DatasetNoReadAccessError(FoundryDevToolsError):
    """Exception is thrown when user is missing 'gatekeeper:view-resource' on the dataset, which normally comes with the Viewer role."""  # noqa: E501

    def __init__(self, dataset: str):
        super().__init__(f"No read access to dataset {dataset}.")


class DatasetHasOpenTransactionError(FoundryAPIError):
    """Exception is thrown when dataset has an open transaction already."""

    message = "Dataset already has an open transaction."


class TransactionTypeMismatchError(FoundryAPIError):
    """Exception thrown if there is an open transaction with a different transaction type."""

    message = "There is an open transaction with a different transaction type than you requested."
