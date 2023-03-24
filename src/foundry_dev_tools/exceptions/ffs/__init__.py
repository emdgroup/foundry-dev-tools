"""
Module for Foundry Filesystem exceptions.
"""
from ..generic import FoundryFileSystemError


class FoundrySimultaneousOpenTransactionError(FoundryFileSystemError):
    # pylint: disable=missing-class-docstring
    def __init__(self, dataset_rid: str, open_transaction_rid):
        super().__init__(
            f"Dataset {dataset_rid} already has open transaction {open_transaction_rid}"
        )
        self.dataset_rid = dataset_rid
        self.open_transaction_rid = open_transaction_rid


class FoundryDeleteInNotSupportedTransactionError(FoundryFileSystemError):
    # pylint: disable=missing-class-docstring
    def __init__(self, path: str):
        super().__init__(
            f"You are trying to delete the file(s) {path} while an UPDATE/SNAPSHOT transaction is open. "
            f"Deleting this file(s) is not supported since it was committed in a previous transaction."
        )
        self.path = path


class FoundryDatasetPathInUrlNotSupportedError(FoundryFileSystemError):
    # pylint: disable=missing-class-docstring
    def __init__(self):
        super().__init__(
            "Using the dataset path in the fsspec url is not supported. Please pass the dataset rid, e.g. "
            "foundry://ri.foundry.main.dataset.aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/<file-path-in-dataset>"
        )
