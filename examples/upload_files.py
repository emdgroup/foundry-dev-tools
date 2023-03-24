"""
Example of how to upload files to a dataset
"""
import requests

from foundry_dev_tools import FoundryRestClient
from foundry_dev_tools.exceptions import DatasetHasOpenTransactionError

DATASET_RID = "ri.foundry.main.dataset.c520b5e1-bc06-438b-8388-34a410621629"


def test_transaction(dataset_rid):
    """
    Try to open add a file to a dataset
    """

    transaction_rid = None
    f_client = FoundryRestClient()
    # first open a transaction or get the already open one
    try:
        # something
        transaction_rid = f_client.open_transaction(dataset_rid)
        print(
            f"\
    Transaction opened sucessfully: \n\
        dataset_rid:         {dataset_rid} \n\
        openTransaction_rid: {transaction_rid}\n"
        )
    except DatasetHasOpenTransactionError as error:

        print(
            f"\
    Transaction already open: \n\
        dataset_rid:         {error.dataset_rid} \n\
        openTransaction_rid: {error.open_transaction_rid} \n\
        response:           {error.response}\n\
    "
        )
        transaction_rid = error.open_transaction_rid
    except requests.exceptions.ConnectionError as error:
        print("Connection error:", error)
        return
    except Exception as error:  # pylint: disable=W0718
        print("something went wrong", error)

    file = "file.csv"
    try:
        f_client.upload_dataset_file(dataset_rid, transaction_rid, file, file)
        print(
            f"\
    File uploaded sucessfully: \n\
        dataset_rid:         {dataset_rid} \n\
        openTransaction_rid: {transaction_rid} \n\
        file:               {file}\n\
    "
        )
    except Exception as error:  # pylint: disable=W0718
        print("Something went wrong", error)

    if transaction_rid is None:
        return

    try:
        f_client.commit_transaction(dataset_rid, transaction_rid)
        print(
            f"\
    Transaction was closed successfully: \n\
        dataset_rid:         {dataset_rid} \n\
        openTransaction_rid: {transaction_rid}\n"
        )
    except Exception as error:  # pylint: disable=W0718
        print("Something went wrong", error)


# test the function
test_transaction(DATASET_RID)
