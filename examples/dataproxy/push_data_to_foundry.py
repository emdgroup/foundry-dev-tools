from foundry_dev_tools import FoundryRestClient, Exceptions
from foundry_dev_tools.Exceptions.DataProxyExceptions import DatasetExceptions

datasetRid = 'ri.foundry.main.dataset.c520b5e1-bc06-438b-8388-34a410621629'


def test_transaction(datasetRid):
    """
    Try to open add a file to a dataset
    """

    transactionRid = None
    f_client = FoundryRestClient()
    # first open a transaction or get the already open one
    try:
        # something
        transactionRid = f_client.open_transaction(datasetRid)
        print(f'\
    Transaction opened sucessfully: \n\
        datasetRid:         {datasetRid} \n\
        openTransactionRid: {transactionRid}\n')
    except DatasetExceptions.DatasetHasOpenTransactionError as e:

        print(
            f"\
    Transaction already open: \n\
        datasetRid:         {e.dataset_rid} \n\
        openTransactionRid: {e.open_transaction_rid} \n\
        response:           {e.response}\n\
    ")
        transactionRid = e.open_transaction_rid
    except Exception as e:
        print("something went wrong", e)

    file = 'file.csv'
    try:
        f_client.upload_dataset_file(
            datasetRid, transactionRid, file, file)
        print(
            f"\
    File uploaded sucessfully: \n\
        datasetRid:         {datasetRid} \n\
        openTransactionRid: {transactionRid} \n\
        file:               {file}\n\
    ")
    except Exception as e:
        print("Something went wrong", e)

    if transactionRid == None:
        return

    try:
        f_client.commit_transaction(datasetRid, transactionRid)
        print(f"\
    Transaction was closed successfully: \n\
        datasetRid:         {datasetRid} \n\
        openTransactionRid: {transactionRid}\n")
    except Exception as e:
        print("Something went wrong", e)


# test the function
test_transaction(datasetRid)
