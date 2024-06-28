import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.testing import assertDataFrameEqual

from foundry_dev_tools.errors.compass import ResourceNotFoundError
from foundry_dev_tools.errors.dataset import (
    DatasetHasNoOpenTransactionError,
    DatasetNotFoundError,
    TransactionTypeMismatchError,
)
from foundry_dev_tools.resources.dataset import Dataset
from foundry_dev_tools.utils import api_types
from tests.integration.conftest import TEST_SINGLETON
from tests.integration.utils import INTEGRATION_TEST_COMPASS_ROOT_PATH


def test_crud_dataset(spark_session, tmp_path):  # noqa: PLR0915
    # check does not exist
    with pytest.raises(ResourceNotFoundError):
        ds = Dataset.from_path(TEST_SINGLETON.ctx, INTEGRATION_TEST_COMPASS_ROOT_PATH + "/test_create_dataset")

    # create
    ds = Dataset.from_path(
        TEST_SINGLETON.ctx, INTEGRATION_TEST_COMPASS_ROOT_PATH + "/test_create_dataset", create_if_not_exist=True
    )
    assert ds.__created__

    # test getting the dataset from_rid
    ds2 = Dataset.from_rid(TEST_SINGLETON.ctx, ds.rid)
    assert ds2.path == ds.path

    assert ds.get_last_transaction() is None

    ds.start_transaction()
    assert ds.transaction is not None
    assert ds.transaction == ds.get_open_transaction()
    assert ds.transaction == ds.get_transactions(1, include_open_exclusive_transaction=True)[0]
    assert ds.transaction["status"] == "OPEN"
    assert ds.transaction["type"] == "APPEND"

    cur_transaction = ds.transaction

    ds.put_file("test.bin", b"")
    ds.commit_transaction()
    with pytest.raises(DatasetHasNoOpenTransactionError):
        _ = ds.transaction

    assert ds.get_last_transaction()["rid"] == cur_transaction["rid"]

    assert len(ds.list_files()) == 1

    ds.put_file("test.bin", b"1", transaction_type=api_types.FoundryTransaction.UPDATE)
    with pytest.raises(DatasetHasNoOpenTransactionError):
        _ = ds.transaction

    test_bin = ds.get_file("test.bin")

    assert test_bin == b"1"

    with ds.transaction_context(transaction_type=api_types.FoundryTransaction.DELETE):
        remove_transaction = ds.transaction
        ds.delete_files(["test.bin"])

    with pytest.raises(DatasetHasNoOpenTransactionError):
        _ = ds.transaction
    assert ds.get_last_transaction()["rid"] == remove_transaction["rid"]

    assert len(ds.list_files()) == 0

    # test abort_on_error
    with pytest.raises(Exception):  # noqa: SIM117,B017,PT012,PT011
        with ds.transaction_context():
            _ = ds.transaction
            raise Exception  # noqa: TRY002

    last_transaction = ds.get_last_transaction()
    assert last_transaction["rid"] == remove_transaction["rid"]

    # test save_dataframe
    df = pd.DataFrame({"a": [0], "b": [1]})
    pd_spark_df = pd.DataFrame({"a": [1], "b": [0]})
    spark_df = spark_session.createDataFrame(pd_spark_df)  # create slightly different dataframe

    ds.save_dataframe(df)
    assert_frame_equal(df, ds.to_pandas())

    ds_spark_branch = TEST_SINGLETON.ctx.get_dataset(ds.rid, branch="spark")
    ds_spark_branch.save_dataframe(spark_df)
    assertDataFrameEqual(ds_spark_branch.to_spark(), spark_df)

    ds.start_transaction(start_transaction_type=api_types.FoundryTransaction.DELETE)

    with pytest.raises(TransactionTypeMismatchError):  # noqa: SIM117
        with ds.transaction_context(transaction_type=api_types.FoundryTransaction.APPEND):
            pass

    to_up = tmp_path.joinpath("to_upload")
    to_up_sub = to_up.joinpath("sub")
    to_up_sub.mkdir(parents=True)

    to_up.joinpath("upload.bin").write_bytes(b"upload")
    to_up_sub.joinpath("upload_sub.bin").write_bytes(b"upload_sub")

    upload_ds = TEST_SINGLETON.ctx.get_dataset(ds.rid, branch="upload")

    files = upload_ds.upload_folder(to_up, max_workers=0).list_files()
    assert sorted([x["logicalPath"] for x in files]) == ["sub/upload_sub.bin", "upload.bin"]

    # # clean up
    ds.add_to_trash().delete_permanently()

    # # check that deletion was successful
    with pytest.raises(DatasetNotFoundError):
        ds.sync()
