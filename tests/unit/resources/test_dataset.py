from unittest import mock

import pytest

from foundry_dev_tools.errors.dataset import DatasetHasNoTransactionsError
from foundry_dev_tools.resources.dataset import Dataset


def test_to_lazy_polars_no_transaction():
    with mock.patch.object(Dataset, "__created__", True):
        ds = Dataset.__new__(Dataset)
        ds.rid = "ri.foundry.main.dataset.test-dataset"
        ds.path = "/test/dataset/path"

        with mock.patch.object(ds, "get_last_transaction", return_value=None):
            with pytest.raises(DatasetHasNoTransactionsError) as exc_info:
                ds.to_lazy_polars()

            error_message = str(exc_info.value)
            assert "Dataset has no committed transactions" in error_message
            assert ds.rid in error_message


def test_to_lazy_polars_transaction_rid_logic():
    with mock.patch.object(Dataset, "__created__", True):
        ds = Dataset.__new__(Dataset)
        ds.rid = "ri.foundry.main.dataset.abc123"
        ds._context = mock.MagicMock()
        ds._context.s3.get_polars_storage_options.return_value = {"aws_access_key_id": "test"}

        with mock.patch("foundry_dev_tools._optional.polars.pl.scan_parquet") as mock_scan:
            mock_scan.return_value = mock.MagicMock()
            ds.to_lazy_polars(transaction_rid="test")

            mock_scan.assert_called_once()
            call_args = mock_scan.call_args
            assert call_args[0][0] == f"s3://{ds.rid}.test/**/*.parquet"
            assert call_args[1]["storage_options"] == ds._context.s3.get_polars_storage_options()
            assert call_args[1]["hive_partitioning"] is True
