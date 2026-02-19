from unittest import mock

import pytest

from foundry_dev_tools.errors.dataset import DatasetHasNoTransactionsError
from foundry_dev_tools.resources.dataset import Dataset


def test_to_lazy_polars_no_transaction():
    with mock.patch.object(Dataset, "__created__", True):
        ds = Dataset.__new__(Dataset)
        ds.rid = "ri.foundry.main.dataset.test-dataset"
        ds.path = "/test/dataset/path"

        # Mock get_last_transaction to return None
        with mock.patch.object(ds, "get_last_transaction", return_value=None):
            # Assert that the correct exception is raised with the expected message
            with pytest.raises(DatasetHasNoTransactionsError) as exc_info:
                ds.to_lazy_polars()

            # Verify the error message contains the expected information
            error_message = str(exc_info.value)
            assert "Dataset has no transactions" in error_message
            assert ds.rid in error_message
