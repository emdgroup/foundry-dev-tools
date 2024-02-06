import pandas as pd
import pytest
from pandas._testing import assert_frame_equal

from foundry_dev_tools.cached_foundry_client import CachedFoundryClient
from tests.integration.conftest import TEST_SINGLETON


@pytest.fixture()
def append_test_dataset():
    return TEST_SINGLETON.generic_upload_dataset_if_not_exists("append_test_dataset_v1")[0]


@pytest.mark.integration()
def test_cached_foundry_client_append(append_test_dataset):
    cfc = CachedFoundryClient()

    df = pd.DataFrame(data={"Name": ["max"]})

    rid1, transaction1 = cfc.save_dataset(
        df,
        dataset_path_or_rid=append_test_dataset.rid,
        branch="master",
        exists_ok=True,
        mode="SNAPSHOT",
    )

    df2 = pd.DataFrame(data={"Name": ["moritz"]})

    rid2, transaction2 = cfc.save_dataset(
        df2,
        dataset_path_or_rid=append_test_dataset.rid,
        branch="master",
        exists_ok=True,
        mode="APPEND",
    )

    path, _ = cfc.fetch_dataset(append_test_dataset.rid)

    df_returned = pd.read_parquet(path)

    assert_frame_equal(
        pd.concat([df, df2]).sort_values(by=["Name"]).reset_index(drop=True),
        df_returned.sort_values(by=["Name"]).reset_index(drop=True),
    )
