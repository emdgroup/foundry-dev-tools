import os
from unittest import mock
from unittest.mock import MagicMock, patch

import fs
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession

import foundry_dev_tools.config
from foundry_dev_tools import CachedFoundryClient
from foundry_dev_tools.foundry_api_client import (
    BranchNotFoundError,
    DatasetNotFoundError,
)
from foundry_dev_tools.utils.spark import get_spark_session

from tests.test_foundry_mock import MockFoundryRestClient

DATASET_RID = "ri.foundry.main.dataset.12345de3-b916-46ba-b097-c4326ea4342e"
DATASET_PATH = "/mock/path/ds1"
TRANSACTION_RID = "transaction1"
API = "foundry_dev_tools.FoundryRestClient"


@pytest.fixture()
def mock_client(tmpdir):
    yield MockFoundryRestClient(fs=fs.open_fs(str(tmpdir)))


def test_config():
    fdt = CachedFoundryClient({"jwt": "secret"})

    assert fdt.api._headers()["Authorization"] == "Bearer secret"
    assert fdt.api._requests_verify_value is not None


@patch(
    API + ".get_dataset_identity",
    MagicMock(
        return_value={
            "dataset_rid": DATASET_RID,
            "dataset_path": DATASET_PATH,
            "last_transaction_rid": TRANSACTION_RID,
        }
    ),
)
@patch(API + ".get_dataset", MagicMock())
@patch(API + ".get_branch", MagicMock())
@patch(API + ".is_dataset_in_trash", MagicMock(return_value=False))
@patch(API + ".open_transaction", MagicMock(return_value=TRANSACTION_RID))
@patch(API + ".commit_transaction", MagicMock())
@patch(API + ".infer_dataset_schema", MagicMock())
@patch(API + ".upload_dataset_schema", MagicMock())
@patch(API + ".upload_dataset_files")
def test_save_pandas(upload_dataset_files):
    fdt = CachedFoundryClient()
    dataset_rid, transaction_id = fdt.save_dataset(
        pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]}),
        dataset_path_or_rid=DATASET_PATH,
        branch="master",
        exists_ok=True,
        mode="SNAPSHOT",
    )

    args = upload_dataset_files.call_args[0]
    kwargs = upload_dataset_files.call_args[1]

    assert args[0] == DATASET_RID
    assert args[1] == TRANSACTION_RID
    # 1 file uploaded
    assert len(args[2]) == 1

    assert dataset_rid == "ri.foundry.main.dataset.12345de3-b916-46ba-b097-c4326ea4342e"
    assert transaction_id == "transaction1"


@patch(
    API + ".get_dataset_identity",
    MagicMock(
        return_value={
            "dataset_rid": DATASET_RID,
            "dataset_path": DATASET_PATH,
            "last_transaction_rid": TRANSACTION_RID,
        }
    ),
)
@patch(API + ".get_dataset", MagicMock())
@patch(API + ".get_branch", MagicMock())
@patch(API + ".is_dataset_in_trash", MagicMock(return_value=False))
@patch(API + ".open_transaction", MagicMock(return_value=TRANSACTION_RID))
@patch(API + ".commit_transaction", MagicMock())
@patch(API + ".infer_dataset_schema", MagicMock())
@patch(API + ".upload_dataset_schema", MagicMock())
@patch(API + ".upload_dataset_files")
def test_save_spark(upload_dataset_files):
    fdt = CachedFoundryClient()
    df = (
        SparkSession.builder.master("local[*]")
        .getOrCreate()
        .createDataFrame([[1, 2]], "a:string, b: string")
    )
    dataset_rid, transaction_id = fdt.save_dataset(
        df,
        dataset_path_or_rid=DATASET_PATH,
        branch="master",
        exists_ok=True,
        mode="SNAPSHOT",
    )

    args = upload_dataset_files.call_args[0]
    kwargs = upload_dataset_files.call_args[1]

    assert args[0] == DATASET_RID
    assert args[1] == TRANSACTION_RID
    # at least two files uploaded, one parquet and one _SUCCESS
    assert len(args[2]) >= 2

    assert dataset_rid == "ri.foundry.main.dataset.12345de3-b916-46ba-b097-c4326ea4342e"
    assert transaction_id == "transaction1"


@patch(
    API + ".get_dataset_identity",
    MagicMock(
        return_value={
            "dataset_rid": DATASET_RID,
            "dataset_path": DATASET_PATH,
            "last_transaction_rid": TRANSACTION_RID,
        }
    ),
)
@patch(API + ".get_dataset", MagicMock())
@patch(API + ".get_branch", MagicMock())
@patch(API + ".is_dataset_in_trash", MagicMock(return_value=False))
@patch(API + ".open_transaction", MagicMock(return_value=TRANSACTION_RID))
@patch(API + ".commit_transaction", MagicMock())
@patch(API + ".infer_dataset_schema", MagicMock())
@patch(API + ".upload_dataset_schema", MagicMock())
@patch(API + ".upload_dataset_files")
def test_save_model(upload_dataset_files):
    fdt = CachedFoundryClient()
    dataset_rid, transaction_id = fdt.save_model(
        {"wow": "awesome_ml_model"},
        dataset_path_or_rid=DATASET_PATH,
        branch="master",
        exists_ok=True,
        mode="SNAPSHOT",
    )

    args = upload_dataset_files.call_args[0]
    kwargs = upload_dataset_files.call_args[1]

    assert args[0] == DATASET_RID
    assert args[1] == TRANSACTION_RID
    # one model object should be uploaded
    assert len(args[2]) == 1
    assert "model.pickle" in args[2].keys()
    assert "model.pickle" in args[2]["model.pickle"]

    assert dataset_rid == "ri.foundry.main.dataset.12345de3-b916-46ba-b097-c4326ea4342e"
    assert transaction_id == "transaction1"


@patch(API + ".abort_transaction")
@patch(API + ".create_branch")
@patch(API + ".create_dataset")
@patch(API + ".upload_dataset_files")
@patch(API + ".upload_dataset_schema")
@patch(API + ".infer_dataset_schema")
@patch(API + ".commit_transaction")
@patch(API + ".open_transaction")
@patch(API + ".is_dataset_in_trash")
@patch(API + ".get_branch")
@patch(API + ".get_dataset")
@patch(API + ".get_dataset_identity")
def test_save_objects(
    get_dataset_identity,
    get_dataset,
    get_branch,
    is_dataset_in_trash,
    open_transaction,
    commit_transaction,
    infer_dataset_schema,
    upload_dataset_schema,
    upload_dataset_files,
    create_dataset,
    create_branch,
    abort_transaction,
):
    fdt = CachedFoundryClient()
    path_file_dict = {"path-in/foundry.pickle": os.getcwd()}

    # happy path, dataset does not exist!
    get_dataset_identity.side_effect = DatasetNotFoundError("dataset_rid")
    create_dataset.return_value = {"rid": DATASET_RID}
    is_dataset_in_trash.return_value = False
    open_transaction.return_value = TRANSACTION_RID
    dataset_rid, transaction_id = fdt._save_objects(
        path_file_dict, DATASET_PATH, "master", exists_ok=False, mode="SNAPSHOT"
    )

    assert dataset_rid == "ri.foundry.main.dataset.12345de3-b916-46ba-b097-c4326ea4342e"
    assert transaction_id == "transaction1"
    commit_transaction.assert_called_once()
    abort_transaction.assert_not_called()
    commit_transaction.reset_mock()
    abort_transaction.reset_mock()
    get_dataset_identity.side_effect = None
    get_dataset_identity.return_value = {
        "dataset_rid": DATASET_RID,
        "dataset_path": DATASET_PATH,
    }

    # Error in upload files -> verify abort transaction is called
    upload_dataset_files.side_effect = IOError()
    with pytest.raises(ValueError):
        fdt._save_objects(
            path_file_dict,
            DATASET_PATH,
            "master",
            exists_ok=True,
            mode="SNAPSHOT",
        )
    commit_transaction.assert_not_called()
    abort_transaction.assert_called_once()
    upload_dataset_files.side_effect = None

    # ValueError is thrown when dataset exists already
    with pytest.raises(ValueError):
        fdt._save_objects(
            path_file_dict,
            DATASET_PATH,
            "master",
            exists_ok=False,
            mode="SNAPSHOT",
        )

    # ValueError is thrown when dataset is in trash
    is_dataset_in_trash.return_value = True
    with pytest.raises(ValueError):
        fdt._save_objects(
            path_file_dict,
            DATASET_PATH,
            "master",
            exists_ok=True,
            mode="SNAPSHOT",
        )
    is_dataset_in_trash.return_value = False

    # branch is created when it does not exist
    get_branch.side_effect = BranchNotFoundError("dataset_rid", "branch")
    create_branch.reset_mock()
    fdt._save_objects(
        path_file_dict,
        DATASET_PATH,
        "does-not-exist",
        exists_ok=False,
        mode="SNAPSHOT",
    )
    create_branch.assert_called_once()


@patch(
    API + ".get_dataset_identity",
    MagicMock(
        return_value={
            "dataset_rid": DATASET_RID,
            "dataset_path": DATASET_PATH,
            "last_transaction_rid": TRANSACTION_RID,
        }
    ),
)
@patch(API + ".get_dataset_schema")
@patch(API + ".list_dataset_files")
@patch(API + ".is_dataset_in_trash")
@patch(API + ".get_dataset_last_transaction_rid")
@patch(API + ".get_dataset_rid")
def test_fetch_dataset(
    get_dataset_rid,
    get_dataset_last_transaction_rid,
    is_dataset_in_trash,
    list_dataset_files,
    get_dataset_schema,
    mocker,
):
    from_foundry_and_cache = mocker.spy(
        CachedFoundryClient, "_download_dataset_and_return_local_path"
    )
    from_cache = mocker.spy(CachedFoundryClient, "_return_local_path_of_cached_dataset")
    online = mocker.spy(CachedFoundryClient, "_get_dataset_identity_online")
    offline = mocker.spy(CachedFoundryClient, "_get_dataset_identity_offline")

    get_dataset_rid.return_value = DATASET_RID
    get_dataset_last_transaction_rid.return_value = TRANSACTION_RID
    is_dataset_in_trash.return_value = False
    df = get_spark_session().createDataFrame([[1]], "col1:int")
    get_dataset_schema.return_value = {
        "fieldSchemaList": [
            {"type": "INTEGER", "name": "col1", "nullable": True, "customMetadata": {}}
        ],
        "dataFrameReaderClass": "com.palantir.foundry.spark.input.ParquetDataFrameReader",
        "customMetadata": {"format": "parquet", "options": {}},
    }
    list_dataset_files.return_value = ["spark/dataset.parquet"]

    def download_dataset_files_mock(
        self,
        dataset_rid: str,
        output_directory: str,
        files: list,
        view="master",
    ):
        path = os.sep.join([output_directory, "spark"])
        os.makedirs(path, exist_ok=True)
        df.write.format("parquet").option("compression", "snappy").save(
            path=path, mode="overwrite"
        )

    from foundry_dev_tools import FoundryRestClient

    backup = FoundryRestClient.download_dataset_files
    FoundryRestClient.download_dataset_files = download_dataset_files_mock

    fdt = CachedFoundryClient()
    path, dataset_identity = fdt.fetch_dataset(DATASET_PATH, "master")
    assert path.split(os.sep)[-1] == TRANSACTION_RID + ".parquet"
    assert dataset_identity == {
        "dataset_rid": DATASET_RID,
        "last_transaction_rid": TRANSACTION_RID,
        "dataset_path": DATASET_PATH,
    }

    online.assert_called()
    online.reset_mock()
    offline.assert_not_called()
    offline.reset_mock()

    from_foundry_and_cache.assert_called()
    from_cache.assert_not_called()
    from_foundry_and_cache.reset_mock()
    from_cache.reset_mock()

    path2, dataset_identity2 = fdt.fetch_dataset(DATASET_PATH, "master")
    from_foundry_and_cache.assert_not_called()
    from_cache.assert_called()
    online.reset_mock()
    offline.reset_mock()

    assert path == path2

    # Offline Mode / transforms_freeze_cache = True
    fdt = CachedFoundryClient({"transforms_freeze_cache": True})
    path3, dataset_identity3 = fdt.fetch_dataset(DATASET_PATH, "master")

    assert path == path2 == path3

    online.assert_not_called()
    online.reset_mock()
    offline.assert_called()
    offline.reset_mock()

    path4, dataset_identity4 = fdt.fetch_dataset(DATASET_RID, "master")

    assert path == path2 == path3 == path4

    FoundryRestClient.download_dataset_files = backup


@patch(
    API + ".get_dataset_identity",
    MagicMock(
        return_value={
            "dataset_rid": DATASET_RID,
            "dataset_path": DATASET_PATH,
            "last_transaction_rid": TRANSACTION_RID,
        }
    ),
)
@patch(API + ".get_dataset_schema")
@patch(API + ".list_dataset_files")
@patch(API + ".is_dataset_in_trash")
@patch(API + ".get_dataset_last_transaction_rid")
@patch(API + ".get_dataset_rid")
def test_load_dataset(
    get_dataset_rid,
    get_dataset_last_transaction_rid,
    is_dataset_in_trash,
    list_dataset_files,
    get_dataset_schema,
    mocker,
):
    from_foundry_and_cache = mocker.spy(
        CachedFoundryClient, "_download_dataset_and_return_local_path"
    )
    from_cache = mocker.spy(CachedFoundryClient, "_return_local_path_of_cached_dataset")
    fdt = CachedFoundryClient()

    get_dataset_rid.return_value = DATASET_RID
    get_dataset_last_transaction_rid.return_value = TRANSACTION_RID
    is_dataset_in_trash.return_value = False
    df = get_spark_session().createDataFrame([[1]], "col1:int")
    list_dataset_files.return_value = ["pandas/dataset.parquet"]
    get_dataset_schema.return_value = {
        "fieldSchemaList": [
            {"type": "INTEGER", "name": "col1", "nullable": True, "customMetadata": {}}
        ],
        "dataFrameReaderClass": "com.palantir.foundry.spark.input.ParquetDataFrameReader",
        "customMetadata": {"format": "parquet", "options": {}},
    }

    def download_dataset_files_mock(
        self,
        dataset_rid: str,
        output_directory: str,
        files: list,
        view="master",
        *,
        branch: str = "master",
    ):
        path = os.sep.join([output_directory, "spark"])
        os.makedirs(path, exist_ok=True)
        df.write.format("parquet").option("compression", "snappy").save(
            path=path, mode="overwrite"
        )

    from foundry_dev_tools import FoundryRestClient

    backup = FoundryRestClient.download_dataset_files
    FoundryRestClient.download_dataset_files = download_dataset_files_mock

    spark_df = fdt.load_dataset(DATASET_PATH, "master")

    from_foundry_and_cache.assert_called()
    from_cache.assert_not_called()
    from_foundry_and_cache.reset_mock()
    from_cache.reset_mock()
    assert_frame_equal(spark_df.toPandas(), df.toPandas())

    fdt.load_dataset(DATASET_PATH, "master")
    from_foundry_and_cache.assert_not_called()
    from_cache.assert_called()

    FoundryRestClient.download_dataset_files = backup


def test_api_client_not_cached(mocker):
    with mocker.patch(
        "foundry_dev_tools.Configuration.get_config",
        side_effect=[
            {
                "jwt": "secret-token-CACHED-FOUNDRY",
                "foundry_url": "https://test.com",
                "cache_dir": foundry_dev_tools.Configuration["cache_dir"],
            },
            {
                "jwt": "secret-token-ONE",
                "foundry_url": "https://test.com",
                "cache_dir": foundry_dev_tools.Configuration["cache_dir"],
            },
            {
                "jwt": "secret-token-TWO",
                "foundry_url": "https://test.com",
                "cache_dir": foundry_dev_tools.Configuration["cache_dir"],
            },
        ],
    ):
        fs = CachedFoundryClient()
        assert fs.config["jwt"] == "secret-token-CACHED-FOUNDRY"
        assert fs.api._config["jwt"] == "secret-token-ONE"
        assert fs.api._config["jwt"] == "secret-token-TWO"


def test_save_string_model(mock_client):
    with mock.patch(
        "foundry_dev_tools.CachedFoundryClient.api",
        mock_client,
    ):
        cfc = CachedFoundryClient()

        model = "simplestring"
        rid, transaction = cfc.save_model(
            model,
            dataset_path_or_rid="/Namespace1/project1/save_model_test",
            branch="master",
            exists_ok=True,
            mode="SNAPSHOT",
        )

        from_foundry = cfc.fetch_dataset("/Namespace1/project1/save_model_test")
        import pickle

        with open(f"{from_foundry[0]}/model.pickle", "rb") as f:
            model_returned = pickle.load(f)

        assert model == model_returned
