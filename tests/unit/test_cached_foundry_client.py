from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession

from foundry_dev_tools.cached_foundry_client import CachedFoundryClient
from foundry_dev_tools.errors.dataset import (
    BranchNotFoundError,
)
from foundry_dev_tools.utils.clients import build_api_url
from tests.unit.mocks import TEST_HOST

DATASET_RID = "ri.foundry.main.dataset.12345de3-b916-46ba-b097-c4326ea4342e"
DATASET_PATH = "/mock/path/ds1"
TRANSACTION_RID = "transaction1"
API = "foundry_dev_tools.foundry_api_client.FoundryRestClient"
CFC = "foundry_dev_tools.cached_foundry_client.CachedFoundryClient"


@patch("os.listdir", MagicMock(return_value=["dataset.parquet"]))
@patch("tempfile.TemporaryDirectory")
@patch(
    CFC + "._save_objects",
    return_value=(
        DATASET_RID,
        TRANSACTION_RID,
    ),
)
@patch(API + ".infer_dataset_schema")
@patch(API + ".upload_dataset_schema")
def test_save_pandas(upload_dataset_schema, infer_dataset_schema, save_objects, temporary_directory, test_context_mock):
    mtp = "mock_tmp_path"
    dsn = "dataset.parquet"
    dsp = mtp + "/" + dsn
    temporary_directory.return_value.__enter__.return_value = mtp
    fdt = CachedFoundryClient(ctx=test_context_mock)
    df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    with patch.object(df, "to_parquet") as pd_to_parquet:
        dataset_rid, transaction_id = fdt.save_dataset(
            df,
            dataset_path_or_rid=DATASET_PATH,
            branch="master",
            exists_ok=True,
            mode="SNAPSHOT",
        )
    assert pd_to_parquet.call_args[0][0] == dsp

    assert save_objects.call_args[0][0] == {"spark/" + dsn: Path(dsp)}
    assert save_objects.call_args[0][1] == DATASET_PATH

    assert dataset_rid == "ri.foundry.main.dataset.12345de3-b916-46ba-b097-c4326ea4342e"
    assert transaction_id == "transaction1"


@patch(
    API + ".get_dataset_identity",
    MagicMock(
        return_value={
            "dataset_rid": DATASET_RID,
            "dataset_path": DATASET_PATH,
            "last_transaction_rid": TRANSACTION_RID,
        },
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
def test_save_spark(upload_dataset_files, test_context_mock):
    fdt = CachedFoundryClient(ctx=test_context_mock)
    df = SparkSession.builder.master("local[*]").getOrCreate().createDataFrame([[1, 2]], "a:string, b: string")
    dataset_rid, transaction_id = fdt.save_dataset(
        df,
        dataset_path_or_rid=DATASET_PATH,
        branch="master",
        exists_ok=True,
        mode="SNAPSHOT",
    )

    args = upload_dataset_files.call_args[0]

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
        },
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
def test_save_model(upload_dataset_files, test_context_mock):
    fdt = CachedFoundryClient(ctx=test_context_mock)
    dataset_rid, transaction_id = fdt.save_model(
        {"wow": "awesome_ml_model"},
        dataset_path_or_rid=DATASET_PATH,
        branch="master",
        exists_ok=True,
        mode="SNAPSHOT",
    )

    args = upload_dataset_files.call_args[0]

    assert args[0] == DATASET_RID
    assert args[1] == TRANSACTION_RID
    # one model object should be uploaded
    assert len(args[2]) == 1
    assert "model.pickle" in args[2]

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
# @patch(API + ".get_dataset_identity")
def test_save_objects(
    # get_dataset_identity,
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
    test_context_mock,
):
    fdt = CachedFoundryClient(ctx=test_context_mock)
    path_file_dict = {"path-in/foundry.pickle": Path.cwd()}

    test_context_mock.mock_adapter.register_uri(
        "GET",
        build_api_url(TEST_HOST.url, "compass", "resources"),
        status_code=204,
    )
    create_dataset.return_value = {"rid": DATASET_RID}
    is_dataset_in_trash.return_value = False
    open_transaction.return_value = TRANSACTION_RID
    dataset_rid, transaction_id = fdt._save_objects(
        path_file_dict,
        DATASET_PATH,
        "master",
        exists_ok=False,
        mode="SNAPSHOT",
    )

    assert dataset_rid == DATASET_RID
    assert transaction_id == TRANSACTION_RID
    commit_transaction.assert_called_once()
    abort_transaction.assert_not_called()
    commit_transaction.reset_mock()
    abort_transaction.reset_mock()
    test_context_mock.mock_adapter.register_uri(
        "GET",
        build_api_url(TEST_HOST.url, "compass", "resources"),
        status_code=200,
        json={
            "rid": DATASET_RID,
            "path": DATASET_PATH,
            "directlyTrashed": False,
            "operations": ["gatekeeper:view-resource"],
        },
    )
    test_context_mock.mock_adapter.register_uri(
        "GET",
        build_api_url(TEST_HOST.url, "foundry-catalog", f"catalog/datasets/{DATASET_RID}/reverse-transactions2/master")
        + "?pageSize=1&includeOpenExclusiveTransaction=False",
        status_code=200,
        json={"values": [{"rid": TRANSACTION_RID, "transaction": None}]},
    )
    # Error in upload files -> verify abort transaction is called
    upload_dataset_files.side_effect = OSError()
    with pytest.raises(ValueError):  # noqa: PT011
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
    with pytest.raises(ValueError):  # noqa: PT011
        fdt._save_objects(
            path_file_dict,
            DATASET_PATH,
            "master",
            exists_ok=False,
            mode="SNAPSHOT",
        )

    # ValueError is thrown when dataset is in trash
    is_dataset_in_trash.return_value = True
    with pytest.raises(ValueError):  # noqa: PT011
        fdt._save_objects(
            path_file_dict,
            DATASET_PATH,
            "master",
            exists_ok=True,
            mode="SNAPSHOT",
        )
    is_dataset_in_trash.return_value = False

    # branch is created when it does not exist
    get_branch.side_effect = BranchNotFoundError()
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
            "last_transaction": {"rid": TRANSACTION_RID, "transaction": {}},
        },
    ),
)
@patch(API + ".get_dataset_schema")
@patch(API + ".list_dataset_files")
@patch(API + ".is_dataset_in_trash")
@patch(API + ".get_dataset_last_transaction")
@patch(API + ".get_dataset_rid")
def test_fetch_dataset(
    get_dataset_rid,
    get_dataset_last_transaction,
    is_dataset_in_trash,
    list_dataset_files,
    get_dataset_schema,
    mocker,
    test_context_mock,
    spark_session,
):
    from_foundry_and_cache = mocker.spy(
        CachedFoundryClient,
        "_download_dataset_and_return_local_path",
    )
    from_cache = mocker.spy(
        CachedFoundryClient,
        "_return_local_path_of_cached_dataset",
    )
    online = mocker.spy(
        CachedFoundryClient,
        "_get_dataset_identity_online",
    )
    offline = mocker.spy(
        CachedFoundryClient,
        "_get_dataset_identity_offline",
    )

    get_dataset_rid.return_value = DATASET_RID
    get_dataset_last_transaction.return_value = {"rid": TRANSACTION_RID}
    is_dataset_in_trash.return_value = False
    df = spark_session.createDataFrame([[1]], "col1:int")
    get_dataset_schema.return_value = {
        "fieldSchemaList": [{"type": "INTEGER", "name": "col1", "nullable": True, "customMetadata": {}}],
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
        path = Path(output_directory).joinpath("spark")
        path.mkdir(parents=True, exist_ok=True)
        df.write.format("parquet").option("compression", "snappy").save(path=os.fspath(path), mode="overwrite")

    from foundry_dev_tools.foundry_api_client import FoundryRestClient

    backup = FoundryRestClient.download_dataset_files
    FoundryRestClient.download_dataset_files = download_dataset_files_mock

    fdt = CachedFoundryClient(ctx=test_context_mock)
    path, dataset_identity = fdt.fetch_dataset(DATASET_PATH, "master")
    assert path.split(os.sep, maxsplit=-1)[-1] == TRANSACTION_RID + ".parquet"
    assert dataset_identity == {
        "dataset_rid": DATASET_RID,
        "last_transaction_rid": TRANSACTION_RID,
        "last_transaction": {"rid": TRANSACTION_RID, "transaction": {}},
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
    test_context_mock.config.transforms_freeze_cache = True

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
            "last_transaction": {
                "rid": TRANSACTION_RID,
                "transaction": {
                    "record": {},
                    "metadata": {
                        "fileCount": 1,
                        "hiddenFileCount": 0,
                        "totalFileSize": 1000,
                        "totalHiddenFileSize": 0,
                    },
                },
            },
        },
    ),
)
@patch(API + ".get_dataset_schema")
@patch(API + ".list_dataset_files")
@patch(API + ".is_dataset_in_trash")
@patch(API + ".get_dataset_rid")
def test_load_dataset(
    get_dataset_rid,
    is_dataset_in_trash,
    list_dataset_files,
    get_dataset_schema,
    mocker,
    test_context_mock,
    spark_session,
):
    from_foundry_and_cache = mocker.spy(
        CachedFoundryClient,
        "_download_dataset_and_return_local_path",
    )
    from_cache = mocker.spy(
        CachedFoundryClient,
        "_return_local_path_of_cached_dataset",
    )
    fdt = CachedFoundryClient(ctx=test_context_mock)

    get_dataset_rid.return_value = DATASET_RID
    is_dataset_in_trash.return_value = False
    df = spark_session.createDataFrame([[1]], "col1:int")
    list_dataset_files.return_value = ["pandas/dataset.parquet"]
    get_dataset_schema.return_value = {
        "fieldSchemaList": [{"type": "INTEGER", "name": "col1", "nullable": True, "customMetadata": {}}],
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
        path = Path(output_directory).joinpath("spark")
        path.mkdir(parents=True, exist_ok=True)
        df.write.format("parquet").option("compression", "snappy").save(path=os.fspath(path), mode="overwrite")

    from foundry_dev_tools.foundry_api_client import FoundryRestClient

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


@patch(
    API + ".get_dataset_identity",
    MagicMock(
        return_value={
            "dataset_rid": DATASET_RID,
            "dataset_path": DATASET_PATH,
            "last_transaction_rid": TRANSACTION_RID,
            "last_transaction": {
                "rid": TRANSACTION_RID,
                "transaction": {
                    "record": {"view": True},
                    "metadata": {
                        "fileCount": 1,
                        "hiddenFileCount": 0,
                        "totalFileSize": 0,
                        "totalHiddenFileSize": 0,
                    },
                },
            },
        },
    ),
)
@patch(API + ".get_dataset_schema")
@patch(API + ".query_foundry_sql")
@patch(API + ".is_dataset_in_trash")
@patch(API + ".get_dataset_rid")
def test_load_dataset_is_view(
    get_dataset_rid,
    is_dataset_in_trash,
    query_foundry_sql,
    get_dataset_schema,
    mocker,
    test_context_mock,
    spark_session,
):
    from_cache = mocker.spy(
        CachedFoundryClient,
        "_return_local_path_of_cached_dataset",
    )
    fdt = CachedFoundryClient(ctx=test_context_mock)

    get_dataset_rid.return_value = DATASET_RID
    is_dataset_in_trash.return_value = False
    df = spark_session.createDataFrame([[1]], "col1:int")
    query_foundry_sql.return_value = df
    get_dataset_schema.return_value = {
        "fieldSchemaList": [{"type": "INTEGER", "name": "col1", "nullable": True, "customMetadata": {}}],
        "dataFrameReaderClass": "com.palantir.foundry.spark.input.ParquetDataFrameReader",
        "customMetadata": {"format": "parquet", "options": {}},
    }

    spark_df = fdt.load_dataset(DATASET_PATH, "master")

    from_cache.assert_called()
    assert query_foundry_sql.call_count == 1
    from_cache.reset_mock()
    assert_frame_equal(spark_df.toPandas(), df.toPandas())

    fdt.load_dataset(DATASET_PATH, "master")
    assert query_foundry_sql.call_count == 1
    from_cache.assert_called()
