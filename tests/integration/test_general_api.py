from __future__ import annotations

import io
import os
from pathlib import Path
from random import choice
from string import ascii_uppercase
from unittest.mock import patch
from urllib.parse import quote

import pandas as pd
import pytest
import requests_mock
from requests_mock.adapter import ANY

from foundry_dev_tools._optional import FakeModule, pyarrow
from foundry_dev_tools.clients.data_proxy import DataProxyClient
from foundry_dev_tools.errors.compass import FolderNotFoundError
from foundry_dev_tools.errors.dataset import (
    BranchesAlreadyExistError,
    BranchNotFoundError,
    DatasetAlreadyExistsError,
    DatasetHasNoSchemaError,
    DatasetHasOpenTransactionError,
    DatasetNotFoundError,
)
from tests.integration.conftest import TEST_SINGLETON
from tests.integration.utils import INTEGRATION_TEST_COMPASS_ROOT_PATH, INTEGRATION_TEST_COMPASS_ROOT_RID, TEST_FOLDER


def test_monster_integration_test():  # noqa: PLR0915, TODO
    branch = "master/with/slash"
    rnd = "".join(choice(ascii_uppercase) for i in range(5))
    dataset_path = f"{INTEGRATION_TEST_COMPASS_ROOT_PATH}/test_api_{rnd}"

    ds = TEST_SINGLETON.v1_client.create_dataset(dataset_path)
    assert "rid" in ds
    assert "fileSystemId" in ds
    with pytest.raises(DatasetAlreadyExistsError) as excinfo:
        TEST_SINGLETON.v1_client.create_dataset(dataset_path)

    assert excinfo.value.dataset_path == dataset_path

    ds_returned = TEST_SINGLETON.v1_client.get_dataset(ds["rid"])
    assert "rid" in ds_returned
    assert "fileSystemId" in ds_returned

    assert TEST_SINGLETON.v1_client.is_dataset_in_trash(dataset_path) is False

    with pytest.raises(DatasetNotFoundError):
        TEST_SINGLETON.v1_client.get_dataset_path(ds["rid"][:-2] + "xx")

    ds_path_returned = TEST_SINGLETON.v1_client.get_dataset_path(ds["rid"])
    assert dataset_path == ds_path_returned

    with pytest.raises(DatasetNotFoundError):
        # can not open transaction for dataset that does not exist
        transaction_rid = TEST_SINGLETON.v1_client.open_transaction("does-not-exist", "SNAPSHOT", branch)

    with pytest.raises(BranchNotFoundError):
        # branch master not found
        transaction_rid = TEST_SINGLETON.v1_client.open_transaction(ds["rid"], "SNAPSHOT", branch)

    with pytest.raises(BranchNotFoundError):
        # branch master not found
        TEST_SINGLETON.v1_client.get_branch(ds["rid"], branch)

    _branch = TEST_SINGLETON.v1_client.create_branch(ds["rid"], branch)
    with pytest.raises(BranchesAlreadyExistError):
        _ = TEST_SINGLETON.v1_client.create_branch(ds["rid"], branch)
    branch_returned = TEST_SINGLETON.v1_client.get_branch(ds["rid"], branch)
    assert _branch == branch_returned

    branch_list = TEST_SINGLETON.v1_client.get_branches(ds["rid"])
    assert len(branch_list) == 1
    assert branch_list[0] == branch

    assert TEST_SINGLETON.v1_client.get_dataset_identity(ds["rid"], branch) == {
        "dataset_path": dataset_path,
        "dataset_rid": ds["rid"],
        "last_transaction_rid": None,
        "last_transaction": None,
    }

    assert TEST_SINGLETON.v1_client.get_dataset_last_transaction_rid(ds["rid"], branch) is None
    assert TEST_SINGLETON.v1_client.get_dataset_last_transaction(ds["rid"], branch) is None

    transaction_rid = TEST_SINGLETON.v1_client.open_transaction(ds["rid"], "SNAPSHOT", branch)

    with pytest.raises(DatasetHasOpenTransactionError) as exc_info:
        TEST_SINGLETON.v1_client.open_transaction(ds["rid"], "SNAPSHOT", branch)
    assert exc_info.value.dataset_rid == ds["rid"]
    assert exc_info.value.open_transaction_rid == transaction_rid

    TEST_SINGLETON.v1_client.upload_dataset_file(ds["rid"], transaction_rid, io.StringIO("col1,col2\n1,2"), "test.csv")

    # file like obj also works
    from io import BytesIO, StringIO

    TEST_SINGLETON.v1_client.upload_dataset_file(
        ds["rid"],
        transaction_rid,
        StringIO("col1,col2\n3,4"),
        "spark/testStringIO.csv",
    )
    TEST_SINGLETON.v1_client.upload_dataset_file(
        ds["rid"],
        transaction_rid,
        BytesIO(b"col1,col2\n5,6"),
        "testBytesIO.csv",
    )

    TEST_SINGLETON.v1_client.commit_transaction(ds["rid"], transaction_rid)

    schema = {
        "fieldSchemaList": [
            {"type": "INTEGER", "name": "col1", "customMetadata": {}},
            {"type": "INTEGER", "name": "col2", "customMetadata": {}},
        ],
        "dataFrameReaderClass": "com.palantir.foundry.spark.input.TextDataFrameReader",
        "customMetadata": {
            "textParserParams": {
                "parser": "CSV_PARSER",
                "charsetName": "UTF-8",
                "fieldDelimiter": ",",
                "recordDelimiter": "\n",
                "quoteCharacter": '"',
                "dateFormat": {},
                "skipLines": 1,
                "jaggedRowBehavior": "THROW_EXCEPTION",
                "parseErrorBehavior": "THROW_EXCEPTION",
            },
        },
    }

    with pytest.raises(DatasetHasNoSchemaError):
        TEST_SINGLETON.v1_client.get_dataset_schema(ds["rid"], transaction_rid, branch)

    TEST_SINGLETON.v1_client.upload_dataset_schema(ds["rid"], transaction_rid, schema, branch)
    retrieved_schema = TEST_SINGLETON.v1_client.get_dataset_schema(ds["rid"], transaction_rid, branch)
    assert retrieved_schema is not None

    stats = TEST_SINGLETON.v1_client.get_dataset_stats(ds["rid"], view=transaction_rid)
    assert stats["sizeInBytes"] == 39
    assert stats["numFiles"] == 3
    assert stats["numTransactions"] == 1
    assert stats["hiddenFilesSizeInBytes"] == 0
    assert stats["numHiddenFiles"] == 0

    last_good_transaction_rid = TEST_SINGLETON.v1_client.open_transaction(ds["rid"], "APPEND", branch)
    TEST_SINGLETON.v1_client.commit_transaction(ds["rid"], last_good_transaction_rid)

    failed_transaction = TEST_SINGLETON.v1_client.open_transaction(ds["rid"], "UPDATE", branch)
    TEST_SINGLETON.v1_client.abort_transaction(ds["rid"], failed_transaction)

    last_transaction_rid = TEST_SINGLETON.v1_client.get_dataset_last_transaction(ds["rid"], branch)["rid"]
    assert last_transaction_rid == last_good_transaction_rid

    TEST_SINGLETON.v1_client.create_branch(
        ds["rid"],
        "from-master",
        _branch["id"],
        _branch["rid"],
    )

    TEST_SINGLETON.v1_client.delete_dataset(ds["rid"])
    assert TEST_SINGLETON.v1_client.is_dataset_in_trash(dataset_path) is True

    with pytest.warns(UserWarning):
        TEST_SINGLETON.v1_client.get_dataset_details(dataset_path_or_rid=dataset_path)

    with pytest.raises(DatasetNotFoundError):
        TEST_SINGLETON.v1_client.delete_dataset(ds["rid"])


def test_schema_inference():
    rnd = "".join(choice(ascii_uppercase) for i in range(5))
    dataset_path = INTEGRATION_TEST_COMPASS_ROOT_PATH + f"/test_api_schema_{rnd}"

    ds = TEST_SINGLETON.v1_client.create_dataset(dataset_path)
    TEST_SINGLETON.v1_client.create_branch(ds["rid"], "master")
    transaction_rid = TEST_SINGLETON.v1_client.open_transaction(ds["rid"], "SNAPSHOT", "master")
    TEST_SINGLETON.v1_client.upload_dataset_file(ds["rid"], transaction_rid, io.StringIO("col1,col2\n1,2"), "test.csv")
    TEST_SINGLETON.v1_client.commit_transaction(ds["rid"], transaction_rid)

    with pytest.raises(DatasetHasNoSchemaError):
        TEST_SINGLETON.v1_client.get_dataset_schema(ds["rid"], transaction_rid, "master")

    inferred_schema = TEST_SINGLETON.v1_client.infer_dataset_schema(ds["rid"], "master")

    TEST_SINGLETON.v1_client.upload_dataset_schema(ds["rid"], transaction_rid, inferred_schema, "master")

    returned_schema = TEST_SINGLETON.v1_client.get_dataset_schema(ds["rid"], transaction_rid, "master")

    # seems to not be returned from infer schema API
    inferred_schema["primaryKey"] = None

    assert inferred_schema == returned_schema

    transaction_rid = TEST_SINGLETON.v1_client.open_transaction(ds["rid"], "SNAPSHOT", "master")
    TEST_SINGLETON.v1_client.upload_dataset_file(
        ds["rid"],
        transaction_rid,
        io.StringIO('col1,co"""l2\n1,2,3'),
        "test.csv",
    )
    TEST_SINGLETON.v1_client.commit_transaction(ds["rid"], transaction_rid)

    with pytest.warns(UserWarning) as warning:
        TEST_SINGLETON.v1_client.infer_dataset_schema(ds["rid"], "master")
    assert (
        str(warning[0].message) == "Foundry Schema inference completed with status 'WARN' "
        'and message \'No column delimiters found. The delimiter (Comma ",") '
        "was our best guess.'."
    )
    TEST_SINGLETON.v1_client.delete_dataset(ds["rid"])


def test_get_dataset_last_transaction():
    transaction_rid = TEST_SINGLETON.v1_client.get_dataset_last_transaction(
        TEST_SINGLETON.iris_new.rid,
        branch=TEST_SINGLETON.iris_new.branch,
    )["rid"]
    assert transaction_rid == TEST_SINGLETON.iris_new_transaction["rid"]
    assert (
        TEST_SINGLETON.v1_client.get_dataset_last_transaction_rid(
            TEST_SINGLETON.iris_new.rid,
            branch=TEST_SINGLETON.iris_new.branch,
        )
        == TEST_SINGLETON.iris_new_transaction["rid"]
    )


def test_get_dataset_stats():
    stats_by_branch = TEST_SINGLETON.v1_client.get_dataset_stats(
        TEST_SINGLETON.iris_new.rid,
        view=TEST_SINGLETON.iris_new.branch,
    )
    expected_iris_size = TEST_FOLDER.joinpath("test_data", "iris", "iris.csv").stat().st_size
    assert stats_by_branch == {
        "sizeInBytes": expected_iris_size,
        "numFiles": 1,
        "hiddenFilesSizeInBytes": 0,
        "numHiddenFiles": 0,
        "numTransactions": 1,
    }
    stats_by_transaction = TEST_SINGLETON.v1_client.get_dataset_stats(
        TEST_SINGLETON.iris_new.rid,
        view=TEST_SINGLETON.iris_new_transaction["rid"],
    )
    assert stats_by_transaction == {
        "sizeInBytes": expected_iris_size,
        "numFiles": 1,
        "hiddenFilesSizeInBytes": 0,
        "numHiddenFiles": 0,
        "numTransactions": 1,
    }


def test_get_dataset_schema():
    expected_response = {
        "branchId": "master",
        "transactionRid": TEST_SINGLETON.iris_new_transaction["rid"],
        "versionId": "00000000-d32e-fe0c-a1ba-829db29e6c50",
        "schema": {
            "fieldSchemaList": [
                {
                    "type": "DOUBLE",
                    "name": "sepal_width",
                    "nullable": None,
                    "userDefinedTypeClass": None,
                    "customMetadata": {},
                    "arraySubtype": None,
                    "precision": None,
                    "scale": None,
                    "mapKeyType": None,
                    "mapValueType": None,
                    "subSchemas": None,
                },
                {
                    "type": "DOUBLE",
                    "name": "sepal_length",
                    "nullable": None,
                    "userDefinedTypeClass": None,
                    "customMetadata": {},
                    "arraySubtype": None,
                    "precision": None,
                    "scale": None,
                    "mapKeyType": None,
                    "mapValueType": None,
                    "subSchemas": None,
                },
                {
                    "type": "DOUBLE",
                    "name": "petal_width",
                    "nullable": None,
                    "userDefinedTypeClass": None,
                    "customMetadata": {},
                    "arraySubtype": None,
                    "precision": None,
                    "scale": None,
                    "mapKeyType": None,
                    "mapValueType": None,
                    "subSchemas": None,
                },
                {
                    "type": "DOUBLE",
                    "name": "petal_length",
                    "nullable": None,
                    "userDefinedTypeClass": None,
                    "customMetadata": {},
                    "arraySubtype": None,
                    "precision": None,
                    "scale": None,
                    "mapKeyType": None,
                    "mapValueType": None,
                    "subSchemas": None,
                },
                {
                    "type": "STRING",
                    "name": "is_setosa",
                    "nullable": None,
                    "userDefinedTypeClass": None,
                    "customMetadata": {},
                    "arraySubtype": None,
                    "precision": None,
                    "scale": None,
                    "mapKeyType": None,
                    "mapValueType": None,
                    "subSchemas": None,
                },
            ],
            "primaryKey": None,
            "dataFrameReaderClass": "com.palantir.foundry.spark.input.TextDataFrameReader",
            "customMetadata": {
                "textParserParams": {
                    "parser": "CSV_PARSER",
                    "charsetName": "UTF-8",
                    "fieldDelimiter": ",",
                    "recordDelimiter": "\n",
                    "quoteCharacter": '"',
                    "dateFormat": {},
                    "skipLines": 1,
                    "jaggedRowBehavior": "THROW_EXCEPTION",
                    "parseErrorBehavior": "THROW_EXCEPTION",
                    "addFilePath": False,
                    "addFilePathInsteadOfUri": False,
                    "addByteOffset": False,
                    "addImportedAt": False,
                    "initialReadTimeout": "1 hour",
                },
            },
        },
        "attribution": {
            "userId": "3c8fbda5-686e-4fcb-ad52-d95e4281d99f",
            "time": "2020-01-30T11:18:35.123747Z",
        },
    }
    schema = TEST_SINGLETON.v1_client.get_dataset_schema(
        TEST_SINGLETON.iris_new.rid,
        TEST_SINGLETON.iris_new_transaction["rid"],
        branch=TEST_SINGLETON.iris_new.branch,
    )
    assert schema == expected_response["schema"]


def test_get_spark_schema_branch_not_exists():
    with pytest.raises(BranchNotFoundError):
        TEST_SINGLETON.v1_client.get_dataset_schema(
            TEST_SINGLETON.iris_new.rid,
            TEST_SINGLETON.iris_new_transaction["rid"],
            branch="branch-does-not-exist",
        )


def test_get_and_download_dataset_files(tmpdir):
    list_of_files = TEST_SINGLETON.v1_client.list_dataset_files(TEST_SINGLETON.iris_new.rid)
    assert list_of_files == ["iris.csv"]
    TEST_SINGLETON.v1_client.download_dataset_files(
        TEST_SINGLETON.iris_new.rid,
        tmpdir,
        list_of_files,
    )
    assert "iris.csv" in os.listdir(tmpdir)


def test_get_csv_of_dataset():
    response = TEST_SINGLETON.v1_client.get_dataset_as_raw_csv(TEST_SINGLETON.iris_new.rid)
    iris = pd.read_csv(io.BytesIO(response.content))
    assert iris.shape == (150, 5)


def test_query_legacy_sql():
    foundry_schema, data = TEST_SINGLETON.v1_client.query_foundry_sql_legacy(
        f"SELECT * FROM `{TEST_SINGLETON.iris_new.rid}` LIMIT 100",
    )
    iris = pd.DataFrame(data=data, columns=[e["name"] for e in foundry_schema["fieldSchemaList"]])
    assert iris.shape == (100, 5)

    with pytest.raises(BranchNotFoundError):
        TEST_SINGLETON.v1_client.query_foundry_sql_legacy(
            f"SELECT * FROM `{TEST_SINGLETON.iris_new.rid}` LIMIT 100",
            branch="shouldnotexist",
        )

    with pytest.raises(DatasetHasNoSchemaError):
        TEST_SINGLETON.v1_client.query_foundry_sql_legacy(
            f"SELECT * FROM `{TEST_SINGLETON.iris_no_schema.rid}` LIMIT 100",
            branch=TEST_SINGLETON.iris_no_schema.branch,
        )


def test_query_sql():
    iris_rid = TEST_SINGLETON.iris_new.rid
    with patch.object(
        DataProxyClient,
        "query_foundry_sql_legacy",
        wraps=TEST_SINGLETON.ctx.data_proxy.query_foundry_sql_legacy,
    ) as spy:
        iris_pdf = TEST_SINGLETON.v1_client.query_foundry_sql(f"SELECT * FROM `{iris_rid}` LIMIT 100")
        assert iris_pdf.shape == (100, 5)

        pa_table = TEST_SINGLETON.v1_client.query_foundry_sql(
            f"SELECT * FROM `{iris_rid}` LIMIT 100",
            return_type="arrow",
        )
        assert pa_table.shape == (100, 5)
        assert pa_table.num_columns == 5
        assert spy.call_count == 0

        iris_fallback = TEST_SINGLETON.v1_client.query_foundry_sql(
            f"SELECT * FROM `{iris_rid}` order by sepal_width LIMIT 100",
        )
        assert iris_fallback.shape == (100, 5)
        # The order by should trigger a computation in spark and today 18/10/2022
        # the Foundry SQL server does not return ARROW but JSON
        assert spy.call_count == 0

        iris_where = TEST_SINGLETON.v1_client.query_foundry_sql(
            f"SELECT * FROM `{iris_rid}` where is_setosa = 'Iris-setosa'",
        )
        assert all(
            iris_where["is_setosa"] == "Iris-setosa",
        ), f"Still multiple classes: {iris_where['is_setosa'].unique()}"
        # The order by should trigger a computation in spark and today 18/10/2022
        # the Foundry SQL server does not return ARROW but JSON
        assert spy.call_count == 0

        orig_pa = pyarrow.pa
        pyarrow.pa = FakeModule("pyarrow")
        iris = TEST_SINGLETON.v1_client.query_foundry_sql(f"SELECT * FROM `{TEST_SINGLETON.iris_new.path}`")
        assert iris.shape == (150, 5)
        assert spy.call_count == 1
        pyarrow.pa = orig_pa


def test_get_folder_children():
    children = TEST_SINGLETON.v1_client.get_child_objects_of_folder(folder_rid=INTEGRATION_TEST_COMPASS_ROOT_RID)
    assert (
        len(
            list(
                filter(
                    lambda child: child["name"] == TEST_SINGLETON.iris_new.path.split("/")[-1],
                    children,
                ),
            ),
        )
        == 1
    )

    rnd = "".join(choice(ascii_uppercase) for i in range(5))
    folder_name = f"test_folder_{rnd}"
    no_children_folder = TEST_SINGLETON.v1_client.create_folder(
        name=folder_name,
        parent_id=INTEGRATION_TEST_COMPASS_ROOT_RID,
    )

    no_children = TEST_SINGLETON.v1_client.get_child_objects_of_folder(folder_rid=no_children_folder["rid"])
    assert len(list(no_children)) == 0

    TEST_SINGLETON.v1_client.move_resource_to_trash(resource_id=no_children_folder["rid"])

    with pytest.raises(FolderNotFoundError):
        next(TEST_SINGLETON.v1_client.get_child_objects_of_folder(folder_rid="ri.compass.main.folder.1337"))

    # Test pagination
    with patch.object(TEST_SINGLETON.ctx.client, "request", wraps=TEST_SINGLETON.ctx.client.request) as spy:
        children = TEST_SINGLETON.v1_client.get_child_objects_of_folder(
            folder_rid=INTEGRATION_TEST_COMPASS_ROOT_RID,
            page_size=1,
        )
        assert spy.call_count == 0
        next(children)
        assert spy.call_count == 1
        next(children)
        assert spy.call_count == 2


def test_download_dataset_files_temporary():
    from pyarrow import parquet

    # Download single file, list call is not triggered
    with patch.object(
        TEST_SINGLETON.ctx.catalog,
        "list_dataset_files",
        wraps=TEST_SINGLETON.ctx.catalog.list_dataset_files,
    ) as spy:
        with TEST_SINGLETON.v1_client.download_dataset_files_temporary(
            dataset_rid=TEST_SINGLETON.complex_dataset.rid,
            view=TEST_SINGLETON.complex_dataset.branch,
        ) as temp_dir:
            p = Path(temp_dir)
            assert p.exists()
            _ = pd.read_parquet(p)
            _ = parquet.ParquetDataset([x for x in p.glob("**/*") if x.is_file()]).read()
        assert not p.exists()
        assert spy.call_count == 1

        with TEST_SINGLETON.v1_client.download_dataset_files_temporary(
            dataset_rid=TEST_SINGLETON.iris_new.rid,
            view=TEST_SINGLETON.iris_new.branch,
        ) as temp_dir:
            p2 = Path(temp_dir)
            assert p2.exists()
            _ = pd.read_csv(p2.joinpath("iris.csv"))
        assert not p2.exists()
        assert spy.call_count == 2

        with TEST_SINGLETON.v1_client.download_dataset_files_temporary(
            dataset_rid=TEST_SINGLETON.iris_new.rid,
            files=["iris.csv"],
            view=TEST_SINGLETON.iris_new.branch,
        ) as temp_dir:
            p3 = Path(temp_dir)
            assert p3.exists()
            iris = pd.read_csv(p3.joinpath("iris.csv"))
        assert p3.exists() is False
        assert iris.shape == (150, 5)
        assert spy.call_count == 2


def test_remove_file_in_open_transaction():
    transaction_rid = TEST_SINGLETON.v1_client.open_transaction(
        TEST_SINGLETON.empty_dataset.rid,
        "UPDATE",
        TEST_SINGLETON.empty_dataset.branch,
    )

    TEST_SINGLETON.v1_client.upload_dataset_file(
        TEST_SINGLETON.empty_dataset.rid,
        transaction_rid,
        io.StringIO("col1,col2\n1,2"),
        "test.csv",
    )

    files = TEST_SINGLETON.v1_client.list_dataset_files(
        TEST_SINGLETON.empty_dataset.rid,
        view=TEST_SINGLETON.empty_dataset.branch,
        include_open_exclusive_transaction=True,
    )
    assert "test.csv" in files

    TEST_SINGLETON.v1_client.remove_dataset_file(
        TEST_SINGLETON.empty_dataset.rid,
        transaction_rid,
        "test.csv",
        recursive=False,
    )
    files = TEST_SINGLETON.v1_client.list_dataset_files(
        TEST_SINGLETON.empty_dataset.rid,
        view=TEST_SINGLETON.empty_dataset.branch,
        include_open_exclusive_transaction=True,
    )
    assert "test.csv" not in files

    TEST_SINGLETON.v1_client.abort_transaction(TEST_SINGLETON.empty_dataset.rid, transaction_rid)


def test_remove_file_from_previous_transaction():
    rid = TEST_SINGLETON.empty_dataset.rid

    transaction_rid = TEST_SINGLETON.v1_client.open_transaction(rid, "SNAPSHOT", TEST_SINGLETON.empty_dataset.branch)

    TEST_SINGLETON.v1_client.upload_dataset_file(rid, transaction_rid, io.StringIO("col1,col2\n1,2"), "test.csv")

    TEST_SINGLETON.v1_client.commit_transaction(rid, transaction_rid)

    files = TEST_SINGLETON.v1_client.list_dataset_files(
        rid,
        view=TEST_SINGLETON.empty_dataset.branch,
        include_open_exclusive_transaction=False,
    )
    assert "test.csv" in files

    transaction_rid = TEST_SINGLETON.v1_client.open_transaction(rid, "DELETE", "master")
    TEST_SINGLETON.v1_client.add_files_to_delete_transaction(rid, transaction_rid, logical_paths=["test.csv"])
    TEST_SINGLETON.v1_client.commit_transaction(rid, transaction_rid)

    files = TEST_SINGLETON.v1_client.list_dataset_files(
        rid,
        view=TEST_SINGLETON.empty_dataset.branch,
        include_open_exclusive_transaction=False,
    )
    assert "test.csv" not in files


def test_download_dataset_file_returns_bytes():
    raw_bytes = TEST_SINGLETON.v1_client.download_dataset_file(
        dataset_rid=TEST_SINGLETON.iris_new.rid,
        output_directory=None,
        foundry_file_path="iris.csv",
    )
    iris = pd.read_csv(io.BytesIO(raw_bytes))
    assert iris.shape == (150, 5)


def test_get_dataset_details_identical_path_rid():
    by_path = TEST_SINGLETON.v1_client.get_dataset_details(dataset_path_or_rid=TEST_SINGLETON.iris_new.path)

    by_rid = TEST_SINGLETON.v1_client.get_dataset_details(dataset_path_or_rid=TEST_SINGLETON.iris_new.rid)

    assert by_path["rid"] == by_rid["rid"]
    assert by_path["path"] == by_rid["path"]


def test_quoting_of_filenames():
    with requests_mock.mock(case_sensitive=True) as m:
        needs_quotation = "prod2_api_v2_reports_query?#2022-06-27T22_24_18.528205Z.json"
        needs_no_quotation = "All 123 - BLUB Extract_new (Export 2021-12-21 18_25)-20211221.140314.csv"
        m.post(url=ANY, status_code=204)
        TEST_SINGLETON.v1_client.upload_dataset_file(
            dataset_rid="rid",
            transaction_rid="transaction_rid",
            path_or_buf=io.BytesIO(),
            path_in_foundry_dataset=needs_quotation,
        )

        TEST_SINGLETON.v1_client.upload_dataset_file(
            dataset_rid="rid",
            transaction_rid="transaction_rid",
            path_or_buf=io.BytesIO(),
            path_in_foundry_dataset=needs_no_quotation,
        )
        history = m.request_history
        assert history[0].query == "logicalPath=prod2_api_v2_reports_query%3F%232022-06-27T22_24_18.528205Z.json"
        assert (
            history[1].query
            == "logicalPath=All+123+-+BLUB+Extract_new+%28Export+2021-12-21+18_25%29-20211221.140314.csv"
        )
        m.get(url=ANY, status_code=200, body=io.BytesIO(b"some-content"))
        TEST_SINGLETON.v1_client.download_dataset_file(
            dataset_rid="rid",
            output_directory=None,
            view="master",
            foundry_file_path=needs_quotation,
        )
        TEST_SINGLETON.v1_client.download_dataset_file(
            dataset_rid="rid",
            output_directory=None,
            view="master",
            foundry_file_path=needs_no_quotation,
        )
        history = m.request_history
        assert history[2].path.split("/")[-1] == quote(needs_quotation)
        assert history[3].path.split("/")[-1] == quote(needs_no_quotation)
