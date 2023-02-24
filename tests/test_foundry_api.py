import io
import os
from pathlib import Path
from random import choice
from string import ascii_uppercase
from unittest.mock import MagicMock, patch
from urllib.parse import quote

import pandas as pd
import pytest
import requests_mock
from requests import HTTPError
from requests_mock.adapter import ANY

import foundry_dev_tools
from foundry_dev_tools import FoundryRestClient
from foundry_dev_tools.foundry_api_client import (
    BranchesAlreadyExistError,
    BranchNotFoundError,
    DatasetAlreadyExistsError,
    DatasetHasNoSchemaError,
    DatasetHasOpenTransactionError,
    DatasetNoReadAccessError,
    DatasetNotFoundError,
    FolderNotFoundError,
)

from tests.conftest import PatchConfig
from tests.utils import (
    INTEGRATION_TEST_COMPASS_ROOT_PATH,
    INTEGRATION_TEST_COMPASS_ROOT_RID,
    TEST_FOLDER,
)


def test_get_config(is_integration_test, client):
    if is_integration_test:
        assert client._headers()["Authorization"] is not None
    else:
        assert client._headers()["Authorization"] == "Bearer 123"
        assert client._verify() is not None
    assert "transforms_sql_sample_row_limit" in client._config
    assert "transforms_sql_dataset_size_threshold" in client._config
    assert "foundry_url" in client._config


@pytest.mark.no_patch_conf
def test_sso_config(mocker, tmpdir):
    with PatchConfig(
        config_overwrite={
            "client_id": "1234",
            "foundry_url": "https://foundry.example.com",
            "grant_type": "authorization_code",
        }
    ):
        if "jwt" in foundry_dev_tools.Configuration:
            del foundry_dev_tools.Configuration["jwt"]
        client = FoundryRestClient()

        mock_get_user_credentials = mocker.patch(
            "palantir_oauth_client.get_user_credentials"
        )
        mock_get_user_credentials.return_value.token = "access-token"

        assert client._headers()["Authorization"] == "Bearer access-token"
        assert "Foundry DevTools" in client._headers()["User-Agent"]


@pytest.mark.integration
def test_monster_integration_test(client):
    BRANCH = "master/with/slash"
    rnd = "".join(choice(ascii_uppercase) for i in range(5))
    dataset_path = str(INTEGRATION_TEST_COMPASS_ROOT_PATH / f"test_api_{rnd}")

    ds = client.create_dataset(dataset_path)
    assert "rid" in ds
    assert "fileSystemId" in ds
    with pytest.raises(DatasetAlreadyExistsError):
        client.create_dataset(dataset_path)

    ds_returned = client.get_dataset(ds["rid"])
    assert "rid" in ds_returned
    assert "fileSystemId" in ds_returned

    assert client.is_dataset_in_trash(dataset_path) is False

    with pytest.raises(DatasetNotFoundError):
        client.get_dataset_path(ds["rid"][:-2] + "xx")

    ds_path_returned = client.get_dataset_path(ds["rid"])
    assert dataset_path == ds_path_returned

    with pytest.raises(DatasetNotFoundError):
        # can not open transaction for dataset that does not exist
        transaction_rid = client.open_transaction("does-not-exist", "SNAPSHOT", BRANCH)

    with pytest.raises(BranchNotFoundError):
        # branch master not found
        transaction_rid = client.open_transaction(ds["rid"], "SNAPSHOT", BRANCH)

    with pytest.raises(BranchNotFoundError):
        # branch master not found
        client.get_branch(ds["rid"], BRANCH)

    branch = client.create_branch(ds["rid"], BRANCH)
    with pytest.raises(BranchesAlreadyExistError):
        _ = client.create_branch(ds["rid"], BRANCH)
    branch_returned = client.get_branch(ds["rid"], BRANCH)
    assert branch == branch_returned
    transaction_rid = client.open_transaction(ds["rid"], "SNAPSHOT", BRANCH)

    with pytest.raises(DatasetHasOpenTransactionError) as exc_info:
        client.open_transaction(ds["rid"], "SNAPSHOT", BRANCH)
    assert exc_info.value.dataset_rid == ds["rid"]
    assert exc_info.value.open_transaction_rid == transaction_rid

    client.upload_dataset_file(
        ds["rid"], transaction_rid, io.StringIO("col1,col2\n1,2"), "test.csv"
    )

    # file like obj also works
    from io import BytesIO, StringIO

    client.upload_dataset_file(
        ds["rid"],
        transaction_rid,
        StringIO("col1,col2\n3,4"),
        "spark/testStringIO.csv",
    )
    client.upload_dataset_file(
        ds["rid"], transaction_rid, BytesIO(b"col1,col2\n5,6"), "testBytesIO.csv"
    )

    client.commit_transaction(ds["rid"], transaction_rid)

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
            }
        },
    }

    with pytest.raises(DatasetHasNoSchemaError):
        client.get_dataset_schema(ds["rid"], transaction_rid, BRANCH)

    client.upload_dataset_schema(ds["rid"], transaction_rid, schema, BRANCH)
    retrieved_schema = client.get_dataset_schema(ds["rid"], transaction_rid, BRANCH)
    assert retrieved_schema is not None

    stats = client.get_dataset_stats(ds["rid"], view=transaction_rid)
    assert stats["sizeInBytes"] == 39
    assert stats["numFiles"] == 3
    assert stats["numTransactions"] == 1
    assert stats["hiddenFilesSizeInBytes"] == 0
    assert stats["numHiddenFiles"] == 0

    last_good_transaction_rid = client.open_transaction(ds["rid"], "APPEND", BRANCH)
    client.commit_transaction(ds["rid"], last_good_transaction_rid)

    failed_transaction = client.open_transaction(ds["rid"], "UPDATE", BRANCH)
    client.abort_transaction(ds["rid"], failed_transaction)

    last_transaction_rid = client.get_dataset_last_transaction_rid(ds["rid"], BRANCH)
    assert last_transaction_rid == last_good_transaction_rid

    client.create_branch(ds["rid"], "from-master", branch["id"], branch["rid"])

    client.delete_dataset(ds["rid"])
    assert client.is_dataset_in_trash(dataset_path) is True

    with pytest.warns(UserWarning) as warning:
        client.get_dataset_details(dataset_path_or_rid=dataset_path)

    with pytest.raises(DatasetNotFoundError):
        client.delete_dataset(ds["rid"])


def test_get_dataset_rid(mocker, is_integration_test, client, iris_dataset):
    if not is_integration_test:
        mock_get = mocker.patch("requests.request")
        # mock_get.return_value = Mock(ok=True)
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "rid": iris_dataset[0],
            "name": "iris",
            "created": {
                "time": "2020-01-30T11:18:00.130419Z",
                "userId": "3c8fbda5-686e-4fcb-ad52-d95e4281d99f",
            },
            "modified": {
                "time": "2020-01-30T11:18:14.111774Z",
                "userId": "3c8fbda5-686e-4fcb-ad52-d95e4281d99f",
            },
            "lastModified": 1580383094111.0,
            "description": None,
            "operations": [
                "compass:edit-project",
                "compass:remove-imports",
                "compass:tags:change-resource-tags",
                "compass:view-project-group",
                "compass:share-link",
                "compass:linked-items:edit",
                "compass:linked-items:view",
                "compass:create-organization",
                "compass:import-resource-to",
                "compass:write-resource",
                "compass:move-between-projects",
                "compass:edit-alias",
                "compass:move-project",
                "compass:apply-markings",
                "gatekeeper:view-resource",
                "compass:read-branch",
                "compass:share-resource",
                "compass:delete",
                "compass:read-resource",
                "compass:open-resource-links",
                "compass:create-project",
                "compass:delete-project-group",
                "compass:import-resource-from",
                "compass:edit-organization",
                "compass:import-resource",
                "compass:change-resource-permission",
                "compass:write-branch",
                "compass:move-resource",
                "compass:view-long-description",
                "compass:edit-project-group",
                "compass:view-project-imports",
                "compass:tags:change-project-resource-tags",
                "compass:tags:change-non-project-resource-tags",
                "compass:move-within-project",
                "compass:create-project-group",
                "compass:view",
                "compass:discover",
                "compass:edit",
                "compass:manage",
            ],
            "urlVariables": {"compass:isProject": "false"},
            "favorite": None,
            "branches": None,
            "defaultBranch": None,
            "defaultBranchWithMarkings": None,
            "branchesCount": None,
            "hasBranches": None,
            "path": None,
            "longDescription": None,
            "directlyTrashed": False,
            "inTrash": None,
            "isAutosave": False,
            "collections": None,
            "namedCollections": None,
            "tags": None,
            "namedTags": None,
            "alias": None,
            "collaborators": None,
            "namedAncestors": None,
            "markings": None,
            "linkedItems": None,
        }
    rid = client.get_dataset_rid(iris_dataset[1])
    assert rid == iris_dataset[0]


@pytest.mark.integration
def test_schema_inference(client):
    rnd = "".join(choice(ascii_uppercase) for i in range(5))
    dataset_path = str(INTEGRATION_TEST_COMPASS_ROOT_PATH / f"test_api_schema_{rnd}")

    ds = client.create_dataset(dataset_path)
    client.create_branch(ds["rid"], "master")
    transaction_rid = client.open_transaction(ds["rid"], "SNAPSHOT", "master")
    client.upload_dataset_file(
        ds["rid"], transaction_rid, io.StringIO("col1,col2\n1,2"), "test.csv"
    )
    client.commit_transaction(ds["rid"], transaction_rid)

    with pytest.raises(DatasetHasNoSchemaError):
        client.get_dataset_schema(ds["rid"], transaction_rid, "master")

    inferred_schema = client.infer_dataset_schema(ds["rid"], "master")

    client.upload_dataset_schema(ds["rid"], transaction_rid, inferred_schema, "master")

    returned_schema = client.get_dataset_schema(ds["rid"], transaction_rid, "master")

    # seems to not be returned from infer schema API
    inferred_schema["primaryKey"] = None

    assert inferred_schema == returned_schema

    transaction_rid = client.open_transaction(ds["rid"], "SNAPSHOT", "master")
    client.upload_dataset_file(
        ds["rid"], transaction_rid, io.StringIO('col1,co"""l2\n1,2,3'), "test.csv"
    )
    client.commit_transaction(ds["rid"], transaction_rid)

    with pytest.warns(UserWarning) as warning:
        client.infer_dataset_schema(ds["rid"], "master")
    assert (
        str(warning[0].message)
        == "Foundry Schema inference completed with status 'WARN' "
        'and message \'No column delimiters found. The delimiter (Comma ",") '
        "was our best guess.'."
    )
    client.delete_dataset(ds["rid"])


def test_get_dataset_last_transaction_rid(client, iris_dataset):
    transaction_rid = client.get_dataset_last_transaction_rid(
        iris_dataset[0], branch=iris_dataset[3]
    )
    assert transaction_rid == iris_dataset[2]


def test_get_dataset_stats(mocker, is_integration_test, client, iris_dataset):
    stats_by_branch = client.get_dataset_stats(iris_dataset[0], view=iris_dataset[3])
    iris_path = f"{TEST_FOLDER.as_posix()}/test_data/iris/iris.csv"
    expected_iris_size = os.path.getsize(iris_path)
    assert stats_by_branch == {
        "sizeInBytes": expected_iris_size,
        "numFiles": 1,
        "hiddenFilesSizeInBytes": 0,
        "numHiddenFiles": 0,
        "numTransactions": 1,
    }
    stats_by_transaction = client.get_dataset_stats(
        iris_dataset[0],
        view=iris_dataset[2],
    )
    assert stats_by_transaction == {
        "sizeInBytes": expected_iris_size,
        "numFiles": 1,
        "hiddenFilesSizeInBytes": 0,
        "numHiddenFiles": 0,
        "numTransactions": 1,
    }


def test_get_dataset_schema(client, iris_dataset):
    expected_response = {
        "branchId": "master",
        "transactionRid": iris_dataset[2],
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
                }
            },
        },
        "attribution": {
            "userId": "3c8fbda5-686e-4fcb-ad52-d95e4281d99f",
            "time": "2020-01-30T11:18:35.123747Z",
        },
    }
    schema = client.get_dataset_schema(
        iris_dataset[0],
        iris_dataset[2],
        branch=iris_dataset[3],
    )
    assert schema == expected_response["schema"]


def test_get_spark_schema_branch_not_exists(client, iris_dataset):
    with pytest.raises(BranchNotFoundError):
        client.get_dataset_schema(
            iris_dataset[0],
            iris_dataset[2],
            branch="branch-does-not-exist",
        )


def test_get_and_download_dataset_files(client, iris_dataset, tmpdir):
    list_of_files = client.list_dataset_files(iris_dataset[0])
    assert list_of_files == ["iris.csv"]
    client.download_dataset_files(
        iris_dataset[0],
        str(tmpdir),
        list_of_files,
    )
    assert "iris.csv" in os.listdir(tmpdir)


@pytest.mark.integration
def test_get_csv_of_dataset(client, iris_dataset):
    response = client.get_dataset_as_raw_csv(iris_dataset[0])
    iris = pd.read_csv(io.BytesIO(response.content))
    assert iris.shape == (150, 5)


@pytest.mark.integration
def test_query_legacy_sql(client, iris_dataset, iris_no_schema_dataset):
    foundry_schema, data = client.query_foundry_sql_legacy(
        f"SELECT * FROM `{iris_dataset[0]}` LIMIT 100"
    )
    iris = pd.DataFrame(
        data=data, columns=[e["name"] for e in foundry_schema["fieldSchemaList"]]
    )
    assert iris.shape == (100, 5)

    with pytest.raises(BranchNotFoundError):
        client.query_foundry_sql_legacy(
            f"SELECT * FROM `{iris_dataset[0]}` LIMIT 100",
            branch="shouldnotexist",
        )

    with pytest.raises(DatasetHasNoSchemaError):
        client.query_foundry_sql_legacy(
            f"SELECT * FROM `{iris_no_schema_dataset[1]}` LIMIT 100",
            branch=iris_no_schema_dataset[3],
        )


@pytest.mark.integration
def test_query_sql(client, mocker, iris_dataset):
    iris_rid = iris_dataset[0]
    spy = mocker.spy(FoundryRestClient, "query_foundry_sql_legacy")

    iris_pdf = client.query_foundry_sql(f"SELECT * FROM `{iris_rid}` LIMIT 100")
    assert iris_pdf.shape == (100, 5)

    pa_table = client.query_foundry_sql(
        f"SELECT * FROM `{iris_rid}` LIMIT 100",
        return_type="arrow",
    )
    assert pa_table.shape == (100, 5)
    assert pa_table.num_columns == 5
    spy.assert_not_called()

    iris_fallback = client.query_foundry_sql(
        f"SELECT * FROM `{iris_rid}` order by sepal_width LIMIT 100"
    )
    assert iris_fallback.shape == (100, 5)
    # The order by should trigger a computation in spark and today 18/10/2022
    # the Foundry SQL server does not return ARROW but JSON
    spy.assert_called()

    spy.reset_mock()

    iris_where = client.query_foundry_sql(
        f"SELECT * FROM `{iris_rid}` where is_setosa = 'Iris-setosa'"
    )
    assert all(
        iris_where["is_setosa"] == "Iris-setosa"
    ), f"Still multiple classes: {iris_where['is_setosa'].unique()}"
    # The order by should trigger a computation in spark and today 18/10/2022
    # the Foundry SQL server does not return ARROW but JSON
    spy.assert_called()


def test_raise_for_status_prints_details(mocker, capsys):
    client = FoundryRestClient()
    from requests.models import Response

    the_response = mocker.Mock(spec=Response)
    the_response.text = "issue_text"
    the_response.status_code = 400
    the_response.raise_for_status.side_effect = HTTPError(
        "message", response=the_response
    )
    mocker.patch("requests.request").return_value = the_response
    with pytest.raises(HTTPError):
        client.get_dataset("test")
    captured = capsys.readouterr()
    assert captured.out == "message\nissue_text\n"
    assert captured.err == ""


@pytest.mark.integration
def test_get_folder_children(client, mocker, iris_dataset):
    children = client.get_child_objects_of_folder(
        folder_rid=INTEGRATION_TEST_COMPASS_ROOT_RID
    )
    assert (
        len(
            list(
                filter(
                    lambda child: child["name"] == iris_dataset[1].split("/")[-1],
                    children,
                )
            )
        )
        == 1
    )

    rnd = "".join(choice(ascii_uppercase) for i in range(5))
    folder_name = f"test_folder_{rnd}"
    no_children_folder = client.create_folder(
        name=folder_name, parent_id=INTEGRATION_TEST_COMPASS_ROOT_RID
    )

    no_children = client.get_child_objects_of_folder(
        folder_rid=no_children_folder["rid"]
    )
    assert len(list(no_children)) == 0

    client.move_resource_to_trash(resource_id=no_children_folder["rid"])

    with pytest.raises(FolderNotFoundError):
        next(
            client.get_child_objects_of_folder(folder_rid="ri.compass.main.folder.1337")
        )

    # Test pagination
    import requests

    spy = mocker.spy(requests, "request")
    children = client.get_child_objects_of_folder(
        folder_rid=INTEGRATION_TEST_COMPASS_ROOT_RID,
        page_size=1,
    )
    spy.assert_not_called()
    next(children)
    spy.assert_called()
    spy.reset_mock()
    next(children)
    spy.assert_called()


def test_download_dataset_files_temporary(
    client, mocker, iris_dataset, complex_dataset_fixture
):
    from pyarrow import parquet

    # Download single file, list call is not triggered
    spy = mocker.spy(type(client), "list_dataset_files")

    with client.download_dataset_files_temporary(
        dataset_rid=complex_dataset_fixture,
        view="master",
    ) as temp_dir:
        assert os.path.exists(temp_dir) is True
        _ = pd.read_parquet(temp_dir)
        _ = parquet.ParquetDataset(
            [x for x in Path(temp_dir).glob("**/*") if x.is_file()]
        ).read()
    assert os.path.exists(temp_dir) is False
    spy.assert_called()
    spy.reset_mock()

    with client.download_dataset_files_temporary(
        dataset_rid=iris_dataset[0],
        view="master",
    ) as temp_dir:
        assert os.path.exists(temp_dir) is True
        _ = pd.read_csv(os.sep.join([temp_dir, "iris.csv"]))
    assert os.path.exists(temp_dir) is False
    spy.assert_called()
    spy.reset_mock()

    with client.download_dataset_files_temporary(
        dataset_rid=iris_dataset[0],
        files=["iris.csv"],
        view="master",
    ) as temp_dir:
        assert os.path.exists(temp_dir) is True
        iris = pd.read_csv(os.sep.join([temp_dir, "iris.csv"]))
    assert os.path.exists(temp_dir) is False
    assert iris.shape == (150, 5)
    spy.assert_not_called()


@pytest.mark.integration
def test_remove_file_in_open_transaction(client, empty_dataset):
    rid = empty_dataset[0]

    transaction_rid = client.open_transaction(rid, "UPDATE", empty_dataset[3])

    client.upload_dataset_file(
        rid, transaction_rid, io.StringIO("col1,col2\n1,2"), "test.csv"
    )

    files = client.list_dataset_files(
        rid, view=empty_dataset[3], include_open_exclusive_transaction=True
    )
    assert "test.csv" in files

    client.remove_dataset_file(rid, transaction_rid, "test.csv", recursive=False)
    files = client.list_dataset_files(
        rid, view=empty_dataset[3], include_open_exclusive_transaction=True
    )
    assert "test.csv" not in files

    client.abort_transaction(rid, transaction_rid)


@pytest.mark.integration
def test_remove_file_from_previous_transaction(client, empty_dataset):
    rid = empty_dataset[0]

    transaction_rid = client.open_transaction(rid, "SNAPSHOT", empty_dataset[3])

    client.upload_dataset_file(
        rid, transaction_rid, io.StringIO("col1,col2\n1,2"), "test.csv"
    )

    client.commit_transaction(rid, transaction_rid)

    files = client.list_dataset_files(
        rid, view=empty_dataset[3], include_open_exclusive_transaction=False
    )
    assert "test.csv" in files

    transaction_rid = client.open_transaction(rid, "DELETE", "master")
    client.add_files_to_delete_transaction(
        rid, transaction_rid, logical_paths=["test.csv"]
    )
    client.commit_transaction(rid, transaction_rid)

    files = client.list_dataset_files(
        rid, view=empty_dataset[3], include_open_exclusive_transaction=False
    )
    assert "test.csv" not in files


def test_download_dataset_file_returns_bytes(client, iris_dataset):
    raw_bytes = client.download_dataset_file(
        dataset_rid=iris_dataset[0],
        output_directory=None,
        foundry_file_path="iris.csv",
    )
    iris = pd.read_csv(io.BytesIO(raw_bytes))
    assert iris.shape == (150, 5)


@pytest.mark.integration
def test_get_dataset_details_identical_path_rid(client, iris_dataset):
    by_path = client.get_dataset_details(dataset_path_or_rid=iris_dataset[1])

    by_rid = client.get_dataset_details(dataset_path_or_rid=iris_dataset[0])

    assert by_path["rid"] == by_rid["rid"]
    assert by_path["path"] == by_rid["path"]


@patch(
    "foundry_dev_tools.FoundryRestClient.get_dataset_details",
    MagicMock(
        return_value={
            "rid": "ri.foundry.main.dataset.1234",
            "name": "data1",
            "created": {
                "time": "2022-04-19T14:03:24.072061923Z",
                "userId": "1234",
            },
            "modified": {
                "time": "2022-04-19T14:03:27.656427209Z",
                "userId": "1234",
            },
            "lastModified": 1650377007656.0,
            "description": None,
            "operations": [
                "compass:read-resource",
                "compass:view-contact-information",
                "compass:discover",
            ],
            "urlVariables": {"compass:isProject": "false"},
            "favorite": None,
            "branches": None,
            "defaultBranch": None,
            "defaultBranchWithMarkings": None,
            "branchesCount": None,
            "hasBranches": None,
            "hasMultipleBranches": None,
            "path": "/path/data1",
            "longDescription": None,
            "directlyTrashed": False,
            "inTrash": None,
            "isAutosave": False,
            "isHidden": False,
            "deprecation": None,
            "collections": None,
            "namedCollections": None,
            "tags": None,
            "namedTags": None,
            "alias": None,
            "collaborators": None,
            "namedAncestors": None,
            "markings": None,
            "projectAccessMarkings": None,
            "linkedItems": None,
            "contactInformation": None,
            "classification": None,
            "disableInheritedPermissions": None,
            "propagatePermissions": None,
        }
    ),
)
def test_get_dataset_details_throws_on_no_read_permissions():
    from foundry_dev_tools import FoundryRestClient

    client = FoundryRestClient()
    with pytest.raises(DatasetNoReadAccessError):
        _ = client.get_dataset_identity(
            dataset_path_or_rid="ri.foundry.main.dataset.1234"
        )


def test_quoting_of_filenames():
    with requests_mock.mock(case_sensitive=True) as m:
        client = FoundryRestClient()
        needs_quotation = "prod2_api_v2_reports_query?#2022-06-27T22_24_18.528205Z.json"
        needs_no_quotation = (
            "All 123 - BLUB Extract_new (Export 2021-12-21 18_25)-20211221.140314.csv"
        )
        m.post(url=ANY, status_code=204)
        client.upload_dataset_file(
            dataset_rid="rid",
            transaction_rid="transaction_rid",
            path_or_buf=io.BytesIO(),
            path_in_foundry_dataset=needs_quotation,
        )

        client.upload_dataset_file(
            dataset_rid="rid",
            transaction_rid="transaction_rid",
            path_or_buf=io.BytesIO(),
            path_in_foundry_dataset=needs_no_quotation,
        )
        history = m.request_history
        assert (
            history[0].query
            == "logicalPath=prod2_api_v2_reports_query%3F%232022-06-27T22_24_18.528205Z.json"
        )
        assert (
            history[1].query
            == "logicalPath=All+123+-+BLUB+Extract_new+%28Export+2021-12-21+18_25%29-20211221.140314.csv"
        )
        m.get(url=ANY, status_code=200, body=io.BytesIO("some-content".encode("UTF-8")))
        client.download_dataset_file(
            dataset_rid="rid",
            output_directory=None,
            view="master",
            foundry_file_path=needs_quotation,
        )
        client.download_dataset_file(
            dataset_rid="rid",
            output_directory=None,
            view="master",
            foundry_file_path=needs_no_quotation,
        )
        history = m.request_history
        assert history[2].path.split("/")[-1] == quote(needs_quotation)
        assert history[3].path.split("/")[-1] == quote(needs_no_quotation)
