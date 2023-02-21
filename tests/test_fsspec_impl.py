import datetime
import io
import random
import string
import threading
import time
from unittest.mock import MagicMock, patch

import fsspec
import pandas as pd
import pytest
from fsspec import register_implementation
from pandas._testing import assert_frame_equal

import foundry_dev_tools.config
from foundry_dev_tools import FoundryRestClient
from foundry_dev_tools.foundry_api_client import DatasetHasOpenTransactionError
from foundry_dev_tools.fsspec_impl import (
    _correct_directories,
    _file_or_directory,
    _get_top_level_folder,
    FoundryDatasetPathInUrlNotSupportedError,
    FoundryDeleteInNotSupportedTransactionError,
    FoundryFileSystem,
    FoundrySimultaneousOpenTransactionError,
)

from tests.utils import generic_upload_dataset_if_not_exists

register_implementation("foundry", FoundryFileSystem)


@pytest.fixture()
def random_file():
    class RandomFileFactory:
        def get(self, length=5):
            return (
                "".join(
                    random.choices(string.ascii_uppercase + string.digits, k=length)
                )
                + ".txt"
            )

    return RandomFileFactory()


@pytest.fixture()
def fsspec_test_folder(is_integration_test):
    if is_integration_test:
        empty_dataset = generic_upload_dataset_if_not_exists(
            client=FoundryRestClient(),
            name="fsspec_test_folder_v1",
            upload_folder=None,
            foundry_schema=None,
        )
        if empty_dataset[4]:  # Newly created
            folder_setup(rid=empty_dataset[0])
        yield empty_dataset
    else:
        yield "empty-rid", "empty-path", None, "empty-branch", False


@pytest.fixture()
def fsspec_write_test_folder(is_integration_test):
    if is_integration_test:
        fsspec_write_test_folder = generic_upload_dataset_if_not_exists(
            client=FoundryRestClient(),
            name="fsspec_write_test_folder_v1",
            upload_folder=None,
            foundry_schema=None,
        )
        yield fsspec_write_test_folder
    else:
        yield "fsspec_write_test_folder-rid", "fsspec_write_test_folder-path", None, "fsspec_write_test_folder-branch", False


def folder_setup(rid: str):
    fc = FoundryRestClient()
    try:
        transaction_id = fc.open_transaction(rid, mode="SNAPSHOT", branch="master")
    except DatasetHasOpenTransactionError as exc:
        fc.abort_transaction(rid, transaction_id=exc.open_transaction_rid)
        transaction_id = fc.open_transaction(rid, mode="SNAPSHOT", branch="master")

    fc.upload_dataset_file(
        rid, transaction_id, io.StringIO("content"), "file_with_content.txt"
    )
    fc.upload_dataset_file(
        rid, transaction_id, io.StringIO(""), "folder/sub_folder/1_in_folder.txt"
    )
    fc.upload_dataset_file(
        rid,
        transaction_id,
        io.StringIO("withContentWillGoAway"),
        "folder/sub_folder/2_in_folder.txt",
    )
    fc.upload_dataset_file(
        rid, transaction_id, io.StringIO("content"), "folder/sub_folder/3_in_folder.txt"
    )
    fc.upload_dataset_file(
        rid, transaction_id, io.StringIO(""), "folder/sub_folder2/file.txt"
    )
    fc.upload_dataset_file(rid, transaction_id, io.StringIO(""), "empty_folder")
    fc.upload_dataset_file(rid, transaction_id, io.StringIO(""), "empty_file.txt")
    fc.upload_dataset_file(
        rid, transaction_id, io.StringIO("header\ncontent1"), "test1.csv"
    )
    fc.upload_dataset_file(
        rid, transaction_id, io.StringIO("header\ncontent2"), "test2.csv"
    )
    fc.commit_transaction(rid, transaction_id)

    transaction_id = fc.open_transaction(rid, mode="UPDATE", branch="master")
    fc.upload_dataset_file(
        rid, transaction_id, io.StringIO(""), "folder/sub_folder/2_in_folder.txt"
    )
    fc.commit_transaction(rid, transaction_id)


def test_file_or_directory_test():
    assert _file_or_directory("3_in_folder.txt", 0, "") == "file"
    assert _file_or_directory("3_in_folder", 0, "") == "directory"
    assert _file_or_directory("folder/sub_folder/3_in_folder.txt", 7, "") == "directory"
    assert (
        _file_or_directory("folder/sub_folder/3_in_folder.txt", 7, "folder/sub_folder/")
        == "file"
    )
    assert (
        _file_or_directory("folder/sub_folder/3_in_folder.txt", 7, "folder/")
        == "directory"
    )
    assert _file_or_directory("data/blah/data.csv", 4, "data/") == "directory"
    assert (
        _file_or_directory(
            "spark/part-00000-e7a71c36-03da-4328-9092-6edb2f1ed317-c000.snappy.parquet",
            27251536,
            "spark",
        )
        == "file"
    )


def test_correct_for_folder():
    input = [
        {
            "logicalPath": "empty_file.txt",
            "physicalPath": "transaction-1",
            "physicalUri": None,
            "transactionRid": "t1",
            "fileMetadata": {"length": 0},
            "isOpen": False,
            "timeModified": "2022-03-18T19:32:20.878Z",
        },
        {
            "logicalPath": "empty_folder",
            "physicalPath": "1",
            "physicalUri": None,
            "transactionRid": "t1",
            "fileMetadata": {"length": 0},
            "isOpen": False,
            "timeModified": "2022-03-18T19:32:20.039Z",
        },
        {
            "logicalPath": "folder/sub_folder/2_in_folder.txt",
            "physicalPath": "1",
            "physicalUri": None,
            "transactionRid": "t2",
            "fileMetadata": {"length": 0},
            "isOpen": False,
            "timeModified": "2022-03-18T19:32:24.148Z",
        },
        {
            "logicalPath": "folder/sub_folder/1_in_folder.txt",
            "physicalPath": "1",
            "physicalUri": None,
            "transactionRid": "t1",
            "fileMetadata": {"length": 0},
            "isOpen": False,
            "timeModified": "2022-03-18T19:32:19.188Z",
        },
        {
            "logicalPath": "folder/sub_folder/3_in_folder.txt",
            "physicalPath": "123",
            "physicalUri": None,
            "transactionRid": "t1",
            "fileMetadata": {"length": 7},
            "isOpen": False,
            "timeModified": "2022-03-24T13:31:01.593Z",
        },
        {
            "logicalPath": "file_with_content.txt",
            "physicalPath": "1",
            "physicalUri": None,
            "transactionRid": "t1",
            "fileMetadata": {"length": 7},
            "isOpen": False,
            "timeModified": "2022-03-18T19:32:18.364Z",
        },
    ]

    res = _correct_directories(input)

    assert {
        "name": "empty_file.txt",
        "size": 0,
        "type": "file",
        "time_modified": "2022-03-18T19:32:20.878Z",
        "transaction_rid": "t1",
        "is_open": False,
    } in res
    assert {
        "name": "file_with_content.txt",
        "size": 7,
        "type": "file",
        "time_modified": "2022-03-18T19:32:18.364Z",
        "transaction_rid": "t1",
        "is_open": False,
    } in res
    assert {
        "name": "empty_folder",
        "size": 0,
        "type": "directory",
        "time_modified": "2022-03-18T19:32:20.039Z",
        "transaction_rid": "t1",
        "is_open": False,
    } in res
    assert {
        "name": "folder",
        "size": 0,
        "type": "directory",
        "time_modified": "2022-03-18T19:32:19.188Z",
        "transaction_rid": "t1",
        "is_open": False,
    } in res
    assert len(res) == 4


def test_correct_for_folder_in_subfolder_file():
    input = [
        {
            "logicalPath": "folder/sub_folder/1_in_folder.txt",
            "physicalPath": "93e9675d-1c5c-4cd7-ac01-a5a7d98ef25f/000000d5-c4e6-150d-a606-32540fd23fe2/",
            "physicalUri": None,
            "transactionRid": "t1",
            "fileMetadata": {"length": 0},
            "isOpen": False,
            "timeModified": "2022-03-21T11:26:56.819Z",
        },
        {
            "logicalPath": "folder/sub_folder/2_in_folder.txt",
            "physicalPath": "93e9675d-1c5c-4cd7-ac01-a5a7d98ef25f/000000d5-c4e6-a6f9-8165-b97574f58d97/",
            "physicalUri": None,
            "transactionRid": "t2",
            "fileMetadata": {"length": 42},
            "isOpen": False,
            "timeModified": "2022-03-21T11:27:01.858Z",
        },
    ]

    res = _correct_directories(input, subfolder_prefix="folder/sub_folder")

    assert {
        "name": "folder/sub_folder/1_in_folder.txt",
        "size": 0,
        "type": "file",
        "time_modified": "2022-03-21T11:26:56.819Z",
        "transaction_rid": "t1",
        "is_open": False,
    } in res
    assert {
        "name": "folder/sub_folder/2_in_folder.txt",
        "size": 42,
        "type": "file",
        "time_modified": "2022-03-21T11:27:01.858Z",
        "transaction_rid": "t2",
        "is_open": False,
    } in res


def test_correct_subfolder():
    input = [
        {
            "logicalPath": "folder/sub_folder/1_in_folder.txt",
            "physicalPath": "93e9675d-1c5c-4cd7-ac01-a5a7d98ef25f/000000d5-c4e6-150d-a606-32540fd23fe2/",
            "physicalUri": None,
            "transactionRid": "t1",
            "fileMetadata": {"length": 0},
            "isOpen": False,
            "timeModified": "2022-03-21T11:26:56.819Z",
        },
        {
            "logicalPath": "folder/sub_folder/2_in_folder.txt",
            "physicalPath": "93e9675d-1c5c-4cd7-ac01-a5a7d98ef25f/000000d5-c4e6-a6f9-8165-b97574f58d97/",
            "physicalUri": None,
            "transactionRid": "t2",
            "fileMetadata": {"length": 0},
            "isOpen": False,
            "timeModified": "2022-03-21T11:27:01.858Z",
        },
        {
            "logicalPath": "folder/sub_folder2/blablub.txt",
            "physicalPath": "93e9675d-1c5c-4cd7-ac01-a5a7d98ef25f/000000d5-c4e6-a6f9-8165-b97574f58d97/",
            "physicalUri": None,
            "transactionRid": "t3",
            "fileMetadata": {"length": 0},
            "isOpen": False,
            "timeModified": "2022-03-21T11:28:01.858Z",
        },
    ]

    res = _correct_directories(input, subfolder_prefix="folder")

    assert {
        "name": "folder/sub_folder",
        "size": 0,
        "type": "directory",
        "time_modified": "2022-03-21T11:26:56.819Z",
        "transaction_rid": "t1",
        "is_open": False,
    } in res

    assert {
        "name": "folder/sub_folder2",
        "size": 0,
        "type": "directory",
        "time_modified": "2022-03-21T11:28:01.858Z",
        "transaction_rid": "t3",
        "is_open": False,
    } in res


def test_correct_subfolder_removesuffix():
    input = [
        {
            "logicalPath": "data/LipidsDataset/test/biological_data.jbl",
            "physicalPath": "d63c3ad9-89ef-49f4-a380-b7698ed7c978/0000013b-7c77-7852-997d-e7df7a003d49/ca2d4ed0-38b3-4fb5-937c-87925c76eb5a",
            "physicalUri": None,
            "transactionRid": "ri.foundry.main.transaction.0000013b-7c77-7852-997d-e7df7a003d49",
            "fileMetadata": {"length": 2939},
            "isOpen": False,
            "timeModified": "2023-01-30T09:23:55.302Z",
        },
        {
            "logicalPath": "data/LipidsDataset/test/experiment_definition.jbl",
            "physicalPath": "d63c3ad9-89ef-49f4-a380-b7698ed7c978/0000013b-7c77-7852-997d-e7df7a003d49/623dab8f-8ded-45b2-bcf5-3363d4570fbf",
            "physicalUri": None,
            "transactionRid": "ri.foundry.main.transaction.0000013b-7c77-7852-997d-e7df7a003d49",
            "fileMetadata": {"length": 2710},
            "isOpen": False,
            "timeModified": "2023-01-30T09:23:54.323Z",
        },
        {
            "logicalPath": "data/LipidsDataset/test/ionisable_lipids.jbl",
            "physicalPath": "d63c3ad9-89ef-49f4-a380-b7698ed7c978/0000013b-7c77-7852-997d-e7df7a003d49/c7c0d168-3369-41f6-9e23-8b5dafb7e238",
            "physicalUri": None,
            "transactionRid": "ri.foundry.main.transaction.0000013b-7c77-7852-997d-e7df7a003d49",
            "fileMetadata": {"length": 3005},
            "isOpen": False,
            "timeModified": "2023-01-30T09:23:53.735Z",
        },
        {
            "logicalPath": "data/LipidsDataset/test/pc_data.jbl",
            "physicalPath": "d63c3ad9-89ef-49f4-a380-b7698ed7c978/0000013b-7c77-7852-997d-e7df7a003d49/db3aadbf-f892-4960-b23e-a8129f2afe0a",
            "physicalUri": None,
            "transactionRid": "ri.foundry.main.transaction.0000013b-7c77-7852-997d-e7df7a003d49",
            "fileMetadata": {"length": 1887},
            "isOpen": False,
            "timeModified": "2023-01-30T09:23:54.781Z",
        },
    ]

    res = _correct_directories(input, subfolder_prefix="data/LipidsDataset")
    assert {
        "name": "data/LipidsDataset/test",
        "size": 0,
        "type": "directory",
        "time_modified": "2023-01-30T09:23:53.735Z",
        "transaction_rid": "ri.foundry.main.transaction.0000013b-7c77-7852-997d-e7df7a003d49",
        "is_open": False,
    } in res


def test_strip_protocol():
    assert (
        FoundryFileSystem._strip_protocol(
            "foundry://ri.foundry.main.dataset.fee62053-77ed-4617-bd01-fc2538366c3f/folder/test.csv"
        )
        == "folder/test.csv"
    )
    assert (
        FoundryFileSystem._strip_protocol(
            "foundry://ri.foundry.main.dataset.fee62053-77ed-4617-bd01-fc2538366c3f/test.csv"
        )
        == "test.csv"
    )
    assert (
        FoundryFileSystem._strip_protocol(
            "foundry://ri.foundry.main.dataset.fee62053-77ed-4617-bd01-fc2538366c3f/"
        )
        == ""
    )
    assert (
        FoundryFileSystem._strip_protocol(
            "foundry://ri.foundry.main.dataset.fee62053-77ed-4617-bd01-fc2538366c3f"
        )
        == ""
    )


@pytest.mark.integration
def test_clean_snapshot_transaction(empty_dataset):
    rid = empty_dataset[0]
    fs = FoundryFileSystem(dataset=rid, branch="master")
    with fs.start_transaction(transaction_type="SNAPSHOT") as transaction:
        assert transaction.transaction_rid is not None
        assert transaction.transaction_type == "SNAPSHOT"


@pytest.mark.integration
def test_read_with_dataset_alias(fsspec_test_folder):
    base_path = fsspec_test_folder[1]
    fs = FoundryFileSystem(dataset=base_path, branch="master")

    with fs.open("folder/sub_folder/3_in_folder.txt") as f:
        assert f.read().decode("UTF-8") == "content"


@pytest.mark.integration
def test_write(fsspec_write_test_folder):
    with fsspec.open(
        f"foundry://{fsspec_write_test_folder[0]}/test.txt",
        "w",
    ) as f:
        f.write("content")
    with fsspec.open(
        f"foundry://{fsspec_write_test_folder[0]}/test.txt",
        "r",
    ) as f:
        content = f.read()
    assert content == "content"


@pytest.mark.integration
def test_read_single_csv(fsspec_test_folder):
    df = pd.read_csv(
        f"foundry://{fsspec_test_folder[0]}/test1.csv",
    )
    assert df.shape == (1, 1)

    df = pd.read_csv(
        f"foundry://{fsspec_test_folder[0]}/test1.csv",
        storage_options={"branch": "master"},
    )
    assert df.shape == (1, 1)


@pytest.mark.integration
def test_read_dask_pyarrow(complex_dataset_fixture):
    import dask
    import dask.dataframe as dd

    dask.config.set(scheduler="synchronous")

    df = dd.read_parquet(
        f"foundry://{complex_dataset_fixture}/",
        storage_options={"branch": "master"},
        engine="pyarrow",
    )
    unique = df["string_column"].unique().compute()
    print(unique)
    print(df.shape)


@pytest.mark.integration
def test_read_dask_fastparquet(complex_dataset_fixture):
    import dask.dataframe as dd

    df = dd.read_parquet(
        f"foundry://{complex_dataset_fixture}/spark",
        storage_options={"branch": "master"},
        engine="fastparquet",
    )
    unique = df["string_column"].unique().compute()
    print(unique)
    print(df.shape)


@pytest.mark.integration
def test_read_write_parquet(fsspec_write_test_folder):
    df = pd.DataFrame(data={"col1": ["row1", "row2"]})
    df.to_parquet(f"foundry://{fsspec_write_test_folder[0]}/test.parquet")
    from_foundry = pd.read_parquet(
        f"foundry://{fsspec_write_test_folder[0]}/test.parquet"
    )
    assert_frame_equal(from_foundry, df)


@pytest.mark.integration
def test_write_transaction(random_file, fsspec_write_test_folder):
    random_file_1 = random_file.get()
    random_file_2 = random_file.get()
    random_file_3 = random_file.get()
    fs = FoundryFileSystem(
        dataset=fsspec_write_test_folder[0],
        branch="master",
    )
    with fs.transaction:
        with fs.open(random_file_1, "w") as f:
            f.write("content")
        with fs.open(random_file_2, "w") as f:
            f.write("content2")

        assert (
            fs.ls(random_file_1)[0]["transaction_rid"]
            == fs.ls(random_file_2)[0]["transaction_rid"]
        )

    with fs.open(random_file_1, "r") as f:
        content = f.read()
    assert content == "content"

    try:
        with fs.transaction:
            with fs.open(random_file_3, "w") as f:
                f.write("content")
            raise KeyboardInterrupt
    except KeyboardInterrupt:
        assert not fs.exists(random_file_3)


@pytest.mark.integration
def test_delete_simple(random_file, fsspec_write_test_folder):
    file = random_file.get()
    fs = FoundryFileSystem(
        dataset=fsspec_write_test_folder[0],
        branch="master",
    )
    with fs.open(file, "w") as f:
        f.write("content")
    assert fs.exists(file)

    fs.rm(file)

    assert not fs.exists(file)


@pytest.mark.integration
def test_delete_simple_folder(random_file, fsspec_write_test_folder):
    random_folder = random_file.get()
    file1 = random_file.get()
    file1_in_folder = f"{random_folder}/{file1}"
    file2 = random_file.get()
    file2_in_folder = f"{random_folder}/{file2}"
    fs = FoundryFileSystem(
        dataset=fsspec_write_test_folder[0],
        branch="master",
    )
    with fs.transaction:
        with fs.open(file1_in_folder, "w") as f:
            f.write("content")
        with fs.open(file2_in_folder, "w") as f:
            f.write("content2")
    ls_result = fs.ls(random_folder, detail=False)
    assert file1_in_folder in ls_result
    assert file2_in_folder in ls_result

    with pytest.raises(IsADirectoryError):
        fs.rm(random_folder)
    assert fs.exists(random_folder)

    fs.rm(random_folder, recursive=True)
    assert not fs.exists(random_folder)


@pytest.mark.integration
def test_delete_from_open_transaction(fsspec_write_test_folder):
    from_previous_t = (
        "".join(random.choices(string.ascii_uppercase + string.digits, k=5)) + ".txt"
    )
    file = "".join(random.choices(string.ascii_uppercase + string.digits, k=5)) + ".txt"
    fs = FoundryFileSystem(
        dataset=fsspec_write_test_folder[0],
        branch="master",
    )
    with fs.open(from_previous_t, "w") as f:
        f.write("exists_before")
    with fs.transaction:
        with fs.open(file, "w") as f:
            f.write("content")
        fs.rm(file)
        with pytest.raises(FoundryDeleteInNotSupportedTransactionError):
            fs.rm(from_previous_t)
    assert not fs.exists(file)
    assert fs.exists(from_previous_t)
    with fs.start_transaction(transaction_type="DELETE"):
        fs.rm(from_previous_t)
    assert not fs.exists(from_previous_t)


@pytest.mark.integration
def test_delete_folder(random_file, fsspec_write_test_folder):
    random_folder = random_file.get()
    random_file = random_file.get()
    file_in_folder = f"{random_folder}/test/{random_file}"
    fs = FoundryFileSystem(
        dataset=fsspec_write_test_folder[0],
        branch="master",
    )
    with fs.transaction:
        with fs.open(file_in_folder, "w") as f:
            f.write("content")
        with pytest.raises(IsADirectoryError):
            fs.rm(random_folder)
        fs.rm(random_folder, recursive=True)
        with pytest.raises(FileNotFoundError):
            fs.rm(random_folder + "/nofile")
    assert not fs.exists(file_in_folder)


@pytest.mark.integration
def test_transaction_already_open_throws(random_file, fsspec_write_test_folder):
    random_folder = random_file.get()
    fs = FoundryFileSystem(
        dataset=fsspec_write_test_folder[0],
        branch="master",
        transaction_backoff=False,
    )
    with fs.transaction:
        with fs.open(random_folder, "w") as f:
            f.write("content")
        with pytest.raises(FoundrySimultaneousOpenTransactionError) as exc_info:
            with fsspec.open(
                f"foundry://{fsspec_write_test_folder[0]}/test.txt",
                "w",
            ) as f:
                f.write("content")
        assert exc_info.value.dataset_rid == fsspec_write_test_folder[0]
        assert exc_info.value.open_transaction_rid == fs._transaction.transaction_rid


@patch(
    "foundry_dev_tools.FoundryRestClient.get_dataset_identity",
    MagicMock(),
)
def test_custom_token_is_used():
    fs = FoundryFileSystem(
        dataset="ri.foundry.main.dataset.fake1bb5-be92-4ad9-aa3e-07c161751234",
        branch="master",
        token="super-secret-token",
    )
    assert fs.api._config["jwt"] == "super-secret-token"
    assert fs.api._headers()["Authorization"] == "Bearer super-secret-token"

    parsed = FoundryFileSystem._get_kwargs_from_urls(
        "foundry://:super-secret-token@ri.foundry.main.dataset.fake1bb5-be92-4ad9-aa3e-07c161751234/test.txt"
    )
    assert parsed["token"] == "super-secret-token"
    assert (
        parsed["dataset"]
        == "ri.foundry.main.dataset.fake1bb5-be92-4ad9-aa3e-07c161751234"
    )


@patch(
    "foundry_dev_tools.FoundryRestClient.get_dataset_identity",
    MagicMock(),
)
def test_branch_extracted():
    parsed = FoundryFileSystem._get_kwargs_from_urls(
        "foundry://dev-iteration:super-secret-token@ri.foundry.main.dataset.fake1bb5-be92-4ad9-aa3e-07c161751234/test.txt"
    )
    assert parsed["token"] == "super-secret-token"
    assert (
        parsed["dataset"]
        == "ri.foundry.main.dataset.fake1bb5-be92-4ad9-aa3e-07c161751234"
    )
    assert parsed["branch"] == "dev-iteration"

    parsed = FoundryFileSystem._get_kwargs_from_urls(
        "foundry://dev-iteration:@ri.foundry.main.dataset.fake1bb5-be92-4ad9-aa3e-07c161751234/test.txt"
    )
    assert "token" not in parsed
    assert (
        parsed["dataset"]
        == "ri.foundry.main.dataset.fake1bb5-be92-4ad9-aa3e-07c161751234"
    )
    assert parsed["branch"] == "dev-iteration"

    parsed = FoundryFileSystem._get_kwargs_from_urls(
        "foundry://dev-iteration@ri.foundry.main.dataset.fake1bb5-be92-4ad9-aa3e-07c161751234/test.txt"
    )
    assert "token" not in parsed
    assert (
        parsed["dataset"]
        == "ri.foundry.main.dataset.fake1bb5-be92-4ad9-aa3e-07c161751234"
    )
    assert parsed["branch"] == "dev-iteration"

    parsed = FoundryFileSystem._get_kwargs_from_urls(
        "foundry://:super-secret-token@ri.foundry.main.dataset.fake1bb5-be92-4ad9-aa3e-07c161751234/test.txt"
    )
    assert parsed["token"] == "super-secret-token"
    assert (
        parsed["dataset"]
        == "ri.foundry.main.dataset.fake1bb5-be92-4ad9-aa3e-07c161751234"
    )
    assert "branch" not in parsed

    parsed = FoundryFileSystem._get_kwargs_from_urls(
        "foundry://ri.foundry.main.dataset.fake1bb5-be92-4ad9-aa3e-07c161751234/test.txt"
    )
    assert "token" not in parsed
    assert (
        parsed["dataset"]
        == "ri.foundry.main.dataset.fake1bb5-be92-4ad9-aa3e-07c161751234"
    )
    assert "branch" not in parsed

    parsed = FoundryFileSystem._get_kwargs_from_urls(
        "foundry://ri.foundry.main.dataset.fake1bb5-be92-4ad9-aa3e-07c161751234"
    )
    assert "token" not in parsed
    assert (
        parsed["dataset"]
        == "ri.foundry.main.dataset.fake1bb5-be92-4ad9-aa3e-07c161751234"
    )
    assert "branch" not in parsed

    with pytest.raises(FoundryDatasetPathInUrlNotSupportedError):
        FoundryFileSystem._get_kwargs_from_urls(
            "foundry:///path/to//fsspec_write_test_folder/test.txt"
        )


@patch(
    "foundry_dev_tools.FoundryRestClient.get_dataset_identity",
    MagicMock(),
)
def test_appending_not_implemented_raises():
    fs = FoundryFileSystem(
        dataset="ri.foundry.main.dataset.fake1bb5-be92-4ad9-aa3e-07c161751234",
        branch="master",
        token="super-secret-token",
    )
    with pytest.raises(NotImplementedError):
        fs.open("test", mode="a")


@patch(
    "foundry_dev_tools.FoundryRestClient.get_dataset_identity",
    MagicMock(),
)
@patch(
    "foundry_dev_tools.FoundryRestClient.list_dataset_files",
    MagicMock(
        return_value=[
            {
                "logicalPath": "empty_file.txt",
                "physicalPath": "transaction-1",
                "physicalUri": None,
                "transactionRid": "t1",
                "fileMetadata": {"length": 0},
                "isOpen": False,
                "timeModified": "2022-03-18T19:32:20.878Z",
            }
        ]
    ),
)
def test_modified():
    fs = FoundryFileSystem(
        dataset="ri.foundry.main.dataset.fake1bb5-be92-4ad9-aa3e-07c161751234"
    )
    assert fs.modified("empty_file.txt") == datetime.datetime(
        2022, 3, 18, 19, 32, 20, 878000, tzinfo=datetime.timezone.utc
    )


@patch(
    "foundry_dev_tools.FoundryRestClient.get_dataset_identity",
    MagicMock(
        return_value={
            "dataset_path": "/path/to/ds",
            "dataset_rid": "ri.foundry.main.dataset.1234",
            "last_transaction_rid": None,
        }
    ),
)
def test_listdir_in_empty_dataset():
    fs = FoundryFileSystem(dataset="ri.foundry.main.dataset.1234")
    with pytest.raises(FileNotFoundError):
        fs.listdir("bla/blubdoesnotexists")


@pytest.mark.integration
def test_two_instances(random_file, fsspec_write_test_folder):
    random_file = random_file.get()
    fs = FoundryFileSystem(
        dataset=fsspec_write_test_folder[0],
        branch="master",
    )
    fs_other_instance = FoundryFileSystem(
        dataset=fsspec_write_test_folder[0],
        branch="master",
        skip_instance_cache=True,
    )
    with fs.transaction:
        with fs.open(random_file, "w") as f:
            f.write("content")
        fs_same_instance = FoundryFileSystem(
            dataset=fsspec_write_test_folder[0],
            branch="master",
        )
        assert fs_same_instance._intrans is True
        assert fs_same_instance.exists(random_file) is True

        assert fs_other_instance._intrans is False
        assert fs_other_instance.exists(random_file) is False

    fs.delete(random_file)
    with pytest.raises(FileNotFoundError):
        with fs_other_instance.open(random_file, "r") as f:
            f.read()


def test_slash_at_end():
    file_details = [
        {
            "logicalPath": "data/blah/data.csv",
            "physicalPath": "8324bd10-ffbe-411d-8d70-fe0ca752ab4d/000000e2-a25f-1c31-a804-8308b0d436dd/539aed08-a648-47d3-affc-51e4b6662203",
            "physicalUri": None,
            "transactionRid": "ri.foundry.main.transaction.000000e2-a25f-1c31-a804-8308b0d436dd",
            "fileMetadata": {"length": 4},
            "isOpen": False,
            "timeModified": "2022-04-25T14:08:47.499Z",
        },
        {
            "logicalPath": "data/blub",
            "physicalPath": "8324bd10-ffbe-411d-8d70-fe0ca752ab4d/000000e2-a395-4c0a-87ce-a60f4d687d55/4948123a-8416-46b5-85aa-aa114f1b43bf",
            "physicalUri": None,
            "transactionRid": "ri.foundry.main.transaction.000000e2-a395-4c0a-87ce-a60f4d687d55",
            "fileMetadata": {"length": 0},
            "isOpen": False,
            "timeModified": "2022-04-25T14:25:01.722Z",
        },
        {
            "logicalPath": "data/blub/data.csv",
            "physicalPath": "8324bd10-ffbe-411d-8d70-fe0ca752ab4d/000000e2-a396-ecf1-acaf-464bb5fe5a3e/675883b3-e481-4888-a12b-6707238f5a6a",
            "physicalUri": None,
            "transactionRid": "ri.foundry.main.transaction.000000e2-a396-ecf1-acaf-464bb5fe5a3e",
            "fileMetadata": {"length": 4},
            "isOpen": False,
            "timeModified": "2022-04-25T14:25:05.420Z",
        },
        {
            "logicalPath": "data/blub/data2.csv",
            "physicalPath": "8324bd10-ffbe-411d-8d70-fe0ca752ab4d/000000e2-a398-a6f1-ac25-c650ea6dc4c8/6c78deed-b401-4ec2-aab7-bd18d517d225",
            "physicalUri": None,
            "transactionRid": "ri.foundry.main.transaction.000000e2-a398-a6f1-ac25-c650ea6dc4c8",
            "fileMetadata": {"length": 4},
            "isOpen": False,
            "timeModified": "2022-04-25T14:25:09.140Z",
        },
    ]
    subfolder_prefix = "data/"
    res = _correct_directories(file_details, subfolder_prefix)
    assert {
        "name": "data/blub",
        "size": 0,
        "type": "directory",
        "time_modified": "2022-04-25T14:25:01.722Z",
        "transaction_rid": "ri.foundry.main.transaction.000000e2-a395-4c0a-87ce-a60f4d687d55",
        "is_open": False,
    } in res

    assert {
        "name": "data/blah",
        "size": 0,
        "type": "directory",
        "time_modified": "2022-04-25T14:08:47.499Z",
        "transaction_rid": "ri.foundry.main.transaction.000000e2-a25f-1c31-a804-8308b0d436dd",
        "is_open": False,
    } in res


def test_get_top_level_folder():
    assert _get_top_level_folder("data/dog", "data/") == "dog"
    assert _get_top_level_folder("data/dog", "data") == "dog"
    assert _get_top_level_folder("data/hubabubba", "data/hubabubba") == ""


@pytest.mark.integration
def test_transaction_retry(random_file, fsspec_write_test_folder):
    file_in_thread = random_file.get()
    file = random_file.get()

    def run_in_thread():
        fs_inner = FoundryFileSystem(
            dataset=fsspec_write_test_folder[0],
            branch="master",
        )
        with fs_inner.transaction:
            time.sleep(3)
            with fs_inner.open(file_in_thread, "w") as f:
                f.write("content")

    thread = threading.Thread(target=run_in_thread)
    thread.start()
    # wait for thread to start transaction
    time.sleep(1)
    fs_no_backoff = FoundryFileSystem(
        dataset=fsspec_write_test_folder[0],
        branch="master",
        transaction_backoff=False,
    )
    with pytest.raises(FoundrySimultaneousOpenTransactionError):
        with fs_no_backoff.open(file, "w") as f:
            f.write("content")
    fs = FoundryFileSystem(
        dataset=fsspec_write_test_folder[0],
        branch="master",
    )
    with fs.open(file, "w") as f:
        f.write("content")

    assert fs.exists(file_in_thread)
    assert fs.exists(file)


@patch(
    "foundry_dev_tools.FoundryRestClient.get_dataset_identity",
    MagicMock(
        return_value={
            "dataset_path": "/path/to/ds",
            "dataset_rid": "ri.foundry.main.dataset.1234",
            "last_transaction_rid": None,
        }
    ),
)
def test_fsspec_api_client_not_cached(mocker):
    with mocker.patch(
        "foundry_dev_tools.Configuration.get_config",
        side_effect=[
            {"jwt": "secret-token-ONE", "foundry_url": "https://test.com"},
            {"jwt": "secret-token-TWO", "foundry_url": "https://test.com"},
            {"jwt": "secret-token-THREE", "foundry_url": "https://test.com"},
        ],
    ):
        fs = FoundryFileSystem(
            dataset="ri.foundry.main.dataset.1234-not-in-instance-cache"
        )
        assert fs.api._config["jwt"] == "secret-token-TWO"
        assert fs.api._config["jwt"] == "secret-token-THREE"


@pytest.mark.integration
def test_ls_trailing_slash_empty_folder(random_file, fsspec_write_test_folder):
    random_folder = random_file.get()
    file1 = random_file.get()
    file1_in_folder = f"{random_folder}/{file1}"

    fs = FoundryFileSystem(
        dataset=fsspec_write_test_folder[0],
        branch="master",
    )
    with fs.transaction:
        # Create empty folder
        with fs.open(random_folder, "w") as f:
            f.write("")
        with fs.open(file1_in_folder, "w") as f:
            f.write("content2")
    ls_result_no_slash = fs.ls(random_folder, detail=False)
    ls_result_with_slash = fs.ls(random_folder + "/", detail=False)
    assert ls_result_no_slash == ls_result_with_slash
