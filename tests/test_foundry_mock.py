import io
import os
import shutil
import time
from pathlib import Path

import fs
import pytest

from foundry_dev_tools.foundry_api_client import (
    BranchesAlreadyExistError,
    DatasetHasOpenTransactionError,
    DatasetNotFoundError,
)
from tests.foundry_mock_client import MockFoundryRestClient


@pytest.fixture()
def root_dir():
    root = Path(__file__).parent.joinpath("foundry_mock_root")
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir()
    return root


def test_create_rids_time_ordered(tmpdir, root_dir):
    bases = [tmpdir, root_dir]
    for base in bases:
        filesystem = fs.open_fs(os.fspath(base))
        client = MockFoundryRestClient(filesystem=filesystem)

        dataset = client.create_dataset("/path/to/ds")
        assert "rid" in dataset
        assert "fileSystemId" in dataset
        get_result = client.get_dataset(dataset_rid=dataset["rid"])
        assert dataset == get_result

        with pytest.raises(KeyError):
            _ = client.create_dataset("/path/to/ds")
        # uuid's are roughly time ordered
        rids = []
        for i in range(
            0,
        ):
            rids.append(client.create_dataset(f"/path/to/ds{i}")["rid"])
            time.sleep(0.001)
        assert rids == sorted(rids)


def test_dataset_not_found(tmpdir, root_dir):
    bases = [tmpdir, root_dir]
    for base in bases:
        filesystem = fs.open_fs(os.fspath(base))
        client = MockFoundryRestClient(filesystem=filesystem)
        with pytest.raises(DatasetNotFoundError):
            _ = client.get_dataset(dataset_rid="blarid")


def test_create_delete(tmpdir, root_dir):
    bases = [tmpdir, root_dir]
    for base in bases:
        filesystem = fs.open_fs(os.fspath(base))
        client = MockFoundryRestClient(filesystem=filesystem)
        dataset = client.create_dataset("/path/to/to_be_deleted")
        client.delete_dataset(dataset["rid"])


def test_create_delete_branch(tmpdir, root_dir):
    bases = [root_dir, tmpdir]
    for base in bases:
        filesystem = fs.open_fs(os.fspath(base))
        client = MockFoundryRestClient(filesystem=filesystem)
        dataset = client.create_dataset("/path/to/ds_with_branch")
        branch = client.create_branch(dataset_rid=dataset["rid"], branch="main")
        assert branch["id"] == "main"
        assert "rid" in branch
        assert "ancestorBranchIds" in branch
        assert "creationTime" in branch
        assert branch["transactionRid"] is None

        branch_returned = client.get_branch(dataset_rid=dataset["rid"], branch="main")

        assert branch == branch_returned

        with pytest.raises(BranchesAlreadyExistError):
            _ = client.create_branch(dataset_rid=dataset["rid"], branch="main")

        client.create_branch(dataset_rid=dataset["rid"], branch="master")

        client.delete_dataset(dataset["rid"])


def test_transactions(tmp_path_factory, root_dir):  # noqa: PLR0915, TODO?
    temp_directory = tmp_path_factory.mktemp("foundry_dev_tools_test_1").as_posix()
    bases = [root_dir, temp_directory]
    for base in bases:
        filesystem = fs.open_fs(os.fspath(base))
        client = MockFoundryRestClient(filesystem=filesystem)

        BRANCH = "master"

        ds = client.create_dataset("/path/to/ds")
        identity_by_path = client.get_dataset_identity(
            dataset_path_or_rid="/path/to/ds"
        )
        identity_by_rid = client.get_dataset_identity(dataset_path_or_rid=ds["rid"])
        assert identity_by_path == identity_by_rid
        assert identity_by_path["last_transaction_rid"] is None
        branch = client.create_branch(dataset_rid=ds["rid"], branch=BRANCH)

        with pytest.raises(DatasetNotFoundError):
            client.get_dataset_identity(dataset_path_or_rid="doesnotExists")
        with pytest.raises(DatasetNotFoundError):
            client.get_dataset_identity(
                dataset_path_or_rid="ri.foundry.main.dataset.12342ede-1530-0cf3-8f56-9a4b2231404c"
            )

        assert client.get_dataset_last_transaction(ds["rid"]) is None

        transaction_rid = client.open_transaction(ds["rid"], "SNAPSHOT", BRANCH)
        branch = client.get_branch(dataset_rid=ds["rid"], branch=BRANCH)
        assert branch["transactionRid"] is None
        assert branch["openTransactionRid"] == transaction_rid

        assert (
            client.get_dataset_identity(dataset_path_or_rid=ds["rid"])[
                "last_transaction_rid"
            ]
            is None
        )

        with pytest.raises(DatasetHasOpenTransactionError) as exc_info:
            client.open_transaction(ds["rid"], "SNAPSHOT", BRANCH)
        assert exc_info.value.dataset_rid == ds["rid"]
        assert exc_info.value.open_transaction_rid == transaction_rid

        tmpdir = tmp_path_factory.mktemp("foundry_dev_tools_test_2")
        path1 = tmpdir.joinpath("test1.csv")
        with path1.open(mode="w") as f:
            f.write("col1,col2\n1,2")
        path2 = tmpdir.joinpath("test2.csv")
        with path2.open(mode="w") as f:
            f.write("col1,col2\n1,2")
        path3 = tmpdir.joinpath("test3.csv")
        with path3.open(mode="w") as f:
            f.write("col1,col2\n1,2")
        client.upload_dataset_file(ds["rid"], transaction_rid, path1, "test1.csv")

        client.upload_dataset_files(
            ds["rid"],
            transaction_rid,
            path_file_dict={"test2.csv": path2, "test3.csv": path3},
        )

        client.upload_dataset_file(
            ds["rid"], transaction_rid, io.StringIO("col1,col2\n1,2"), "test4.csv"
        )

        client.upload_dataset_file(
            ds["rid"],
            transaction_rid,
            io.StringIO("col1,col2\n3,4"),
            "spark/test5.csv",
        )
        client.upload_dataset_file(
            ds["rid"],
            transaction_rid,
            io.StringIO("col1,col2\n3,4"),
            "spark/test6.csv",
        )
        client.upload_dataset_file(
            ds["rid"], transaction_rid, io.BytesIO(b"col1,col2\n5,6"), "test7.csv"
        )

        client.upload_dataset_file(ds["rid"], transaction_rid, io.BytesIO(), ".hidden")

        client.upload_dataset_file(ds["rid"], transaction_rid, io.BytesIO(), "_hidden")
        client.upload_dataset_file(
            ds["rid"], transaction_rid, io.BytesIO(), "folder1/.hidden"
        )
        client.upload_dataset_file(
            ds["rid"], transaction_rid, io.BytesIO(), "folder/_hidden/file"
        )
        client.upload_dataset_file(
            ds["rid"], transaction_rid, io.BytesIO(), ".folder/hidden"
        )
        client.upload_dataset_file(
            ds["rid"], transaction_rid, io.BytesIO(), "_folder/hidden"
        )
        client.upload_dataset_file(
            ds["rid"], transaction_rid, io.BytesIO(), "_folder/.hidden"
        )

        client.upload_dataset_file(
            ds["rid"], transaction_rid, io.BytesIO(), "folder/_middle/hidden"
        )

        client.commit_transaction(ds["rid"], transaction_rid)

        identity_with_transaction_rid = client.get_dataset_identity(
            dataset_path_or_rid=ds["rid"]
        )
        assert identity_with_transaction_rid["last_transaction_rid"] == transaction_rid

        assert client.get_dataset_last_transaction(ds["rid"])["rid"] == transaction_rid

        branch = client.get_branch(dataset_rid=ds["rid"], branch=BRANCH)
        assert branch["transactionRid"] == transaction_rid
        assert branch["openTransactionRid"] is None

        files_by_branch = client.list_dataset_files(
            dataset_rid=ds["rid"], view=BRANCH, detail=True
        )
        files_by_transaction = client.list_dataset_files(
            dataset_rid=ds["rid"], view=transaction_rid, detail=True
        )
        assert files_by_branch == files_by_transaction
        assert len(files_by_branch) == 7

        assert [
            "spark/test5.csv",
            "spark/test6.csv",
            "test1.csv",
            "test2.csv",
            "test3.csv",
            "test4.csv",
            "test7.csv",
        ] == sorted([file["logicalPath"] for file in files_by_transaction])

        files_hidden_included = client.list_dataset_files(
            dataset_rid=ds["rid"], exclude_hidden_files=False, view=BRANCH, detail=True
        )
        assert len(files_hidden_included) == 15
        assert [
            ".folder/hidden",
            ".hidden",
            "_folder/.hidden",
            "_folder/hidden",
            "_hidden",
            "folder/_hidden/file",
            "folder/_middle/hidden",
            "folder1/.hidden",
            "spark/test5.csv",
            "spark/test6.csv",
            "test1.csv",
            "test2.csv",
            "test3.csv",
            "test4.csv",
            "test7.csv",
        ] == sorted([file["logicalPath"] for file in files_hidden_included])

        tmp_output_dir = tmp_path_factory.mktemp("foundry_dev_tools_test_3").as_posix()
        path = client.download_dataset_file(
            dataset_rid=ds["rid"],
            view=BRANCH,
            output_directory=os.fspath(tmp_output_dir),
            foundry_file_path="test7.csv",
        )
        _ = client.download_dataset_file(
            dataset_rid=ds["rid"],
            view=BRANCH,
            output_directory=os.fspath(tmp_output_dir),
            foundry_file_path="spark/test6.csv",
        )
        assert sorted(
            [
                file.as_posix().replace(tmp_output_dir, "")[1:]
                for file in Path(tmp_output_dir).glob("**/*")
                if file.is_file()
            ]
        ) == ["spark/test6.csv", "test7.csv"]
        with Path(path).open(encoding="UTF-8") as f:
            assert f.read() == "col1,col2\n5,6"

        tmp_output_dir_2 = tmp_path_factory.mktemp(
            "foundry_dev_tools_test_4"
        ).as_posix()
        client.download_dataset_files(
            dataset_rid=ds["rid"],
            output_directory=os.fspath(tmp_output_dir_2),
            view=BRANCH,
        )
        assert sorted(
            [
                file.as_posix().replace(tmp_output_dir_2, "")[1:]
                for file in Path(tmp_output_dir_2).glob("**/*")
                if file.is_file()
            ]
        ) == [
            "spark/test5.csv",
            "spark/test6.csv",
            "test1.csv",
            "test2.csv",
            "test3.csv",
            "test4.csv",
            "test7.csv",
        ]


def test_dataset_schema(tmp_path_factory, root_dir):
    temp_directory = tmp_path_factory.mktemp("foundry_dev_tools_test_100").as_posix()
    bases = [root_dir, temp_directory]
    for base in bases:
        filesystem = fs.open_fs(os.fspath(base))
        client = MockFoundryRestClient(filesystem=filesystem)

        BRANCH = "master"

        ds = client.create_dataset("/path/to/ds")
        client.create_branch(dataset_rid=ds["rid"], branch=BRANCH)

        with pytest.raises(KeyError):
            client.upload_dataset_schema(
                dataset_rid=ds["rid"],
                transaction_rid=None,
                schema={"test": "schema"},
            )

        transaction_rid = client.open_transaction(ds["rid"], "SNAPSHOT", BRANCH)
        client.upload_dataset_schema(
            dataset_rid=ds["rid"],
            transaction_rid=transaction_rid,
            schema={"test": "schema"},
        )
        client.commit_transaction(dataset_rid=ds["rid"], transaction_id=transaction_rid)

        schema = client.get_dataset_schema(
            dataset_rid=ds["rid"], transaction_rid=transaction_rid
        )

        assert schema == {"test": "schema"}

        client.upload_dataset_schema(
            dataset_rid=ds["rid"],
            transaction_rid=transaction_rid,
            schema={"other": "value"},
        )
        schema = client.get_dataset_schema(
            dataset_rid=ds["rid"], transaction_rid=transaction_rid
        )

        assert schema == {"other": "value"}
