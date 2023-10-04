import datetime
import io
import json
import os
import shutil
from pathlib import Path
from typing import IO, TYPE_CHECKING, AnyStr

import fs
import timeflake
from fs.base import FS
from fs.errors import DirectoryExists, ResourceNotFound

from foundry_dev_tools.foundry_api_client import (
    BranchesAlreadyExistError,
    BranchNotFoundError,
    DatasetHasNoSchemaError,
    DatasetHasOpenTransactionError,
    DatasetNotFoundError,
    FoundryRestClient,
)
from foundry_dev_tools.utils.caches.spark_caches import _read
from foundry_dev_tools.utils.converter.foundry_spark import (
    foundry_schema_to_dataset_format,
    foundry_schema_to_read_options,
    foundry_schema_to_spark_schema,
)

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa
    import pyspark


class MockFoundryRestClient(FoundryRestClient):
    def query_foundry_sql(
        self, query, branch="master", return_type="pandas"
    ) -> "pd.core.frame.DataFrame | pa.Table | pyspark.sql.DataFrame":
        if return_type != "spark":
            raise NotImplementedError
        dataset_path_or_rid = query.split("`")[1]
        row_limit = int(query.split("`")[2].replace("LIMIT", "").strip())
        identity = self.get_dataset_identity(
            dataset_path_or_rid=dataset_path_or_rid, branch=branch
        )
        foundry_schema = self.get_dataset_schema(
            dataset_rid=identity["dataset_rid"],
            transaction_rid=identity["last_transaction_rid"],
            branch=branch,
        )
        schema = foundry_schema_to_spark_schema(foundry_schema)
        read_options = foundry_schema_to_read_options(foundry_schema)
        with self.download_dataset_files_temporary(
            dataset_rid=identity["dataset_rid"], view=identity["last_transaction_rid"]
        ) as temp_folder:
            df = _read(
                path=temp_folder,
                dataset_format=foundry_schema_to_dataset_format(foundry_schema),
                schema=schema,
                read_options=read_options,
            ).limit(row_limit)
            # This is a hack:
            # The parquet files are deleted when this context manager is closed,
            # so we keep the df in memory by triggering an action on it.
            df.cache()
            df.show(n=1, truncate=False)
            return df

    def get_dataset_stats(self, dataset_rid: str, view: str = "master") -> dict:
        # view can be branch or transaction rid
        if view.startswith("ri.foundry.main.transaction"):
            transaction_rid = view
        else:
            transaction_rid = self.get_branch(dataset_rid=dataset_rid, branch=view)[
                "transactionRid"
            ]
        num_transactions = len(self._load_transactions(dataset_rid=dataset_rid))
        files = self.list_dataset_files(
            dataset_rid=dataset_rid,
            exclude_hidden_files=True,
            view=transaction_rid,
            detail=True,
        )
        files_with_hidden = self.list_dataset_files(
            dataset_rid=dataset_rid,
            exclude_hidden_files=False,
            view=transaction_rid,
            detail=True,
        )
        return {
            "sizeInBytes": sum(file["fileMetadata"]["length"] for file in files),
            "numFiles": len(files),
            "hiddenFilesSizeInBytes": sum(
                file["fileMetadata"]["length"]
                for file in files_with_hidden
                if file["logicalPath"] not in [file2["logicalPath"] for file2 in files]
            ),
            "numHiddenFiles": len(files_with_hidden) - len(files),
            "numTransactions": num_transactions,
        }

    def get_dataset_last_transaction(
        self, dataset_rid: str, branch="master"
    ) -> "dict | None":
        transactions = self._load_transactions(dataset_rid=dataset_rid)
        if len(transactions) == 0:
            return None
        return transactions[0]

    def get_dataset_identity(
        self, dataset_path_or_rid: str, branch="master", check_read_access=True
    ):
        if "ri.foundry.main.dataset" in dataset_path_or_rid:
            dataset_path = self._rid_to_fs_path(dataset_path_or_rid)
            dataset_rid = dataset_path_or_rid
        else:
            dataset_path = dataset_path_or_rid
            try:
                dataset_files = self.filesystem.listdir(dataset_path)
            except ResourceNotFound as e:
                raise DatasetNotFoundError(dataset_path) from e
            maybe_dataset_rid_file = [
                file
                for file in dataset_files
                if file.startswith(".ri.foundry.main.dataset.")
            ]
            if len(maybe_dataset_rid_file) != 1:
                raise DatasetNotFoundError(dataset_path_or_rid)
            dataset_rid = fs.path.basename(maybe_dataset_rid_file[0])[1:]
        transactions = self._load_transactions(dataset_rid=dataset_rid)
        if (
            len(transactions) == 0
            or len(transactions) == 1
            and transactions[0]["status"] == "OPEN"
        ):
            return {
                "dataset_path": dataset_path,
                "dataset_rid": dataset_rid,
                "last_transaction_rid": None,
                "last_transaction": None,
            }
        if len(transactions) > 1 and transactions[0]["status"] == "OPEN":
            last_transaction_rid = transactions[1]["rid"]
        else:
            last_transaction_rid = transactions[0]["rid"]

        stats = self.get_dataset_stats(
            dataset_rid=dataset_rid, view=last_transaction_rid
        )
        return {
            "dataset_path": dataset_path,
            "dataset_rid": dataset_rid,
            "last_transaction_rid": last_transaction_rid,
            "last_transaction": {
                "rid": last_transaction_rid,
                "transaction": {
                    "record": {},
                    "metadata": {
                        "fileCount": stats["numFiles"],
                        "hiddenFileCount": stats["numHiddenFiles"],
                        "totalFileSize": stats["sizeInBytes"],
                        "totalHiddenFileSize": stats["hiddenFilesSizeInBytes"],
                    },
                },
            },
        }

    def _get_transaction(self, dataset_rid: str, transaction_rid: str) -> dict:
        transactions = self._load_transactions(dataset_rid=dataset_rid)
        maybe_transaction = [
            transaction
            for transaction in transactions
            if transaction["rid"] == transaction_rid
        ]
        if len(maybe_transaction) != 1:
            raise KeyError(transaction_rid)
        return maybe_transaction[0]

    def get_dataset_schema(
        self, dataset_rid: str, transaction_rid: str, branch="master"
    ):
        transaction = self._get_transaction(
            dataset_rid=dataset_rid, transaction_rid=transaction_rid
        )
        _ = self.get_branch(dataset_rid=dataset_rid, branch=branch)
        root_path = self._rid_to_fs_path(transaction_rid)
        if not transaction["schemaRid"]:
            raise DatasetHasNoSchemaError(dataset_rid, transaction_rid, branch)
        with self.filesystem.open(
            fs.path.join(root_path, f".{transaction['schemaRid']}"), "r"
        ) as f:
            return json.load(f)

    def upload_dataset_schema(
        self, dataset_rid: str, transaction_rid: str, schema: dict, branch="master"
    ):
        transaction = self._get_transaction(
            dataset_rid=dataset_rid, transaction_rid=transaction_rid
        )
        old_schema_rid = transaction["schemaRid"]
        root_path = self._rid_to_fs_path(transaction_rid)
        schema_rid = self._generate_resource_identifier(
            resource_type="ri.foundry.main.schema"
        )
        transaction["schemaRid"] = schema_rid
        schema_file_path = fs.path.join(root_path, f".{schema_rid}")
        with self.filesystem.open(schema_file_path, "w") as f:
            json.dump(schema, f)
        self._upsert_transaction(dataset_rid=dataset_rid, transaction=transaction)
        if old_schema_rid:
            old_schema_file_path = fs.path.join(root_path, f".{old_schema_rid}")
            self.filesystem.remove(old_schema_file_path)

    def download_dataset_files(
        self,
        dataset_rid: str,
        output_directory: str,
        files: "list | None" = None,
        view: str = "master",
        parallel_processes: "int | None" = None,
    ) -> "list[str]":
        return super().download_dataset_files(
            dataset_rid, output_directory, files, view, parallel_processes=1
        )

    def download_dataset_file(
        self,
        dataset_rid: str,
        output_directory: "str | None",
        foundry_file_path: str,
        view: str = "master",
    ) -> "str | bytes":
        # view can be branch or transaction rid
        transaction_rid = (
            view
            if view.startswith("ri.foundry.main.transaction")
            else self.get_branch(dataset_rid=dataset_rid, branch=view)["transactionRid"]
        )
        root_path = self._rid_to_fs_path(rid=transaction_rid)
        if output_directory:
            dest_path = Path(output_directory).joinpath(foundry_file_path)
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            with dest_path.open("wb") as fdst, self.filesystem.open(
                fs.path.join(root_path, foundry_file_path),
                "rb",
            ) as fsrc:
                shutil.copyfileobj(fsrc=fsrc, fdst=fdst)
            # TODO pathlib
            return os.fspath(dest_path)

        with self.filesystem.open(
            fs.path.join(root_path, foundry_file_path),
            "rb",
        ) as fsrc:
            return fsrc.read()

    def list_dataset_files(
        self,
        dataset_rid: str,
        exclude_hidden_files: bool = True,
        view: str = "master",
        logical_path: "str | None" = None,
        detail: bool = False,
        *,
        include_open_exclusive_transaction: bool = False,
        branch: "str | None" = None,
    ) -> list:
        # view can be branch or transaction rid
        if view.startswith("ri.foundry.main.transaction"):
            transaction_rid = view
        else:
            transaction_rid = self.get_branch(dataset_rid=dataset_rid, branch=view)[
                "transactionRid"
            ]
        root_path = self._rid_to_fs_path(rid=transaction_rid)
        result = []

        walker = fs.walk.Walker.bind(self.filesystem)
        file_or_folder_infos = list(walker.info(path=root_path, namespaces=["details"]))
        files_info = [
            (
                file_or_folder[0],
                file_or_folder[1],
                file_or_folder[0].replace(root_path + "/", ""),
            )
            for file_or_folder in file_or_folder_infos
            if file_or_folder[1].is_file
        ]

        def _file_should_be_ignored(filename):
            if filename.startswith(
                ".ri.foundry.main.transaction"
            ) or filename.startswith(".ri.foundry.main.schema"):
                return True
            return False

        files_info = [
            file
            for file in files_info
            if _file_should_be_ignored(file[1].name) is False
        ]

        if exclude_hidden_files:
            files_info = [
                file
                for file in files_info
                if not any(
                    part
                    for part in file[2].split("/")
                    if (part.startswith(".") or part.startswith("_"))
                )
            ]

        for file in files_info:
            result.append(
                {
                    "logicalPath": str(file[2]),
                    "physicalPath": file[0],
                    "physicalUri": None,
                    "transactionRid": transaction_rid,
                    "fileMetadata": {"length": file[1].size},
                    "isOpen": False,
                    "timeModified": file[1].created.isoformat()[0:23] + "Z"
                    if file[1].created
                    else file[1].modified.isoformat()[0:23] + "Z",
                }
            )
        if detail:
            return result
        return [file["logicalPath"] for file in result]

    def upload_dataset_files(
        self,
        dataset_rid: str,
        transaction_rid: str,
        path_file_dict: dict,
        parallel_processes: "int | None" = None,
    ) -> None:
        super().upload_dataset_files(
            dataset_rid, transaction_rid, path_file_dict, parallel_processes=1
        )

    def get_dataset_path(self, dataset_rid: str) -> str:
        return self._rid_to_fs_path(rid=dataset_rid)

    def is_dataset_in_trash(self, dataset_path: str):
        return False

    def _upsert_transaction(self, dataset_rid: str, transaction: dict):
        transactions = self._load_transactions(dataset_rid)
        transactions_new = [
            transaction
            if transaction_inner["rid"] == transaction["rid"]
            else transaction_inner
            for transaction_inner in transactions
        ]
        self._write_transactions(dataset_rid=dataset_rid, transactions=transactions_new)

    def commit_transaction(self, dataset_rid: str, transaction_id: str) -> None:
        transaction = self._get_transaction(
            dataset_rid=dataset_rid, transaction_rid=transaction_id
        )
        transaction["status"] = "COMMITTED"
        transaction["closeTime"] = self._current_datetime()
        self._upsert_transaction(dataset_rid=dataset_rid, transaction=transaction)
        branches = self._load_branches(dataset_rid=dataset_rid)
        branch_found = [
            branch_inner
            for branch_inner in branches
            if branch_inner["openTransactionRid"] == transaction_id
        ]
        if len(branch_found) == 0:
            raise KeyError()
        branch = branch_found[0]
        branch["openTransactionRid"] = None
        branch["transactionRid"] = transaction_id
        branches = [
            branch if branch_inner["id"] == branch["id"] else branch_inner
            for branch_inner in branches
        ]
        self._write_branches(dataset_rid=dataset_rid, branches=branches)

    def upload_dataset_file(
        self,
        dataset_rid: str,
        transaction_rid,
        path_or_buf: "str | Path | IO[AnyStr]",
        path_in_foundry_dataset: str,
    ) -> None:
        transaction_path = self._rid_to_fs_path(rid=transaction_rid)
        if path_in_foundry_dataset[0] == "/":
            raise ValueError("path_in_foundry_dataset can not start with /")
        folder_path = fs.path.join(
            transaction_path, fs.path.split(path_in_foundry_dataset)[0]
        )
        if "/" in path_in_foundry_dataset:
            # check if file exists and is empty, in that case replace with folder
            # can happen with empty s3 keys
            if (
                self.filesystem.isfile(folder_path)
                and self.filesystem.getsize(folder_path) == 0
            ):
                self.filesystem.remove(folder_path)
            self.filesystem.makedirs(folder_path, recreate=True)
        binary_flag = ""
        if isinstance(path_or_buf, io.IOBase) and not isinstance(
            path_or_buf, io.TextIOBase
        ):  # heuristic if BytesIO or StringIO ...
            binary_flag = "b"
        if not hasattr(path_or_buf, "read"):  # we read files always in binary mode
            binary_flag = "b"
        with self.filesystem.open(
            fs.path.join(folder_path, fs.path.split(path_in_foundry_dataset)[1]),
            mode=f"w{binary_flag}",
        ) as f:
            if hasattr(path_or_buf, "read"):
                shutil.copyfileobj(fsrc=path_or_buf, fdst=f)
            else:
                with open(path_or_buf, mode=f"r{binary_flag}") as fsrc:
                    shutil.copyfileobj(fsrc=fsrc, fdst=f)

    def open_transaction(
        self, dataset_rid: str, mode: str = "SNAPSHOT", branch: str = "master"
    ) -> str:
        # Check if dataset_rid / branch combination exists
        got_branch = self.get_branch(dataset_rid=dataset_rid, branch=branch)
        # Check if dataset has open transaction
        transactions = self._load_transactions(dataset_rid)
        is_open = [
            transaction
            for transaction in transactions
            if transaction["status"] == "OPEN"
        ]
        if len(is_open) > 0:
            raise DatasetHasOpenTransactionError(
                dataset_rid=dataset_rid, open_transaction_rid=is_open[0]["rid"]
            )
        rid = self._generate_resource_identifier(
            resource_type="ri.foundry.main.transaction"
        )
        transaction_path = fs.path.join(self._rid_to_fs_path(dataset_rid), rid)
        self.filesystem.makedir(transaction_path)
        with self.filesystem.open(fs.path.join(transaction_path, f".{rid}"), "w") as f:
            f.write("")
        # I know this does not scale and runs in linear time
        transactions.insert(
            0,
            {
                "rid": rid,
                "datasetRid": dataset_rid,
                "status": "OPEN",
                "type": mode,
                "startTime": self._current_datetime(),
                "closeTime": None,
                "schemaRid": None,
            },
        )

        self._write_transactions(dataset_rid, transactions)
        branches = self._load_branches(dataset_rid=dataset_rid)
        got_branch["openTransactionRid"] = rid
        branches = [
            got_branch if branch_inner["id"] == got_branch["id"] else branch_inner
            for branch_inner in branches
        ]
        self._write_branches(dataset_rid=dataset_rid, branches=branches)
        return rid

    def _load_transactions(self, dataset_rid: str) -> list:
        dataset_path = self._rid_to_fs_path(rid=dataset_rid)
        dataset_transactions = fs.path.join(dataset_path, ".transactions")
        if self.filesystem.exists(dataset_transactions):
            with self.filesystem.open(dataset_transactions, "r") as f:
                return json.load(f)
        else:
            return []

    def _write_transactions(self, dataset_rid: str, transactions: list):
        dataset_path = self._rid_to_fs_path(rid=dataset_rid)
        with self.filesystem.open(
            fs.path.join(dataset_path, ".transactions"), "w"
        ) as f:
            json.dump(transactions, f)

    def _load_branches(self, dataset_rid: str) -> list:
        dataset_path = self._rid_to_fs_path(rid=dataset_rid)
        dataset_branches = fs.path.join(dataset_path, ".branches")
        if self.filesystem.exists(dataset_branches):
            with self.filesystem.open(dataset_branches, "r") as f:
                return json.load(f)
        else:
            return []

    def _write_branches(self, dataset_rid: str, branches: list):
        dataset_path = self._rid_to_fs_path(rid=dataset_rid)
        with self.filesystem.open(fs.path.join(dataset_path, ".branches"), "w") as f:
            json.dump(branches, f)

    def create_branch(
        self,
        dataset_rid: str,
        branch: str,
        parent_branch: "str | None" = None,
        parent_branch_id: "str | None" = None,
    ) -> dict:
        branch_rid = self._generate_resource_identifier(
            resource_type="ri.foundry.main.branch"
        )
        branches = self._load_branches(dataset_rid=dataset_rid)
        does_exists = [
            branch_inner for branch_inner in branches if branch_inner["id"] == branch
        ]
        if len(does_exists) != 0:
            raise BranchesAlreadyExistError(dataset_rid, branch)
        ret_branch = {
            "id": branch,
            "rid": branch_rid,
            "ancestorBranchIds": [],
            "creationTime": self._current_datetime(),
            "transactionRid": None,
        }
        branches.append(ret_branch)
        self._write_branches(dataset_rid=dataset_rid, branches=branches)
        return ret_branch

    def get_branch(self, dataset_rid: str, branch: str) -> dict:
        _ = self.get_dataset(dataset_rid)
        branches = self._load_branches(dataset_rid=dataset_rid)
        branch_found = [
            branch_inner for branch_inner in branches if branch_inner["id"] == branch
        ]
        if len(branch_found) == 0:
            raise BranchNotFoundError(dataset_rid, branch)
        return branch_found[0]

    def _create_compass_folder_if_not_exists(self, folder_path: str):
        if not self.filesystem.exists(folder_path):
            self.filesystem.makedir(folder_path)
            rid = self._generate_resource_identifier(
                resource_type="ri.compass.main.folder"
            )
            with self.filesystem.open(fs.path.join(folder_path, f".{rid}"), "w") as f:
                f.write("")

    def _create_parent_compass_folders(self, compass_path):
        split_path = compass_path.split("/")
        suffix = ""
        for i in range(1, len(split_path) - 1):
            suffix = suffix + "/" + split_path[i]
            self._create_compass_folder_if_not_exists(folder_path=suffix)

    def create_dataset(self, dataset_path: str) -> dict:
        rid = self._generate_resource_identifier()
        self._create_parent_compass_folders(compass_path=dataset_path)
        try:
            self.filesystem.makedir(dataset_path)
        except DirectoryExists as exc:
            raise KeyError(f"Dataset '{dataset_path}' already exists.") from exc
        with self.filesystem.open(fs.path.join(dataset_path, f".{rid}"), "w") as f:
            f.write("")
        return {"rid": rid, "fileSystemId": str(self.filesystem)}

    def get_dataset(self, dataset_rid: str) -> dict:
        files_count = self.filesystem.glob(f"**/*.{dataset_rid}").count().files
        if files_count == 0:
            raise DatasetNotFoundError(dataset_rid)
        if files_count > 1:
            raise ValueError("This should not happen.")
        return {"rid": dataset_rid, "fileSystemId": str(self.filesystem)}

    def delete_dataset(self, dataset_rid: str) -> None:
        dataset_path = self._rid_to_fs_path(rid=dataset_rid)
        self.filesystem.removetree(dataset_path)

    def __init__(self, filesystem: FS):
        super().__init__()
        self.filesystem: FS = filesystem
        if str(self.filesystem) == "<memfs>":
            raise ValueError(
                "<memfs> not supported due to threading issues in multiprocessing."
            )

    def _filter_files_in_dataset(self, files: list) -> list:
        filter_1 = [
            file for file in files if not file.startswith(".ri.foundry.main.dataset")
        ]
        filter_2 = [file for file in filter_1 if not file.startswith(".branches")]
        return [
            file
            for file in filter_2
            if not file.startswith("ri.foundry.main.transaction")
        ]

    @staticmethod
    def _generate_resource_identifier(resource_type="ri.foundry.main.dataset"):
        flake = timeflake.random().uuid
        return f"{resource_type}.{flake!s}"

    @staticmethod
    def _current_datetime():
        return datetime.datetime.utcnow().isoformat()[0:23] + "Z"

    def _rid_to_fs_path(self, rid: str) -> str:
        glob_matches = list(self.filesystem.glob(f"**/*.{rid}"))
        if len(glob_matches) == 0 and "ri.foundry.main.dataset" in rid:
            raise DatasetNotFoundError(rid)
        if len(glob_matches) == 0:
            raise KeyError(rid)
        if len(glob_matches) != 1:
            raise ValueError("This should not happen.")
        return fs.path.split(glob_matches[0].path)[0]
