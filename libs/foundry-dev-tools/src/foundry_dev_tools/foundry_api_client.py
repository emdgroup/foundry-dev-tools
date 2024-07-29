"""Contains FoundryRestClient and FoundrySqlClient and exception classes.

One of the gaols of this module is to be self-contained so that it can be
dropped into any python installation with minimal dependency to 'requests'
Optional dependencies for the SQL functionality to work are pandas and pyarrow.

"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
import warnings
from contextlib import contextmanager
from os import PathLike
from pathlib import Path
from typing import IO, TYPE_CHECKING, AnyStr, Literal, overload

from foundry_dev_tools.config.config import (
    get_config_dict,
    parse_credentials_config,
    parse_general_config,
    path_from_path_or_str,
)
from foundry_dev_tools.config.context import FoundryContext
from foundry_dev_tools.errors.compass import ResourceNotFoundError
from foundry_dev_tools.errors.dataset import (
    DatasetHasNoSchemaError,
    DatasetHasNoTransactionsError,
    DatasetNoReadAccessError,
    DatasetNotFoundError,
)
from foundry_dev_tools.errors.handling import ErrorHandlingConfig
from foundry_dev_tools.utils import api_types
from foundry_dev_tools.utils.api_types import assert_in_literal
from foundry_dev_tools.utils.compat import v1_to_v2_config

if TYPE_CHECKING:
    from collections.abc import Iterator

    import pandas as pd
    import pyarrow as pa
    import pyspark
    import requests


LOGGER = logging.getLogger(__name__)


class FoundryRestClient:
    """Foundry RestClient compatibility shim for Foundry DevTools v2."""

    def __init__(self, config: dict | None = None, ctx: FoundryContext | None = None):
        """Create an instance of FoundryRestClient.

        Args:
            config: config dictionary which tries to get parsed into the v2 configuration, to be backwards compatible
            ctx: or just pass the v2 FoundryContext directly instead of the 'old' configuration,
                the config dict will be ignored
        Examples:
            >>> fc = FoundryRestClient()
            >>> fc = FoundryRestClient(config={"jwt": "<token>"})
            >>> fc = FoundryRestClient(config={"client_id": "<client_id>"})

            >>> ctx = FoundryContext()
            >>> fc = FoundryRestClient(ctx=ctx)
        """
        if ctx:
            self.ctx = ctx
        else:
            if config:
                tp, _config = v1_to_v2_config(config)
            else:
                _config = get_config_dict()
                tp = parse_credentials_config(_config)
            self.ctx = FoundryContext(parse_general_config(_config), tp)

    def create_dataset(self, dataset_path: api_types.FoundryPath) -> dict:
        """Creates an empty dataset in Foundry.

        Args:
            dataset_path: Path in Foundry, where this empty dataset should be created
                for example: /Global/Foundry Operations/Foundry Support/iris_new

        Returns:
            dict:
                with keys rid and fileSystemId.
                The key rid contains the dataset_rid which is the unique identifier of a dataset.

        """
        return self.ctx.catalog.api_create_dataset(dataset_path).json()

    def get_dataset(self, dataset_rid: api_types.DatasetRid) -> dict:
        """Gets dataset_rid and fileSystemId.

        Args:
            dataset_rid (str): Dataset rid

        Returns:
            dict:
                with the keys rid and fileSystemId

        Raises:
            DatasetNotFoundError: if dataset does not exist
        """
        return self.ctx.catalog.api_get_dataset(dataset_rid).json()

    def delete_dataset(self, dataset_rid: api_types.DatasetRid):
        """Deletes a dataset in Foundry and moves it to trash.

        Args:
            dataset_rid (str): Unique identifier of the dataset

        Raises:
            DatasetNotFoundError: if dataset does not exist

        """
        self.ctx.catalog.api_delete_dataset(dataset_rid)
        self.move_resource_to_trash(rid=dataset_rid)

    def move_resource_to_trash(self, rid: api_types.Rid):
        """Moves a Compass resource (e.g. dataset or folder) to trash.

        Args:
            rid (str): rid of the resource

        """
        self.ctx.compass.api_add_to_trash({rid})

    def create_branch(
        self,
        dataset_rid: api_types.DatasetRid,
        branch: str,
        parent_branch_id: str | None = None,
        parent_branch: api_types.TransactionRid | None = None,
    ) -> dict:
        """Creates a new branch in a dataset.

        If dataset is 'new', only parameter dataset_rid and branch are required.

        Args:
            dataset_rid: Unique identifier of the dataset
            branch: The branch name to create
            parent_branch: The transaction id, to branch off
            parent_branch_id: The name of the parent branch, if empty creates new root branch

        Returns:
            dict:
                the response as a json object

        """
        return self.ctx.catalog.api_create_branch(dataset_rid, branch, parent_branch, parent_branch_id).json()

    def update_branch(
        self,
        dataset_rid: api_types.Rid,
        branch: str,
        parent_branch: str | api_types.TransactionRid | None = None,
    ) -> dict:
        """Updates the latest transaction of branch 'branch' to the latest transaction of branch 'parent_branch'.

        Args:
            dataset_rid: Unique identifier of the dataset
            branch: The branch to update (e.g. master)
            parent_branch: the name of the branch to copy the last transaction from or a transaction rid

        Returns:
            dict:
                example below for the branch response
        .. code-block:: python

         {
             "id": "..",
             "rid": "ri.foundry.main.branch...",
             "ancestorBranchIds": [],
             "creationTime": "",
             "transactionRid": "ri.foundry.main.transaction....",
         }

        """
        return self.ctx.catalog.api_update_branch(dataset_rid, branch, parent_branch).json()

    def get_branches(self, dataset_rid: api_types.DatasetRid) -> list[str]:
        """Returns a list of branches available a dataset.

        Args:
            dataset_rid: Unique identifier of the dataset

        Returns:
            list[str]:
                list of dataset branch names

        """
        return self.ctx.catalog.api_get_branches(dataset_rid).json()

    def get_branch(self, dataset_rid: api_types.DatasetRid, branch: api_types.DatasetBranch) -> dict:
        """Returns branch information.

        Args:
            dataset_rid: Unique identifier of the dataset
            branch: Branch name

        Returns:
            dict:
                with keys id (name) and rid (unique id) of the branch.

        Raises:
             BranchNotFoundError: if branch does not exist.

        """
        return self.ctx.catalog.api_get_branch(dataset_rid, branch).json()

    def open_transaction(self, dataset_rid: str, mode: str = "SNAPSHOT", branch: str = "master") -> str:
        """Opens a new transaction on a dataset.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            mode (str):
                APPEND: append files,
                SNAPSHOT: replace all,
                UPDATE: replace file if exists, keep existing files
            branch (str): dataset branch

        Returns:
            str:
                the transaction ID

        Raises:
            BranchNotFoundError: if branch does not exist
            DatasetNotFoundError: if dataset does not exist
            DatasetHasOpenTransactionError: if dataset has an open transaction
        """
        assert_in_literal(mode, api_types.FoundryTransaction, "mode")

        transaction_type = mode
        transaction_id = self.ctx.catalog.api_start_transaction(
            dataset_rid, branch, start_transaction_type=transaction_type
        ).json()["rid"]

        # update type of transaction, default is APPEND
        if mode != "APPEND":
            self.ctx.catalog.api_set_transaction_type(
                dataset_rid,
                transaction_id,
                transaction_type=transaction_type,
            )
        return transaction_id

    def remove_dataset_file(
        self,
        dataset_rid: str,
        transaction_id: str,
        logical_path: str,
        recursive: bool = False,
    ):
        """Removes the given file from an open transaction.

        If the logical path matches a file exactly then only that file
        will be removed, regardless of the value of recursive.
        If the logical path represents a directory, then all
        files prefixed with the logical path followed by '/'
        will be removed when recursive is true and no files will be
        removed when recursive is false.
        If the given logical path does not match a file or directory then this call
        is ignored and does not throw an exception.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            transaction_id (str): transaction rid
            logical_path (str): logical path in the backing filesystem
            recursive (bool): recurse into subdirectories

        """
        self.ctx.catalog.api_remove_dataset_file(dataset_rid, transaction_id, logical_path, recursive)

    def add_files_to_delete_transaction(self, dataset_rid: str, transaction_id: str, logical_paths: list[str]):
        """Adds files in an open DELETE transaction.

        Files added to DELETE transactions affect
        the dataset view by removing files from the view.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            transaction_id (str): transaction rid
            logical_paths (List[str]): files in the dataset to delete

        """
        self.ctx.catalog.api_add_files_to_delete_transaction(dataset_rid, transaction_id, logical_paths)

    def commit_transaction(self, dataset_rid: str, transaction_id: str):
        """Commits a transaction, should be called after file upload is complete.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            transaction_id (str): transaction id

        Raises:
            KeyError: when there was an issue with committing
        """
        self.ctx.catalog.api_commit_transaction(dataset_rid, transaction_id)

    def abort_transaction(self, dataset_rid: str, transaction_id: str):
        """Aborts a transaction. Dataset will remain on transaction N-1.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            transaction_id (str): transaction id

        Raises:
            KeyError: When abort transaction fails

        """
        self.ctx.catalog.api_abort_transaction(dataset_rid, transaction_id)

    def get_dataset_transactions(
        self,
        dataset_rid: str,
        branch: str = "master",
        last: int = 50,
        include_open_exclusive_transaction: bool = False,
    ) -> list[api_types.SecuredTransaction]:
        """Returns the transactions of a dataset / branch combination.

        Returns last 50 transactions by default, pagination not implemented.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            branch (str): Branch
            last (int): last
            include_open_exclusive_transaction (bool): include_open_exclusive_transaction

        Returns:
            dict:
                of transaction information.

        Raises:
            BranchNotFoundError: if branch not found
            DatasetHasNoTransactionsError: If the dataset has not transactions

        """
        response = self.ctx.catalog.api_get_reverse_transactions2(
            dataset_rid,
            branch,
            last,
            include_open_exclusive_transaction=include_open_exclusive_transaction,
        )
        if values := response.json().get("values"):
            return values
        raise DatasetHasNoTransactionsError(response, dataset_rid=dataset_rid, branch=branch)

    def get_dataset_last_transaction(
        self,
        dataset_rid: str,
        branch: str = "master",
    ) -> api_types.SecuredTransaction | None:
        """Returns the last transaction of a dataset / branch combination.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            branch (str): Branch

        Returns:
            dict | None:
                response from transaction API or None if dataset has no transaction.

        """
        try:
            return self.get_dataset_transactions(dataset_rid, branch, last=1)[0]
        except DatasetHasNoTransactionsError:
            return None

    def get_dataset_last_transaction_rid(self, dataset_rid: str, branch: str = "master") -> str | None:
        """Returns the last transaction rid of a dataset / branch combination.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            branch (str): Branch

        Returns:
            str | None:
                transaction rid or None if dataset has no transaction.

        """
        last_transaction = self.get_dataset_last_transaction(dataset_rid, branch)
        if last_transaction:
            return last_transaction["rid"]
        return None

    def upload_dataset_file(
        self,
        dataset_rid: str,
        transaction_rid: str,
        path_or_buf: str | Path | IO[AnyStr],
        path_in_foundry_dataset: str,
    ) -> requests.Response:
        """Uploads a file like object to a path in a foundry dataset.

        Args:
            dataset_rid: Unique identifier of the dataset
            transaction_rid: transaction id
            path_or_buf: A str or file handle,
                file path or object
            path_in_foundry_dataset: The path in the dataset, to which the file
                is uploaded.

        """
        if isinstance(path_or_buf, str):
            path_or_buf = Path(path_or_buf)
        if isinstance(path_or_buf, Path):
            return self.ctx.data_proxy.upload_dataset_file(
                dataset_rid,
                transaction_rid,
                path_or_buf,
                path_in_foundry_dataset,
            )
        return self.ctx.data_proxy.api_put_file(dataset_rid, transaction_rid, path_in_foundry_dataset, path_or_buf)

    def upload_dataset_files(
        self,
        dataset_rid: str,
        transaction_rid: str,
        path_file_dict: dict,
        parallel_processes: int | None = None,
    ):
        """Uploads multiple local files to a foundry dataset.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            transaction_rid (str): transaction id
            parallel_processes (int | None): Set number of threads for upload
            path_file_dict (dict): A dictionary with the following structure:

        .. code-block:: python

         {
         '<path_in_foundry_dataset>': '<local_file_path>',
         ...
         }

        """
        path_file_dict_pathlib = {k: Path(v) for k, v in path_file_dict.items()}
        self.ctx.data_proxy.upload_dataset_files(
            dataset_rid,
            transaction_rid,
            path_file_dict_pathlib,
            max_workers=parallel_processes,
        )

    def get_dataset_details(self, dataset_path_or_rid: str) -> dict:
        """Returns the resource information of a dataset.

        Args:
            dataset_path_or_rid (str): The full path or rid to the dataset

        Returns:
            dict:
                the json response of the api

        Raises:
            DatasetNotFoundError: if dataset not found

        """
        try:
            if "ri.foundry.main.dataset" in dataset_path_or_rid:
                response = self.ctx.compass.api_get_resource(dataset_path_or_rid, decoration={"path"})
            else:
                response = self.ctx.compass.api_get_resource_by_path(dataset_path_or_rid, decoration={"path"})
        except ResourceNotFoundError as rnfe:
            raise DatasetNotFoundError(rnfe.response, rnfe.info, **rnfe.kwargs) from rnfe

        as_json = response.json()
        if as_json["directlyTrashed"]:
            warnings.warn(f"Dataset '{dataset_path_or_rid}' is in foundry trash.")
        return as_json

    def get_child_objects_of_folder(self, folder_rid: str, page_size: int | None = None) -> Iterator[dict]:
        """Returns the child objects of a compass folder.

        Args:
            folder_rid (str): Compass folder rid,
                e.g. ri.compass.main.folder.f549ae09-9534-44c7-967a-6c86b2339231
            page_size (int): to control the pageSize manually

        Yields:
            dict:
                information about child objects

        Raises:
            FolderNotFoundError: if folder not found
        """
        yield from self.ctx.compass.get_child_objects_of_folder(folder_rid, limit=page_size)

    def create_folder(self, name: str, parent_id: str) -> dict:
        """Creates an empty folder in compass.

        Args:
            name (str): name of the new folder
            parent_id (str): rid of the parent folder,
                e.g. ri.compass.main.folder.aca0cce9-2419-4978-bb18-d4bc6e50bd7e

        Returns:
            dict:
                with keys rid and name and other properties.

        """
        return self.ctx.compass.api_create_folder(name, parent_id).json()

    def get_dataset_rid(self, dataset_path: str) -> str:
        """Returns the rid of a dataset, uses dataset_path as input.

        Args:
            dataset_path (str): The full path to the dataset

        Returns:
            str:
                the dataset_rid

        """
        return self.get_dataset_details(dataset_path)["rid"]

    def get_dataset_path(self, dataset_rid: str) -> str:
        """Returns the path of a dataset as str.

        Args:
            dataset_rid (str): The rid of the dataset

        Returns:
            str:
                the dataset_path

        Raises:
            DatasetNotFoundError: if dataset was not found

        """
        return self.ctx.compass.api_get_path(
            dataset_rid,
            error_handling=ErrorHandlingConfig({204: DatasetNotFoundError}, dataset_rid=dataset_rid),
        ).json()

    def get_dataset_paths(self, dataset_rids: list) -> dict:
        """Returns a list of paths for a list of passed rid's of a dataset.

        Args:
            dataset_rids (list): The rid's of datasets

        Returns:
            dict:
                the dataset_path as dict of string

        """
        return self.ctx.compass.get_paths(dataset_rids)

    def is_dataset_in_trash(self, dataset_path: str) -> bool:
        """Returns true if dataset is in compass trash.

        Args:
            dataset_path (str): The path to the dataset

        Returns:
            bool:
                true if dataset is in trash

        """
        in_trash = self.get_dataset_details(dataset_path)["directlyTrashed"]
        if in_trash is None:
            in_trash = False
        return in_trash

    def get_dataset_schema(self, dataset_rid: str, transaction_rid: str | None = None, branch: str = "master") -> dict:
        """Returns the foundry dataset schema for a dataset, transaction, branch combination.

        Args:
            dataset_rid (str): The rid of the dataset
            transaction_rid (str): The rid of the transaction
            branch (str): The branch

        Returns:
            dict:
                with foundry dataset schema

        Raises:
            DatasetNotFoundError: if dataset was not found
            DatasetHasNoSchemaError: if dataset has no scheme
            BranchNotFoundError: if branch was not found
            KeyError: if the combination of dataset_rid, transaction_rid and branch was not found

        """
        try:
            return self.ctx.metadata.api_get_dataset_schema(
                dataset_rid=dataset_rid,
                branch=branch,
                transaction_rid=transaction_rid,
            ).json()["schema"]
        except DatasetHasNoSchemaError as e:
            # we don't know if the branch does not exist, or if there is just no schema
            # this will raise an error if the branch does not exist
            self.get_branch(dataset_rid, branch)

            # otherwise raise no schema
            raise e from None

    def upload_dataset_schema(
        self,
        dataset_rid: str,
        transaction_rid: str,
        schema: dict,
        branch: str = "master",
    ) -> requests.Response:
        """Uploads the foundry dataset schema for a dataset, transaction, branch combination.

        Args:
            dataset_rid (str): The rid of the dataset
            transaction_rid (str): The rid of the transaction
            schema (dict): The foundry schema
            branch (str): The branch

        """
        return self.ctx.metadata.api_upload_dataset_schema(dataset_rid, transaction_rid, schema, branch)

    def infer_dataset_schema(self, dataset_rid: str, branch: str = "master") -> dict:
        """Calls the foundry-schema-inference service to infer the dataset schema.

        Returns dict with foundry schema, if status == SUCCESS

        Args:
            dataset_rid (str): The dataset rid
            branch (str): The branch

        Returns:
            dict:
                with dataset schema, that can be used to call upload_dataset_schema

        Raises:
            ValueError: if foundry schema inference failed
        """
        return self.ctx.schema_inference.infer_dataset_schema(dataset_rid, branch)

    def get_dataset_identity(
        self,
        dataset_path_or_rid: str,
        branch: api_types.DatasetBranch = "master",
        check_read_access: bool = True,
    ) -> api_types.DatasetIdentity:
        """Returns the identity of this dataset (dataset_path, dataset_rid, last_transaction_rid, last_transaction).

        Args:
            dataset_path_or_rid (str): Path to dataset (e.g. /Global/...)
                or rid of dataset (e.g. ri.foundry.main.dataset...)
            branch (str): branch of the dataset
            check_read_access (bool): default is True, checks if the user has read access ('gatekeeper:view-resource')
                to the dataset otherwise exception is thrown

        Returns:
            dict:
                with the keys 'dataset_path', 'dataset_rid', 'last_transaction_rid', 'last_transaction'

        Raises:
            DatasetNoReadAccessError: if you have no read access for that dataset

        """
        dataset_details = self.get_dataset_details(dataset_path_or_rid)
        dataset_rid = dataset_details["rid"]
        dataset_path = dataset_details["path"]
        if check_read_access and "gatekeeper:view-resource" not in dataset_details["operations"]:
            raise DatasetNoReadAccessError(
                dataset_path,
            )
        last_transaction = self.get_dataset_last_transaction(dataset_rid, branch)
        return {
            "dataset_path": dataset_path,
            "dataset_rid": dataset_rid,
            "last_transaction_rid": last_transaction["rid"] if last_transaction else None,
            "last_transaction": last_transaction,
        }

    def list_dataset_files(
        self,
        dataset_rid: str,
        exclude_hidden_files: bool = True,
        view: str = "master",
        logical_path: str | None = None,
        detail: bool = False,
        *,
        include_open_exclusive_transaction: bool = False,
    ) -> list:
        """Returns list of internal filenames of a dataset.

        Args:
            dataset_rid (str): the dataset rid
            exclude_hidden_files (bool): if hidden files should be excluded (e.g. _log files)
            view (str): branch or transaction rid of the dataset
            logical_path (str): If logical_path is absent, returns all files in the view.
                If logical_path matches a file exactly, returns just that file.
                Otherwise, returns all files in the "directory" of logical_path:
                (a slash is added to the end of logicalPath if necessary and a prefix-match is performed)
            detail (bool): if passed as True, returns complete response from catalog API, otherwise only
                           returns logicalPath
            include_open_exclusive_transaction (bool): if files added in open transaction should be returned
                                                       as well in the response

        Returns:
            list:
                filenames

        Raises:
            DatasetNotFound: if dataset was not found
        """
        files = self.ctx.catalog.list_dataset_files(
            dataset_rid,
            end_ref=view,
            page_size=1000,
            logical_path=logical_path,
            exclude_hidden_files=exclude_hidden_files,
            include_open_exclusive_transaction=include_open_exclusive_transaction,
        )
        if detail:
            return files
        return [f["logicalPath"] for f in files]

    def get_dataset_stats(self, dataset_rid: str, view: str = "master") -> dict:
        """Returns response from foundry catalogue stats endpoint.

        Args:
            dataset_rid (str): the dataset rid
            view (str): branch or transaction rid of the dataset

        Returns:
            dict:
                sizeInBytes, numFiles, hiddenFilesSizeInBytes, numHiddenFiles, numTransactions

        """
        return self.ctx.catalog.api_get_dataset_stats(dataset_rid, view).json()

    def foundry_stats(self, dataset_rid: str, end_transaction_rid: str, branch: str = "master") -> dict:
        """Returns row counts and size of the dataset/view.

        Args:
            dataset_rid (str): The dataset RID.
            end_transaction_rid (str): The specific transaction RID,
                which will be used to return the statistics.
            branch (str): The branch to query

        Returns:
            dict:
                With the following structure:
                {
                datasetRid: str,
                branch: str,
                endTransactionRid: str,
                schemaId: str,
                computedDatasetStats: {
                rowCount: str | None,
                sizeInBytes: str,
                columnStats: { "...": {nullCount: str | None, uniqueCount: str | None, avgLength: str | None, maxLength: str | None,} },
                },
                }
        """  # noqa: E501
        return self.ctx.foundry_stats.api_foundry_stats(dataset_rid, end_transaction_rid, branch).json()

    def download_dataset_file(
        self,
        dataset_rid: str,
        output_directory: str | None,
        foundry_file_path: str,
        view: str = "master",
    ) -> str | bytes:
        """Downloads a single foundry dataset file into a directory.

        Creates sub folder if necessary.

        Args:
            dataset_rid (str): the dataset rid
            output_directory (str | None): the local output directory for the file or None
                if None is passed, byte content of file is returned
            foundry_file_path (str): the file_path on the foundry file system
            view (str): branch or transaction rid of the dataset

        Returns:
            str | bytes:
                local file path in case output_directory was passed
                or file content as bytes

        Raises:
            ValueError: If download failed

        """
        if output_directory:
            return os.fspath(
                self.ctx.data_proxy.download_dataset_file(dataset_rid, Path(output_directory), foundry_file_path),
            )
        return self.ctx.data_proxy.api_get_file_in_view(
            dataset_rid,
            view,
            foundry_file_path,
            stream=True,
        ).content

    def download_dataset_files(
        self,
        dataset_rid: str,
        output_directory: PathLike[str] | str,
        files: list | None = None,
        view: str = "master",
        parallel_processes: int | None = None,
    ) -> list[str]:
        """Downloads files of a dataset (in parallel) to a local output directory.

        Args:
            dataset_rid (str): the dataset rid
            files (list | None): list of files or None, in which case all files are downloaded
            output_directory (str): the output directory for the files
                default value is calculated: multiprocessing.cpu_count() - 1
            view (str): branch or transaction rid of the dataset
            parallel_processes (int | None): Set number of threads for upload

        Returns:
            List[str]:
                path to downloaded files

        """
        return [
            os.fspath(p)
            for p in self.ctx.data_proxy.download_dataset_files(
                dataset_rid,
                path_from_path_or_str(output_directory),
                set(files) if files else None,
                view,
                parallel_processes,
            )
        ]

    @contextmanager
    def download_dataset_files_temporary(
        self,
        dataset_rid: str,
        files: list | None = None,
        view: str = "master",
        parallel_processes: int | None = None,
    ) -> Iterator[str]:
        """Downloads all files of a dataset to a temporary directory.

        Which is deleted when the context is closed. Function returns the temporary directory.
        Example usage:

        >>> import parquet
        >>> import pandas as pd
        >>> from pathlib import Path
        >>> with client.download_dataset_files_temporary(dataset_rid='ri.foundry.main.dataset.1', view='master') as \
        >>> temp_folder:
        >>>     # Read using Pandas
        >>>     df = pd.read_parquet(temp_folder)
        >>>     # Read using pyarrow, here we pass only the files, which are normally in subfolder 'spark'
        >>>     pq = parquet.ParquetDataset(path_or_paths=[x for x in Path(temp_dir).glob('**/*') if x.is_file()])

        Args:
            dataset_rid (str): the dataset rid
            files (List[str]): list of files or None, in which case all files are downloaded
            view (str): branch or transaction rid of the dataset
            parallel_processes (int): Set number of threads for download

        Yields:
            Iterator[str]:
                path to temporary folder containing root of dataset files

        """
        temp_output_directory = tempfile.mkdtemp(suffix=f"foundry_dev_tools-{dataset_rid}")
        _ = self.download_dataset_files(
            dataset_rid=dataset_rid,
            output_directory=temp_output_directory,
            files=files,
            view=view,
            parallel_processes=parallel_processes,
        )
        try:
            yield temp_output_directory
        finally:
            shutil.rmtree(temp_output_directory)

    def get_dataset_as_raw_csv(self, dataset_rid: str, branch: api_types.DatasetBranch = "master") -> requests.Response:
        """Uses csv API to download a dataset as csv.

        Args:
            dataset_rid (str): the dataset rid
            branch (str): branch of the dataset

        Returns:
            :external+requests:py:class:`~requests.Response`:
                with the csv stream.
                Can be converted to a pandas DataFrame
                >>> pd.read_csv(io.BytesIO(response.content))

        """
        return self.ctx.data_proxy.api_get_dataset_as_csv2(dataset_rid, branch)

    @overload
    def query_foundry_sql_legacy(
        self,
        query: str,
        return_type: Literal["pandas"],
        branch: api_types.Ref = ...,
        sql_dialect: api_types.SqlDialect = ...,
        timeout: int = ...,
    ) -> pd.core.frame.DataFrame: ...

    @overload
    def query_foundry_sql_legacy(
        self,
        query: str,
        return_type: Literal["spark"],
        branch: api_types.Ref = ...,
        sql_dialect: api_types.SqlDialect = ...,
        timeout: int = ...,
    ) -> pyspark.sql.DataFrame: ...

    @overload
    def query_foundry_sql_legacy(
        self,
        query: str,
        return_type: Literal["arrow"],
        branch: api_types.Ref = ...,
        sql_dialect: api_types.SqlDialect = ...,
        timeout: int = ...,
    ) -> pa.Table: ...

    @overload
    def query_foundry_sql_legacy(
        self,
        query: str,
        return_type: Literal["raw"],
        branch: api_types.Ref = ...,
        sql_dialect: api_types.SqlDialect = ...,
        timeout: int = ...,
    ) -> tuple[dict, list[list]]: ...

    @overload
    def query_foundry_sql_legacy(
        self,
        query: str,
        return_type: api_types.SQLReturnType = ...,
        branch: api_types.Ref = ...,
        sql_dialect: api_types.SqlDialect = ...,
        timeout: int = ...,
    ) -> tuple[dict, list[list]] | pd.core.frame.DataFrame | pa.Table | pyspark.sql.DataFrame: ...

    def query_foundry_sql_legacy(
        self,
        query: str,
        return_type: api_types.SQLReturnType = "raw",
        branch: api_types.Ref = "master",
        sql_dialect: api_types.SqlDialect = "SPARK",
        timeout: int = 600,
    ) -> tuple[dict, list[list]] | pd.core.frame.DataFrame | pa.Table | pyspark.sql.DataFrame:
        """Queries the dataproxy query API with spark SQL.

        Example:
            query_foundry_sql_legacy(query="SELECT * FROM `/Global/Foundry Operations/Foundry Support/iris`",
                                        branch="master")

        Args:
            query: the sql query
            branch: the branch of the dataset / query
            return_type: See :py:class:`foundry_dev_tools.utils.api_types.SQLReturnType`
            sql_dialect: the SQL dialect used for the query
            timeout: the query request timeout

        Returns:
            tuple (dict, list):
                (foundry_schema, data)
                data: contains the data matrix,
                foundry_schema: the foundry schema (fieldSchemaList key).
                Can be converted to a pandas Dataframe, see below

            .. code-block:: python

             foundry_schema, data = self.query_foundry_sql_legacy(query, branch)
             df = pd.DataFrame(
                 data=data, columns=[e["name"] for e in foundry_schema["fieldSchemaList"]]
             )

        Raises:
            ValueError: if return_type is not in :py:class:SQLReturnType
            DatasetHasNoSchemaError: if dataset has no schema
            BranchNotFoundError: if branch was not found
        """
        return self.ctx.data_proxy.query_foundry_sql_legacy(
            query,
            return_type=return_type,
            branch=branch,
            sql_dialect=sql_dialect,
            timeout=timeout,
        )

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal["pandas"],
        branch: api_types.Ref = ...,
        sql_dialect: api_types.SqlDialect = ...,
        timeout: int = ...,
    ) -> pd.core.frame.DataFrame: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal["spark"],
        branch: api_types.Ref = ...,
        sql_dialect: api_types.SqlDialect = ...,
        timeout: int = ...,
    ) -> pyspark.sql.DataFrame: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal["arrow"],
        branch: api_types.Ref = ...,
        sql_dialect: api_types.SqlDialect = ...,
        timeout: int = ...,
    ) -> pa.Table: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal["raw"],
        branch: api_types.Ref = ...,
        sql_dialect: api_types.SqlDialect = ...,
        timeout: int = ...,
    ) -> tuple[dict, list[list]]: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: api_types.SQLReturnType = ...,
        branch: api_types.Ref = ...,
        sql_dialect: api_types.SqlDialect = ...,
        timeout: int = ...,
    ) -> tuple[dict, list[list]] | pd.core.frame.DataFrame | pa.Table | pyspark.sql.DataFrame: ...

    def query_foundry_sql(
        self,
        query: str,
        return_type: api_types.SQLReturnType = "pandas",
        branch: api_types.Ref = "master",
        sql_dialect: api_types.SqlDialect = "SPARK",
        timeout: int = 600,
    ) -> tuple[dict, list[list]] | pd.core.frame.DataFrame | pa.Table | pyspark.sql.DataFrame:
        """Queries the Foundry SQL server with spark SQL dialect.

        Uses Arrow IPC to communicate with the Foundry SQL Server Endpoint.

        Falls back to query_foundry_sql_legacy in case pyarrow is not installed or the query does not return
        Arrow Format.

        Example:
            df1 = client.query_foundry_sql("SELECT * FROM `/Global/Foundry Operations/Foundry Support/iris`")
            query = ("SELECT col1 FROM `{start_transaction_rid}:{end_transaction_rid}@{branch}`.`{dataset_path_or_rid}` WHERE filterColumns = 'value1' LIMIT 1")
            df2 = client.query_foundry_sql(query)

        Args:
            query: The SQL Query in Foundry Spark Dialect (use backticks instead of quotes)
            branch: the dataset branch
            sql_dialect: the sql dialect
            return_type: See :py:class:foundry_dev_tools.foundry_api_client.SQLReturnType
            timeout: Query Timeout, default value is 600 seconds

        Returns:
            :external+pandas:py:class:`~pandas.DataFrame` | :external+pyarrow:py:class:`~pyarrow.Table` | :external+spark:py:class:`~pyspark.sql.DataFrame`:

            A pandas DataFrame, Spark DataFrame or pyarrow.Table with the result.

        Raises:
            ValueError: Only direct read eligible queries can be returned as arrow Table.

        """  # noqa: E501
        return self.ctx.foundry_sql_server.query_foundry_sql(
            query,
            return_type=return_type,
            branch=branch,
            sql_dialect=sql_dialect,
            timeout=timeout,
        )

    def get_user_info(self) -> dict:
        """Returns the multipass user info.

        Returns:
            dict:

        .. code-block:: python

           {
               "id": "<multipass-id>",
               "username": "<username>",
               "attributes": {
                   "multipass:email:primary": ["<email>"],
                   "multipass:given-name": ["<given-name>"],
                   "multipass:organization": ["<your-org>"],
                   "multipass:organization-rid": ["ri.multipass..organization. ..."],
                   "multipass:family-name": ["<family-name>"],
                   "multipass:upn": ["<upn>"],
                   "multipass:realm": ["<your-company>"],
                   "multipass:realm-name": ["<your-org>"],
               },
           }

        """
        return self.ctx.multipass.api_me().json()

    def get_group(self, group_id: str) -> dict:
        """Returns the multipass group information.

        Returns:
            dict:
                The API response

        .. code-block:: python

            {
                'id': '<id>',
                'name': '<groupname>',
                'attributes': {
                'multipass:realm': ['palantir-internal-realm'],
                'multipass:organization': ['<your-org>'],
                'multipass:organization-rid': ['ri.multipass..organization.<...>'],
                'multipass:realm-name': ['Palantir Internal']
            }

        """
        return self.ctx.multipass.api_get_group(group_id).json()

    def delete_group(self, group_id: str) -> requests.Response:
        """Deletes multipass group.

        Args:
            group_id (str): the group id to delete
        """
        return self.ctx.multipass.api_delete_group(group_id)

    def create_third_party_application(
        self,
        client_type: api_types.MultipassClientType,
        display_name: str,
        description: str | None,
        grant_types: list[api_types.MultipassGrantType],
        redirect_uris: list | None,
        logo_uri: str | None,
        organization_rid: str,
        allowed_organization_rids: list | None = None,
        resources: list[api_types.Rid] | None = None,
        operations: list[str] | None = None,
        marking_ids: list[str] | None = None,
        role_set_id: str | None = None,
        role_grants: dict[str, list[str]] | None = None,
        **kwargs,
    ) -> dict:
        """Creates Foundry Third Party application (TPA).

        https://www.palantir.com/docs/foundry/platform-security-third-party/third-party-apps-overview/
        User must have 'Manage OAuth 2.0 clients' workflow permissions.

        Args:
            client_type: Server Application (CONFIDENTIAL) or
                Native or single-page application (PUBLIC)
            display_name: Display name of the TPA
            description: Long description of the TPA
            grant_types: Usually, ["AUTHORIZATION_CODE", "REFRESH_TOKEN"] (authorization code grant)
                or ["REFRESH_TOKEN", "CLIENT_CREDENTIALS"] (client credentials grant)
            redirect_uris: Redirect URLs of TPA, used in combination with AUTHORIZATION_CODE grant
            logo_uri: URI or embedded image 'data:image/png;base64,<...>'
            organization_rid: Parent Organization of this TPA
            allowed_organization_rids: Passing None or empty list means TPA is activated for all
                Foundry organizations
            resources: Resources allowed to access by the client, otherwise no resource restrictions
            operations: Operations the client can be granted, otherwise no operation restrictions
            marking_ids: Markings allowed to access by the client, otherwise no marking restrictions
            role_set_id: roles allowed for this client, defaults to `oauth2-client`
            role_grants: mapping between roles and principal ids dict[role id,list[principal id]]
            **kwargs: gets passed to :py:meth:`APIClient.api_request`


        See below for the structure

        .. code-block:: python

            {
                "clientId":"<...>",
                "clientSecret":"<...>",
                "clientType":"<CONFIDENTIAL/PUBLIC>",
                "organizationRid":"<...>",
                "displayName":"<...>",
                "description":null,
                "logoUri":null,
                "grantTypes":[<"AUTHORIZATION_CODE","REFRESH_TOKEN","CLIENT_CREDENTIALS">],
                "redirectUris":[],
                "allowedOrganizationRids":[]
            }

        """
        return self.ctx.multipass.api_create_third_party_application(
            client_type=client_type,
            display_name=display_name,
            description=description,
            grant_types=grant_types,
            redirect_uris=redirect_uris,
            logo_uri=logo_uri,
            organization_rid=organization_rid,
            allowed_organization_rids=allowed_organization_rids,
            resources=resources,
            operations=operations,
            marking_ids=marking_ids,
            role_set_id=role_set_id,
            role_grants=role_grants,
            **kwargs,
        ).json()

    def delete_third_party_application(self, client_id: str) -> requests.Response:
        """Deletes a Third Party Application.

        Args:
            client_id (str): The unique identifier of the TPA.
        """
        return self.ctx.multipass.api_delete_third_party_application(client_id)

    def update_third_party_application(
        self,
        client_id: str,
        client_type: api_types.MultipassClientType,
        display_name: str,
        description: str | None,
        grant_types: list[api_types.MultipassGrantType],
        redirect_uris: list | None,
        logo_uri: str | None,
        organization_rid: str,
        allowed_organization_rids: list | None = None,
        resources: list[api_types.Rid] | None = None,
        operations: list[str] | None = None,
        marking_ids: list[str] | None = None,
        role_set_id: str | None = None,
        **kwargs,
    ) -> dict:
        """Updates Foundry Third Party application (TPA).

        https://www.palantir.com/docs/foundry/platform-security-third-party/third-party-apps-overview/
        User must have 'Manage OAuth 2.0 clients' workflow permissions.

        Args:
            client_id: The unique identifier of the TPA.
            client_type: Server Application (CONFIDENTIAL) or
                Native or single-page application (PUBLIC)
            display_name: Display name of the TPA
            description: Long description of the TPA
            grant_types: Usually, ["AUTHORIZATION_CODE", "REFRESH_TOKEN"] (authorization code grant)
                or ["REFRESH_TOKEN", "CLIENT_CREDENTIALS"] (client credentials grant)
            redirect_uris: Redirect URLs of TPA, used in combination with AUTHORIZATION_CODE grant
            logo_uri: URI or embedded image 'data:image/png;base64,<...>'
            organization_rid: Parent Organization of this TPA
            allowed_organization_rids: Passing None or empty list means TPA is activated for all
                Foundry organizations
            resources: Resources allowed to access by the client, otherwise no resource restrictions
            operations: Operations the client can be granted, otherwise no operation restrictions
            marking_ids: Markings allowed to access by the client, otherwise no marking restrictions
            role_set_id: roles allowed for this client, defaults to `oauth2-client`
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Reponse in following structure:

        .. code-block:: python

            {
                "clientId":"<...>",
                "clientType":"<CONFIDENTIAL/PUBLIC>",
                "organizationRid":"<...>",
                "displayName":"<...>",
                "description":null,
                "logoUri":null,
                "grantTypes":[<"AUTHORIZATION_CODE","REFRESH_TOKEN","CLIENT_CREDENTIALS">],
                "redirectUris":[],
                "allowedOrganizationRids":[]
            }

        """
        return self.ctx.multipass.api_update_third_party_application(
            client_id=client_id,
            client_type=client_type,
            display_name=display_name,
            description=description,
            grant_types=grant_types,
            redirect_uris=redirect_uris,
            logo_uri=logo_uri,
            organization_rid=organization_rid,
            allowed_organization_rids=allowed_organization_rids,
            resources=resources,
            operations=operations,
            marking_ids=marking_ids,
            role_set_id=role_set_id,
            **kwargs,
        ).json()

    def rotate_third_party_application_secret(
        self,
        client_id: str,
    ) -> dict:
        """Rotates Foundry Third Party application (TPA) secret.

        Args:
            client_id (str): The unique identifier of the TPA.

        Returns:
            dict:
                See below for the structure

        .. code-block:: python

            {
                "clientId":"<...>",
                "clientSecret": "<...>",
                "clientType":"<CONFIDENTIAL/PUBLIC>",
                "organizationRid":"<...>",
                "displayName":"<...>",
                "description":null,
                "logoUri":null,
                "grantTypes":[<"AUTHORIZATION_CODE","REFRESH_TOKEN","CLIENT_CREDENTIALS">],
                "redirectUris":[],
                "allowedOrganizationRids":[]
            }

        """
        return self.ctx.multipass.api_rotate_third_party_application_secret(client_id).json()

    def enable_third_party_application(
        self,
        client_id: str,
        operations: list | None = None,
        resources: list | None = None,
        marking_ids: list[str] | None = None,
        grant_types: list[api_types.MultipassGrantType] | None = None,
        require_consent: bool = True,
        **kwargs,
    ) -> dict:
        """Enables Foundry Third Party application (TPA).

        Args:
            client_id: The unique identifier of the TPA.
            operations: Scopes that this TPA is allowed to use (To be confirmed)
                if None or empty list is passed, all scopes will be activated.
            resources: Compass Project RID's that this TPA is allowed to access,
                if None or empty list is passed, unrestricted access will be given.
            marking_ids: Marking Ids that this TPA is allowed to access,
                if None or empty list is passed, unrestricted access will be given.
            grant_types: Grant types that this TPA is allowed to use to access resources,
                if None is passed, no grant type restrictions
                if an empty list is passed, no grant types are allowed for this TPA
            require_consent: Wether users need to provide consent for this application to act on their behalf,
                defaults to true
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Response with the following structure:

        .. code-block:: python

            {
                "client": {
                    "clientId": "<...>",
                    "organizationRid": "ri.multipass..organization.<...>",
                    "displayName": "<...>",
                    "description": None,
                    "logoUri": None,
                },
                "installation": {"resources": [], "operations": [], "markingIds": None},
            }

        """
        return self.ctx.multipass.api_enable_third_party_application(
            client_id,
            operations=operations,
            resources=resources,
            marking_ids=marking_ids,
            grant_types=grant_types,
            require_consent=require_consent,
            **kwargs,
        ).json()

    def start_checks_and_build(
        self,
        repository_id: str,
        ref_name: str,
        commit_hash: str,
        file_paths: list[str],
    ) -> dict:
        """Starts checks and builds.

        Args:
            repository_id (str): the repository id where the transform is located
            ref_name (str): the git ref_name for the branch
            commit_hash (str): the git commit hash
            file_paths (List[str]): a list of python transform files

        Returns:
            dict: the JSON API response
        """
        return self.ctx.jemma.start_checks_and_builds(repository_id, ref_name, commit_hash, set(file_paths))

    def get_build(self, build_rid: str) -> dict:
        """Get information about the build.

        Args:
            build_rid (str): the build RID

        Returns:
            dict: the JSON API response
        """
        return self.ctx.build2.api_get_build_report(build_rid).json()

    def get_job_report(self, job_rid: str) -> dict:
        """Get the report for a job.

        Args:
            job_rid (str): the job RID

        Returns:
            dict: the job report response

        """
        return self.ctx.build2.api_get_job_report(job_rid).json()

    def get_s3fs_storage_options(self) -> dict:
        """Get the foundry s3 credentials in the s3fs storage_options format.

        Example:
            >>> fc = FoundryRestClient()
            >>> storage_options = fc.get_s3fs_storage_options()
            >>> df = pd.read_parquet(
            ...     "s3://ri.foundry.main.dataset.<uuid>/spark", storage_options=storage_options
            ... )
        """
        return self.ctx.s3.get_s3fs_storage_options()

    def get_boto3_s3_client(self, **kwargs):  # noqa: ANN201
        """Returns the boto3 s3 client with credentials applied and endpoint url set.

        See :py:attr:`foundry_dev_tools.clients.s3_client.api_assume_role_with_webidentity`.

        Example:
            >>> from foundry_dev_tools import FoundryRestClient
            >>> fc = FoundryRestClient()
            >>> s3_client = fc.get_boto3_client()
            >>> s3_client
        Args:
            **kwargs: gets passed to :py:meth:`boto3.session.Session.client`, `endpoint_url` will be overwritten
        """
        return self.ctx.s3.get_boto3_client(**kwargs)

    def get_boto3_s3_resource(self, **kwargs):  # noqa: ANN201
        """Returns boto3 s3 resource with credentials applied and endpoint url set.

        Args:
            **kwargs: gets passed to :py:meth:`boto3.session.Session.resource`, `endpoint_url` will be overwritten
        """
        return self.ctx.s3.get_boto3_client(**kwargs)

    def get_s3_credentials(self, expiration_duration: int = 3600) -> dict:
        """Parses the AssumeRoleWithWebIdentity response and caches the credentials.

        See :py:attr:`foundry_dev_tools.clients.s3_client.api_assume_role_with_webidentity`.
        """
        return self.ctx.s3.get_credentials(expiration_duration)
