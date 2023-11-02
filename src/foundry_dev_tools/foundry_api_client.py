"""Contains FoundryRestClient and FoundrySqlClient and exception classes.

One of the gaols of this module is to be self-contained so that it can be
dropped into any python installation with minimal dependency to 'requests'
Optional dependencies for the SQL functionality to work are pandas and pyarrow.

"""
from __future__ import annotations

import base64
import builtins
import functools
import logging
import multiprocessing
import os
import platform
import shutil
import sys
import tempfile
import time
import warnings
from contextlib import contextmanager
from enum import Enum, EnumMeta
from itertools import repeat
from pathlib import Path
from typing import IO, TYPE_CHECKING, AnyStr
from urllib.parse import quote, quote_plus

import palantir_oauth_client
import requests

import foundry_dev_tools.config

from .__about__ import __version__

if TYPE_CHECKING:
    from collections.abc import Iterator

    import pandas as pd
    import pyarrow as pa
    import pyspark
# On Python 3.8 on macOS, the default start method for new processes was
#  switched to 'spawn' by default due to 'fork' potentially causing crashes.
# These crashes haven't yet been observed with libE, but with 'spawn' runs,
#  warnings about leaked semaphore objects are displayed instead.
# The next several statements enforce 'fork' on macOS (Python 3.8)
if platform.system() == "Darwin":
    from multiprocessing import set_start_method

    set_start_method("fork", force=True)

LOGGER = logging.getLogger(__name__)

DEFAULT_REQUESTS_CONNECT_TIMEOUT = 10
DEFAULT_HEADERS = {
    "User-Agent": requests.utils.default_user_agent(
        f"foundry-dev-tools/{__version__}/python-requests"
    ),
    "Content-Type": "application/json",
}
DEFAULT_TPA_SCOPES = [
    "offline_access",
    "compass:view",
    "compass:edit",
    "compass:discover",
    "api:write-data",
    "api:read-data",
]


@contextmanager
def _poolcontext(*args, **kwargs):
    pool = multiprocessing.Pool(*args, **kwargs)
    yield pool
    pool.terminate()


class EnumContainsMeta(EnumMeta):
    """Metaclass for the SQLReturnType.

    It implements a proper __contains__ like 3.12 does.
    """

    def __contains__(cls, value):
        """Backported a __contains__ from 3.12."""
        if sys.version_info >= (3, 12):
            return EnumMeta.__contains__(cls, value)
        if isinstance(value, cls) and value._name_ in cls._member_map_:
            return True
        return value in cls._value2member_map_


class SQLReturnType(str, Enum, metaclass=EnumContainsMeta):
    """The return_types for sql queries.

    PANDAS, PD: :external+pandas:py:class:`pandas.DataFrame` (pandas)
    ARROW, PYARROW, PA: :external+pyarrow:py:class:`pyarrow.Table` (arrow)
    SPARK, PYSPARK: :external+spark:py:class:`~pyspark.sql.DataFrame` (spark)
    RAW: Tuple of (foundry_schema, data) (raw) (can only be used in legacy)
    """

    PANDAS = "pandas"
    PD = PANDAS
    SPARK = "spark"
    PYSPARK = SPARK
    ARROW = "arrow"
    PYARROW = ARROW
    PA = ARROW
    RAW = "raw"


class FoundryRestClient:
    """Zero dependency python client for Foundry's REST APIs."""

    def __init__(self, config: dict | None = None):
        """Create an instance of FoundryRestClient.

        Args:
            config (dict):^
             A dictionary containing basic configuration how to authenticate with Foundry.
             If config=None is passed, the Configuration class from foundry_dev_tools.config
             will be used to automatically apply a config based on the
             '~/.foundry-dev-tools/config' file.
             Configuration entries can be overwritten by specifying them in the config dict.
             In case this class is used without foundry_dev_tools.config.Configuration
             pass at least one of
             'jwt' or
             'client_id' (in case you have a third-party-app registration):

        Examples:
            >>> fc = FoundryRestClient()
            >>> fc = FoundryRestClient(config={'jwt': '<token>'})
            >>> fc = FoundryRestClient(config={'client_id': '<client_id>'})
        """
        self._config = foundry_dev_tools.config.Configuration.get_config(config)
        self._api_base = self._config["foundry_url"]
        self.catalog = f"{self._api_base}/foundry-catalog/api"
        self.compass = f"{self._api_base}/compass/api"
        self.metadata = f"{self._api_base}/foundry-metadata/api"
        self.data_proxy = f"{self._api_base}/foundry-data-proxy/api"
        self.schema_inference = f"{self._api_base}/foundry-schema-inference/api"
        self.multipass = f"{self._api_base}/multipass/api"
        self.foundry_sql_server_api = f"{self._api_base}/foundry-sql-server/api"
        self.jemma = f"{self._api_base}/jemma/api"
        self.builds2 = f"{self._api_base}/build2/api"
        self.foundry_stats_api = f"{self._api_base}/foundry-stats/api"
        self._requests_verify_value = _determine_requests_verify_value(self._config)
        self._requests_session = requests.Session()
        self._requests_session.verify = self._requests_verify_value
        self._aiosession = None
        self._boto3_session = None
        self._s3_url = self._api_base + "/io/s3"

    def _headers(self):
        return {
            **DEFAULT_HEADERS,
            "Authorization": f"Bearer {_get_auth_token(self._config)}",
        }

    def _request(self, *args, read_timeout: int | None = None, **kwargs):
        kwargs.setdefault("headers", self._headers())
        kwargs["timeout"] = (
            (DEFAULT_REQUESTS_CONNECT_TIMEOUT, read_timeout)
            if read_timeout is None
            else DEFAULT_REQUESTS_CONNECT_TIMEOUT
        )
        return self._requests_session.request(*args, **kwargs)

    def create_dataset(self, dataset_path: str) -> dict:
        """Creates an empty dataset in Foundry.

        Args:
            dataset_path (str): Path in Foundry, where this empty dataset should be created
                for example: /Global/Foundry Operations/Foundry Support/iris_new

        Returns:
            dict:
                with keys rid and fileSystemId.
                The key rid contains the dataset_rid which is the unique identifier of a dataset.

        """
        response = self._request(
            "POST",
            f"{self.catalog}/catalog/datasets",
            json={"path": dataset_path},
        )
        if (
            response.status_code == requests.codes.bad
            and "DuplicateDatasetName" in response.text
        ):
            rid = self.get_dataset_rid(dataset_path=dataset_path)
            raise DatasetAlreadyExistsError(dataset_path=dataset_path, dataset_rid=rid)
        _raise_for_status_verbose(response)
        return response.json()

    def get_dataset(self, dataset_rid: str) -> dict:
        """Gets dataset_rid and fileSystemId.

        Args:
            dataset_rid (str): Dataset rid

        Returns:
            dict:
                with the keys rid and fileSystemId

        Raises:
            DatasetNotFoundError: if dataset does not exist
        """
        response = self._request(
            "GET",
            f"{self.catalog}/catalog/datasets/{dataset_rid}",
        )
        if response.status_code == requests.codes.no_content and not response.text:
            raise DatasetNotFoundError(dataset_rid, response=response)
        _raise_for_status_verbose(response)
        return response.json()

    def delete_dataset(self, dataset_rid: str):
        """Deletes a dataset in Foundry and moves it to trash.

        Args:
            dataset_rid (str): Unique identifier of the dataset

        Raises:
            DatasetNotFoundError: if dataset does not exist

        """
        response = self._request(
            "DELETE",
            f"{self.catalog}/catalog/datasets",
            json={"rid": dataset_rid},
        )
        if (
            response.status_code == requests.codes.bad
            and "DatasetsNotFound" in response.text
        ):
            raise DatasetNotFoundError(dataset_rid, response=response)
        _raise_for_status_verbose(response)
        self.move_resource_to_trash(resource_id=dataset_rid)

    def move_resource_to_trash(self, resource_id: str):
        """Moves a Compass resource (e.g. dataset or folder) to trash.

        Args:
            resource_id (str): rid of the resource

        """
        response_trash = self._request(
            "POST",
            f"{self.compass}/batch/trash/add",
            data=f'["{resource_id}"]',
        )
        if response_trash.status_code != requests.codes.no_content:
            raise KeyError(
                f"Issue while moving resource '{resource_id}' to foundry trash."
            )
        _raise_for_status_verbose(response_trash)

    def create_branch(
        self,
        dataset_rid: str,
        branch: str,
        parent_branch: str | None = None,
        parent_branch_id: str | None = None,
    ) -> dict:
        """Creates a new branch in a dataset.

        If dataset is 'new', only parameter dataset_rid and branch are required.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            branch (str): The branch to create
            parent_branch (str): The name of the parent branch, if empty creates new root branch
            parent_branch_id (str): The unique id of the parent branch, if empty creates new root branch

        Returns:
            dict:
                the response as a json object

        """
        response = self._request(
            "POST",
            f"{self.catalog}/catalog/datasets/{dataset_rid}/branchesUnrestricted2/{quote_plus(branch)}",
            json={"parentBranchId": parent_branch, "parentRef": parent_branch_id},
        )
        if (
            response.status_code == requests.codes.bad
            and "BranchesAlreadyExist" in response.text
        ):
            raise BranchesAlreadyExistError(dataset_rid, branch, response=response)
        _raise_for_status_verbose(response)
        return response.json()

    def update_branch(
        self, dataset_rid: str, branch: str, parent_branch: str | None = None
    ) -> dict:
        """Updates the latest transaction of branch 'branch' to the latest transaction of branch 'parent_branch'.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            branch (str): The branch to update (e.g. master)
            parent_branch (str): the name of the branch to copy the last transaction from

        Returns:
            dict:
                example below for the branch response
        .. code-block:: python

         {
            'id': '..',
            'rid': 'ri.foundry.main.branch...',
            'ancestorBranchIds': [],
            'creationTime': '',
            'transactionRid': 'ri.foundry.main.transaction....'
         }

        """
        response = self._request(
            "POST",
            f"{self.catalog}/catalog/datasets/{dataset_rid}/branchesUpdate2/{quote_plus(branch)}",
            data=f'"{parent_branch}"',
        )
        _raise_for_status_verbose(response)
        return response.json()

    def get_branch(self, dataset_rid: str, branch: str) -> dict:
        """Returns branch information.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            branch (str): Branch name

        Returns:
            dict:
                with keys id (name) and rid (unique id) of the branch.

        Raises:
             BranchNotFoundError: if branch does not exist.

        """
        response = self._request(
            "GET",
            f"{self.catalog}/catalog/datasets/{dataset_rid}/branches2/{quote_plus(branch)}",
        )
        if response.status_code == requests.codes.no_content and not response.text:
            raise BranchNotFoundError(dataset_rid, branch, response=None)
        _raise_for_status_verbose(response)
        return response.json()

    def open_transaction(
        self, dataset_rid: str, mode: str = "SNAPSHOT", branch: str = "master"
    ) -> str:
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
        response = self._request(
            "POST",
            f"{self.catalog}/catalog/datasets/{dataset_rid}/transactions",
            json={"branchId": f"{branch}", "record": {}},
        )
        if (
            response.status_code == requests.codes.bad
            and "BranchesNotFound" in response.text
        ):
            raise BranchNotFoundError(dataset_rid, branch, response=response)
        if (
            response.status_code == requests.codes.bad
            and "InvalidArgument" in response.text
        ):
            raise DatasetNotFoundError(dataset_rid, response=response)
        if (
            response.status_code == requests.codes.bad
            and "SimultaneousOpenTransactionsNotAllowed" in response.text
        ):
            open_transaction_rid = response.json()["parameters"]["openTransactionRid"]
            raise DatasetHasOpenTransactionError(
                dataset_rid=dataset_rid,
                open_transaction_rid=open_transaction_rid,
                response=response,
            )
        _raise_for_status_verbose(response)
        # rid holds transaction id
        response_json = response.json()
        transaction_id = response_json["rid"]
        # update type of transaction, default is APPEND
        if mode in ("UPDATE", "SNAPSHOT", "DELETE"):
            response_update = self._request(
                "POST",
                f"{self.catalog}/catalog/datasets/{dataset_rid}/transactions/{transaction_id}",
                data=f'"{mode}"',
            )
            _raise_for_status_verbose(response_update)
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
        response = self._request(
            "POST",
            f"{self.catalog}/catalog/datasets/{dataset_rid}/transactions/{transaction_id}/files/remove",
            params={"logicalPath": logical_path, "recursive": recursive},
        )
        _raise_for_status_verbose(response)

    def add_files_to_delete_transaction(
        self, dataset_rid: str, transaction_id: str, logical_paths: list[str]
    ):
        """Adds files in an open DELETE transaction.

        Files added to DELETE transactions affect
        the dataset view by removing files from the view.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            transaction_id (str): transaction rid
            logical_paths (List[str]): files in the dataset to delete

        """
        response = self._request(
            "POST",
            f"{self.catalog}/catalog/datasets/{dataset_rid}/transactions/{transaction_id}/files/addToDeleteTransaction",
            json={"logicalPaths": logical_paths},
        )
        _raise_for_status_verbose(response)

    def commit_transaction(self, dataset_rid: str, transaction_id: str):
        """Commits a transaction, should be called after file upload is complete.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            transaction_id (str): transaction id

        Raises:
            KeyError: when there was an issue with committing
        """
        response = self._request(
            "POST",
            f"{self.catalog}/catalog/datasets/{dataset_rid}/transactions/{transaction_id}/commit",
            json={"record": {}},
        )
        _raise_for_status_verbose(response)
        if response.status_code != requests.codes.no_content:
            raise KeyError(
                f"{dataset_rid} issue while committing transaction {transaction_id}"
            )

    def abort_transaction(self, dataset_rid: str, transaction_id: str):
        """Aborts a transaction. Dataset will remain on transaction N-1.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            transaction_id (str): transaction id

        Raises:
            KeyError: When abort transaction fails

        """
        response = self._request(
            "POST",
            f"{self.catalog}/catalog/datasets/{dataset_rid}/transactions/{transaction_id}/abortWithMetadata",
            json={"record": {}},
        )
        _raise_for_status_verbose(response)
        if response.status_code != requests.codes.no_content:
            raise KeyError(
                f"{dataset_rid} issue while aborting transaction {transaction_id}"
            )

    def get_dataset_transactions(
        self,
        dataset_rid: str,
        branch: str = "master",
        last: int = 50,
        include_open_exclusive_transaction: bool = False,
    ) -> dict:
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
        response = self._request(
            "GET",
            f"{self.catalog}/catalog/datasets/{dataset_rid}/reverse-transactions2/{quote_plus(branch)}",
            params={
                "pageSize": last,
                "includeOpenExclusiveTransaction": include_open_exclusive_transaction,
            },
        )
        if response.status_code in (404, 400):
            raise BranchNotFoundError(dataset_rid, branch, response=response)
        _raise_for_status_verbose(response)
        as_json = response.json()
        if "values" in as_json and len(response.json()["values"]) > 0:
            return response.json()["values"]
        raise DatasetHasNoTransactionsError(dataset_rid, response=response)

    def get_dataset_last_transaction(
        self, dataset_rid: str, branch: str = "master"
    ) -> dict | None:
        """Returns the last transaction of a dataset / branch combination.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            branch (str): Branch

        Returns:
            dict | None:
                response from transaction API or None if dataset has no transaction.

        """
        try:
            return self.get_dataset_transactions(dataset_rid, branch)[0]
        except DatasetHasNoTransactionsError:
            return None

    def get_dataset_last_transaction_rid(
        self, dataset_rid: str, branch: str = "master"
    ) -> str | None:
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
    ):
        """Uploads a file like object to a path in a foundry dataset.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            transaction_rid (str): transaction id
            path_or_buf (str | :py:class:`~pathlib.Path` | :py:class:`~typing.IO`): A str or file handle,
                file path or object
            path_in_foundry_dataset (str): The path in the dataset, to which the file
                is uploaded.

        """
        original_open = open
        builtins.open = (
            lambda f, *args, **kwargs: f
            if hasattr(f, "read")
            else original_open(f, *args, **kwargs)
        )
        with open(path_or_buf, "rb") as file:
            response = self._request(
                "POST",
                f"{self.data_proxy}/dataproxy/datasets/{dataset_rid}/transactions/{transaction_rid}/putFile",
                params={"logicalPath": path_in_foundry_dataset},
                data=file,
                headers={
                    "Content-Type": "application/octet-stream",
                    "Authorization": self._headers()["Authorization"],
                },
            )
        builtins.open = original_open
        _raise_for_status_verbose(response)
        if response.status_code != requests.codes.no_content:
            raise ValueError(
                f"Issue while uploading file {path_or_buf} "
                f"to dataset: {dataset_rid}, "
                f"transaction_rid: {transaction_rid}. "
                f"Response status: {response.status_code},"
                f"Response text: {response.text}"
            )

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
        if not parallel_processes:
            parallel_processes = (
                1
                if platform.system() == "Windows"
                else (multiprocessing.cpu_count() - 1)
            )
        if parallel_processes == 1 or len(path_file_dict) == 1:
            for key, value in path_file_dict.items():
                self.upload_dataset_file(
                    dataset_rid,
                    transaction_rid,
                    path_or_buf=value,
                    path_in_foundry_dataset=key,
                )
        else:
            with _poolcontext(processes=parallel_processes) as pool:
                pool.starmap(
                    self.upload_dataset_file,
                    zip(
                        repeat(dataset_rid),
                        repeat(transaction_rid),
                        path_file_dict.values(),
                        path_file_dict.keys(),
                    ),
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
        if "ri.foundry.main.dataset" in dataset_path_or_rid:
            response = self._request(
                "GET",
                f"{self.compass}/resources/{dataset_path_or_rid}",
                params={"decoration": "path"},
            )
        else:
            response = self._request(
                "GET",
                f"{self.compass}/resources",
                params={"path": dataset_path_or_rid, "decoration": "path"},
            )
        _raise_for_status_verbose(response)
        if response.status_code != requests.codes.ok:
            raise DatasetNotFoundError(dataset_path_or_rid, response=response)
        as_json = response.json()
        if as_json["directlyTrashed"]:
            warnings.warn(f"Dataset '{dataset_path_or_rid}' is in foundry trash.")
        return as_json

    def get_child_objects_of_folder(
        self, folder_rid: str, page_size: int | None = None
    ) -> Iterator[dict]:
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
        query_params = {"pageToken": None, "limit": page_size}
        while True:
            response = self._request(
                "GET",
                f"{self.compass}/folders/{folder_rid}/children",
                params=query_params,
            )
            if response.status_code == requests.codes.not_found:
                raise FolderNotFoundError(folder_rid, response=response)
            _raise_for_status_verbose(response)
            response_as_json = response.json()
            yield from response_as_json["values"]
            if response_as_json["nextPageToken"] is None:
                break

            query_params["pageToken"] = response_as_json["nextPageToken"]

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
        response = self._request(
            "POST",
            f"{self.compass}/folders",
            json={"name": name, "parentId": parent_id},
        )
        _raise_for_status_verbose(response)
        return response.json()

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
        rid_path_dict = self.get_dataset_paths([dataset_rid])
        if dataset_rid not in rid_path_dict:
            raise DatasetNotFoundError(dataset_rid)
        return rid_path_dict[dataset_rid]

    def get_dataset_paths(self, dataset_rids: list) -> dict:
        """Returns a list of paths for a list of passed rid's of a dataset.

        Args:
            dataset_rids (list): The rid's of datasets

        Returns:
            dict:
                the dataset_path as dict of string

        """
        batch_size = 100
        batches = [
            dataset_rids[i : i + batch_size]
            for i in range(0, len(dataset_rids), batch_size)
        ]
        result = {}
        for batch in batches:
            response = self._request(
                "POST",
                f"{self.compass}/batch/paths",
                json=batch,
            )
            _raise_for_status_verbose(response)
            result = {**result, **response.json()}
        return result

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

    def get_dataset_schema(
        self, dataset_rid: str, transaction_rid: str, branch: str = "master"
    ) -> dict:
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
        response = self._request(
            "GET",
            f"{self.metadata}/schemas/datasets/{dataset_rid}/branches/{quote_plus(branch)}",
            params={"endTransactionRid": transaction_rid},
        )
        if response.status_code == requests.codes.forbidden:
            raise DatasetNotFoundError(dataset_rid, response=response)
        if response.status_code == requests.codes.no_content:
            # here we don't know if schema does not exist or branch does not exist
            # we ask api for branch information, if branch does not exist, exception is thrown
            self.get_branch(dataset_rid, branch)
            raise DatasetHasNoSchemaError(dataset_rid, transaction_rid, branch)
        if response.status_code == requests.codes.not_found or (
            "errorName" in response.json()
            and response.json()["errorName"] == "Catalog:BranchesNotFound"
        ):
            raise BranchNotFoundError(dataset_rid, branch, response=response)
        if response.status_code != requests.codes.ok:
            raise KeyError(
                f"{dataset_rid}, {branch}, {transaction_rid} combination not found"
            )
        return response.json()["schema"]

    def upload_dataset_schema(
        self,
        dataset_rid: str,
        transaction_rid: str,
        schema: dict,
        branch: str = "master",
    ):
        """Uploads the foundry dataset schema for a dataset, transaction, branch combination.

        Args:
            dataset_rid (str): The rid of the dataset
            transaction_rid (str): The rid of the transaction
            schema (dict): The foundry schema
            branch (str): The branch

        """
        response = self._request(
            "POST",
            f"{self.metadata}/schemas/datasets/{dataset_rid}/branches/{quote_plus(branch)}",
            params={"endTransactionRid": transaction_rid},
            json=schema,
        )
        _raise_for_status_verbose(response)

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
        response = self._request(
            "POST",
            f"{self.schema_inference}/datasets/{dataset_rid}/branches/{quote_plus(branch)}/schema",
            json={},
        )
        _raise_for_status_verbose(response)
        parsed_response = response.json()
        if parsed_response["status"] == "SUCCESS":
            return parsed_response["data"]["foundrySchema"]
        if parsed_response["status"] == "WARN":
            warnings.warn(
                "Foundry Schema inference completed with status "
                f"'{parsed_response['status']}' "
                f"and message '{parsed_response['message']}'.",
                UserWarning,
            )
            return parsed_response["data"]["foundrySchema"]
        raise ValueError(
            "Foundry Schema inference failed with status "
            f"'{parsed_response['status']}' "
            f"and message '{parsed_response['message']}'."
        )

    def get_dataset_identity(
        self, dataset_path_or_rid: str, branch="master", check_read_access=True
    ) -> dict:
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
        if (
            check_read_access
            and "gatekeeper:view-resource" not in dataset_details["operations"]
        ):
            raise DatasetNoReadAccessError(dataset_rid)
        last_transaction = self.get_dataset_last_transaction(dataset_rid, branch)
        return {
            "dataset_path": dataset_path,
            "dataset_rid": dataset_rid,
            "last_transaction_rid": last_transaction["rid"]
            if last_transaction
            else None,
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

        def _inner_get(next_page_token=None):
            response = self._request(
                "GET",
                f"{self.catalog}/catalog/datasets/{dataset_rid}/views2/{quote_plus(view)}/files",
                params={
                    "pageSize": 10000000,
                    "includeOpenExclusiveTransaction": include_open_exclusive_transaction,
                    "logicalPath": logical_path,
                    "excludeHiddenFiles": exclude_hidden_files,
                    "pageStartLogicalPath": next_page_token,
                },
            )
            _raise_for_status_verbose(response)
            if response.status_code != requests.codes.ok:
                raise DatasetNotFoundError(dataset_rid, response=response)
            return response.json()

        result = []
        batch_result = _inner_get(next_page_token=None)
        result.extend(batch_result["values"])
        while batch_result["nextPageToken"] is not None:
            batch_result = _inner_get(next_page_token=batch_result["nextPageToken"])
            result.extend(batch_result["values"])
        if detail:
            return result
        return [file["logicalPath"] for file in result]

    def get_dataset_stats(self, dataset_rid: str, view: str = "master") -> dict:
        """Returns response from foundry catalogue stats endpoint.

        Args:
            dataset_rid (str): the dataset rid
            view (str): branch or transaction rid of the dataset

        Returns:
            dict:
                sizeInBytes, numFiles, hiddenFilesSizeInBytes, numHiddenFiles, numTransactions

        """
        response = self._request(
            "GET",
            f"{self.catalog}/catalog/datasets/{dataset_rid}/views/{quote_plus(view)}/stats",
        )
        _raise_for_status_verbose(response)
        return response.json()

    def foundry_stats(
        self, dataset_rid: str, end_transaction_rid: str, branch: str = "master"
    ) -> dict:
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
                datasetRid:str,
                branch:str,
                endTransactionRid:str,
                schemaId:str,
                computedDatasetStats:{
                    rowCount:str|None,
                    sizeInBytes:str,
                    columnStats:{
                        "...":{
                            nullCount:str|None,
                            uniqueCount:str|None,
                            avgLength:str|None,
                            maxLength:str|None
                            }
                        }
                    }
                }
        """
        response = self._request(
            "POST",
            f"{self.foundry_stats_api}/computed-stats-v2/get-v2",
            json={
                "datasetRid": dataset_rid,
                "branch": branch,
                "endTransactionRid": end_transaction_rid,
            },
        )
        _raise_for_status_verbose(response)
        return response.json()

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
        if output_directory is None:
            return self._download_dataset_file(
                dataset_rid, view, foundry_file_path, stream=False
            ).content

        local_path = Path(output_directory).joinpath(foundry_file_path)
        file_dir = local_path.parent
        if file_dir.is_file() and file_dir.stat().st_size == 0:
            file_dir.unlink()
        file_dir.mkdir(exist_ok=True, parents=True)
        if local_path.is_dir():
            return ""
        resp = self._download_dataset_file(
            dataset_rid, view, foundry_file_path, stream=True
        )
        _raise_for_status_verbose(resp)
        with local_path.open(mode="wb") as out_file:
            resp.raw.decode_content = True
            shutil.copyfileobj(resp.raw, out_file)

        resp.close()
        return os.fspath(local_path)

    def _download_dataset_file(self, dataset_rid, view, foundry_file_path, stream=True):
        response = self._request(
            "GET",
            f"{self.data_proxy}/dataproxy/datasets/{dataset_rid}/views/{quote_plus(view)}/{quote(foundry_file_path)}",
            stream=stream,
        )
        _raise_for_status_verbose(response)
        return response

    def download_dataset_files(
        self,
        dataset_rid: str,
        output_directory: str,
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
        if files is None:
            files = self.list_dataset_files(
                dataset_rid=dataset_rid, exclude_hidden_files=True, view=view
            )
        local_paths = []
        if not parallel_processes:
            parallel_processes = (
                1
                if platform.system() == "Windows"
                else (multiprocessing.cpu_count() - 1)
            )
        if parallel_processes == 1 or len(files) == 1:
            for file in files:
                local_paths.append(
                    self.download_dataset_file(
                        dataset_rid, output_directory, file, view
                    )
                )
        else:
            with _poolcontext(processes=parallel_processes) as pool:
                local_paths = list(
                    pool.starmap(
                        self.download_dataset_file,
                        zip(
                            repeat(dataset_rid),
                            repeat(output_directory),
                            files,
                            repeat(view),
                        ),
                    )
                )
        return list(filter(lambda p: p != "", local_paths))

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
        temp_output_directory = tempfile.mkdtemp(
            suffix=f"foundry_dev_tools-{dataset_rid}"
        )
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

    def get_dataset_as_raw_csv(
        self, dataset_rid: str, branch="master"
    ) -> requests.Response:
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
        response = self._request(
            "GET",
            f"{self.data_proxy}/dataproxy/datasets/{dataset_rid}/branches/{quote_plus(branch)}/csv",
            params={"includeColumnNames": True, "includeBom": True},
            stream=True,
        )

        _raise_for_status_verbose(response)
        return response

    def query_foundry_sql_legacy(
        self,
        query: str,
        branch: str = "master",
        return_type: SQLReturnType = SQLReturnType.RAW,
        timeout: int = 600,
    ) -> (
        tuple[dict, list[list]]
        | pd.core.frame.DataFrame
        | pyspark.sql.DataFrame
        | pa.Table
    ):
        """Queries the dataproxy query API with spark SQL.

        Example:
            client.query_foundry_sql_legacy(query="SELECT * FROM `/Global/Foundry Operations/Foundry Support/iris`",
                                            branch="master")

        Args:
            query (str): the sql query
            branch (str): the branch of the dataset / query
            return_type (SQLReturnType): See :py:class:foundry_dev_tools.foundry_api_client.SQLReturnType
            timeout (int): the query timeout, default value is 600 seconds

        Returns:
            Tuple (dict, list):
                foundry_schema, data.
                data contains the data matrix,
                foundry_schema the foundry schema (fieldSchemaList key).
                Can be converted to a pandas Dataframe, see below

            .. code-block:: python

             foundry_schema, data = self.query_foundry_sql_legacy(query, branch)
             df = pd.DataFrame(data=data, columns=[e['name'] for e in foundry_schema['fieldSchemaList']])

        Raises:
            ValueError: if return_type is not in :py:class:SQLReturnType
            DatasetHasNoSchemaError: if dataset has no schema
            BranchNotFoundError: if branch was not found
        """
        # if return_type is a str this will also work, this will get fixed in python 3.12
        # where we would be able to use `return_type not in SQLReturnType`
        if return_type not in SQLReturnType:
            raise ValueError(
                f"return_type ({return_type}) should be a member of foundry_dev_tools.foundry_api_client.SQLReturnType"
            )
        response = self._request(
            "POST",
            f"{self.data_proxy}/dataproxy/queryWithFallbacks",
            params={"fallbackBranchIds": [branch]},
            json={"query": query},
            read_timeout=timeout,
        )

        if (
            response.status_code == requests.codes.not_found
            and response.json()["errorName"] == "DataProxy:SchemaNotFound"
        ):
            dataset_rid = response.json()["parameters"]["datasetRid"]
            raise DatasetHasNoSchemaError(dataset_rid, branch=branch)
        if (
            response.status_code == requests.codes.bad
            and response.json()["errorName"]
            == "DataProxy:FallbackBranchesNotSpecifiedInQuery"
        ):
            # FallbackBranchesNotSpecifiedInQuery does not sound right,
            # but this indicates that the branch does not exist
            raise BranchNotFoundError(
                response.json()["parameters"]["datasetRid"],
                branch,
                response=response,
            )

        _raise_for_status_verbose(response)
        response_json = response.json()
        if return_type == SQLReturnType.RAW:
            return response_json["foundrySchema"], response_json["rows"]
        if return_type == SQLReturnType.PANDAS:
            import pandas as pd

            return pd.DataFrame(
                data=response_json["rows"],
                columns=[
                    e["name"] for e in response_json["foundrySchema"]["fieldSchemaList"]
                ],
            )
        if return_type == SQLReturnType.ARROW:
            import pyarrow as pa

            return pa.table(
                data=response_json["rows"],
                names=[
                    e["name"] for e in response_json["foundrySchema"]["fieldSchemaList"]
                ],
            )
        if return_type == SQLReturnType.SPARK:
            from foundry_dev_tools.utils.converter.foundry_spark import (
                foundry_schema_to_spark_schema,
                foundry_sql_data_to_spark_dataframe,
            )

            return foundry_sql_data_to_spark_dataframe(
                data=response_json["rows"],
                spark_schema=foundry_schema_to_spark_schema(
                    response_json["foundrySchema"]
                ),
            )
        return None

    def query_foundry_sql(
        self,
        query: str,
        branch: str = "master",
        return_type: SQLReturnType = SQLReturnType.PANDAS,
        timeout: int = 600,
    ) -> pd.core.frame.DataFrame | pa.Table | pyspark.sql.DataFrame:
        """Queries the Foundry SQL server with spark SQL dialect.

        Uses Arrow IPC to communicate with the Foundry SQL Server Endpoint.

        Falls back to query_foundry_sql_legacy in case pyarrow is not installed or the query does not return
        Arrow Format.

        Example:
            df1 = client.query_foundry_sql("SELECT * FROM `/Global/Foundry Operations/Foundry Support/iris`")
            query = ("SELECT col1 FROM `{start_transaction_rid}:{end_transaction_rid}@{branch}`.`{dataset_path_or_rid}` WHERE filterColumns = 'value1' LIMIT 1")
            df2 = client.query_foundry_sql(query)

        Args:
            query (str): The SQL Query in Foundry Spark Dialect (use backticks instead of quotes)
            branch (str): The branch of the dataset / query
            return_type (SQLReturnType): See :py:class:foundry_dev_tools.foundry_api_client.SQLReturnType
            timeout (int): Query Timeout, default value is 600 seconds

        Returns:
            :external+pandas:py:class:`~pandas.DataFrame` | :external+pyarrow:py:class:`~pyarrow.Table` | :external+spark:py:class:`~pyspark.sql.DataFrame`:

            A pandas DataFrame, Spark DataFrame or pyarrow.Table with the result.

        Raises:
            ValueError: Only direct read eligible queries can be returned as arrow Table.

        """  # noqa: E501
        # if return_type is a str this will also work, this will get fixed in python 3.12
        # where we would be able to use `return_type not in SQLReturnType`
        if return_type not in SQLReturnType:
            raise ValueError(
                f"return_type ({return_type}) should be a member of foundry_dev_tools.foundry_api_client.SQLReturnType"
            )
        if return_type != SQLReturnType.RAW:
            try:
                return self._query_fsql(
                    query=query, branch=branch, return_type=return_type
                )
            except (
                FoundrySqlSerializationFormatNotImplementedError,
                ImportError,
            ) as exc:
                if return_type == SQLReturnType.ARROW:
                    raise ValueError(
                        "Only direct read eligible queries can be returned as arrow Table. "
                        "Consider using setting return_type to 'pandas'."
                    ) from exc

        warnings.warn("Falling back to query_foundry_sql_legacy!")
        return self.query_foundry_sql_legacy(
            query=query, branch=branch, return_type=return_type, timeout=timeout
        )

    def get_user_info(self) -> dict:
        """Returns the multipass user info.

        Returns:
            dict:

        .. code-block:: python

           {
                'id': '<multipass-id>',
                'username': '<username>',
                'attributes': {'multipass:email:primary': ['<email>'],
                'multipass:given-name': ['<given-name>'],
                'multipass:organization': ['<your-org>'],
                'multipass:organization-rid': ['ri.multipass..organization. ...'],
                'multipass:family-name': ['<family-name>'],
                'multipass:upn': ['<upn>'],
                'multipass:realm': ['<your-company>'],
                'multipass:realm-name': ['<your-org>']}
           }

        """
        response = self._request(
            "GET",
            f"{self.multipass}/me",
        )
        _raise_for_status_verbose(response)
        return response.json()

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
        response = self._request(
            "GET",
            f"{self.multipass}/groups/{group_id}",
        )
        _raise_for_status_verbose(response)
        return response.json()

    def delete_group(self, group_id: str):
        """Deletes multipass group.

        Args:
            group_id (str): the group id to delete
        """
        response = self._request(
            "DELETE",
            f"{self.multipass}/administration/groups/{group_id}",
        )
        _raise_for_status_verbose(response)

    def create_third_party_application(
        self,
        client_type: str,
        display_name: str,
        description: str | None,
        grant_types: list,
        redirect_uris: list | None,
        logo_uri: str | None,
        organization_rid: str,
        allowed_organization_rids: list | None = None,
    ) -> dict:
        """Creates Foundry Third Party application (TPA).

        https://www.palantir.com/docs/foundry/platform-security-third-party/third-party-apps-overview/
        User must have 'Manage OAuth 2.0 clients' workflow permissions.

        Args:
            client_type (str): Server Application (CONFIDENTIAL) or
                Native or single-page application (PUBLIC)
            display_name (str): Display name of the TPA
            description (str | None): Long description of the TPA
            grant_types (list): Usually, ["AUTHORIZATION_CODE", "REFRESH_TOKEN"] (authorization code grant)
                or ["REFRESH_TOKEN", "CLIENT_CREDENTIALS"] (client credentials grant)
            redirect_uris (list | None): Redirect URLs of TPA, used in combination with AUTHORIZATION_CODE grant
            logo_uri (str | None): URI or embedded image 'data:image/png;base64,<...>'
            organization_rid (str): Parent Organization of this TPA
            allowed_organization_rids (list): Passing None or empty list means TPA is activated for all
                Foundry organizations

        Returns:
            dict:
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
        if client_type not in ["CONFIDENTIAL", "PUBLIC"]:
            raise AssertionError(
                f"client_type ({client_type}) needs to be one of CONFIDENTIAL or PUBLIC"
            )
        for grant in grant_types:
            if grant not in [
                "AUTHORIZATION_CODE",
                "REFRESH_TOKEN",
                "CLIENT_CREDENTIALS",
            ]:
                raise AssertionError(
                    f"grant ({grant}) needs to be one of AUTHORIZATION_CODE, REFRESH_TOKEN or CLIENT_CREDENTIALS"
                )
        if allowed_organization_rids is None:
            allowed_organization_rids = []
        response = self._request(
            "POST",
            f"{self.multipass}/clients",
            json={
                "allowedOrganizationRids": allowed_organization_rids,
                "clientType": client_type,
                "displayName": display_name,
                "description": description,
                "grantTypes": grant_types,
                "redirectUris": redirect_uris,
                "logoUri": logo_uri,
                "organizationRid": organization_rid,
            },
        )
        _raise_for_status_verbose(response)
        return response.json()

    def delete_third_party_application(self, client_id: str):
        """Deletes a Third Party Application.

        Args:
            client_id (str): The unique identifier of the TPA.
        """
        response = self._request(
            "DELETE",
            f"{self.multipass}/clients/{client_id}",
        )
        _raise_for_status_verbose(response)

    def update_third_party_application(
        self,
        client_id: str,
        client_type: str,
        display_name: str,
        description: str | None,
        grant_types: list,
        redirect_uris: list | None,
        logo_uri: str | None,
        organization_rid: str,
        allowed_organization_rids: list | None = None,
    ) -> dict:
        """Updates Foundry Third Party application (TPA).

        https://www.palantir.com/docs/foundry/platform-security-third-party/third-party-apps-overview/
        User must have 'Manage OAuth 2.0 clients' workflow permissions.

        Args:
            client_id (str): The unique identifier of the TPA.
            client_type (str): Server Application (CONFIDENTIAL) or
                Native or single-page application (PUBLIC)
            display_name (str): Display name of the TPA
            description (str): Long description of the TPA
            grant_types (list): Usually, ["AUTHORIZATION_CODE", "REFRESH_TOKEN"] (authorization code grant)
                or ["REFRESH_TOKEN", "CLIENT_CREDENTIALS"] (client credentials grant)
            redirect_uris (list): Redirect URLs of TPA, used in combination with AUTHORIZATION_CODE grant
            logo_uri (str): URI or embedded image 'data:image/png;base64,<...>'
            organization_rid (str): Parent Organization of this TPA
            allowed_organization_rids (list): Passing None or empty list means TPA is activated for all
                Foundry organizations

        Returns:
            dict:
                With the following structure

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
        if client_type not in ["CONFIDENTIAL", "PUBLIC"]:
            raise AssertionError(
                f"client_type ({client_type}) needs to be one of CONFIDENTIAL or PUBLIC"
            )
        for grant in grant_types:
            if grant not in [
                "AUTHORIZATION_CODE",
                "REFRESH_TOKEN",
                "CLIENT_CREDENTIALS",
            ]:
                raise AssertionError(
                    f"grant ({grant}) needs to be one of AUTHORIZATION_CODE, REFRESH_TOKEN or CLIENT_CREDENTIALS"
                )
        if allowed_organization_rids is None:
            allowed_organization_rids = []
        response = self._request(
            "PUT",
            f"{self.multipass}/clients/{client_id}",
            json={
                "allowedOrganizationRids": allowed_organization_rids,
                "clientType": client_type,
                "displayName": display_name,
                "description": description,
                "grantTypes": grant_types,
                "redirectUris": redirect_uris,
                "logoUri": logo_uri,
                "organizationRid": organization_rid,
            },
        )
        _raise_for_status_verbose(response)
        return response.json()

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
        response = self._request(
            "PUT",
            f"{self.multipass}/clients/{client_id}/rotateSecret",
        )
        _raise_for_status_verbose(response)
        return response.json()

    def enable_third_party_application(
        self,
        client_id: str,
        operations: list | None,
        resources: list | None,
        require_consent: bool = True,
    ) -> dict:
        """Enables Foundry Third Party application (TPA).

        Args:
            client_id (str): The unique identifier of the TPA.
            operations (list): Scopes that this TPA is allowed to use (To be confirmed)
                if None or empty list is passed, all scopes will be activated.
            resources (list): Compass Project RID's that this TPA is allowed to access,
                if None or empty list is passed, unrestricted access will be given.
            require_consent (bool): When enabled, users in the org will not be prompted
                to authorize the application themselves.

        Returns:
            dict:
                With the following structure

        .. code-block:: python

            {
                "client": {
                    "clientId": "<...>",
                    "organizationRid": "ri.multipass..organization.<...>",
                    "displayName": "<...>",
                    "description": None,
                    "logoUri": None,
                },
                "installation": {"resources": [], "operations": [], "markingIds": None, "requireConsent": True},
            }

        """
        if operations is None:
            operations = []
        if resources is None:
            resources = []
        response = self._request(
            "PUT",
            f"{self.multipass}/client-installations/{client_id}",
            json={
                "operations": operations,
                "resources": resources,
                "requireConsent": require_consent,
            },
        )
        _raise_for_status_verbose(response)
        return response.json()

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
        response = self._request(
            "POST",
            f"{self.jemma}/builds",
            json={
                "jobs": [
                    {
                        "name": "Checks",
                        "type": "exec",
                        "parameters": {
                            "repositoryTarget": {
                                "repositoryRid": repository_id,
                                "refName": ref_name,
                                "commitHash": commit_hash,
                            }
                        },
                        "reuseExistingJob": True,
                    },
                    {
                        "name": "Build initialization",
                        "type": "foundry-run-build",
                        "parameters": {
                            "fallbackBranches": [],
                            "filePaths": file_paths,
                            "rids": [],
                            "buildParameters": {},
                        },
                        "reuseExistingJob": True,
                    },
                ],
                "reuseExistingJobs": True,
            },
        )
        _raise_for_status_verbose(response)
        return response.json()

    def get_build(self, build_rid: str) -> dict:
        """Get information about the build.

        Args:
            build_rid (str): the build RID

        Returns:
            dict: the JSON API response
        """
        response = self._request(
            "GET",
            f"{self.builds2}/info/builds2/{build_rid}",
        )
        _raise_for_status_verbose(response)
        return response.json()

    def get_job_report(self, job_rid: str) -> dict:
        """Get the report for a job.

        Args:
            job_rid (str): the job RID

        Returns:
            dict: the job report response

        """
        response = self._request(
            "GET",
            f"{self.builds2}/info/jobs3/{job_rid}",
        )
        _raise_for_status_verbose(response)
        return response.json()

    def _execute_fsql_query(self, query: str, branch="master", timeout=600) -> dict:
        response = self._request(
            "POST",
            f"{self.foundry_sql_server_api}/queries/execute",
            json={
                "dialect": "SPARK",
                "fallbackBranchIds": [branch],
                "parameters": {
                    "type": "unnamedParameterValues",
                    "unnamedParameterValues": [],
                },
                "query": query,
                "serializationProtocol": "ARROW_V1",
                "timeout": timeout,
            },
        )
        _transform_bad_request_response_to_exception(response)
        _raise_for_status_verbose(response)
        return response.json()

    def _poll_fsql_query_status(self, initial_response_json: dict, timeout=600):
        start_time = time.time()
        query_id = initial_response_json["queryId"]
        response_json = initial_response_json
        while response_json["status"]["type"] == "running":
            response = self._request(
                "GET",
                f"{self.foundry_sql_server_api}/queries/{query_id}/status",
                json={},
            )
            _raise_for_status_verbose(response)
            response_json = response.json()
            if response_json["status"]["type"] == "failed":
                raise FoundrySqlQueryFailedError(response=response)
            if time.time() > start_time + timeout:
                raise FoundrySqlQueryClientTimedOutError(timeout)
            time.sleep(0.2)

    def _read_fsql_query_results_arrow(
        self, query_id: str
    ) -> pa.ipc.RecordBatchStreamReader:
        import pyarrow as pa

        headers = self._headers()
        headers["Accept"] = "application/octet-stream"
        headers["Content-Type"] = "application/json"
        # Couldn't get preload_content=False and gzip content-encoding to work together
        # If no bytesIO wrapper is used, response.read(1, cache_content=true)
        # does not return the first character but an empty byte (no idea why).
        # So it is essentially a trade-off between download time and memory consumption.
        # Download time seems to be a lot faster (2x) with gzip encoding turned on, while
        # memory consumption increases by the amount of raw bytes returned from the sql server.
        # I will optimize for faster downloads, for now. This decision can be revisited later.
        #
        # 01/2022: Moving to 'requests' instead of 'urllib3', did some experiments again
        # and noticed that preloading content is significantly faster than stream=True
        #
        # 10/2023: Use Slice of response content instead of bytesIO Wrapper
        #

        response = self._request(
            "GET",
            f"{self.foundry_sql_server_api}/queries/{query_id}/results",
            headers=headers,
        )

        arrow_format = chr(response.content[0])
        if arrow_format != "A":
            # Queries are direct read eligible when:
            # The dataset files are in a supported format.
            # The formats currently supported by direct read are Parquet, CSV, Avro, and Soho.

            # The query does not require SQL compute. Queries which contain aggregate, join, order by,
            # and filter predicates are not direct read eligible.

            # The query does not select from a column with a type that is ineligible for direct read.
            # Ineligible types are array, map, and struct.

            # May 2023: ARROW_V1 seems to consistently return ARROW format and not fallback to JSON.

            raise FoundrySqlSerializationFormatNotImplementedError()

        return pa.ipc.RecordBatchStreamReader(response.content[1:])

    def _query_fsql(
        self,
        query: str,
        branch: str = "master",
        return_type: SQLReturnType = SQLReturnType.PANDAS,
        timeout: int = 600,
    ) -> pd.core.frame.DataFrame | pa.Table | pyspark.sql.DataFrame:
        # if return_type is a str this will also work, this will get fixed in python 3.12
        # where we would be able to use `return_type not in SQLReturnType`
        if return_type not in SQLReturnType:
            raise ValueError(
                f"return_type ({return_type}) should be a member of foundry_dev_tools.foundry_api_client.SQLReturnType"
            )

        response_json = self._execute_fsql_query(query, branch=branch, timeout=timeout)
        query_id = response_json["queryId"]
        status = response_json["status"]

        if status != {"ready": {}, "type": "ready"}:
            self._poll_fsql_query_status(
                initial_response_json=response_json, timeout=timeout
            )

        arrow_stream_reader = self._read_fsql_query_results_arrow(query_id=query_id)
        if return_type == SQLReturnType.PANDAS:
            return arrow_stream_reader.read_pandas()
        if return_type == SQLReturnType.SPARK:
            from foundry_dev_tools.utils.converter.foundry_spark import (
                arrow_stream_to_spark_dataframe,
            )

            return arrow_stream_to_spark_dataframe(arrow_stream_reader)
        return arrow_stream_reader.read_all()

    def get_s3_credentials(self, expiration_duration: int = 3600) -> dict:
        """Returns s3 credentials for foundry.

        Credentials for the S3-compatible API for Foundry datasets:
        https://www.palantir.com/docs/foundry/data-integration/foundry-s3-api/index.html

        Recommended to use the client/resource methods directly, they also set the required endpoint_url:
        :py:attr:`foundry_dev_tools.foundry_api_client.FoundryRestClient.get_s3_client`
        :py:attr:`foundry_dev_tools.foundry_api_client.FoundryRestClient.get_s3_resource`

        Args:
            expiration_duration: seconds the credentials will be valid, seems to support values between 0 and 3600,
                3600 is the default
        """
        return parse_s3_credentials_response(
            self._requests_session.post(
                self._s3_url,
                params={
                    "Action": "AssumeRoleWithWebIdentity",
                    "WebIdentityToken": _get_auth_token(self._config),
                    "DurationSeconds": expiration_duration,
                },
            ).text
        )

    def get_s3fs_storage_options(self) -> dict:
        """Get the foundry s3 credentials in the s3fs storage_options format.

        Example:
            >>> fc = FoundryRestClient()
            >>> storage_options = fc.get_s3fs_storage_options()
            >>> df = pd.read_parquet("s3://ri.foundry.main.dataset.<uuid>/spark", storage_options=storage_options)
        """
        return {
            "session": self._get_aiobotocore_session(),
            "endpoint_url": self._s3_url,
        }

    def _get_boto3_session(self):
        """Returns the boto3 session with foundry s3 credentials applied.

        See :py:attr:`foundry_dev_tools.foundry_api_client.FoundryRestClient.get_s3_credentials`.
        """
        if self._boto3_session is None:
            import boto3
            import botocore.session

            from foundry_dev_tools.utils.s3 import CustomFoundryCredentialProvider

            session = botocore.session.Session()
            session.set_config_variable("region", "foundry")
            cred_provider = session.get_component("credential_provider")
            cred_provider.insert_before(
                "env", CustomFoundryCredentialProvider(self, session)
            )
            self._boto3_session = boto3.Session(botocore_session=session)
        return self._boto3_session

    def _get_aiobotocore_session(self):
        """Returns an aiobotocore session with foundry s3 credentials applied.

        See :py:attr:`foundry_dev_tools.foundry_api_client.FoundryRestClient.get_s3_credentials`.
        """
        if self._aiosession is None:
            import aiobotocore.session

            from foundry_dev_tools.utils.async_s3 import (
                CustomAsyncFoundryCredentialProvider,
            )

            session = aiobotocore.session.AioSession()
            session.set_config_variable("region", "foundry")
            cred_provider = session.get_component("credential_provider")
            cred_provider.insert_before(
                "env", CustomAsyncFoundryCredentialProvider(self, session)
            )
            self._aiosession = session
        return self._aiosession

    def get_boto3_s3_client(self, **kwargs):
        """Returns the boto3 s3 client with credentials applied and endpoint url set.

        See :py:attr:`foundry_dev_tools.foundry_api_client.FoundryRestClient.get_s3_credentials`.

        Example:
            >>> from foundry_dev_tools import FoundryRestClient
            >>> fc = FoundryRestClient()
            >>> s3_client = fc.get_boto3_s3_client()
            >>> s3_client
        Args:
            **kwargs: gets passed to :py:meth:`boto3.session.Session.client`, `endpoint_url` will be overwritten
        """
        kwargs["endpoint_url"] = self._s3_url
        return self._get_boto3_session().client("s3", **kwargs)

    def get_boto3_s3_resource(self, **kwargs):
        """Returns boto3 s3 resource with credentials applied and endpoint url set.

        Args:
            **kwargs: gets passed to :py:meth:`boto3.session.Session.resource`, `endpoint_url` will be overwritten
        """
        kwargs["endpoint_url"] = self._s3_url
        return self._get_boto3_session().resource("s3", **kwargs)


def _transform_bad_request_response_to_exception(response):
    if (
        response.status_code == requests.codes.bad
        and response.json()["errorName"] == "FoundrySqlServer:InvalidDatasetNoSchema"
    ):
        raise DatasetHasNoSchemaError("SQL")
    if (
        response.status_code == requests.codes.bad
        and response.json()["errorName"]
        == "FoundrySqlServer:InvalidDatasetCannotAccess"
    ):
        raise BranchNotFoundError(
            response.json()["parameters"]["datasetRid"],
            _extract_branch_from_sql_error(response),
        )
    if (
        response.status_code == requests.codes.bad
        and response.json()["errorName"]
        == "FoundrySqlServer:InvalidDatasetPathNotFound"
    ):
        raise DatasetNotFoundError(response.json()["parameters"]["path"])


def _extract_branch_from_sql_error(response):
    try:
        return (
            response.json()["parameters"]["userFriendlyMessage"]
            .split("[")[1]
            .replace("]", "")
        )
    except Exception as e:
        print(e, file=sys.stderr)
        return None


def _raise_for_status_verbose(response: requests.Response):
    """Executes response.raise_for_status but shows more info.

    Args:
        response (requests.Response): request response

    Raises:
        :external+requests:py:class:`~requests.exceptions.HTTPError`:
            but shows more info than normally
    """
    try:
        response.raise_for_status()
        if "Server-Timing" in response.headers:
            LOGGER.debug(
                "%s %s %s",
                response.request.method,
                response.url,
                response.headers["Server-Timing"],
            )
    except requests.exceptions.HTTPError as error:
        print(error, file=sys.stderr)
        print(error.response.text, file=sys.stderr)
        raise error


def _determine_requests_verify_value(config):
    if "requests_ca_bundle" in config:
        return config["requests_ca_bundle"]
    return True


def lru_with_ttl(*, ttl_seconds: int, maxsize: int = 128):
    """A decorator to apply LRU in-memory cache to a function with defined maximum(!) TTL in seconds.

    Be design an actual TTL may be shorter then the
    passed value (in rare randomized cases). But it can't be higher.

    Args:
        ttl_seconds (int): TTL for a cache record in seconds
        maxsize (int): Maximum size of the LRU cache (a functools.lru_cache argument)

    Returns:
        decorated:
    """

    def deco(wrapped_function):
        @functools.lru_cache(maxsize=maxsize)
        def cached_with_ttl(*args, ttl_hash, **kwargs):
            return wrapped_function(*args, **kwargs)

        def inner(*args, **kwargs):
            return cached_with_ttl(
                *args, ttl_hash=round(time.time() / ttl_seconds), **kwargs
            )

        return inner

    return deco


def _get_auth_token(config) -> str:
    if "jwt" in config and config["jwt"] is not None:
        return config["jwt"]
    if "client_id" in config and config["grant_type"] != "client_credentials":
        return _get_palantir_oauth_token(
            foundry_url=config["foundry_url"],
            client_id=config["client_id"],
            client_secret=config.get("client_secret"),
            scopes=config.get("scopes"),
        )
    if (
        "client_id" in config
        and "client_secret" in config
        and config["grant_type"] == "client_credentials"
    ):
        scopes = config.get("scopes")
        if isinstance(scopes, list):
            scopes = " ".join(scopes)
        return _get_oauth2_client_credentials_token(
            foundry_url=config["foundry_url"],
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            scopes=scopes,
            requests_verify_value=_determine_requests_verify_value(config),
        )["access_token"]
    raise ValueError(
        "Please provide at least one of: \n"
        "foundry token (config key: 'jwt') or "
        "Client Id (config key: 'client_id') \n"
        "in configuration."
    )


@lru_with_ttl(ttl_seconds=3600)
def _get_oauth2_client_credentials_token(
    foundry_url: str,
    client_id: str,
    client_secret: str,
    scopes: str,
    requests_verify_value: str | bool = True,
):
    """Function to interact with the multipass token endpoint to retrieve a token using the client_credentials flow.

    https://www.palantir.com/docs/foundry/platform-security-third-party/writing-oauth2-clients/#token-endpoint

    Args:
        foundry_url (str): The foundry url
        client_id (str): The unique identifier of the TPA.
        client_secret (str): The application's client secret that was issued during application registration.
        scopes (str): String of whitespace delimited scopes (e.g. 'api:read-data api:write-data') or None
            if user wants to request all scopes of the service user
        requests_verify_value (str | bool): Path to certificate bundle, or bool (True/False)

    Returns:
        dict:
            See example for client_credentials flow below

    .. code-block:: python

       {
            'access_token': '<...>',
            'refresh_token': None,
            'scope': None,
            'expires_in': 3600,
            'token_type': 'bearer'
       }

    """
    headers = {
        "User-Agent": requests.utils.default_user_agent(
            f"foundry-dev-tools/{__version__}/python-requests"
        ),
        "Authorization": "Basic "
        + base64.b64encode(bytes(client_id + ":" + client_secret, "ISO-8859-1")).decode(
            "ascii"
        ),
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = requests.request(
        "POST",
        f"{foundry_url}/multipass/api/oauth2/token",
        data={"grant_type": "client_credentials", "scope": scopes}
        if scopes
        else {"grant_type": "client_credentials"},
        headers=headers,
        verify=requests_verify_value,
        timeout=DEFAULT_REQUESTS_CONNECT_TIMEOUT,
    )
    _raise_for_status_verbose(response)
    return response.json()


@lru_with_ttl(ttl_seconds=3600)
def _get_palantir_oauth_token(
    foundry_url: str,
    client_id: str,
    client_secret: str | None = None,
    scopes: list | None = None,
) -> str:
    # Scopes are mandatory in authorization code grant
    if scopes is None:
        scopes = DEFAULT_TPA_SCOPES
    credentials = palantir_oauth_client.get_user_credentials(
        scopes=scopes,
        hostname=foundry_url,
        client_id=client_id,
        client_secret=client_secret,
        use_local_webserver=False,
    )

    return credentials.token


def parse_s3_credentials_response(requests_response_text):
    """Parses the AssumeRoleWithWebIdentity XML response."""
    return {
        "access_key": requests_response_text[
            requests_response_text.find("<AccessKeyId>")
            + len("<AccessKeyId>") : requests_response_text.rfind("</AccessKeyId>")
        ],
        "secret_key": requests_response_text[
            requests_response_text.find("<SecretAccessKey>")
            + len("<SecretAccessKey>") : requests_response_text.rfind(
                "</SecretAccessKey>"
            )
        ],
        "token": requests_response_text[
            requests_response_text.find("<SessionToken>")
            + len("<SessionToken>") : requests_response_text.rfind("</SessionToken>")
        ],
        "expiry_time": requests_response_text[
            requests_response_text.find("<Expiration>")
            + len("<Expiration>") : requests_response_text.rfind("</Expiration>")
        ],
    }


class FoundryDevToolsError(Exception):
    """Metaclass for :class:`FoundryAPIError` and :class:`foundry_dev_tools.fsspec_impl.FoundryFileSystemError`.

    Catch all foundry_dev_tools errors:

    >>> try:
    >>>     fun() # raise DatasetNotFoundError or any other
    >>> except FoundryDevToolsError:
    >>>     print("Some foundry_dev_tools error")

    """


class FoundryAPIError(FoundryDevToolsError):
    """Parent class for all foundry api errors.

    Some children of this Error can take arguments
    which can later be used in an except block e.g.:

    >>> try:
    >>>     abcd() # raises DatasetNotFoundError
    >>> except DatasetNotFoundError as e:
    >>>     print(e.dataset_rid)

    Also, all "child" errors can be catched with this parent class e.g.:

    >>> try:
    >>>     abcd() # could raise DatasetHasNoSchemaError or DatasetNotFoundError
    >>> except FoundryAPIError as e:
    >>>     print(e.dataset_rid)

    """


class DatasetHasNoSchemaError(FoundryAPIError):
    """Exception is thrown when dataset has no associated schema."""

    def __init__(
        self,
        dataset_rid: str,
        transaction_rid: str | None = None,
        branch: str | None = None,
        response: requests.Response | None = None,
    ):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset which has no schema
             transaction_rid (str | None): transaction_rid if available
             branch (str | None): dataset branch if available
             response (requests.Response | None): requests response if available
        """
        super().__init__(
            f"Dataset {dataset_rid} "
            + (
                f"on transaction {transaction_rid} "
                if transaction_rid is not None
                else ""
            )
            + (f"on branch {branch} " if branch is not None else "")
            + "has no schema.\n"
            + (response.text if response is not None else "")
        )
        self.dataset_rid = dataset_rid
        self.transaction_rid = transaction_rid
        self.branch = branch
        self.response = response


class BranchNotFoundError(FoundryAPIError):
    """Exception is thrown when no transaction exists for a dataset in a specific branch."""

    def __init__(
        self,
        dataset_rid: str,
        branch: str,
        transaction_rid: str | None = None,
        response: requests.Response | None = None,
    ):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset_rid for the branch that does not exist
             transaction_rid (str): transaction_rid if available
             branch (str | None): dataset branch
             response (requests.Response | None): requests response if available
        """
        super().__init__(
            f"Dataset {dataset_rid} "
            + (
                f"on transaction {transaction_rid}"
                if transaction_rid is not None
                else ""
            )
            + f"has no branch {branch}.\n"
            + (response.text if response is not None else "")
        )
        self.dataset_rid = dataset_rid
        self.branch = branch
        self.transaction_rid = transaction_rid
        self.response = response


class BranchesAlreadyExistError(FoundryAPIError):
    """Exception is thrown when branch exists already."""

    def __init__(
        self,
        dataset_rid: str,
        branch_rid: str,
        response: requests.Response | None = None,
    ):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset_rid for the branch that already exists
             branch_rid (str): dataset branch which already exists
             response (requests.Response | None): requests response if available
        """
        super().__init__(
            f"Branch {branch_rid} already exists in {dataset_rid}.\n"
            + (response.text if response is not None else "")
        )
        self.dataset_rid = dataset_rid
        self.branch_rid = branch_rid
        self.response = response


class DatasetNotFoundError(FoundryAPIError):
    """Exception is thrown when dataset does not exist."""

    def __init__(self, dataset_rid: str, response: requests.Response | None = None):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset which can't be found
             response (requests.Response | None): requests response if available
        """
        super().__init__(
            f"Dataset {dataset_rid} not found.\n"
            + (response.text if response is not None else "")
        )
        self.dataset_rid = dataset_rid
        self.response = response


class DatasetAlreadyExistsError(FoundryAPIError):
    """Exception is thrown when dataset already exists."""

    def __init__(self, dataset_path: str, dataset_rid: str):
        super().__init__(f"Dataset {dataset_path=} {dataset_rid=} already exists.")
        self.dataset_rid = dataset_rid
        self.dataset_path = dataset_path


class FolderNotFoundError(FoundryAPIError):
    """Exception is thrown when compass folder does not exist."""

    def __init__(self, folder_rid: str, response: requests.Response | None = None):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             folder_rid (str): folder_rid which can't be found
             response (requests.Response | None): requests response if available
        """
        super().__init__(
            f"Compass folder {folder_rid} not found; "
            "If you are sure your folder_rid is correct, "
            "and you have access, check if your jwt token "
            "is still valid!\n" + (response.text if response is not None else "")
        )
        self.folder_rid = folder_rid
        self.response = response


class DatasetHasNoTransactionsError(FoundryAPIError):
    """Exception is thrown when dataset has no transactions."""

    def __init__(self, dataset_rid: str, response: requests.Response | None = None):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset which has no transactions
             response (requests.Response | None): requests response if available
        """
        super().__init__(
            f"Dataset {dataset_rid} has no transactions.\n"
            + (response.text if response is not None else "")
        )
        self.dataset_rid = dataset_rid
        self.response = response


class DatasetNoReadAccessError(FoundryAPIError):
    """Exception is thrown when user is missing 'gatekeeper:view-resource' on the dataset, which normally comes with the Viewer role."""  # noqa: E501

    def __init__(self, dataset_rid: str, response: requests.Response | None = None):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset which can't be read
             response (requests.Response | None): requests response if available
        """
        super().__init__(
            f"No read access to dataset {dataset_rid}.\n"
            + (response.text if response is not None else "")
        )
        self.dataset_rid = dataset_rid
        self.response = response


class DatasetHasOpenTransactionError(FoundryAPIError):
    """Exception is thrown when dataset has an open transaction already."""

    def __init__(
        self,
        dataset_rid: str,
        open_transaction_rid: str,
        response: requests.Response | None = None,
    ):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset which has an open transaction
             open_transaction_rid (str): transaction_rid which is open
             response (requests.Response | None): requests response if available
        """
        super().__init__(
            f"Dataset {dataset_rid} already has open transaction {open_transaction_rid}.\n"
            + (response.text if response is not None else "")
        )
        self.dataset_rid = dataset_rid
        self.open_transaction_rid = open_transaction_rid
        self.response = response


class FoundrySqlQueryFailedError(FoundryAPIError):
    """Exception is thrown when SQL Query execution failed."""

    def __init__(self, response: requests.Response | None = None):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             response (requests.Response | None): requests response if available
        """
        super().__init__(
            "Foundry SQL Query Failed\n"
            + (response.text if response is not None else "")
        )
        self.response = response


class FoundrySqlQueryClientTimedOutError(FoundryAPIError):
    """Exception is thrown when the Query execution time exceeded the client timeout value."""

    def __init__(self, timeout: int):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             timeout (int): value of the reached timeout
        """
        super().__init__(f"The client timeout value of {timeout} has been reached")
        self.timeout = timeout


class FoundrySqlSerializationFormatNotImplementedError(FoundryAPIError):
    """Exception is thrown when the Query results are not sent in arrow ipc format."""

    def __init__(self):
        super().__init__(
            "Serialization formats other than arrow ipc not implemented in Foundry DevTools."
        )
