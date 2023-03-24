# pylint: disable=too-many-lines
"""Contains FoundryRestClient and FoundrySqlClient and exception classes.

One of the gaols of this module is to be self-contained so that it can be
dropped into any python installation with minimal dependency to 'requests'
Optional dependencies for the SQL functionality to work are pandas and pyarrow.

"""
import base64
import builtins
import functools
import io
import json
import logging
import multiprocessing
import os
import platform
import shutil
import tempfile
import time
import warnings
from contextlib import contextmanager
from itertools import repeat
from os import makedirs, path, remove, sep
from pathlib import Path
from typing import AnyStr, IO, Iterator, List, Optional, Tuple, Union
from urllib.parse import quote, quote_plus

import requests

import foundry_dev_tools
from foundry_dev_tools import __version__ as fdt_version

# On Python 3.8 on macOS, the default start method for new processes was
#  switched to 'spawn' by default due to 'fork' potentially causing crashes.
# These crashes haven't yet been observed with libE, but with 'spawn' runs,
#  warnings about leaked semaphore objects are displayed instead.
# The next several statements enforce 'fork' on macOS (Python 3.8)
if platform.system() == "Darwin":
    from multiprocessing import set_start_method

    set_start_method("fork", force=True)

LOGGER = logging.getLogger(__name__)


@contextmanager
def _poolcontext(*args, **kwargs):
    # pylint: disable=missing-function-docstring,consider-using-with
    pool = multiprocessing.Pool(*args, **kwargs)
    yield pool
    pool.terminate()


class FoundryRestClient:
    # pylint: disable=too-many-public-methods, too-many-instance-attributes
    """Zero dependency python client for Foundry's REST APIs."""

    def __init__(self, config: dict = None):
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
        self._config = foundry_dev_tools.Configuration.get_config(config)
        api_base = self._config["foundry_url"]
        self.catalog = f"{api_base}/foundry-catalog/api"
        self.compass = f"{api_base}/compass/api"
        self.metadata = f"{api_base}/foundry-metadata/api"
        self.data_proxy = f"{api_base}/foundry-data-proxy/api"
        self.schema_inference = f"{api_base}/foundry-schema-inference/api"
        self.multipass = f"{api_base}/multipass/api"
        self._api_base = api_base
        self._requests_verify_value = _determine_requests_verify_value(self._config)

    def _headers(self):
        return {
            "User-Agent": requests.utils.default_user_agent(
                f"Foundry DevTools/{fdt_version}/python-requests"
            ),
            "Content-Type": "application/json",
            "Authorization": f"Bearer {_get_auth_token(self._config)}",
        }

    def _verify(self):
        return self._requests_verify_value

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
        response = _request(
            "POST",
            f"{self.catalog}/catalog/datasets",
            headers=self._headers(),
            verify=self._verify(),
            json={"path": dataset_path},
        )
        if response.status_code == 400 and "DuplicateDatasetName" in response.text:
            raise DatasetAlreadyExistsError(dataset_path)
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
        response = _request(
            "GET",
            f"{self.catalog}/catalog/datasets/{dataset_rid}",
            headers=self._headers(),
            verify=self._verify(),
        )
        if response.status_code == 204 and response.text == "":
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
        response = _request(
            "DELETE",
            f"{self.catalog}/catalog/datasets",
            headers=self._headers(),
            verify=self._verify(),
            json={"rid": dataset_rid},
        )
        if response.status_code == 400 and "DatasetsNotFound" in response.text:
            raise DatasetNotFoundError(dataset_rid, response=response)
        _raise_for_status_verbose(response)
        self.move_resource_to_trash(resource_id=dataset_rid)

    def move_resource_to_trash(self, resource_id: str):
        """Moves a Compass resource (e.g. dataset or folder) to trash.

        Args:
            resource_id (str): rid of the resource

        """
        response_trash = _request(
            "POST",
            f"{self.compass}/batch/trash/add",
            headers=self._headers(),
            verify=self._verify(),
            data=f'["{resource_id}"]',
        )
        if response_trash.status_code != 204:
            raise KeyError(
                f"Issue while moving resource '{resource_id}' to foundry trash."
            )
        _raise_for_status_verbose(response_trash)

    def create_branch(
        self,
        dataset_rid: str,
        branch: str,
        parent_branch: str = None,
        parent_branch_id: str = None,
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
        response = _request(
            "POST",
            f"{self.catalog}/catalog/datasets/{dataset_rid}/"
            f"branchesUnrestricted2/{quote_plus(branch)}",
            headers=self._headers(),
            verify=self._verify(),
            json={"parentBranchId": parent_branch, "parentRef": parent_branch_id},
        )
        if response.status_code == 400 and "BranchesAlreadyExist" in response.text:
            raise BranchesAlreadyExistError(dataset_rid, branch, response=response)
        _raise_for_status_verbose(response)
        return response.json()

    def update_branch(
        self, dataset_rid: str, branch: str, parent_branch: str = None
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
        response = _request(
            "POST",
            f"{self.catalog}/catalog/datasets/{dataset_rid}/"
            f"branchesUpdate2/{quote_plus(branch)}",
            headers=self._headers(),
            verify=self._verify(),
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
        response = _request(
            "GET",
            f"{self.catalog}/catalog/datasets/{dataset_rid}/"
            f"branches2/{quote_plus(branch)}",
            headers=self._headers(),
            verify=self._verify(),
        )
        if response.status_code == 204 and response.text == "":
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
        response = _request(
            "POST",
            f"{self.catalog}/catalog/datasets/{dataset_rid}/" f"transactions",
            headers=self._headers(),
            verify=self._verify(),
            json={"branchId": f"{branch}", "record": {}},
        )
        if response.status_code == 400 and "BranchesNotFound" in response.text:
            raise BranchNotFoundError(dataset_rid, branch, response=response)
        if response.status_code == 400 and "InvalidArgument" in response.text:
            raise DatasetNotFoundError(dataset_rid, response=response)
        if (
            response.status_code == 400
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
            response_update = _request(
                "POST",
                f"{self.catalog}/catalog/datasets/{dataset_rid}/"
                f"transactions/{transaction_id}",
                headers=self._headers(),
                verify=self._verify(),
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
        response = _request(
            "POST",
            f"{self.catalog}/catalog/datasets/{dataset_rid}/"
            f"transactions/{transaction_id}/files/remove",
            headers=self._headers(),
            verify=self._verify(),
            params={"logicalPath": logical_path, "recursive": recursive},
        )
        _raise_for_status_verbose(response)

    def add_files_to_delete_transaction(
        self, dataset_rid: str, transaction_id: str, logical_paths: List[str]
    ):
        """Adds files in an open DELETE transaction.

        Files added to DELETE transactions affect
        the dataset view by removing files from the view.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            transaction_id (str): transaction rid
            logical_paths (List[str]): files in the dataset to delete

        """
        response = _request(
            "POST",
            f"{self.catalog}/catalog/datasets/{dataset_rid}/"
            f"transactions/{transaction_id}/files/addToDeleteTransaction",
            headers=self._headers(),
            verify=self._verify(),
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
        response = _request(
            "POST",
            f"{self.catalog}/catalog/datasets/{dataset_rid}/"
            f"transactions/{transaction_id}/commit",
            headers=self._headers(),
            verify=self._verify(),
            json={"record": {}},
        )
        _raise_for_status_verbose(response)
        if response.status_code != 204:
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
        response = _request(
            "POST",
            f"{self.catalog}/catalog/datasets/{dataset_rid}/"
            f"transactions/{transaction_id}/abortWithMetadata",
            headers=self._headers(),
            verify=self._verify(),
            json={"record": {}},
        )
        _raise_for_status_verbose(response)
        if response.status_code != 204:
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
        response = _request(
            "GET",
            f"{self.catalog}/catalog/datasets/"
            f"{dataset_rid}/reverse-transactions2/{quote_plus(branch)}",
            headers=self._headers(),
            verify=self._verify(),
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

    def get_dataset_last_transaction_rid(
        self, dataset_rid: str, branch: str = "master"
    ) -> Optional[str]:
        """Returns the last transaction of a dataset / branch combination.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            branch (str): Branch

        Returns:
            Optional[str]:
                last_transaction_rid as string or None if dataset has no transaction.

        """
        try:
            return self.get_dataset_transactions(dataset_rid, branch)[0]["rid"]
        except DatasetHasNoTransactionsError:
            return None

    def upload_dataset_file(
        self,
        dataset_rid: str,
        transaction_rid: str,
        path_or_buf: Union[str, Path, IO[AnyStr]],
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
            response = _request(
                "POST",
                f"{self.data_proxy}/dataproxy/datasets/{dataset_rid}/"
                f"transactions/{transaction_rid}/putFile",
                params={"logicalPath": path_in_foundry_dataset},
                data=file.read(),
                headers={
                    "Content-Type": "application/octet-stream",
                    "Authorization": self._headers()["Authorization"],
                },
            )
        builtins.open = original_open
        _raise_for_status_verbose(response)
        if response.status_code != 204:
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
        parallel_processes: int = None,
    ):
        """Uploads multiple local files to a foundry dataset.

        Args:
            dataset_rid (str): Unique identifier of the dataset
            transaction_rid (str): transaction id
            parallel_processes (Optional[int): Set number of threads for upload
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
            response = _request(
                "GET",
                f"{self.compass}/resources/{dataset_path_or_rid}",
                headers=self._headers(),
                verify=self._verify(),
                params={"decoration": "path"},
            )
        else:
            response = _request(
                "GET",
                f"{self.compass}/resources",
                headers=self._headers(),
                verify=self._verify(),
                params={"path": dataset_path_or_rid, "decoration": "path"},
            )
        _raise_for_status_verbose(response)
        if response.status_code != 200:
            raise DatasetNotFoundError(dataset_path_or_rid, response=response)
        as_json = response.json()
        if as_json["directlyTrashed"]:
            warnings.warn(f"Dataset '{dataset_path_or_rid}' is in foundry trash.")
        return as_json

    def get_child_objects_of_folder(
        self, folder_rid: str, page_size: int = None
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
            response = _request(
                "GET",
                f"{self.compass}/folders/{folder_rid}/children",
                headers=self._headers(),
                params=query_params,
                verify=self._verify(),
            )
            if response.status_code == 404:
                raise FolderNotFoundError(folder_rid, response=response)
            _raise_for_status_verbose(response)
            response_as_json = response.json()
            for value in response_as_json["values"]:
                yield value
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
        response = _request(
            "POST",
            f"{self.compass}/folders",
            headers=self._headers(),
            verify=self._verify(),
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
            response = _request(
                "POST",
                f"{self.compass}/batch/paths",
                headers=self._headers(),
                verify=self._verify(),
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
        response = _request(
            "GET",
            f"{self.metadata}/schemas/datasets/"
            f"{dataset_rid}/branches/{quote_plus(branch)}",
            params={"endTransactionRid": transaction_rid},
            headers=self._headers(),
            verify=self._verify(),
        )
        if response.status_code == 403:
            raise DatasetNotFoundError(dataset_rid, response=response)
        if response.status_code == 204:
            # here we don't know if schema does not exist or branch does not exist
            # we ask api for branch information, if branch does not exist, exception is thrown
            self.get_branch(dataset_rid, branch)
            raise DatasetHasNoSchemaError(dataset_rid, transaction_rid, branch)
        if response.status_code == 404 or (
            "errorName" in response.json()
            and response.json()["errorName"] == "Catalog:BranchesNotFound"
        ):
            raise BranchNotFoundError(dataset_rid, branch, response=response)
        if response.status_code != 200:
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
        response = _request(
            "POST",
            f"{self.metadata}/schemas/datasets/"
            f"{dataset_rid}/branches/{quote_plus(branch)}",
            params={"endTransactionRid": transaction_rid},
            json=schema,
            headers=self._headers(),
            verify=self._verify(),
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
        response = _request(
            "POST",
            f"{self.schema_inference}/datasets/"
            f"{dataset_rid}/branches/{quote_plus(branch)}/schema",
            json={},
            headers=self._headers(),
            verify=self._verify(),
        )
        _raise_for_status_verbose(response)
        parsed_response = response.json()
        if parsed_response["status"] == "SUCCESS":
            return parsed_response["data"]["foundrySchema"]
        if parsed_response["status"] == "WARN":
            warnings.warn(
                f"Foundry Schema inference completed with status "
                f"'{parsed_response['status']}' "
                f"and message '{parsed_response['message']}'.",
                UserWarning,
            )
            return parsed_response["data"]["foundrySchema"]
        raise ValueError(
            f"Foundry Schema inference failed with status "
            f"'{parsed_response['status']}' "
            f"and message '{parsed_response['message']}'."
        )

    def get_dataset_identity(
        self, dataset_path_or_rid: str, branch="master", check_read_access=True
    ) -> dict:
        """Returns the current identity of this dataset (dataset_path, dataset_rid, last_transaction_rid).

        Args:
            dataset_path_or_rid (str): Path to dataset (e.g. /Global/...)
                or rid of dataset (e.g. ri.foundry.main.dataset...)
            branch (str): branch of the dataset
            check_read_access (bool): default is True, checks if the user has read access ('gatekeeper:view-resource')
                to the dataset otherwise exception is thrown

        Returns:
            dict:
                with the keys 'dataset_path', 'dataset_rid' and 'last_transaction_rid'

        Raises:
            DatasetNoReadAccessError: if you have no read access for that dataset

        """
        dataset_details = self.get_dataset_details(dataset_path_or_rid)
        dataset_rid = dataset_details["rid"]
        dataset_path = dataset_details["path"]
        if check_read_access:
            if "gatekeeper:view-resource" not in dataset_details["operations"]:
                raise DatasetNoReadAccessError(dataset_rid)
        return {
            "dataset_path": dataset_path,
            "dataset_rid": dataset_rid,
            "last_transaction_rid": self.get_dataset_last_transaction_rid(
                dataset_rid, branch
            ),
        }

    def list_dataset_files(
        self,
        dataset_rid: str,
        exclude_hidden_files: bool = True,
        view: str = "master",
        logical_path: str = None,
        detail: bool = False,
        *,
        include_open_exclusive_transaction: bool = False,
    ) -> list:
        # pylint: disable=too-many-arguments
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
            response = _request(
                "GET",
                f"{self.catalog}/catalog/datasets/"
                f"{dataset_rid}/views2/{quote_plus(view)}/files",
                headers=self._headers(),
                verify=self._verify(),
                params={
                    "pageSize": 10000000,
                    "includeOpenExclusiveTransaction": include_open_exclusive_transaction,
                    "logicalPath": logical_path,
                    "excludeHiddenFiles": exclude_hidden_files,
                    "pageStartLogicalPath": next_page_token,
                },
            )
            _raise_for_status_verbose(response)
            if response.status_code != 200:
                raise DatasetNotFoundError(dataset_rid, response=response)
            response_json = response.json()
            return response_json

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
        response = _request(
            "GET",
            f"{self.catalog}/catalog/datasets/"
            f"{dataset_rid}/views/{quote_plus(view)}"
            f"/stats",
            headers=self._headers(),
            verify=self._verify(),
        )
        _raise_for_status_verbose(response)
        return response.json()

    def download_dataset_file(
        self,
        dataset_rid: str,
        output_directory: Optional[str],
        foundry_file_path: str,
        view: str = "master",
    ) -> Union[str, bytes]:
        """Downloads a single foundry dataset file into a directory.

        Creates sub folder if necessary.

        Args:
            dataset_rid (str): the dataset rid
            output_directory (Optional[str]): the local output directory for the file or None
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
        file_directory = sep.join(
            [output_directory, path.sep.join(foundry_file_path.split("/")[0:-1])]
        )
        makedirs(output_directory, exist_ok=True)
        if "/" in foundry_file_path:
            # check if file exists and is empty, in that case replace with folder
            # can happen with empty s3 keys
            if path.isfile(file_directory) and path.getsize(file_directory) == 0:
                remove(file_directory)
            makedirs(file_directory, exist_ok=True)
        file_name = foundry_file_path.split("/")[-1]
        local_file_path = os.path.normpath(sep.join([file_directory, file_name]))
        resp = self._download_dataset_file(
            dataset_rid, view, foundry_file_path, stream=True
        )
        if path.isdir(local_file_path):
            resp.close()
            return ""
        with open(local_file_path, "wb") as out_file:
            if resp.status_code != 200:
                raise ValueError(
                    f"Issue while downloading {dataset_rid}, "
                    f"file {foundry_file_path}, branch/transaction_rid {view}."
                    f"Response status was {resp.status_code}"
                )
            resp.raw.decode_content = True
            shutil.copyfileobj(resp.raw, out_file)

        resp.close()
        return local_file_path

    def _download_dataset_file(self, dataset_rid, view, foundry_file_path, stream=True):
        response = _request(
            "GET",
            f"{self.data_proxy}/dataproxy/datasets/"
            f"{dataset_rid}/views/{quote_plus(view)}/{quote(foundry_file_path)}",
            headers=self._headers(),
            stream=stream,
        )
        _raise_for_status_verbose(response)
        return response

    def download_dataset_files(
        self,
        dataset_rid: str,
        output_directory: str,
        files: list = None,
        view: str = "master",
        parallel_processes: int = None,
    ) -> List[str]:
        # pylint: disable=too-many-arguments, unused-argument
        """Downloads files of a dataset (in parallel) to a local output directory.

        Args:
            dataset_rid (str): the dataset rid
            files (Optional[list]): list of files or None, in which case all files are downloaded
            output_directory (str): the output directory for the files
                default value is calculated: multiprocessing.cpu_count() - 1
            view (str): branch or transaction rid of the dataset
            parallel_processes (Optional[int]): Set number of threads for upload

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
        files: list = None,
        view: str = "master",
        parallel_processes: Optional[int] = None,
    ) -> str:
        """Downloads all files of a dataset to a temporary directory.

        Which is deleted when the context is closed. Function returns the temporary directory.
        Example usage:

        >>> with client.download_dataset_files_temporary(dataset_rid='ri.foundry.main.dataset.1', view='master') as \
            temp_folder:
        >>>     # Read using Pandas
        >>>     df = pd.read_parquet(temp_folder)
        >>>     # Read using pyarrow, here we pass only the files, which are normally in subfolder 'spark'
        >>>     from pathlib import Path
        >>>     pq = parquet.ParquetDataset(path_or_paths=[x for x in Path(temp_dir).glob('**/*') if x.is_file()])

        Args:
            dataset_rid (str): the dataset rid
            files (List[str]): list of files or None, in which case all files are downloaded
            view (str): branch or transaction rid of the dataset
            parallel_processes (int): Set number of threads for download

        Returns:
            str:
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
        response = _request(
            "GET",
            f"{self.data_proxy}/dataproxy/datasets/"
            f"{dataset_rid}/branches/{quote_plus(branch)}/csv",
            headers=self._headers(),
            verify=self._verify(),
            params={"includeColumnNames": True, "includeBom": True},
            stream=True,
        )

        _raise_for_status_verbose(response)
        return response

    def query_foundry_sql_legacy(
        self, query: str, branch: str = "master"
    ) -> (dict, list):
        """Queries the dataproxy query API with spark SQL.

        Example:
            client.query_foundry_sql_legacy(query="SELECT * FROM `/Global/Foundry Operations/Foundry Support/iris`",
                                            branch="master")

        Args:
            query (str): the sql query
            branch (str): the branch of the dataset / query

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
            DatasetHasNoSchemaError: if dataset has no schema
            BranchNotFoundError: if branch was not found
        """
        response = _request(
            "POST",
            f"{self.data_proxy}/dataproxy/queryWithFallbacks",
            headers=self._headers(),
            verify=self._verify(),
            params={"fallbackBranchIds": [branch]},
            json={"query": query},
        )

        if (
            response.status_code == 404
            and response.json()["errorName"] == "DataProxy:SchemaNotFound"
        ):
            dataset_rid = response.json()["parameters"]["datasetRid"]
            raise DatasetHasNoSchemaError(dataset_rid, branch=branch)
        if (
            response.status_code == 400
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
        return response_json["foundrySchema"], response_json["rows"]

    def query_foundry_sql(
        self, query, branch="master", return_type="pandas"
    ) -> "pandas.core.frame.DataFrame | pyarrow.Table | pyspark.sql.DataFrame":
        # pylint: disable=line-too-long
        """Queries the Foundry SQL server with spark SQL dialect.

        Uses Arrow IPC to communicate with the Foundry SQL Server Endpoint.
        Example:
            client.query_foundry_sql("SELECT * FROM `/Global/Foundry Operations/Foundry Support/iris`")

        Args:
            query (str): the sql query
            branch (str): the branch of the dataset / query
            return_type (str, pandas | arrow | spark): Whether to return
                :external+pandas:py:class:`pandas.DataFrame`, :external+pyarrow:py:class:`pyarrow.Table` or
                :external+spark:py:class:`~pyspark.sql.DataFrame`

        Returns:
            :external+pandas:py:class:`~pandas.DataFrame` | :external+pyarrow:py:class:`~pyarrow.Table` | :external+spark:py:class:`~pyspark.sql.DataFrame`:

            A pandas DataFrame, Spark DataFrame or pyarrow.Table with the result.

        Raises:
            ValueError: Only direct read eligible queries can be returned as arrow Table.

        """
        assert return_type in {"pandas", "arrow", "spark"}
        _assert_pyarrow_packages_available()
        _assert_pandas_packages_available()
        foundry_sql_client = FoundrySqlClient(config=self._config, branch=branch)
        try:
            return foundry_sql_client.query(query=query, return_type=return_type)
        except FoundrySqlSerializationFormatNotImplementedError as exc:
            if return_type == "arrow":
                raise ValueError(
                    "Only direct read eligible queries can be returned as arrow Table. "
                    "Consider using setting return_type to 'pandas'."
                ) from exc
            foundry_schema, data = self.query_foundry_sql_legacy(query, branch)

            if return_type == "pandas":
                # pylint: disable=import-outside-toplevel
                import pandas as pd

                return pd.DataFrame(
                    data=data,
                    columns=[e["name"] for e in foundry_schema["fieldSchemaList"]],
                )
            # pylint: disable=import-outside-toplevel
            from foundry_dev_tools.utils.converter.foundry_spark import (
                foundry_schema_to_spark_schema,
                foundry_sql_data_to_spark_dataframe,
            )

            return foundry_sql_data_to_spark_dataframe(
                data=data,
                spark_schema=foundry_schema_to_spark_schema(foundry_schema),
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
        response = _request(
            "GET",
            f"{self.multipass}/me",
            headers=self._headers(),
            verify=self._verify(),
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
        response = _request(
            "GET",
            f"{self.multipass}/groups/{group_id}",
            headers=self._headers(),
            verify=self._verify(),
        )
        _raise_for_status_verbose(response)
        return response.json()

    def delete_group(self, group_id: str):
        """Deletes multipass group.

        Args:
            group_id (str): the group id to delete
        """
        response = _request(
            "DELETE",
            f"{self.multipass}/administration/groups/{group_id}",
            headers=self._headers(),
            verify=self._verify(),
        )
        _raise_for_status_verbose(response)

    def create_third_party_application(
        self,
        client_type: str,
        display_name: str,
        description: "str | None",
        grant_types: list,
        redirect_uris: "list | None",
        logo_uri: "str | None",
        organization_rid: str,
        allowed_organization_rids: list = None,
    ) -> dict:
        # pylint: disable=too-many-arguments
        """Creates Foundry Third Party application (TPA).

        https://www.palantir.com/docs/foundry/platform-security-third-party/third-party-apps-overview/
        User must have 'Manage OAuth 2.0 clients' workflow permissions.

        Args:
            client_type (str): Server Application (CONFIDENTIAL) or
                Native or single-page application (PUBLIC)
            display_name (str): Display name of the TPA
            description (Optional[str]): Long description of the TPA
            grant_types (list): Usually, ["AUTHORIZATION_CODE", "REFRESH_TOKEN"] (authorization code grant)
                or ["REFRESH_TOKEN", "CLIENT_CREDENTIALS"] (client credentials grant)
            redirect_uris (Optional[list]): Redirect URLs of TPA, used in combination with AUTHORIZATION_CODE grant
            logo_uri (Optional[str]): URI or embedded image 'data:image/png;base64,<...>'
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
        assert client_type in ["CONFIDENTIAL", "PUBLIC"]
        for grant in grant_types:
            assert grant in [
                "AUTHORIZATION_CODE",
                "REFRESH_TOKEN",
                "CLIENT_CREDENTIALS",
            ]
        if allowed_organization_rids is None:
            allowed_organization_rids = []
        response = _request(
            "POST",
            f"{self.multipass}/clients",
            headers=self._headers(),
            verify=self._verify(),
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
        response = _request(
            "DELETE",
            f"{self.multipass}/clients/{client_id}",
            headers=self._headers(),
            verify=self._verify(),
        )
        _raise_for_status_verbose(response)

    def update_third_party_application(
        self,
        client_id: str,
        client_type: str,
        display_name: str,
        description: "str | None",
        grant_types: list,
        redirect_uris: "list | None",
        logo_uri: "str | None",
        organization_rid: str,
        allowed_organization_rids: list = None,
    ) -> dict:
        # pylint: disable=too-many-arguments
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
        assert client_type in ["CONFIDENTIAL", "PUBLIC"]
        for grant in grant_types:
            assert grant in [
                "AUTHORIZATION_CODE",
                "REFRESH_TOKEN",
                "CLIENT_CREDENTIALS",
            ]
        if allowed_organization_rids is None:
            allowed_organization_rids = []
        response = _request(
            "PUT",
            f"{self.multipass}/clients/{client_id}",
            headers=self._headers(),
            verify=self._verify(),
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
        response = _request(
            "PUT",
            f"{self.multipass}/clients/{client_id}/rotateSecret",
            headers=self._headers(),
            verify=self._verify(),
        )
        _raise_for_status_verbose(response)
        return response.json()

    def enable_third_party_application(
        self, client_id: str, operations: "list | None", resources: "list | None"
    ) -> dict:
        """Enables Foundry Third Party application (TPA).

        Args:
            client_id (str): The unique identifier of the TPA.
            operations (list): Scopes that this TPA is allowed to use (To be confirmed)
                if None or empty list is passed, all scopes will be activated.
            resources (list): Compass Project RID's that this TPA is allowed to access,
                if None or empty list is passed, unrestricted access will be given.

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
                "installation": {"resources": [], "operations": [], "markingIds": None},
            }

        """
        if operations is None:
            operations = []
        if resources is None:
            resources = []
        response = _request(
            "PUT",
            f"{self.multipass}/client-installations/{client_id}",
            headers=self._headers(),
            verify=self._verify(),
            json={"operations": operations, "resources": resources},
        )
        _raise_for_status_verbose(response)
        return response.json()


class FoundrySqlClient:
    # pylint: disable=too-few-public-methods
    """Class to interact with Foundry's SQL Server using ARROW IPC Protocol."""

    def __init__(self, config: dict = None, branch="master"):
        """Construct an instance of FoundrySqlClient.

        Args:
            config (dict): configuration dictionary, equivalent to FoundryRestClient
            branch (str):  default = master, all queries will be executed against this default branch

        """
        _assert_pyarrow_packages_available()
        self._config = foundry_dev_tools.Configuration.get_config(config)
        self._requests_verify_value = _determine_requests_verify_value(self._config)
        self.foundry_sql_server_api = (
            f"{self._config['foundry_url']}/foundry-sql-server/api"
        )
        self._headers = self._get_initial_headers(session_authorization=None)
        self.branch = branch
        self.session_id, self.session_auth_token = self._establish_session(
            branch=branch
        )
        self._headers = self._get_initial_headers(
            session_authorization=self.session_auth_token
        )

    def _get_initial_headers(self, session_authorization=None):
        return {
            "User-Agent": requests.utils.default_user_agent(
                f"Foundry DevTools/{fdt_version}/python-requests"
            ),
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate, br",
            "Content-Type": "application/json",
            "Session-Authorization": session_authorization,
            "Authorization": f"Bearer {_get_auth_token(self._config)}",
        }

    def _establish_session(self, branch="master") -> Tuple[str, str]:
        response = _request(
            "POST",
            f"{self.foundry_sql_server_api}/sessions",
            headers=self._headers,
            verify=self._requests_verify_value,
            json={
                "configuration": {
                    "fallbackBranchIds": [branch],
                    "queryTimeoutMillis": None,
                },
                "protocolVersion": None,
            },
        )
        _raise_for_status_verbose(response)
        response_json = response.json()
        session_id = response_json["sessionId"]
        session_auth_token = response_json["sessionAuthToken"]
        return session_id, session_auth_token

    def _prepare_and_execute(self, query: str) -> dict:
        response = _request(
            "POST",
            f"{self.foundry_sql_server_api}/sessions/"
            f"{self.session_id}/prepare-and-execute-statement",
            headers=self._headers,
            verify=self._requests_verify_value,
            json={
                "dialect": "SPARK",
                "parameters": {
                    "type": "unnamedParameterValues",
                    "unnamedParameterValues": [],
                },
                "query": query,
                "serializationProtocol": "ARROW",
            },
        )
        _transform_bad_request_response_to_exception(response)
        _raise_for_status_verbose(response)
        return response.json()

    def _poll_for_query_completion(self, initial_response_json: dict, timeout=600):
        start_time = time.time()
        statement_id = initial_response_json["statementId"]
        response_json = initial_response_json
        while response_json["status"]["type"] == "running":
            response = _request(
                "POST",
                f"{self.foundry_sql_server_api}/" f"statements/{statement_id}/status",
                headers=self._headers,
                verify=self._requests_verify_value,
                json={},
            )
            _raise_for_status_verbose(response)
            response_json = response.json()
            if response_json["status"]["type"] == "failed":
                raise FoundrySqlQueryFailedError(response=response)
            if time.time() > start_time + timeout:
                raise FoundrySqlQueryClientTimedOutError(timeout)
            time.sleep(0.2)

    def _read_results_arrow(
        self, statement_id: str
    ) -> "pyarrow.ipc.RecordBatchStreamReader":
        headers = self._headers
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

        response = _request(
            "POST",
            f"{self.foundry_sql_server_api}/" f"statements/{statement_id}/results",
            data=json.dumps({}).encode("utf-8"),
            headers=headers,
        )
        bytes_io = io.BytesIO(response.content)
        arrow_format = bytes_io.read(1).decode("UTF-8")
        if arrow_format != "A":
            # Queries are direct read eligible when:
            # The dataset files are in a supported format.
            # The formats currently supported by direct read are Parquet, CSV, Avro, and Soho.

            # The query does not require SQL compute. Queries which contain aggregate, join, order by,
            # and filter predicates are not direct read eligible.

            # The query does not select from a column with a type that is ineligible for direct read.
            # Ineligible types are array, map, and struct.

            raise FoundrySqlSerializationFormatNotImplementedError()
        # pylint: disable=import-outside-toplevel
        import pyarrow as pa

        return pa.ipc.RecordBatchStreamReader(bytes_io)

    def query(
        self, query: str, timeout=600, return_type="pandas"
    ) -> "pandas.core.frame.DataFrame | pyarrow.Table | pyspark.sql.DataFrame":
        """Queries the foundry sql endpoint. Current dialect is hard-coded to SPARK.

        Example Queries:

        .. code-block:: python

         # direct read if dataset is backed by parquet files, no size limit
         df1 = client.query(f'SELECT * FROM `{dataset_path}`')

         query = ("SELECT col1 FROM `{start_transaction_rid}:{end_transaction_rid}@{branch}`.`{dataset_path_or_rid}`"
                  "WHERE filterColumns = 'value1' LIMIT 1")
         df2 = client.query(query)

        Args:
            query (str): Query
            timeout (float): default value 600 seconds
            return_type (str: pandas | arrow | spark): return type, one of pandas, arrow or spark.
                Whether to return :external+pandas:py:class:`~pandas.DataFrame`,
                :external+spark:py:class:`~pyspark.sql.DataFrame`
                or :external+pyarrow:py:class:`~pyarrow.Table`

        Returns:
             :external+pandas:py:class:`~pandas.DataFrame` | :external+spark:py:class:`~pyspark.sql.DataFrame` |
                :external+pyarrow:py:class:`~pyarrow.Table`:
                A pandas dataframe, pyspark dataframe or pyarrow Table with the result



        """
        assert return_type in {"pandas", "arrow", "spark"}

        response_json = self._prepare_and_execute(query)
        statement_id = response_json["statementId"]

        self._poll_for_query_completion(
            initial_response_json=response_json, timeout=timeout
        )
        arrow_stream_reader = self._read_results_arrow(statement_id)
        if return_type == "pandas":
            return arrow_stream_reader.read_pandas()
        if return_type == "spark":
            # pylint: disable=import-outside-toplevel
            from foundry_dev_tools.utils.converter.foundry_spark import (
                arrow_stream_to_spark_dataframe,
            )

            return arrow_stream_to_spark_dataframe(arrow_stream_reader)
        return arrow_stream_reader.read_all()


def _transform_bad_request_response_to_exception(response):
    if (
        response.status_code == 400
        and response.json()["errorName"] == "FoundrySqlServer:InvalidDatasetNoSchema"
    ):
        raise DatasetHasNoSchemaError("SQL")
    if (
        response.status_code == 400
        and response.json()["errorName"]
        == "FoundrySqlServer:InvalidDatasetCannotAccess"
    ):
        raise BranchNotFoundError("SQL", "SQL")
    if (
        response.status_code == 400
        and response.json()["errorName"]
        == "FoundrySqlServer:InvalidDatasetPathNotFound"
    ):
        raise DatasetNotFoundError("SQL")


def _assert_pyarrow_packages_available():
    try:
        # pylint: disable=import-outside-toplevel,unused-import
        import pyarrow
    except ImportError as err:
        raise ValueError(
            "Please install package 'pyarrow' to use SQL functionality."
        ) from err


def _assert_pandas_packages_available():
    try:
        # pylint: disable=import-outside-toplevel,unused-import
        import pandas as pd
    except ImportError as err:
        raise ValueError(
            "Please install package 'pandas' to use SQL functionality."
        ) from err


def _is_palantir_oauth_client_installed():
    try:
        # pylint: disable=import-outside-toplevel,unused-import,import-error
        import palantir_oauth_client

        return palantir_oauth_client
    except ImportError:
        try:
            # pylint: disable=import-outside-toplevel,import-error
            import pyfoundry_auth

            warnings.warn(
                "\nPyFoundry Auth has been renamed to Palantir Oauth Client\n"
                "Please uninstall pyfoundry-auth and install palantir-oauth-client\n",
                DeprecationWarning,
            )
            return pyfoundry_auth
        except ImportError:
            return False


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
        print(error)
        print(error.response.text)
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
        # pylint: disable=unused-argument
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
            client_secret=config["client_secret"]
            if "client_secret" in config
            else None,
        )
    if (
        "client_id" in config
        and "client_secret" in config
        and config["grant_type"] == "client_credentials"
    ):
        scopes = config["scopes"]
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
    scopes: "str",
    requests_verify_value: "str | bool" = True,
):
    """Function to interact with the multipass token endpoint to retrieve a token using the client_credentials flow.

    https://www.palantir.com/docs/foundry/platform-security-third-party/writing-oauth2-clients/#token-endpoint

    Args:
        foundry_url (str): The foundry url
        client_id (str): The unique identifier of the TPA.
        client_secret (str): The application's client secret that was issued during application registration.
        scopes (str): String of whitespace delimited scopes (e.g. 'api:read-data api:write-data')
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
            f"Foundry DevTools/{fdt_version}/python-requests"
        ),
        "Authorization": "Basic "
        + base64.b64encode(bytes(client_id + ":" + client_secret, "ISO-8859-1")).decode(
            "ascii"
        ),
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = _request(
        "POST",
        f"{foundry_url}/multipass/api/oauth2/token",
        data={"grant_type": "client_credentials", "scope": scopes},
        headers=headers,
        verify=requests_verify_value,
    )
    _raise_for_status_verbose(response)
    return response.json()


@lru_with_ttl(ttl_seconds=3600)
def _get_palantir_oauth_token(
    foundry_url: str, client_id: str, client_secret: str = None
) -> str:
    if oauth_provider := _is_palantir_oauth_client_installed():

        credentials = oauth_provider.get_user_credentials(
            scopes=[
                "offline_access",
                "compass:view",
                "compass:edit",
                "compass:discover",
                "api:write-data",
                "api:read-data",
            ],
            hostname=foundry_url,
            client_id=client_id,
            client_secret=client_secret,
            use_local_webserver=False,
        )

    else:
        raise ValueError(
            "You provided a 'client_id' for Foundry SSO but "
            "palantir-oauth-client is not installed.\n"
        )

    return credentials.token


def _request(*args, **kwargs):
    kwargs["timeout"] = 599
    # pylint: disable=missing-timeout
    return requests.request(*args, **kwargs)


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
        transaction_rid: Optional[str] = None,
        branch: Optional[str] = None,
        response: Optional[requests.Response] = None,
    ):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset which has no schema
             transaction_rid (Optional[str]): transaction_rid if available
             branch (Optional[str]): dataset branch if available
             response (Optional[requests.Response]): requests response if available
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
        transaction_rid: Optional[str] = None,
        response: Optional[requests.Response] = None,
    ):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset_rid for the branch that does not exist
             transaction_rid (str): transaction_rid if available
             branch (Optional[str]): dataset branch
             response (Optional[requests.Response]): requests response if available
        """
        super().__init__(
            f"Dataset {dataset_rid} "
            + (
                f"on transaction {transaction_rid}"
                if transaction_rid is not None
                else ""
            )
            + " has no branch {branch}.\n"
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
        response: Optional[requests.Response] = None,
    ):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset_rid for the branch that already exists
             branch_rid (str): dataset branch which already exists
             response (Optional[requests.Response]): requests response if available
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

    def __init__(self, dataset_rid: str, response: Optional[requests.Response] = None):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset which can't be found
             response (Optional[requests.Response]): requests response if available
        """
        super().__init__(
            f"Dataset {dataset_rid} not found.\n"
            + (response.text if response is not None else "")
        )
        self.dataset_rid = dataset_rid
        self.response = response


class DatasetAlreadyExistsError(FoundryAPIError):
    """Exception is thrown when dataset already exists."""

    def __init__(self, dataset_rid: str):
        super().__init__(f"Dataset {dataset_rid} already exists.")


class FolderNotFoundError(FoundryAPIError):
    """Exception is thrown when compass folder does not exist."""

    def __init__(self, folder_rid: str, response: Optional[requests.Response] = None):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             folder_rid (str): folder_rid which can't be found
             response (Optional[requests.Response]): requests response if available
        """
        super().__init__(
            f"Compass folder {folder_rid} not found; "
            f"If you are sure your folder_rid is correct, "
            f"and you have access, check if your jwt token "
            f"is still valid!\n" + (response.text if response is not None else "")
        )
        self.folder_rid = folder_rid
        self.response = response


class DatasetHasNoTransactionsError(FoundryAPIError):
    """Exception is thrown when dataset has no transactions."""

    def __init__(self, dataset_rid: str, response: Optional[requests.Response] = None):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset which has no transactions
             response (Optional[requests.Response]): requests response if available
        """
        super().__init__(
            f"Dataset {dataset_rid} has no transactions.\n"
            + (response.text if response is not None else "")
        )
        self.dataset_rid = dataset_rid
        self.response = response


class DatasetNoReadAccessError(FoundryAPIError):
    """Exception is thrown when user is missing 'gatekeeper:view-resource' on the dataset,
    which normally comes with the Viewer role."""

    def __init__(self, dataset_rid: str, response: Optional[requests.Response] = None):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset which can't be read
             response (Optional[requests.Response]): requests response if available
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
        response: Optional[requests.Response] = None,
    ):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             dataset_rid (str): dataset which has an open transaction
             open_transaction_rid (str): transaction_rid which is open
             response (Optional[requests.Response]): requests response if available
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

    def __init__(self, response: Optional[requests.Response] = None):
        """Pass parameters to constructor for later use and uniform error messages.

        Args:
             response (Optional[requests.Response]): requests response if available
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
        super().__init__(f"The client timeout value of {timeout} has " f"been reached")
        self.timeout = timeout


class FoundrySqlSerializationFormatNotImplementedError(FoundryAPIError):
    """Exception is thrown when the Query results are not sent in arrow ipc format."""

    def __init__(self):
        super().__init__(
            "Serialization formats "
            "other than arrow ipc "
            "not implemented in "
            "Foundry DevTools."
        )
