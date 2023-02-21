"""Module contains the fsspec implementation for Palantir Foundry."""
import sys
from datetime import datetime
from typing import Optional, Union

import backoff
from fs.opener.parse import parse_fs_url
from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile

from . import FoundryRestClient
from .foundry_api_client import DatasetHasOpenTransactionError, FoundryDevToolsError

DEFAULT_BRANCH = "master"


class FoundryFileSystem(AbstractFileSystem):  # noqa
    # pylint:disable=abstract-method
    """fsspec implementation for the Palantir Foundry Filesystem.

    Args:
        dataset (str): dataset_rid or dataset path
        branch (str): dataset branch. Defaults to 'master'.
        token (str): explicitely pass token. By default, FoundryFileSystem gets the token from the configuration file
            or using SSO.
        transaction_backoff (bool): if FoundryFileSystem should retry to open a transaction, in case one is already
            open. The default value is 'True' and FoundryFileSystem will retry 60 seconds to open a transaction.
        **kwargs: passed to underlying fsspec.AbstractFileSystem
    """

    protocol = "foundry"

    def __init__(
        self,
        dataset: str,
        branch: str = DEFAULT_BRANCH,
        token: str = None,
        transaction_backoff: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.token = token
        self.dataset_identity = self.api.get_dataset_identity(
            dataset_path_or_rid=dataset, branch=branch
        )
        self.dataset_identity["branch"] = branch
        self.transaction_rid = None
        self.transaction_backoff = transaction_backoff

    @property
    def api(self) -> FoundryRestClient:
        """We are creating a new instance of the client everytime it is used.

        Because it might be used in a request or session context in the AppService where
        every request/session has a token provided by a request header.

        Returns:
            FoundryRestClient:
                newly created api client

        """
        if self.token:
            return FoundryRestClient({"jwt": self.token})
        return FoundryRestClient()

    @classmethod
    def _strip_protocol(cls, path):
        """Return only subpath from foundry://<branch>:<token>@<dataset-rid>/<subpath e.g. test.txt>."""
        if "foundry://" in path:
            no_foundry = path.lstrip("foundry").lstrip("://")
            if "/" in no_foundry:
                return no_foundry[
                    no_foundry.index("/") + 1 :
                ]  # everything after dataset_rid/ is the path
            return ""  # no subpath passed
        return path

    @staticmethod
    def _get_kwargs_from_urls(path):
        parsed = parse_fs_url(path)
        return_kwargs = {"dataset": parsed.resource.split("/")[0]}
        if "ri.foundry.main.dataset" not in return_kwargs["dataset"]:
            raise FoundryDatasetPathInUrlNotSupportedError()
        potential_token = parsed.password if parsed.password != "" else None
        if potential_token:
            return_kwargs["token"] = potential_token
        potential_branch = (
            parsed.username if bool(parsed.username and parsed.username != "") else None
        )
        if potential_branch:
            return_kwargs["branch"] = potential_branch
        return return_kwargs

    def ls(self, path: str, detail: bool = True, **kwargs):
        """List files in the path specified.

        Args:
            path (str): path you want to list
            detail (bool):
                returns the complete catalog api response, see also
                :py:meth:`~foundry_dev_tools.foundry_api_client.FoundryRestClient.list_dataset_files`
            exclude_hidden_files (bool): to list also hidden files, set it to false

        Returns:
            list:
                list of files in `path`
        """
        exclude_hidden_files = kwargs.pop("exclude_hidden_files", True)
        if path in (self.dataset_identity["dataset_path"], "", "**/**", "/."):
            # root
            path = None
        if not self.dataset_identity[
            "last_transaction_rid"
        ]:  # empty dataset, no transaction
            raise FileNotFoundError(path)
        file_details = self.api.list_dataset_files(
            dataset_rid=self.dataset_identity["dataset_rid"],
            exclude_hidden_files=exclude_hidden_files,
            logical_path=path,
            view=self.dataset_identity["branch"],
            detail=True,
            include_open_exclusive_transaction=self._intrans,
        )
        if (
            len(file_details) == 1
            and file_details[0]["fileMetadata"]["length"] == 0
            and path is not None
        ):
            file_details = self.api.list_dataset_files(
                dataset_rid=self.dataset_identity["dataset_rid"],
                exclude_hidden_files=exclude_hidden_files,
                logical_path=path + "/",
                view=self.dataset_identity["branch"],
                detail=True,
                include_open_exclusive_transaction=self._intrans,
            )
        file_details_corrected = _correct_directories(
            file_details, subfolder_prefix=path
        )
        if detail:
            return file_details_corrected
        return [file["name"] for file in file_details_corrected]

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        # pylint: disable=too-many-arguments
        if "cache_type" in kwargs:
            kwargs.pop("cache_type")
        if self._intrans:
            autocommit = False
        return FoundryFile(
            self, path, mode=mode, autocommit=autocommit, cache_type="all", **kwargs
        )

    def modified(self, path):
        """Return the modified timestamp of a file as a datetime.datetime."""
        return datetime.fromisoformat(
            self.info(path)["time_modified"].replace("Z", "+00:00")
        )

    def rm(
        self,
        path: Union[str, list],
        recursive: bool = False,
        maxdepth: Optional[int] = None,
    ):
        """Delete files.

        Args:
            path (Union[str,list]):
                File(s) to delete.
            recursive (bool):
                If file(s) are directories, recursively delete contents and then
                also remove the directory
            maxdepth (Optional[int]):
                Depth to pass to walk for finding files to delete, if recursive.
                If None, there will be no limit and infinite recursion may be
                possible.

        Raises:
            IsADirectoryError: when recursive is false and the path(s) provided is a directory
            FoundryDeleteInNotSupportedTransactionError:
                    if a directory is being deleted, path_to_delete contains (subdirectories)
                    Foundry does not complain that we send remove_dataset_file requests for those as well.
                    However, if those directories are not at all part of one of the files_created_in_this_transaction
                    the user is trying to delete a file from a previous committed transaction which is not possible
                    in an UPDATE transaction

        """
        is_directory = bool(self.info(path)["type"] == "directory")
        if is_directory and recursive is False:
            raise IsADirectoryError()
        expanded_path = self.expand_path(path, recursive=recursive, maxdepth=maxdepth)
        if not self._intrans:
            with self.start_transaction(transaction_type="DELETE") as transaction:
                self.api.add_files_to_delete_transaction(
                    dataset_rid=self.dataset_identity["dataset_rid"],
                    transaction_id=transaction.transaction_rid,
                    logical_paths=expanded_path,
                )
        else:
            if self.transaction.transaction_type == "DELETE":
                self.api.add_files_to_delete_transaction(
                    dataset_rid=self.dataset_identity["dataset_rid"],
                    transaction_id=self._transaction.transaction_rid,
                    logical_paths=expanded_path,
                )
            elif self.transaction.transaction_type == "UPDATE":
                files_created_in_this_transaction = [
                    f.path for f in self.transaction.files
                ]
                for path_to_delete in reversed(expanded_path):
                    if not any(
                        (
                            bool(path_to_delete in file)
                            for file in files_created_in_this_transaction
                        )
                    ):
                        raise FoundryDeleteInNotSupportedTransactionError(
                            path_to_delete
                        )
                    self.api.remove_dataset_file(
                        dataset_rid=self.dataset_identity["dataset_rid"],
                        transaction_id=self._transaction.transaction_rid,
                        logical_path=path_to_delete,
                    )
            else:
                raise FoundryDeleteInNotSupportedTransactionError(expanded_path)

    @property
    def transaction(self):
        """A context within which files are committed together upon exit.

        Requires the file class to implement `.commit()` and `.discard()`
        for the normal and exception cases.
        """
        if self._transaction is None:
            self._transaction = FoundryTransaction(self)
        return self._transaction

    def start_transaction(
        self, transaction_type="UPDATE"
    ):  # pylint: disable=arguments-differ
        """Begin write transaction for deferring files, non-context version."""
        self._intrans = True
        self._transaction = FoundryTransaction(self, transaction_type=transaction_type)
        return self.transaction


class FoundryFile(AbstractBufferedFile):
    """Helper class for files referenced in the FoundryFileSystem."""

    DEFAULT_BLOCK_SIZE = (
        sys.float_info.max
    )  # setting blocksize to very large value until we implement multipart upload

    def __init__(
        self,
        fs,
        path,
        mode="rb",
        block_size="default",
        autocommit=True,
        cache_type="all",
        cache_options=None,
        size=None,
        **kwargs,
    ):
        # pylint: disable=too-many-arguments
        assert cache_type == "all", "Only the AllBytes cache is supported"
        if "a" in mode:
            raise NotImplementedError(
                "appending to a file not implemented in FoundryFileSystem"
            )
        super().__init__(
            fs,
            path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options or {},
            size=size,
            **kwargs,
        )

    def _fetch_range(self, start, end):
        assert start == 0, "Only reading of the complete file is supported"
        return self.fs.api.download_dataset_file(
            dataset_rid=self.fs.dataset_identity["dataset_rid"],
            output_directory=None,
            foundry_file_path=self.path,
            view=self.fs.dataset_identity["branch"],
        )

    def _initiate_upload(self):
        """Internal function to start a file upload."""
        if not self.fs._intrans:  # pylint: disable=protected-access
            transaction = self.fs.start_transaction()
            transaction.start()

    def _upload_chunk(self, final=False):
        """Internal function to add a chunk of data to a started upload."""
        assert final is True, "chunked uploading not supported"
        self.buffer.seek(0)

        self.fs.api.upload_dataset_file(
            dataset_rid=self.fs.dataset_identity["dataset_rid"],
            transaction_rid=self.fs._transaction.transaction_rid,  # pylint: disable=protected-access
            path_in_foundry_dataset=self.path,
            path_or_buf=self.buffer,
        )
        if final is True and self.autocommit:
            self.fs.end_transaction()

        return False


class FoundryFileSystemError(FoundryDevToolsError):
    """Parent class for alle FoundryFileSystem errors.

    See also :class:`foundry_dev_tools.foundry_api_client.FoundryDevToolsError` and
        :class:`foundry_dev_tools.foundry_api_client.FoundryAPIError`
    """


class FoundrySimultaneousOpenTransactionError(FoundryFileSystemError):
    # pylint: disable=missing-class-docstring
    def __init__(self, dataset_rid: str, open_transaction_rid):
        super().__init__(
            f"Dataset {dataset_rid} already has open transaction {open_transaction_rid}"
        )
        self.dataset_rid = dataset_rid
        self.open_transaction_rid = open_transaction_rid


class FoundryDeleteInNotSupportedTransactionError(FoundryFileSystemError):
    # pylint: disable=missing-class-docstring
    def __init__(self, path: str):
        super().__init__(
            f"You are trying to delete the file(s) {path} while an UPDATE/SNAPSHOT transaction is open. "
            f"Deleting this file(s) is not supported since it was committed in a previous transaction."
        )
        self.path = path


class FoundryDatasetPathInUrlNotSupportedError(FoundryFileSystemError):
    # pylint: disable=missing-class-docstring
    def __init__(self):
        super().__init__(
            "Using the dataset path in the fsspec url is not supported. Please pass the dataset rid, e.g. "
            "foundry://ri.foundry.main.dataset.aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/<file-path-in-dataset>"
        )


class FoundryTransaction:
    """Filesystem transaction write context.

    Gathers files for deferred commit or discard, so that several write
    operations can be finalized semi-atomically. This works by having this
    instance as the ``.transaction`` attribute of the given filesystem
    """

    def __init__(self, fs, transaction_type="UPDATE"):
        """Initialize a FoundryTransaction.

        Args:
            fs (FoundryFileSystem): the filesystem for this transaction
            transaction_type (str): the transaction type TODO

        """
        self.fs = fs
        self.files = []
        self.transaction_type = transaction_type
        self.transaction_rid = None
        self.backoff = self.fs.transaction_backoff

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """End transaction and commit, if exit is not due to exception."""
        # only commit if there was no exception
        self.complete(commit=exc_type is None)
        self.fs._intrans = False
        self.fs._transaction = None

    def start(self):
        """Start a transaction on this FileSystem."""

        def _start(self_i):
            try:
                self_i.transaction_rid = self_i.fs.api.open_transaction(
                    dataset_rid=self_i.fs.dataset_identity["dataset_rid"],
                    mode=self_i.transaction_type,
                    branch=self_i.fs.dataset_identity["branch"],
                )
                self_i.files = []  # clean up after previous failed completions
                self_i.fs._intrans = True  # pylint: disable=protected-access
            except DatasetHasOpenTransactionError as exception:
                raise FoundrySimultaneousOpenTransactionError(
                    dataset_rid=exception.dataset_rid,
                    open_transaction_rid=exception.open_transaction_rid,
                ) from exception

        if self.backoff:
            start_with_potential_backoff = backoff.on_exception(
                wait_gen=backoff.constant,
                exception=FoundrySimultaneousOpenTransactionError,
                interval=1,
                max_tries=60,
            )(_start)
        else:
            start_with_potential_backoff = _start
        return start_with_potential_backoff(self_i=self)

    def complete(self, commit=True):
        """Finish transaction: commit or discard all deferred files."""
        if commit:
            self.fs.api.commit_transaction(
                dataset_rid=self.fs.dataset_identity["dataset_rid"],
                transaction_id=self.transaction_rid,
            )
            self.fs.dataset_identity["last_transaction_rid"] = self.transaction_rid
        else:
            self.fs.api.abort_transaction(
                dataset_rid=self.fs.dataset_identity["dataset_rid"],
                transaction_id=self.transaction_rid,
            )
        self.files = []
        self.fs._intrans = False  # pylint: disable=protected-access


def _correct_directories(file_details, subfolder_prefix=""):
    if subfolder_prefix is None:
        subfolder_prefix = ""
    result = []
    visited_folders = []
    for file in file_details:
        file_type = _file_or_directory(
            file["logicalPath"], file["fileMetadata"]["length"], subfolder_prefix
        )
        if file_type == "file":
            result.append(
                {
                    "name": file["logicalPath"],
                    "size": file["fileMetadata"]["length"],
                    "type": file_type,
                    "time_modified": file["timeModified"],
                    "transaction_rid": file["transactionRid"],
                    "is_open": file["isOpen"],
                }
            )
        else:
            top_level_folder = _get_top_level_folder(
                file["logicalPath"], subfolder_prefix
            )
            full_path = str(f"{subfolder_prefix.rstrip('/')}/{top_level_folder}")
            if full_path in visited_folders:
                pass
            else:
                # get earliest timeModified and corresponding transactionRid
                # first created object in a folder "creates" folder
                folders_with_same_top_level = [
                    detail
                    for detail in file_details
                    if detail["logicalPath"].startswith(
                        full_path.lstrip("/").rstrip("/")
                    )
                ]
                first_created_object = min(
                    folders_with_same_top_level, key=lambda x: x["timeModified"]
                )

                result.append(
                    {
                        "name": full_path.lstrip("/").rstrip("/"),
                        "size": 0,
                        "type": file_type,
                        "time_modified": first_created_object["timeModified"],
                        "transaction_rid": first_created_object["transactionRid"],
                        "is_open": first_created_object["isOpen"],
                    }
                )
                visited_folders.append(full_path)

    return result


def _file_or_directory(file_or_folder_name, size, subfolder_prefix=""):
    return_value = "file"
    if len(subfolder_prefix) > 0 and subfolder_prefix[-1] != "/":
        subfolder_prefix = subfolder_prefix + "/"
    file_or_folder_name = _remove_prefix(file_or_folder_name, subfolder_prefix)
    if (("." not in file_or_folder_name) or ("/" in file_or_folder_name)) and size == 0:
        return_value = "directory"
    if "/" in file_or_folder_name and size != 0:
        return_value = "directory"
    return return_value


def _get_top_level_folder(path, subfolder_prefix):
    tmp_path = path[len(subfolder_prefix) :]
    if len(tmp_path) > 0 and tmp_path[0] == "/":
        tmp_path = tmp_path.lstrip("/")
    if "/" in tmp_path:
        tmp_path = tmp_path.split("/")[0]
    return tmp_path


def _remove_prefix(text, prefix):
    # Remove once we drop Python 3.8 support and replace with
    # in-build removeprefix() function
    if text.startswith(prefix):
        return text[len(prefix) :]
    return text
