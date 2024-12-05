"""Implementation of the foundry-catalog API."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from urllib.parse import quote_plus

from foundry_dev_tools.clients.api_client import APIClient
from foundry_dev_tools.errors.dataset import BranchNotFoundError, DatasetNotFoundError
from foundry_dev_tools.errors.handling import ErrorHandlingConfig
from foundry_dev_tools.utils import api_types
from foundry_dev_tools.utils.api_types import assert_in_literal

if TYPE_CHECKING:
    import requests


class CatalogClient(APIClient):
    """To be implemented/transferred."""

    api_name = "foundry-catalog"

    def list_dataset_files(
        self,
        dataset_rid: api_types.DatasetRid,
        end_ref: api_types.View = "master",
        page_size: int = 1000,
        logical_path: api_types.PathInDataset | None = None,
        page_start_logical_path: api_types.PathInDataset | None = None,
        start_transaction_rid: api_types.TransactionRid | None = None,
        include_open_exclusive_transaction: bool = False,
        exclude_hidden_files: bool = False,
        temporary_credentials_auth_token: str | None = None,
    ) -> list:
        """Same as :py:meth:`CatalogClient.api_get_dataset_view_files3`, but iterates through all pages.

        Args:
            dataset_rid: the dataset rid
            end_ref: branch or transaction rid of the dataset
            page_size: the maximum page size returned
            logical_path: If logical_path is absent, returns all files in the view.
                If logical_path matches a file exactly, returns just that file.
                Otherwise, returns all files in the "directory" of logical_path:
                (a slash is added to the end of logicalPath if necessary and a prefix-match is performed)
            page_start_logical_path: if specified page starts at the given path,
                otherwise at the beginning of the file list
            start_transaction_rid: if a startTransactionRid is given, the view starting at the startTransactionRid
                and ending at the endRef is returned
            include_open_exclusive_transaction: if files added in open transaction should be returned
                as well in the response
            exclude_hidden_files: if hidden files should be excluded (e.g. _log files)
            temporary_credentials_auth_token: to generate temporary credentials for presigned URLs

        Returns:
            list[FileResourcesPage]:
                .. code-block:: python

                    [
                        {
                            "logicalPath": "..",
                            "pageStartLogicalPath": "..",
                            "includeOpenExclusiveTransaction": "..",
                            "excludeHiddenFiles": "..",
                        },
                    ]
        """

        def _inner_get(page_start_logical_path: str | None = None) -> dict:
            return self.api_get_dataset_view_files3(
                dataset_rid=dataset_rid,
                end_ref=end_ref,
                page_size=page_size,
                logical_path=logical_path,
                page_start_logical_path=page_start_logical_path,
                include_open_exclusive_transaction=include_open_exclusive_transaction,
                exclude_hidden_files=exclude_hidden_files,
                start_transaction_rid=start_transaction_rid,
                temporary_credentials_auth_token=temporary_credentials_auth_token,
            ).json()

        result: list[dict] = []
        first_result = _inner_get(page_start_logical_path=page_start_logical_path)
        result.extend(first_result["values"])
        next_page_token = first_result.get("nextPageToken", None)
        while next_page_token is not None:
            batch_result = _inner_get(page_start_logical_path=next_page_token)
            next_page_token = batch_result.get("nextPageToken", None)
            result.extend(batch_result["values"])  # type: ignore[arg-type]
        return result

    def api_get_dataset_view_files3(
        self,
        dataset_rid: api_types.DatasetRid,
        end_ref: api_types.View,
        page_size: int,
        logical_path: api_types.PathInDataset | None = None,
        page_start_logical_path: api_types.PathInDataset | None = None,
        include_open_exclusive_transaction: bool = False,
        exclude_hidden_files: bool = False,
        start_transaction_rid: api_types.View | None = None,
        temporary_credentials_auth_token: str | None = None,
        **kwargs,
    ) -> requests.Response:
        """Returns files in the dataset view matching the specified parameters.

        Args:
            dataset_rid: the dataset rid
            end_ref: branch or transaction rid of the dataset
            page_size: the maximum page size returned
            logical_path: If logical_path is absent, returns all files in the view.
                If logical_path matches a file exactly, returns just that file.
                Otherwise, returns all files in the "directory" of logical_path:
                (a slash is added to the end of logicalPath if necessary and a prefix-match is performed)
            page_start_logical_path: if specified page starts at the given path,
                otherwise at the beginning of the file list
            include_open_exclusive_transaction: if files added in open transaction should be returned
                as well in the response
            exclude_hidden_files: if hidden files should be excluded (e.g. _log files)
            start_transaction_rid: if a startTransactionRid is given, the view starting at the startTransactionRid
                and ending at the endRef is returned
            temporary_credentials_auth_token: to generate temporary credentials for presigned URLs
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            response:
                the response contains a json dict with the following keys:
                values: an array of file resource objects
                nextPageToken: which can be used for the next request as `start_transaction_rid`
        """
        params = {"pageSize": page_size}
        if start_transaction_rid:
            params["startTransactionRid"] = start_transaction_rid
        get_dataset_view_files_request = {}
        if logical_path:
            get_dataset_view_files_request["logicalPath"] = logical_path
        if page_start_logical_path:
            get_dataset_view_files_request["pageStartLogicalPath"] = page_start_logical_path
        if include_open_exclusive_transaction:
            get_dataset_view_files_request["includeOpenExclusiveTransaction"] = include_open_exclusive_transaction
        if exclude_hidden_files:
            get_dataset_view_files_request["excludeHiddenFiles"] = exclude_hidden_files
        return self.api_request(
            "PUT",
            f"catalog/datasets/{dataset_rid}/views/{quote_plus(end_ref)}/files3",
            params=params,
            headers={"Temporary-Credentials-Authorization": temporary_credentials_auth_token},
            json=get_dataset_view_files_request,
            error_handling=ErrorHandlingConfig(
                DatasetNotFoundError,
                dataset_rid=dataset_rid,
                logical_path=logical_path,
            ),
            **kwargs,
        )

    def api_get_events(
        self,
        types: set[str],
        limit: int | None = None,
        page_token: str | None = None,
        **kwargs,
    ) -> requests.Response:
        """Returns a page of events filtered by the :any:`types` parameter.

        Args:
            types: types to filter for
            limit: limit the maximum numbers of events per page
            page_token: for pagination
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "GET",
            "catalog/events",
            params={"types": types, "limit": limit, "pageToken": page_token},
            **kwargs,
        )

    def api_create_dataset(
        self,
        dataset_path: api_types.FoundryPath,
        **kwargs,
    ) -> requests.Response:
        """Creates dataset at specified path.

        Args:
            dataset_path: path on foundry for the to be created dataset
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "POST",
            "catalog/datasets",
            json={"path": dataset_path},
            error_handling=ErrorHandlingConfig(dataset_path=dataset_path),
            **kwargs,
        )

    def api_get_dataset(self, dataset_rid: api_types.DatasetRid, **kwargs) -> requests.Response:
        """Returns rid and fileSystemId of dataset.

        Args:
            dataset_rid: the dataset rid
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "GET",
            f"catalog/datasets/{dataset_rid}",
            error_handling=ErrorHandlingConfig({204: DatasetNotFoundError}, dataset_rid=dataset_rid),
            **kwargs,
        )

    def api_delete_dataset(self, dataset_rid: api_types.DatasetRid, **kwargs) -> requests.Response:
        """Deletes the dataset.

        Args:
            dataset_rid: the dataset rid
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "DELETE",
            "catalog/datasets",
            json={"rid": dataset_rid},
            **kwargs,
        )

    def api_set_transaction_type(
        self,
        dataset_rid: api_types.DatasetRid,
        transaction_rid: api_types.TransactionRid,
        transaction_type: api_types.FoundryTransaction,
        **kwargs,
    ) -> requests.Response:
        """Set transaction type.

        Args:
            dataset_rid: dataset rid
            transaction_rid: transaction rid
            transaction_type: foundry transaction type, see :py:class:`api_types.FoundryTransaction`
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        assert_in_literal(transaction_type, api_types.FoundryTransaction, "transaction_type")

        return self.api_request(
            "POST",
            f"catalog/datasets/{dataset_rid}/transactions/{transaction_rid}",
            data=f'"{transaction_type}"',
            **kwargs,
        )

    def api_start_transaction(
        self,
        dataset_rid: api_types.DatasetRid,
        branch_id: api_types.DatasetBranch,
        record: dict[str, Any] | None = None,
        provenance: dict | None = None,
        user_id: str | None = None,
        start_transaction_type: api_types.FoundryTransaction | None = None,
        **kwargs,
    ) -> requests.Response:
        """Start a transaction on a dataset.

        Args:
            dataset_rid: dataset rid to start transaction on
            branch_id: the dataset branch
            record: record
            provenance: provenance for transaction
            user_id: start transaction as another user, needs `foundry:set-user-id` permissions
            start_transaction_type: transaction type, default is `APPEND`
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        post_json = {"branchId": branch_id, "record": record or {}}
        if provenance is not None:
            post_json["provenance"] = provenance
        if user_id is not None:
            post_json["userId"] = user_id
        if start_transaction_type is not None:
            assert_in_literal(start_transaction_type, api_types.FoundryTransaction, "start_transaction_type")
            post_json["startTransactionType"] = start_transaction_type

        return self.api_request(
            "POST",
            f"catalog/datasets/{dataset_rid}/transactions",
            json=post_json,
            error_handling=ErrorHandlingConfig(
                {"Default:InvalidArgument": DatasetNotFoundError},
                dataset_rid=dataset_rid,
                branch_id=branch_id,
            ),
            **kwargs,
        )

    def api_commit_transaction(
        self,
        dataset_rid: api_types.DatasetRid,
        transaction_rid: api_types.TransactionRid,
        record: dict[str, Any] | None = None,
        provenance: dict | None = None,
        do_sever_inherited_permissions: bool | None = None,
        **kwargs,
    ) -> requests.Response:
        """Commit a transaction on a dataset.

        Args:
            dataset_rid: dataset rid to start transaction on
            transaction_rid: the transaction to commit
            record: record
            provenance: provenance for transaction
            do_sever_inherited_permissions: wether dependant conditions are removed on the transaction
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        post_json = {"transactionRid": transaction_rid, "record": record or {}}
        if provenance is not None:
            post_json["provenance"] = provenance
        if do_sever_inherited_permissions is not None:
            post_json["doSeverInheritedPermissions"] = do_sever_inherited_permissions  # type: ignore[assignment]

        return self.api_request(
            "POST",
            f"catalog/datasets/{dataset_rid}/transactions/{transaction_rid}/commit",
            json=post_json,
            error_handling=ErrorHandlingConfig(dataset_rid=dataset_rid, transaction_rid=transaction_rid),
            **kwargs,
        )

    def api_abort_transaction(
        self,
        dataset_rid: api_types.DatasetRid,
        transaction_rid: api_types.TransactionRid,
        record: dict[str, Any] | None = None,
        provenance: dict | None = None,
        do_sever_inherited_permissions: bool | None = None,
        **kwargs,
    ) -> requests.Response:
        """Abort a transaction on a dataset.

        Args:
            dataset_rid: dataset rid to start transaction on
            transaction_rid: the transaction to commit
            record: record
            provenance: provenance for transaction
            do_sever_inherited_permissions: wether dependant conditions are removed on the transaction
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        post_json = {"transactionRid": transaction_rid, "record": record or {}}
        if provenance is not None:
            post_json["provenance"] = provenance
        if do_sever_inherited_permissions is not None:
            post_json["doSeverInheritedPermissions"] = do_sever_inherited_permissions  # type: ignore[assignment]

        return self.api_request(
            "POST",
            f"catalog/datasets/{dataset_rid}/transactions/{transaction_rid}/abortWithMetadata",
            json=post_json,
            **kwargs,
        )

    def api_get_transaction(self, dataset_rid: api_types.DatasetRid, ref: api_types.Ref, **kwargs) -> requests.Response:
        """Get the transaction for a given ref."""
        return self.api_request("GET", f"catalog/datasets/{dataset_rid}/transactions/{ref}", **kwargs)

    def api_get_reverse_transactions2(
        self,
        dataset_rid: api_types.DatasetRid,
        start_ref: api_types.View,
        page_size: int,
        end_transaction_rid: api_types.TransactionRid | None = None,
        include_open_exclusive_transaction: bool | None = False,
        allow_deleted_dataset: bool | None = None,
        **kwargs,
    ) -> requests.Response:
        """Get reverse transactions.

        Args:
            dataset_rid: dataset rid to get transactions
            start_ref: at what ref to start listing
            page_size: response page entry size
            end_transaction_rid: at what transaction to stop listing
            include_open_exclusive_transaction: include open exclusive transaction
            allow_deleted_dataset: respond even if dataset was deleted
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        params = {"pageSize": page_size}
        if end_transaction_rid is not None:
            params["endTransactionRid"] = end_transaction_rid  # type: ignore[assignment]
        if include_open_exclusive_transaction is not None:
            params["includeOpenExclusiveTransaction"] = include_open_exclusive_transaction
        if allow_deleted_dataset is not None:
            params["allowDeletedDataset"] = allow_deleted_dataset
        return self.api_request(
            "GET",
            f"catalog/datasets/{dataset_rid}/reverse-transactions2/{quote_plus(start_ref)}",
            params=params,
            **kwargs,
        )

    def api_create_branch(
        self,
        dataset_rid: api_types.DatasetRid,
        branch_id: api_types.DatasetBranch,
        parent_ref: api_types.TransactionRid | None = None,
        parent_branch_id: api_types.DatasetBranch | None = None,
        **kwargs,
    ) -> requests.Response:
        """Creates a branch on a dataset.

        Args:
            dataset_rid: the dataset
            branch_id: the branch to create
            parent_ref: optionally the transaction off which the branch will be based
            parent_branch_id: optionally a parent branch name, otherwise a root branch
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "POST",
            f"catalog/datasets/{dataset_rid}/branchesUnrestricted2/{quote_plus(branch_id)}",
            json={"parentRef": parent_ref, "parentBranchId": parent_branch_id},
            **kwargs,
        )

    def api_update_branch(
        self,
        dataset_rid: api_types.DatasetRid,
        branch: api_types.DatasetBranch,
        parent_ref: api_types.View | None = None,
        **kwargs,
    ) -> requests.Response:
        """Updates the latest transaction of branch 'branch' to the latest transaction of branch 'parent_branch'.

        Args:
            dataset_rid: Unique identifier of the dataset
            branch: The branch to update (e.g. master)
            parent_ref: the name of the branch to copy the last transaction from or a transaction rid
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

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
        return self.api_request(
            "POST",
            f"catalog/datasets/{dataset_rid}/branchesUpdate2/{quote_plus(branch)}",
            data=f'"{parent_ref}"',
            **kwargs,
        )

    def api_get_branch(
        self,
        dataset_rid: api_types.DatasetRid,
        branch: api_types.DatasetBranch,
        **kwargs,
    ) -> requests.Response:
        """Returns branch information.

        Args:
            dataset_rid: Unique identifier of the dataset
            branch: Branch name
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            dict:
                with keys id (name) and rid (unique id) of the branch.

        """
        return self.api_request(
            "GET",
            f"catalog/datasets/{dataset_rid}/branches2/{quote_plus(branch)}",
            error_handling=ErrorHandlingConfig({204: BranchNotFoundError}, dataset_rid=dataset_rid, branch=branch),
            **kwargs,
        )

    def api_get_branches(
        self,
        dataset_rid: api_types.DatasetRid,
        **kwargs,
    ) -> requests.Response:
        """Returns branch names of dataset.

        Args:
            dataset_rid: Unique identifier of the dataset
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            list[str]:
                list of dataset branch names
        """
        return self.api_request(
            "GET",
            f"catalog/datasets/{dataset_rid}/branches",
            **kwargs,
        )

    def api_remove_dataset_file(
        self,
        dataset_rid: api_types.DatasetRid,
        transaction_id: api_types.TransactionRid,
        logical_path: api_types.FoundryPath,
        recursive: bool = False,
        **kwargs,
    ) -> requests.Response:
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
            dataset_rid: Unique identifier of the dataset
            transaction_id: transaction rid
            logical_path: logical path in the backing filesystem
            recursive: recurse into subdirectories
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        """
        return self.api_request(
            "POST",
            f"catalog/datasets/{dataset_rid}/transactions/{transaction_id}/files/remove",
            params={"logicalPath": logical_path, "recursive": recursive},
            **kwargs,
        )

    def api_add_files_to_delete_transaction(
        self,
        dataset_rid: api_types.DatasetRid,
        transaction_id: api_types.TransactionRid,
        logical_paths: list[api_types.PathInDataset],
        **kwargs,
    ) -> requests.Response:
        """Adds files in an open DELETE transaction.

        Files added to DELETE transactions affect
        the dataset view by removing files from the view.

        Args:
            dataset_rid: Unique identifier of the dataset
            transaction_id: transaction rid
            logical_paths: files in the dataset to delete
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        """
        return self.api_request(
            "POST",
            f"catalog/datasets/{dataset_rid}/transactions/{transaction_id}/files/addToDeleteTransaction",
            json={"logicalPaths": logical_paths},
            **kwargs,
        )

    def api_get_dataset_stats(
        self,
        dataset_rid: api_types.DatasetRid,
        end_ref: api_types.View = "master",
        **kwargs,
    ) -> requests.Response:
        """Returns response from foundry catalogue stats endpoint.

        Args:
            dataset_rid: the dataset rid
            end_ref: branch or transaction rid of the dataset
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        dict:
            sizeInBytes, numFiles, hiddenFilesSizeInBytes, numHiddenFiles, numTransactions

        """
        return self.api_request(
            "GET",
            f"catalog/datasets/{dataset_rid}/views/{quote_plus(end_ref)}/stats",
            **kwargs,
        )
