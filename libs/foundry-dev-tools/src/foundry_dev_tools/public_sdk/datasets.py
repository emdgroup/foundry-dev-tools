"""Dataset operations backed by the Foundry public v2 API."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from foundry_dev_tools.clients.api_client import PublicAPIClient

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from pandas import DataFrame as PandasDataFrame
    from polars import DataFrame as PolarsDataFrame
    from pyarrow import Table as ArrowTable

    from foundry_dev_tools.config.context import FoundryContext
    from foundry_dev_tools.utils.public_api_types import (
        Dataset as PublicDataset,
    )
    from foundry_dev_tools.utils.public_api_types import (
        DatasetBranch as PublicDatasetBranch,
    )
    from foundry_dev_tools.utils.public_api_types import (
        DatasetFile as PublicDatasetFile,
    )
    from foundry_dev_tools.utils.public_api_types import (
        DatasetTransaction as PublicDatasetTransaction,
    )


class PublicDatasetsClient(PublicAPIClient):
    """Client for ``/api/v2/datasets`` endpoints."""

    api_name = "datasets"

    def __init__(self, context: FoundryContext) -> None:
        super().__init__(context)

    def get_dataset(self, dataset_rid: str) -> PublicDataset:
        """Return basic metadata for ``dataset_rid``.

        This is a thin wrapper around ``GET /api/v2/datasets/{datasetRid}``.
        """
        response = self.api_request("GET", dataset_rid, api_version="v2")
        return response.json()

    def get_branch(self, dataset_rid: str, branch_name: str) -> PublicDatasetBranch:
        """Return metadata for a single branch.

        Args:
            dataset_rid: Dataset RID to query.
            branch_name: Branch identifier (for example ``master``).
        """
        response = self.api_request("GET", f"{dataset_rid}/branches/{branch_name}", api_version="v2")
        return response.json()

    def _paginate(
        self,
        method: Literal["GET", "POST"],
        api_path: str,
        *,
        api_version: str,
        api_preview: bool = False,
        params: Mapping[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Generic helper to iterate over public API pages.

        Public API v2 list endpoints return: {"data": [...], "nextPageToken": "..."}
        """
        page_token: str | None = None
        while True:
            request_params: dict[str, Any] | None
            if params or page_token:
                request_params = dict(params or {})
                if page_token:
                    request_params["pageToken"] = page_token
            else:
                request_params = None

            response = self.api_request(
                method,
                api_path,
                api_version=api_version,
                api_preview=api_preview,
                params=request_params,
                json=json,
            )
            payload = response.json()

            items = payload.get("data", [])
            yield from items

            page_token = payload.get("nextPageToken")
            if not page_token:
                break

    def iter_branches(
        self,
        dataset_rid: str,
        *,
        page_size: int | None = None,
    ) -> Iterator[PublicDatasetBranch]:
        """Yield all branches for a dataset."""
        params = {"pageSize": page_size} if page_size is not None else None
        yield from self._paginate(
            "GET",
            f"{dataset_rid}/branches",
            api_version="v2",
            api_preview=False,
            params=params,
        )

    def list_branches(
        self,
        dataset_rid: str,
        *,
        page_size: int | None = None,
    ) -> list[PublicDatasetBranch]:
        """Return a list with all branches for ``dataset_rid``."""
        return list(self.iter_branches(dataset_rid, page_size=page_size))

    def iter_transactions(
        self,
        dataset_rid: str,
        *,
        branch_name: str,
        page_size: int | None = None,
        include_open_transactions: bool = False,
    ) -> Iterator[PublicDatasetTransaction]:
        """Yield transactions for ``branch_name`` on ``dataset_rid``."""
        params: dict[str, Any] = {"branchName": branch_name}
        if page_size is not None:
            params["pageSize"] = page_size
        if include_open_transactions:
            params["includeOpenTransactions"] = "true"

        yield from self._paginate(
            "GET",
            f"{dataset_rid}/transactions",
            api_version="v2",
            api_preview=True,
            params=params,
        )

    def list_transactions(
        self,
        dataset_rid: str,
        *,
        branch_name: str,
        page_size: int | None = None,
        include_open_transactions: bool = False,
    ) -> list[PublicDatasetTransaction]:
        """Return all transactions for ``branch_name``."""
        return list(
            self.iter_transactions(
                dataset_rid,
                branch_name=branch_name,
                page_size=page_size,
                include_open_transactions=include_open_transactions,
            )
        )

    def iter_files(
        self,
        dataset_rid: str,
        *,
        branch_name: str | None = None,
        transaction_rid: str | None = None,
        page_size: int | None = None,
        path_prefix: str | None = None,
        include_hidden_files: bool | None = None,
    ) -> Iterator[PublicDatasetFile]:
        """Yield files from a dataset branch or transaction."""
        self._validate_view_selector(branch_name=branch_name, transaction_rid=transaction_rid)

        params: dict[str, Any] = {}
        if page_size is not None:
            params["pageSize"] = page_size
        if branch_name is not None:
            params["branchName"] = branch_name
        if transaction_rid is not None:
            params["transactionRid"] = transaction_rid
        if path_prefix is not None:
            params["pathPrefix"] = path_prefix
        if include_hidden_files is not None:
            params["includeHiddenFiles"] = "true" if include_hidden_files else "false"

        yield from self._paginate(
            "GET",
            f"{dataset_rid}/files",
            api_version="v2",
            api_preview=False,
            params=params or None,
        )

    def list_files(
        self,
        dataset_rid: str,
        *,
        branch_name: str | None = None,
        transaction_rid: str | None = None,
        page_size: int | None = None,
        path_prefix: str | None = None,
        include_hidden_files: bool | None = None,
    ) -> list[PublicDatasetFile]:
        """Return a list of files for the selected view."""
        return list(
            self.iter_files(
                dataset_rid,
                branch_name=branch_name,
                transaction_rid=transaction_rid,
                page_size=page_size,
                path_prefix=path_prefix,
                include_hidden_files=include_hidden_files,
            )
        )

    def _read_table_response(
        self,
        dataset_rid: str,
        *,
        branch_name: str | None,
        transaction_rid: str | None,
        columns: Sequence[str] | None,
        row_limit: int | None,
    ) -> bytes:
        """Fetch Arrow IPC bytes from readTable endpoint."""
        self._validate_view_selector(branch_name=branch_name, transaction_rid=transaction_rid)

        params: dict[str, Any] = {"format": "ARROW"}

        if branch_name is not None:
            params["branchName"] = branch_name
        elif transaction_rid is not None:
            params["startTransactionRid"] = transaction_rid
            params["endTransactionRid"] = transaction_rid

        if columns is not None:
            params["columns"] = list(columns)

        if row_limit is not None:
            params["rowLimit"] = row_limit

        response = self.api_request(
            "GET",
            f"{dataset_rid}/readTable",
            api_version="v2",
            params=params,
            headers={"Accept": "application/octet-stream"},
        )
        return response.content

    def read_table_arrow(
        self,
        dataset_rid: str,
        *,
        branch_name: str | None = "master",
        transaction_rid: str | None = None,
        columns: Sequence[str] | None = None,
        row_limit: int | None = None,
    ) -> ArrowTable:
        """Return a :class:`pyarrow.Table` produced by ``readTable``.

        Either ``branch_name`` or ``transaction_rid`` must be provided. By default the
        master branch is used.
        """
        raw_bytes = self._read_table_response(
            dataset_rid,
            branch_name=branch_name,
            transaction_rid=transaction_rid,
            columns=columns,
            row_limit=row_limit,
        )
        from foundry_dev_tools._optional.pyarrow import pa

        reader = pa.ipc.open_stream(raw_bytes)
        return reader.read_all()

    def read_table_pandas(
        self,
        dataset_rid: str,
        *,
        branch_name: str | None = "master",
        transaction_rid: str | None = None,
        columns: Sequence[str] | None = None,
        row_limit: int | None = None,
    ) -> PandasDataFrame:
        """Return a :class:`pandas.DataFrame` for the dataset view."""
        table = self.read_table_arrow(
            dataset_rid,
            branch_name=branch_name,
            transaction_rid=transaction_rid,
            columns=columns,
            row_limit=row_limit,
        )
        return table.to_pandas()

    def read_table_polars(
        self,
        dataset_rid: str,
        *,
        branch_name: str | None = "master",
        transaction_rid: str | None = None,
        columns: Sequence[str] | None = None,
        row_limit: int | None = None,
    ) -> PolarsDataFrame:
        """Return a :class:`polars.DataFrame` for the dataset view."""
        from foundry_dev_tools._optional.polars import pl

        table = self.read_table_arrow(
            dataset_rid,
            branch_name=branch_name,
            transaction_rid=transaction_rid,
            columns=columns,
            row_limit=row_limit,
        )
        return pl.from_arrow(table)

    @staticmethod
    def _validate_view_selector(*, branch_name: str | None, transaction_rid: str | None) -> None:
        if branch_name and transaction_rid:
            msg = "Provide either 'branch_name' or 'transaction_rid', not both."
            raise ValueError(msg)
        if branch_name is None and transaction_rid is None:
            msg = "Either 'branch_name' or 'transaction_rid' must be provided."
            raise ValueError(msg)
