"""Dataset helper class."""

from __future__ import annotations

import io
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, overload

from foundry_dev_tools.errors.compass import ResourceNotFoundError
from foundry_dev_tools.errors.dataset import (
    BranchNotFoundError,
    DatasetHasNoOpenTransactionError,
    DatasetNotFoundError,
    TransactionTypeMismatchError,
)
from foundry_dev_tools.resources import resource

if TYPE_CHECKING:
    import sys
    from collections.abc import Iterator
    from typing import IO, AnyStr, Literal, Self

    if sys.version_info < (3, 11):
        from typing_extensions import Self
    else:
        from typing import Self

    import pandas as pd
    import pandas.core.frame
    import polars.dataframe.frame
    import pyarrow as pa
    import pyspark.sql

    from foundry_dev_tools.config.context import FoundryContext
    from foundry_dev_tools.utils import api_types


class Dataset(resource.Resource):
    """Helper class for datasets."""

    _branch: api_types.Branch = None
    _transaction: api_types.Transaction | None = None
    __created__: bool | None = None
    rid_start: ClassVar[str] = "ri.foundry.main.dataset"

    def __init__(self, *args, **kwargs) -> None:
        if self.__created__ is None:
            msg = (
                "This class cannot be instantiated directly."
                " Please use Dataset.create, Dataset.from_path or Dataset.from_rid."
            )
            raise RuntimeError(msg)
        super().__init__(*args, **kwargs)

    @property
    def branch(self) -> api_types.Branch:
        """The branch of the dataset.

        If self._branch is not set, and the default "master" branch does not exist, it will be created.
        """
        if not self._branch:
            # create branch with default name, if the branch is not set
            # (e.g. when created via get_resource or directly instantiated)
            self.switch_branch("master", create_branch_if_not_exists=True)
        return self._branch

    @branch.setter
    def branch(self, branch: api_types.DatasetBranch):
        self._branch = branch

    @classmethod
    def from_rid(
        cls,
        context: FoundryContext,
        rid: api_types.Rid,
        /,
        *,
        branch: api_types.Ref | api_types.Branch = "master",
        create_branch_if_not_exists: bool = True,
        parent_ref: api_types.TransactionRid | None = None,
        parent_branch_id: api_types.DatasetBranch | None = None,
        **kwargs,
    ) -> Dataset:
        """Returns dataset at path.

        Args:
            context: the foundry context for the dataset
            rid: the rid of the dataset
            branch: the branch of the dataset
            create_branch_if_not_exists: create branch if branch does not exist
            parent_ref: optionally the transaction off which the branch will be based
                (only used if branch needs to be created)
            parent_branch_id: optionally a parent branch name, otherwise a root branch
                (only used if branch needs to be created)
            **kwargs: passed to :py:meth:`foundry_dev_tools.resources.resource.Resource.from_rid`
        """
        inst = super().from_rid(context, rid, **kwargs)
        if isinstance(branch, dict):
            inst.branch = branch
        else:
            inst.switch_branch(
                branch=branch,
                create_branch_if_not_exists=create_branch_if_not_exists,
                parent_ref=parent_ref,
                parent_branch_id=parent_branch_id,
            )
        return inst

    @classmethod
    def from_path(
        cls,
        context: FoundryContext,
        path: api_types.FoundryPath,
        /,
        *,
        branch: api_types.Ref = "master",
        create_if_not_exist: bool = False,
        create_branch_if_not_exists: bool = True,
        parent_ref: api_types.TransactionRid | None = None,
        parent_branch_id: api_types.DatasetBranch | None = None,
        **kwargs,
    ) -> Dataset:
        """Returns dataset at path.

        Args:
            context: the foundry context for the dataset
            path: the path where the dataset is located on foundry
            branch: the branch of the dataset
            create_if_not_exist: if the dataset does not exist, create it and the branch
            create_branch_if_not_exists: create branch if branch does not exist,
                branch always will be created if resource does not exist
            parent_ref: optionally the transaction off which the branch will be based
                (only used if branch needs to be created)
            parent_branch_id: optionally a parent branch name, otherwise a root branch
                (only used if branch needs to be created)
            **kwargs: passed to :py:meth:`foundry_dev_tools.resources.resource.Resource.from_path`
        """
        try:
            inst = super().from_path(context, path, **kwargs)
            inst.switch_branch(
                branch=branch,
                create_branch_if_not_exists=create_branch_if_not_exists,
                parent_ref=parent_ref,
                parent_branch_id=parent_branch_id,
            )
            inst.__created__ = False
        except ResourceNotFoundError:
            if create_if_not_exist:
                return cls.create(context, path, branch, parent_ref=parent_ref, parent_branch_id=parent_branch_id)
            raise
        else:
            return inst

    @classmethod
    def create(
        cls,
        context: FoundryContext,
        path: api_types.FoundryPath,
        branch: api_types.Ref,
        parent_ref: api_types.Ref | None = None,
        parent_branch_id: api_types.Ref | None = None,
    ) -> Dataset:
        """Create a foundry dataset.

        See :py:meth:`~foundry_dev_tools.clients.catalog.CatalogClient.api_create_dataset`.
        """
        rid = context.catalog.api_create_dataset(path).json()["rid"]

        inst = cls.from_rid(context, rid)
        inst.switch_branch(branch=branch, parent_ref=parent_ref, parent_branch_id=parent_branch_id)
        inst.__created__ = True
        return inst

    def create_branch(
        self,
        branch: api_types.DatasetBranch,
        parent_ref: api_types.TransactionRid | None = None,
        parent_branch_id: api_types.DatasetBranch | None = None,
    ) -> Self:
        """Creates a branch on a dataset and switches to it.

        Args:
            branch: the branch to create
            parent_ref: optionally the transaction off which the branch will be based
            parent_branch_id: optionally a parent branch name, otherwise a root branch
        """
        self.branch = self._context.catalog.api_create_branch(
            dataset_rid=self.rid,
            branch_id=branch,
            parent_ref=parent_ref,
            parent_branch_id=parent_branch_id,
        ).json()
        return self

    def switch_branch(
        self,
        branch: api_types.DatasetBranch,
        create_branch_if_not_exists: bool = False,
        parent_ref: api_types.TransactionRid | None = None,
        parent_branch_id: api_types.DatasetBranch | None = None,
    ) -> Self:
        """Switch to another branch.

        Args:
            branch: the name of the branch to switch to
            create_branch_if_not_exists: create branch if branch does not exist,
                branch always will be created if resource does not exist
            parent_ref: optionally the transaction off which the branch will be based
                (only used if branch needs to be created)
            parent_branch_id: optionally a parent branch name, otherwise a root branch
                (only used if branch needs to be created)

        """
        try:
            self.branch = self.get_branch(branch)
        except BranchNotFoundError:
            if create_branch_if_not_exists:
                self.create_branch(branch=branch, parent_ref=parent_ref, parent_branch_id=parent_branch_id)
            else:
                raise
        return self

    def get_branch(self, branch: api_types.DatasetBranch) -> api_types.Branch:
        """Returns the branch resource."""
        return self._context.catalog.api_get_branch(
            self.rid,
            branch,
        ).json()

    def get_transactions(
        self,
        page_size: int,
        end_transaction_rid: api_types.TransactionRid | None = None,
        include_open_exclusive_transaction: bool | None = False,
        allow_deleted_dataset: bool | None = None,
        **kwargs,
    ) -> list[api_types.Transaction]:
        """Get reverse transactions.

        Args:
            page_size: response page entry size
            end_transaction_rid: at what transaction to stop listing
            include_open_exclusive_transaction: include open exclusive transaction
            allow_deleted_dataset: respond even if dataset was deleted
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return [
            v["transaction"]
            for v in self._context.catalog.api_get_reverse_transactions2(
                self.rid,
                self.branch["id"],
                page_size=page_size,
                end_transaction_rid=end_transaction_rid,
                include_open_exclusive_transaction=include_open_exclusive_transaction,
                allow_deleted_dataset=allow_deleted_dataset,
                **kwargs,
            ).json()["values"]
        ]

    def get_last_transaction(self) -> api_types.Transaction | None:
        """Returns the last transaction or None if there are no transactions."""
        v = self.get_transactions(
            page_size=1,
            include_open_exclusive_transaction=True,
        )
        if v is not None and len(v) > 0:
            return v[0]
        return None

    def get_open_transaction(self) -> api_types.Transaction | None:
        """Gets the open transaction or None."""
        last = self.get_last_transaction()
        if last is not None and last["status"] == "OPEN":
            return last
        return None

    def start_transaction(self, start_transaction_type: api_types.FoundryTransaction | None = None) -> Self:
        """Start a transaction on the dataset."""
        self._transaction = self._context.catalog.api_start_transaction(
            self.rid,
            self.branch["id"],
            record={},
            start_transaction_type=start_transaction_type,
        ).json()
        if start_transaction_type is not None and start_transaction_type != "APPEND":
            self._context.catalog.api_set_transaction_type(
                self.rid,
                self.transaction["rid"],
                start_transaction_type,
            )
        return self

    @property
    def transaction(self) -> api_types.Transaction:
        """Get the current transaction or raise Error if there is no open transaction."""
        if self._transaction is not None:
            return self._transaction  # type: ignore[return-value]
        self._transaction = self.get_open_transaction()
        if self._transaction is not None:
            return self._transaction
        raise DatasetHasNoOpenTransactionError(f"{self.path=} {self.rid=}")  # noqa: EM102,TRY003

    def commit_transaction(self) -> Self:
        """Commit the transaction on the dataset."""
        if self.transaction is not None:
            self._context.catalog.api_commit_transaction(self.rid, self.transaction["rid"])
            self._transaction = None
        return self

    def abort_transaction(self) -> Self:
        """Commit the transaction on the dataset."""
        if self.transaction is not None:
            self._context.catalog.api_abort_transaction(self.rid, self.transaction["rid"])
            self._transaction = None
        return self

    def upload_files(
        self,
        path_file_dict: dict[api_types.PathInDataset, Path],
        max_workers: int | None = None,
        **kwargs,
    ) -> Self:
        """Uploads multiple local files to a foundry dataset.

        Args:
            max_workers: Set number of threads for upload
            path_file_dict: A dictionary which maps the path in the dataset -> local file path
            **kwargs: get passed to :py:meth:`foundry_dev_tools.resources.dataset.Dataset.transaction_context`
        """
        with self.transaction_context(**kwargs):
            self._context.data_proxy.upload_dataset_files(
                dataset_rid=self.rid,
                transaction_rid=self.transaction["rid"],
                path_file_dict=path_file_dict,
                max_workers=max_workers,
            )
        return self

    def upload_file(
        self,
        file_path: Path,
        path_in_foundry_dataset: api_types.PathInDataset,
        **kwargs,
    ) -> Self:
        """Upload a file to the dataset.

        Args:
            file_path: local file path to upload
            path_in_foundry_dataset: file path inside the foundry dataset
            transaction_type: if this dataset does not have an open transaction,
                opens a transaction with the specified type.
            **kwargs: get passed to :py:meth:`foundry_dev_tools.resources.dataset.Dataset.transaction_context`
        """
        with self.transaction_context(**kwargs):
            self._context.data_proxy.upload_dataset_file(
                dataset_rid=self.rid,
                transaction_rid=self.transaction["rid"],
                path=file_path,
                path_in_foundry_dataset=path_in_foundry_dataset,
            )
        return self

    def upload_folder(
        self,
        folder_path: Path,
        max_workers: int | None = None,
        **kwargs,
    ) -> Self:
        """Uploads all files contained in the folder to the dataset.

        The default transaction type is UPDATE.

        Args:
            folder_path: the folder to upload
            max_workers: Set number of threads for upload
            **kwargs: get passed to :py:meth:`foundry_dev_tools.resources.dataset.Dataset.transaction_context`
        """
        kwargs.setdefault("transaction_type", "UPDATE")
        self.upload_files(
            {str(f.relative_to(folder_path)): f for f in folder_path.rglob("*") if f.is_file()},
            max_workers=max_workers,
            **kwargs,
        )
        return self

    def delete_files(self, logical_paths: list[api_types.PathInDataset], **kwargs) -> Self:
        """Adds files in an open DELETE transaction.

        Files added to DELETE transactions affect
        the dataset view by removing files from the view.

        Args:
            logical_paths: files in the dataset to delete
            **kwargs: get passed to :py:meth:`foundry_dev_tools.resources.dataset.Dataset.transaction_context`
                       (transaction_type is forced to DELETE)

        """
        with self.transaction_context(transaction_type="DELETE", **kwargs):
            self._context.catalog.api_add_files_to_delete_transaction(
                self.rid, self.transaction["rid"], logical_paths=logical_paths
            )
        return self

    def remove_file(
        self,
        logical_path: api_types.FoundryPath,
        recursive: bool = False,
    ) -> Self:
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
            logical_path: logical path in the backing filesystem
            recursive: recurse into subdirectories
        """
        self._context.catalog.api_remove_dataset_file(
            self.rid,
            self.transaction["rid"],
            logical_path=logical_path,
            recursive=recursive,
        )
        return self

    def put_file(
        self,
        logical_path: api_types.PathInDataset,
        file_data: str | bytes | IO[AnyStr],
        overwrite: bool | None = None,
        **kwargs,
    ) -> Self:
        """Opens, writes, and closes a file under the specified dataset and transaction.

        Args:
            dataset_rid: dataset rid
            transaction_rid: transaction rid
            logical_path: file path in dataset
            file_data: content of the file
            overwrite: defaults to false, if true -> Overwrite the file if it already exists in the transaction.
            **kwargs: get passed to :py:meth:`foundry_dev_tools.resources.dataset.Dataset.transaction_context`
        """
        with self.transaction_context(**kwargs):
            self._context.data_proxy.api_put_file(
                self.rid,
                self.transaction["rid"],
                logical_path=logical_path,
                file_data=file_data,
                overwrite=overwrite,
            )
        return self

    def download_files(
        self,
        output_directory: Path,
        paths_in_dataset: set[api_types.PathInDataset] | None = None,
        max_workers: int | None = None,
    ) -> list[Path]:
        """Downloads multiple files from a dataset and saves them in the output directory.

        Args:
            output_directory: the directory where the files will be saved
            paths_in_dataset: the files to download, if None (the default) it will download all files
            max_workers: how many connections to use in parallel to download the files
        """
        return self._context.data_proxy.download_dataset_files(
            dataset_rid=self.rid,
            output_directory=output_directory,
            view=self.branch["id"],
            files=paths_in_dataset,
            max_workers=max_workers,
        )

    def get_file(
        self,
        path_in_dataset: api_types.PathInDataset,
        start_transaction_rid: api_types.TransactionRid | None = None,
        range_header: str | None = None,
    ) -> bytes:
        """Get bytes of a file in a Dataset.

        Args:
            path_in_dataset: the file to get
            start_transaction_rid: start transaction rid
            range_header: HTTP range header

        """
        return self._context.data_proxy.api_get_file_in_view(
            dataset_rid=self.rid,
            end_ref=self.branch["id"],
            logical_path=path_in_dataset,
            start_transaction_rid=start_transaction_rid,
            range_header=range_header,
        ).content

    def download_file(self, output_directory: Path, path_in_dataset: api_types.PathInDataset) -> Path:
        """Downloads the file to the output directory.

        Args:
            path_in_dataset: the file to download
            output_directory: the directory where the file will be saved
        """
        return self._context.data_proxy.download_dataset_file(
            dataset_rid=self.rid,
            output_directory=output_directory,
            foundry_file_path=path_in_dataset,
            view=self.branch["id"],
        )

    @contextmanager
    def download_files_temporary(
        self,
        paths_in_dataset: list[api_types.PathInDataset] | None = None,
        max_workers: int | None = None,
    ) -> Iterator[Path]:
        """Downloads dataset files to temporary directory and cleans it up afterwards.

        A wrapper around :py:meth:`foundry_dev_tools.resources.dataset.Dataset.download_files`
        together with :py:class:`tempfile.TemporaryDirectory` as a contextmanager.

        Args:
            paths_in_dataset: the files to download, if None (the default) it will download all files
            max_workers: how many connections to use in parallel to download the files
        """
        with tempfile.TemporaryDirectory(suffix=f"foundry_dev_tools-{self.rid}") as tmp_dir:
            path = Path(tmp_dir)
            _ = self.download_files(output_directory=path, paths_in_dataset=paths_in_dataset, max_workers=max_workers)
            yield path

    def save_dataframe(
        self,
        df: pandas.core.frame.DataFrame | polars.dataframe.frame.DataFrame | pyspark.sql.DataFrame,
        transaction_type: api_types.FoundryTransaction = "SNAPSHOT",
        foundry_schema: api_types.FoundrySchema | None = None,
    ) -> Self:
        """Saves a dataframe to Foundry. If the dataset in Foundry does not exist it is created.

        If the branch does not exist, it is created. If the dataset exists, an exception is thrown.
        If exists_ok=True is passed, the dataset is overwritten.
        Creates SNAPSHOT transactions by default.

        Args:
            df: A pyspark, pandas or polars DataFrame to upload
            dataset_path_or_rid: path or rid of the dataset in which the object should be stored.
            branch: Branch of the dataset in which the object should be stored
            exists_ok: By default, this method creates a new dataset.
                Pass exists_ok=True to overwrite according to strategy from parameter 'mode'
            transaction_type: Foundry Transaction type, see
                :py:class:`foundry_dev_tools.utils.api_types.FoundryTransaction`
            foundry_schema: use a custom foundry schema instead of the infered one
        """
        with self.transaction_context(transaction_type=transaction_type):
            from foundry_dev_tools._optional.pandas import pd
            from foundry_dev_tools._optional.polars import pl

            # TODO needed?
            # to be backwards compatible to most readers, that expect files
            # to be under spark/
            folder = str(round(time.time() * 1000)) if transaction_type == "APPEND" else "spark"
            parquet_compression = "snappy"

            use_pandas = not pd.__fake__ and isinstance(df, pd.DataFrame)
            use_polars = not pl.__fake__ and isinstance(df, pl.DataFrame)
            if use_pandas or use_polars:
                buf = io.BytesIO()
                schema_flavor = "spark"

                if use_pandas:
                    # write pandas dataframe as parquet to buffer
                    df.to_parquet(
                        buf,
                        compression=parquet_compression,
                        flavor=schema_flavor,
                        index=False,
                    )
                else:
                    # write polars dataframe as parquet to buffer
                    df.write_parquet(
                        buf,
                        compression=parquet_compression,
                        use_pyarrow=True,
                        pyarrow_options={"flavor": schema_flavor},
                    )

                buf.seek(0)  # go to beginning of file
                self.put_file(
                    folder + "/dataset.parquet",
                    buf,
                )
            else:
                with tempfile.TemporaryDirectory() as path:
                    p_path = Path(path)
                    df.write.format("parquet").option("compression", parquet_compression).save(
                        path=path,
                        mode="overwrite",
                    )

                    filenames = list(
                        filter(lambda file: not file.endswith(".crc"), os.listdir(path)),
                    )
                    path_file_dict = {f"{folder}/{file}": p_path.joinpath(file) for file in filenames}
                    self._context.data_proxy.upload_dataset_files(
                        self.rid,
                        self.transaction["rid"],
                        path_file_dict=path_file_dict,
                    )

            transaction = self.transaction
        if foundry_schema is None:
            foundry_schema = self.infer_schema()
        self.upload_schema(transaction["rid"], foundry_schema)
        return self

    def infer_schema(self) -> dict:
        """Returns the infered dataset schema."""
        return self._context.schema_inference.infer_dataset_schema(self.rid, self.branch["id"])

    def upload_schema(self, transaction_rid: api_types.TransactionRid, schema: api_types.FoundrySchema) -> Self:
        """Uploads the foundry dataset schema for a dataset, transaction, branch combination.

        Args:
            transaction_rid: The rid of the transaction
            schema: The foundry schema
        """
        self._context.metadata.api_upload_dataset_schema(
            dataset_rid=self.rid,
            transaction_rid=transaction_rid,
            schema=schema,
            branch=self.branch["id"],
        )
        return self

    def list_files(
        self,
        end_ref: api_types.View | None = None,
        page_size: int = 1000,
        logical_path: api_types.PathInDataset | None = None,
        page_start_logical_path: api_types.PathInDataset | None = None,
        include_open_exclusive_transaction: bool = False,
        exclude_hidden_files: bool = False,
        temporary_credentials_auth_token: str | None = None,
    ) -> list:
        """Wraps :py:meth:`foundry_dev_tools.clients.CatalogClient.list_dataset_files`.

        Args:
            end_ref: branch or transaction rid of the dataset, defaults to the current branch
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
        return self._context.catalog.list_dataset_files(
            self.rid,
            end_ref=end_ref or self.branch["id"],
            page_size=page_size,
            logical_path=logical_path,
            page_start_logical_path=page_start_logical_path,
            include_open_exclusive_transaction=include_open_exclusive_transaction,
            exclude_hidden_files=exclude_hidden_files,
            temporary_credentials_auth_token=temporary_credentials_auth_token,
        )

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal["pandas"],
        sql_dialect: api_types.SqlDialect = ...,
        timeout: int = ...,
    ) -> pd.core.frame.DataFrame: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal["spark"],
        sql_dialect: api_types.SqlDialect = ...,
        timeout: int = ...,
    ) -> pyspark.sql.DataFrame: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal["arrow"],
        sql_dialect: api_types.SqlDialect = ...,
        timeout: int = ...,
    ) -> pa.Table: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal["raw"],
        sql_dialect: api_types.SqlDialect = ...,
        timeout: int = ...,
    ) -> tuple[dict, list[list]]: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: api_types.SQLReturnType = ...,
        sql_dialect: api_types.SqlDialect = ...,
        timeout: int = ...,
    ) -> tuple[dict, list[list]] | pd.core.frame.DataFrame | pa.Table | pyspark.sql.DataFrame: ...

    def query_foundry_sql(
        self,
        query: str,
        return_type: api_types.SQLReturnType = "pandas",
        sql_dialect: api_types.SqlDialect = "SPARK",
        timeout: int = 600,
    ) -> tuple[dict, list[list]] | pd.DataFrame | pa.Table | pyspark.sql.DataFrame:
        """Wrapper around :py:meth:`foundry_dev_tools.clients.foundry_sql_server.FoundrySqlServerClient.query_foundry_sql`.

        But it automatically prepends the dataset location, so instead of:
        >>> ctx.foundry_sql_server.query_foundry_sql("SELECT * FROM `/path/to/dataset` WHERE a=1")
        You can just use:
        >>> ds = ctx.get_dataset_by_path("/path/to/dataset")
        >>> ds.query_foundry_sql("SELECT * WHERE a=1")
        """  # noqa: E501
        # This is a quick fix. Foundry Sql Server seems to decide that
        # "FROM `rid` SELECT *" is not direct read eligible.
        # tracking: ri.issues.main.issue.da8272ca-9e78-4100-af3f-6ac40aaecf51
        query_string = f"SELECT * FROM `{self.rid}`" if query == "SELECT *" else f"FROM `{self.rid}` {query}"  # noqa: S608
        return self._context.foundry_sql_server.query_foundry_sql(
            query=query_string,
            return_type=return_type,
            branch=self.branch["id"],
            sql_dialect=sql_dialect,
            timeout=timeout,
        )

    def to_spark(self) -> pyspark.sql.DataFrame:
        """Get dataset as :py:class:`pyspark.sql.DataFrame`.

        Via :py:meth:`foundry_dev_tools.resources.dataset.Dataset.query_foundry_sql`
        """
        return self.query_foundry_sql("SELECT *", return_type="spark")

    def to_arrow(self) -> pa.Table:
        """Get dataset as a :py:class:`pyarrow.Table`.

        Via :py:meth:`foundry_dev_tools.resources.dataset.Dataset.query_foundry_sql`
        """
        return self.query_foundry_sql("SELECT *", return_type="arrow")

    def to_pandas(self) -> pandas.core.frame.DataFrame:
        """Get dataset as a :py:class:`pandas.DataFrame`.

        Via :py:meth:`foundry_dev_tools.resources.dataset.Dataset.query_foundry_sql`
        """
        return self.query_foundry_sql("SELECT *", return_type="pandas")

    def to_polars(self) -> polars.dataframe.frame.DataFrame:
        """Get dataset as a :py:class:`polars.DataFrame`.

        Via :py:meth:`foundry_dev_tools.resources.dataset.Dataset.query_foundry_sql`
        """
        try:
            import polars as pl
        except ImportError as e:
            msg = "The optional 'polars' package is not installed. Please install it to use the 'to_polars' method"
            raise ImportError(msg) from e
        return pl.from_arrow(self.to_arrow())

    @contextmanager
    def transaction_context(
        self,
        transaction_type: api_types.FoundryTransaction | None = None,
        abort_on_error: bool = True,
    ):
        """Handles transactions for dataset functions.

        If there is no open transaction it will start one.
        If there is already an open transaction it will check if the transaction_type is correct.
        If this context manager started the transaction it will also commit or abort it (if abort_on_error is True).

        Args:
            abort_on_error: if an error happens while in transaction_context and this is set to true
                it will abort the transaction instead of committing it. Only takes effect if this
                context manager also started the transaction.
            transaction_type: if there is no open transaction it will open a transaction with this type,
                if there is already an open transaction and it does not match this transaction_type it will
                raise an Error

        """
        created = False

        # first check if there is already an open transaction
        if self._transaction is None:
            self._transaction = self.get_open_transaction()

        if self._transaction is None:
            self.start_transaction(transaction_type)
            created = True
        elif transaction_type is not None and self.transaction["type"] != transaction_type:
            raise TransactionTypeMismatchError(
                requested_transaction_type=transaction_type, open_transaction_type=self.transaction["type"]
            )

        try:
            yield
        except:
            if abort_on_error and created:
                self.abort_transaction()
            raise
        else:
            if created:
                self.commit_transaction()

    def sync(self) -> Self:
        """Fetches the attributes again + the dataset branch information."""
        # update branch information
        self.switch_branch(self.branch["id"])
        try:
            return super().sync()
        except ResourceNotFoundError as e:
            raise DatasetNotFoundError from e

    def _get_repr_dict(self) -> dict:
        d = super()._get_repr_dict()
        d["branch"] = self.branch["id"]
        return d


resource.RID_CLASS_REGISTRY[Dataset.rid_start] = Dataset
