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
    DatasetHasNoOpenTransactionError,
)
from foundry_dev_tools.resources import resource
from foundry_dev_tools.utils import api_types

if TYPE_CHECKING:
    from typing import IO, AnyStr, Literal

    import pandas as pd
    import pandas.core.frame
    import pyarrow as pa
    import pyspark.sql

    from foundry_dev_tools.config.context import FoundryContext


class Dataset(resource.Resource):
    """Helper class for datasets."""

    branch: str
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

    @classmethod
    def from_rid(
        cls,
        context: FoundryContext,
        rid: api_types.Rid,
        /,
        *,
        branch: api_types.Ref = "master",
        **kwargs,
    ) -> Dataset:
        """Returns dataset at path.

        Args:
            context: the foundry context for the dataset
            rid: the rid of the dataset
            branch: the branch of the dataset
            **kwargs: passed to :py:meth:`foundry_dev_tools.resources.resource.Resource.from_rid`
        """
        inst = super().from_rid(context, rid, **kwargs)
        inst.branch = branch
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
        **kwargs,
    ) -> Dataset:
        """Returns dataset at path.

        Args:
            context: the foundry context for the dataset
            path: the path where the dataset is located on foundry
            branch: the branch of the dataset
            create_if_not_exist: if the dataset does not exist, create it
            **kwargs: passed to :py:meth:`foundry_dev_tools.resources.resource.Resource.from_path`
        """
        try:
            inst = super().from_path(context, path, **kwargs)
            inst.branch = branch
            inst.__created__ = False
        except ResourceNotFoundError:
            if create_if_not_exist:
                return cls.create(context, path, branch)
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
        context.catalog.api_create_branch(
            rid,
            branch,
            parent_ref=parent_ref,
            parent_branch_id=parent_branch_id,
        )

        inst = cls.from_rid(context, rid, branch=branch)
        inst.__created__ = True
        return inst

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
                self.branch,
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

    def start_transaction(self, start_transaction_type: api_types.FoundryTransaction | None = None):
        """Start a transaction on the dataset."""
        self._transaction = self._context.catalog.api_start_transaction(
            self.rid,
            self.branch,
            record={},
            start_transaction_type=start_transaction_type,
        ).json()
        if start_transaction_type is not None and start_transaction_type is not api_types.FoundryTransaction.APPEND:
            self._context.catalog.api_set_transaction_type(
                self.rid,
                self.transaction["rid"],
                start_transaction_type,
            )

    @property
    def transaction(self) -> api_types.Transaction:
        """Get the current transaction or raise Error if there is no open transaction."""
        if self._transaction is not None:
            return self._transaction  # type: ignore[return-value]
        self._transaction = self.get_open_transaction()
        if self._transaction is not None:
            return self._transaction
        raise DatasetHasNoOpenTransactionError(f"{self.path=} {self.rid=}")  # noqa: EM102,TRY003

    def commit_transaction(self):
        """Commit the transaction on the dataset."""
        if self.transaction is not None:
            self._context.catalog.api_commit_transaction(self.rid, self.transaction["rid"])
            self._transaction = None

    def abort_transaction(self):
        """Commit the transaction on the dataset."""
        if self.transaction is not None:
            self._context.catalog.api_abort_transaction(self.rid, self.transaction["rid"])
            self._transaction = None

    def upload_files(
        self,
        path_file_dict: dict[api_types.PathInDataset, Path],
        max_workers: int | None = None,
        **kwargs,
    ) -> api_types.Transaction:
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
            return self.transaction

    def upload_file(
        self,
        file_path: Path,
        path_in_foundry_dataset: api_types.PathInDataset,
        **kwargs,
    ) -> api_types.Transaction:
        """Upload a file to the dataset.

        Args:
            file_path: local file path to upload
            path_in_foundry_dataset: file path inside the foundry dataset
            transaction_type: if this dataset does not have an open transaction,
                opens a transaction with the specified type.
            start_transaction: if true, a transaction will be created if no transaction is currently open
            finish_transaction: if true, the transaction will be closed (committed or aborted) after the upload.
            **kwargs: get passed to :py:meth:`foundry_dev_tools.resources.dataset.Dataset.transaction_context`
        """
        with self.transaction_context(**kwargs):
            self._context.data_proxy.upload_dataset_file(
                dataset_rid=self.rid,
                transaction_rid=self.transaction["rid"],
                path=file_path,
                path_in_foundry_dataset=path_in_foundry_dataset,
            )
            return self.transaction

    def upload_folder(self, folder_path: Path, **kwargs) -> api_types.Transaction:
        """Uploads all files contained in the folder to the dataset.

        The default transaction type is UPDATE.

        Args:
            folder_path: the folder to upload
            **kwargs: passed to :py:meth:`foundry_dev_tools.resources.dataset.Dataset.upload_files`

        Returns:
            the transaction used for uploading the files
        """
        kwargs.setdefault("transaction_type", api_types.FoundryTransaction.UPDATE)
        return self.upload_files({str(f.relative_to(folder_path)): f for f in folder_path.rglob("*")}, **kwargs)

    def put_file(
        self,
        logical_path: api_types.PathInDataset,
        file_data: str | bytes | IO[AnyStr],
        overwrite: bool | None = None,
        **kwargs,
    ) -> api_types.Transaction:
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
            return self.transaction

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
            view=self.branch,
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
            end_ref=self.branch,
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
            view=self.branch,
        )

    @contextmanager
    def download_files_temporary(
        self,
        paths_in_dataset: list[api_types.PathInDataset] | None = None,
        max_workers: int | None = None,
    ) -> iter[Path]:
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
        df: pandas.core.frame.DataFrame | pyspark.sql.DataFrame,
        transaction_type: api_types.FoundryTransaction = api_types.FoundryTransaction.SNAPSHOT,
        foundry_schema: api_types.FoundrySchema | None = None,
    ) -> api_types.Transaction:
        """Saves a dataframe to Foundry. If the dataset in Foundry does not exist it is created.

        If the branch does not exist, it is created. If the dataset exists, an exception is thrown.
        If exists_ok=True is passed, the dataset is overwritten.
        Creates SNAPSHOT transactions by default.

        Args:
            df: A pyspark  or pandas DataFrame to upload
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

            # TODO needed?
            # to be backwards compatible to most readers, that expect files
            # to be under spark/
            folder = str(round(time.time() * 1000)) if transaction_type == "APPEND" else "spark"
            if not pd.__fake__ and isinstance(df, pd.DataFrame):
                buf = io.BytesIO()
                df.to_parquet(
                    buf,
                    compression="snappy",
                    flavor="spark",
                    index=False,
                )
                buf.seek(0)  # go to beginning of file
                self.put_file(
                    folder + "/dataset.parquet",
                    buf,
                    start_transaction=False,
                    finish_transaction=False,
                )
            else:
                with tempfile.TemporaryDirectory() as path:
                    p_path = Path(path)
                    df.write.format("parquet").option("compression", "snappy").save(
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
        return transaction

    def infer_schema(self) -> dict:
        """Returns the infered dataset schema."""
        return self._context.schema_inference.infer_dataset_schema(self.rid, self.branch)

    def upload_schema(self, transaction_rid: api_types.TransactionRid, schema: api_types.FoundrySchema):
        """Uploads the foundry dataset schema for a dataset, transaction, branch combination.

        Args:
            transaction_rid: The rid of the transaction
            schema: The foundry schema
        """
        self._context.metadata.api_upload_dataset_schema(
            dataset_rid=self.rid,
            transaction_rid=transaction_rid,
            schema=schema,
            branch=self.branch,
        )

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal[api_types.SQLReturnType.PANDAS],
        sql_dialect: api_types.SqlDialect = ...,
        timeout: int = ...,
    ) -> pd.core.frame.DataFrame: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal[api_types.SQLReturnType.SPARK],
        sql_dialect: api_types.SqlDialect = ...,
        timeout: int = ...,
    ) -> pyspark.sql.DataFrame: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal[api_types.SQLReturnType.ARROW],
        sql_dialect: api_types.SqlDialect = ...,
        timeout: int = ...,
    ) -> pa.Table: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal[api_types.SQLReturnType.RAW],
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
        return_type: api_types.SQLReturnType = api_types.SQLReturnType.PANDAS,
        sql_dialect: api_types.SqlDialect = api_types.SqlDialect.SPARK,
        timeout: int = 600,
    ) -> tuple[dict, list[list]] | pd.DataFrame | pa.Table | pyspark.sql.DataFrame:
        """Wrapper around :py:meth:`foundry_dev_tools.clients.foundry_sql_server.FoundrySqlServerClient.query_foundry_sql`.

        But it automatically prepends the dataset location, so instead of:
        >>> ctx.foundry_sql_server.query_foundry_sql("SELECT * FROM `/path/to/dataset` WHERE a=1")
        You can just use:
        >>> ds = ctx.get_dataset_by_path("/path/to/dataset")
        >>> ds.query_foundry_sql("SELECT * WHERE a=1")
        """  # noqa: E501
        return self._context.foundry_sql_server.query_foundry_sql(
            f"FROM `{self.rid}` {query}",
            return_type=return_type,
            branch=self.branch,
            sql_dialect=sql_dialect,
            timeout=timeout,
        )

    def to_spark(self) -> pyspark.sql.DataFrame:
        """Get dataset as :py:class:`pyspark.sql.DataFrame`.

        Via :py:meth:`foundry_dev_tools.resources.dataset.Dataset.query_foundry_sql`
        """
        return self.query_foundry_sql("SELECT *", return_type=api_types.SQLReturnType.SPARK)

    def to_arrow(self) -> pa.Table:
        """Get dataset as a :py:class:`pyarrow.Table`.

        Via :py:meth:`foundry_dev_tools.resources.dataset.Dataset.query_foundry_sql`
        """
        return self.query_foundry_sql("SELECT *", return_type=api_types.SQLReturnType.ARROW)

    def to_pandas(self) -> pandas.core.frame.DataFrame:
        """Get dataset as a :py:class:`pandas.DataFrame`.

        Via :py:meth:`foundry_dev_tools.resources.dataset.Dataset.query_foundry_sql`
        """
        return self.query_foundry_sql("SELECT *", return_type=api_types.SQLReturnType.PANDAS)

    @contextmanager
    def transaction_context(
        self,
        start_transaction: bool = True,
        finish_transaction: bool = True,
        transaction_type: api_types.FoundryTransaction | None = None,
    ):
        """Handles transactions for dataset functions.

        This function deduplicates code for multiple functions used in this class.
        The sole purpose is to have a unified context manager
        that handles transactions in the way specified.

        If start_transaction is True it will start a transaction and throw an error if there is already an transaction.
        If start_transaction is False it will get the current open transaction and throw an error if there is none open.
        If finish_transaction is True it will abort the transaction if an exception occurs, otherwise it will commit it.
        If finish_transaction is False it will leave the transaction open for further use.


        Args:
            start_transaction: start transaction if true, otherwise get open transaction
            finish_transaction: abort/commit the transaction if true, otherwise leave the transaction open
            transaction_type: if start_transaction is True, it will use this transaction_type

        """
        if start_transaction:
            self.start_transaction(transaction_type)
        elif self.transaction is not None:
            # there is an open transaction
            # otherwise this call above will throw an error
            pass

        try:
            yield
        except:
            if finish_transaction:
                self.abort_transaction()
            raise
        else:
            if finish_transaction:
                self.commit_transaction()


resource.RID_CLASS_REGISTRY[Dataset.rid_start] = Dataset
