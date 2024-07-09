"""The DataProxy API client."""

from __future__ import annotations

import shutil
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from itertools import repeat
from typing import IO, TYPE_CHECKING, AnyStr, Literal, overload
from urllib.parse import quote, quote_plus

from foundry_dev_tools.clients.api_client import APIClient
from foundry_dev_tools.errors.handling import ErrorHandlingConfig
from foundry_dev_tools.errors.meta import FoundryAPIError
from foundry_dev_tools.utils.api_types import (
    DatasetRid,
    PathInDataset,
    Ref,
    SqlDialect,
    SQLReturnType,
    TransactionRid,
    View,
    assert_in_literal,
)

if TYPE_CHECKING:
    from pathlib import Path

    import pandas as pd
    import pyarrow as pa
    import pyspark
    import requests


class DataProxyClient(APIClient):
    """DataProxyClient class that implements methods from the 'foundry-data-proxy' API."""

    api_name = "foundry-data-proxy"

    def upload_dataset_file(
        self,
        dataset_rid: DatasetRid,
        transaction_rid: TransactionRid,
        path: Path,
        path_in_foundry_dataset: PathInDataset,
    ) -> requests.Response:
        """Uploads a file into a foundry dataset.

        Args:
            dataset_rid: Unique identifier of the dataset
            transaction_rid: transaction id
            path: File to upload
            path_in_foundry_dataset: The destination path in the dataset

        """
        # when changing code, check Dataset.upload_file, too
        with path.open("rb") as file_data:
            return self.api_put_file(
                dataset_rid,
                transaction_rid,
                path_in_foundry_dataset,
                file_data,
            )

    def upload_dataset_files(
        self,
        dataset_rid: DatasetRid,
        transaction_rid: TransactionRid,
        path_file_dict: dict[PathInDataset, Path],
        max_workers: int | None = None,
    ):
        """Uploads multiple local files to a foundry dataset.

        Args:
            dataset_rid: dataset rid
            transaction_rid: transaction id
            max_workers: Set number of threads for upload
            path_file_dict: A dictionary with the following structure:

        .. code-block:: python

         {
         '<path_in_foundry_dataset>': '<local_file_path>',
         ...
         }

        """
        if (max_workers is not None and max_workers <= 1) or len(path_file_dict) <= 1:
            for key, value in path_file_dict.items():
                self.upload_dataset_file(
                    dataset_rid,
                    transaction_rid,
                    path=value,
                    path_in_foundry_dataset=key,
                )
        else:
            with ThreadPoolExecutor(
                max_workers=max_workers,
            ) as pool:
                for future in [
                    pool.submit(self.upload_dataset_file, dataset_rid, transaction_rid, v, k)
                    for k, v in path_file_dict.items()
                ]:
                    future.result()

    @overload
    def query_foundry_sql_legacy(
        self,
        query: str,
        return_type: Literal["pandas"],
        branch: Ref = ...,
        sql_dialect: SqlDialect = ...,
        timeout: int = ...,
    ) -> pd.core.frame.DataFrame: ...

    @overload
    def query_foundry_sql_legacy(
        self,
        query: str,
        return_type: Literal["spark"],
        branch: Ref = ...,
        sql_dialect: SqlDialect = ...,
        timeout: int = ...,
    ) -> pyspark.sql.DataFrame: ...

    @overload
    def query_foundry_sql_legacy(
        self,
        query: str,
        return_type: Literal["arrow"],
        branch: Ref = ...,
        sql_dialect: SqlDialect = ...,
        timeout: int = ...,
    ) -> pa.Table: ...

    @overload
    def query_foundry_sql_legacy(
        self,
        query: str,
        return_type: Literal["raw"],
        branch: Ref = ...,
        sql_dialect: SqlDialect = ...,
        timeout: int = ...,
    ) -> tuple[dict, list[list]]: ...

    @overload
    def query_foundry_sql_legacy(
        self,
        query: str,
        return_type: SQLReturnType = ...,
        branch: Ref = ...,
        sql_dialect: SqlDialect = ...,
        timeout: int = ...,
    ) -> tuple[dict, list[list]] | pd.core.frame.DataFrame | pa.Table | pyspark.sql.DataFrame: ...

    def query_foundry_sql_legacy(
        self,
        query: str,
        return_type: SQLReturnType = "raw",
        branch: Ref = "master",
        sql_dialect: SqlDialect = "SPARK",
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
        assert_in_literal(return_type, SQLReturnType, "return_type")

        response = self.api_query_with_fallbacks2(
            query,
            [branch],
            sql_dialect,
            error_handling=ErrorHandlingConfig(branch=branch),
            timeout=timeout,
        )

        response_json = response.json()
        if return_type == "raw":
            return response_json["foundrySchema"], response_json["rows"]
        if return_type == "pandas":
            from foundry_dev_tools._optional.pandas import pd

            return pd.DataFrame(
                data=response_json["rows"],
                columns=[e["name"] for e in response_json["foundrySchema"]["fieldSchemaList"]],
            )
        if return_type == "arrow":
            from foundry_dev_tools._optional.pyarrow import pa

            return pa.table(
                data=response_json["rows"],
                names=[e["name"] for e in response_json["foundrySchema"]["fieldSchemaList"]],
            )
        if return_type == "spark":
            from foundry_dev_tools.utils.converter.foundry_spark import (
                foundry_schema_to_spark_schema,
                foundry_sql_data_to_spark_dataframe,
            )

            return foundry_sql_data_to_spark_dataframe(
                data=response_json["rows"],
                spark_schema=foundry_schema_to_spark_schema(
                    response_json["foundrySchema"],
                ),
            )
        return None

    def download_dataset_file(
        self,
        dataset_rid: DatasetRid,
        output_directory: Path,
        foundry_file_path: PathInDataset,
        view: View = "master",
    ) -> Path:
        """Downloads a single foundry dataset file into a directory.

        The local folder (and its parents) will be created if it does not exist.

        If you want the bytes instead of downloading it to a file
        use :py:meth:`DataProxyClient.api_get_file_in_view` directly.

        Args:
            dataset_rid: the dataset rid
            output_directory: the local output directory for the file
            foundry_file_path: the file_path on the foundry file system
            view: branch or transaction rid of the dataset

        Returns:
            Path: local file path
        """
        local_path = output_directory.joinpath(foundry_file_path)
        local_path.parent.mkdir(exist_ok=True, parents=True)
        resp = self.api_get_file_in_view(
            dataset_rid,
            view,
            foundry_file_path,
            stream=True,
        )
        with local_path.open(mode="wb+") as out_file:
            resp.raw.decode_content = True
            shutil.copyfileobj(resp.raw, out_file)

        return local_path

    def download_dataset_files(
        self,
        dataset_rid: DatasetRid,
        output_directory: Path,
        files: set[PathInDataset] | None = None,
        view: View = "master",
        max_workers: int | None = None,
    ) -> list[Path]:
        """Downloads files of a dataset (in parallel) to a local output directory.

        Args:
            dataset_rid: the dataset rid
            files: list of files or None, in which case all files are downloaded
            output_directory: the output directory for the files
            view: branch or transaction rid of the dataset
            max_workers: Set number of threads for upload

        Returns:
            list[str]: path to downloaded files
        """
        if files is None:
            file_resources = self.context.catalog.list_dataset_files(
                dataset_rid=dataset_rid,
                exclude_hidden_files=True,
                end_ref=view,
            )
            files = {fr["logicalPath"] for fr in file_resources}
        if len(files) == 0:
            msg = f"Dataset {dataset_rid} does not contain any files to download."
            raise FoundryAPIError(info=msg)
        if max_workers == 1 or len(files) == 1:
            local_paths = [self.download_dataset_file(dataset_rid, output_directory, file, view) for file in files]
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                local_paths = list(
                    pool.map(
                        partial(self.download_dataset_file, dataset_rid, output_directory),
                        files,
                        repeat(view),
                    ),
                )

        return list(filter(lambda p: p != "", local_paths))

    def api_put_file(
        self,
        dataset_rid: DatasetRid,
        transaction_rid: TransactionRid,
        logical_path: PathInDataset,
        file_data: str | bytes | IO[AnyStr],
        overwrite: bool | None = None,
        **kwargs,
    ) -> requests.Response:
        """Opens, writes, and closes a file under the specified dataset and transaction.

        Args:
            dataset_rid: dataset rid
            transaction_rid: transaction rid
            logical_path: file path in dataset
            file_data: content of the file
            overwrite: defaults to false, if true -> Overwrite the file if it already exists in the transaction.
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        # when changing code, check Dataset.put_file, too
        params = {"logicalPath": logical_path}
        if overwrite is not None:
            params["overwrite"] = overwrite  # type: ignore[assignment]
        return self.api_request(
            "POST",
            f"dataproxy/datasets/{dataset_rid}/transactions/{transaction_rid}/putFile",
            headers={"Content-Type": "application/octet-stream"},
            params=params,
            data=file_data,
            error_handling=ErrorHandlingConfig(
                info="Issue while uploading file to dataset",
                dataset_rid=dataset_rid,
                transaction_rid=transaction_rid,
                logical_path=logical_path,
            ),
            **kwargs,
        )

    def api_get_file(
        self,
        dataset_rid: DatasetRid,
        transaction_rid: TransactionRid,
        logical_path: PathInDataset,
        range_header: str | None = None,
        requests_stream: bool = True,
        **kwargs,
    ) -> requests.Response:
        """Returns a file from the specified dataset and transaction.

        Args:
            dataset_rid: dataset rid
            transaction_rid: transaction rid
            logical_path: path in dataset
            range_header: HTTP range header
            requests_stream: passed to :py:meth:`requests.Session.request` as `stream`
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "GET",
            f"dataproxy/datasets/{dataset_rid}/transactions/{transaction_rid}/{quote(logical_path)}",
            headers={"Range": range_header} if range_header else None,
            stream=requests_stream,
            **kwargs,
        )

    def api_get_file_in_view(
        self,
        dataset_rid: DatasetRid,
        end_ref: View,
        logical_path: PathInDataset,
        start_transaction_rid: TransactionRid | None = None,
        range_header: str | None = None,
        **kwargs,
    ) -> requests.Response:
        """Returns a file from the specified dataset and end ref.

        Args:
            dataset_rid: dataset rid
            end_ref: end ref/view
            logical_path: PathInDataset
            start_transaction_rid: start transaction rid
            range_header: HTTP range header
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "GET",
            f"dataproxy/datasets/{dataset_rid}/views/{quote_plus(end_ref)}/{quote(logical_path)}",
            params={"startTransactionRid": start_transaction_rid} if start_transaction_rid else None,
            headers={"Range": range_header} if range_header else None,
            **kwargs,
        )

    def api_get_files(
        self,
        dataset_rid: DatasetRid,
        transaction_rid: TransactionRid,
        logical_paths: set[PathInDataset],
        requests_stream: bool = True,
        **kwargs,
    ) -> requests.Response:
        """Returns specified files as a zip archive.

        If logical_paths is an empty set, it will return all files of the transaction.

        Args:
            dataset_rid: dataset rid
            transaction_rid: transaction rid
            logical_paths: a set with paths in the dataset
            requests_stream: passed to :py:meth:`requests.Session.request` as `stream`
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "GET",
            f"dataproxy/batch/datasets/{dataset_rid}/transactions/{transaction_rid}",
            params={"logicalPaths": logical_paths},
            stream=requests_stream,
            **kwargs,
        )

    def api_get_files_in_view(
        self,
        dataset_rid: DatasetRid,
        end_ref: View,
        logical_paths: set[PathInDataset],
        start_transaction_rid: TransactionRid | None = None,
        stream: bool = True,
        **kwargs,
    ) -> requests.Response:
        """Returns specified files by logical_paths and end_ref in a zip archive.

        Args:
            dataset_rid: dataset rid
            end_ref: end ref/view
            logical_paths: set of paths in the dataset
            start_transaction_rid: transaction rid
            stream: passed to :py:meth:`requests.Session.request`
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        if start_transaction_rid:
            params = {
                "logicalPaths": logical_paths,
                "startTransactionRid": start_transaction_rid,
            }
        else:
            params = {"logicalPaths": logical_paths}
        return self.api_request(
            "GET",
            f"dataproxy/batch/datasets/{dataset_rid}/views/{quote_plus(end_ref)}",
            params=params,
            stream=stream,
            **kwargs,
        )

    def api_get_dataset_as_csv2(
        self,
        dataset_rid: DatasetRid,
        branch_id: Ref,
        start_transaction_rid: TransactionRid | None = None,
        end_transaction_rid: TransactionRid | None = None,
        include_column_names: bool = True,
        include_bom: bool = True,
        **kwargs,
    ) -> requests.Response:
        """Gets dataset data with each record as a CSV line.

        Args:
            dataset_rid: the dataset rid
            branch_id: branch of the dataset
            start_transaction_rid: start transaction rid
            end_transaction_rid: end transaction rid
            include_column_names: include column names
            include_bom: include bom
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            :external+requests:py:class:`~requests.Response`:
                with the csv stream.
                Can be converted to a pandas DataFrame
                >>> pd.read_csv(io.BytesIO(response.content))
        """
        params = {"includeColumnNames": include_column_names, "includeBom": include_bom}
        if start_transaction_rid is not None:
            params = {"startTransactionRid": start_transaction_rid, **params}
        if end_transaction_rid is not None:
            params = {"endTransactionRid": end_transaction_rid, **params}
        return self.api_request(
            "GET",
            f"dataproxy/datasets/{dataset_rid}/branches/{quote_plus(branch_id)}/csv2",
            params=params,
            stream=True,
            **kwargs,
        )

    def api_query_with_fallbacks2(
        self,
        query: str,
        fallback_branch_ids: list[str],
        dialect: SqlDialect = "SPARK",
        **kwargs,
    ) -> requests.Response:
        """Queries for data from 1 or more tables and returns the results as JSON.

        Args:
            query: the SQL query
            fallback_branch_ids: fallback branch ids
            dialect: the SqlDialect of the query, see :py:meth:`foundry_dev_tools.utils.api_types.SqlDialect`
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        assert_in_literal(dialect, SqlDialect, "dialect")

        return self.api_request(
            "POST",
            "dataproxy/queryWithFallbacks2",
            params={"fallbackBranchIds": fallback_branch_ids},
            json={"query": query, "dialect": dialect},
            **kwargs,
        )
