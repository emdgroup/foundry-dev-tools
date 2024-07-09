"""Implementation of the foundry-sql-server API."""

from __future__ import annotations

import time
import warnings
from typing import TYPE_CHECKING, Literal, overload

from foundry_dev_tools.clients.api_client import APIClient
from foundry_dev_tools.errors.handling import ErrorHandlingConfig
from foundry_dev_tools.errors.sql import (
    FoundrySqlQueryClientTimedOutError,
    FoundrySqlQueryFailedError,
    FoundrySqlSerializationFormatNotImplementedError,
)
from foundry_dev_tools.utils.api_types import Ref, SqlDialect, SQLReturnType, assert_in_literal

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa
    import pyspark
    import requests


class FoundrySqlServerClient(APIClient):
    """FoundrySqlServerClient class that implements methods from the 'foundry-sql-server' API."""

    api_name = "foundry-sql-server"

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal["pandas"],
        branch: Ref = ...,
        sql_dialect: SqlDialect = ...,
        timeout: int = ...,
    ) -> pd.core.frame.DataFrame: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal["spark"],
        branch: Ref = ...,
        sql_dialect: SqlDialect = ...,
        timeout: int = ...,
    ) -> pyspark.sql.DataFrame: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal["arrow"],
        branch: Ref = ...,
        sql_dialect: SqlDialect = ...,
        timeout: int = ...,
    ) -> pa.Table: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal["raw"],
        branch: Ref = ...,
        sql_dialect: SqlDialect = ...,
        timeout: int = ...,
    ) -> tuple[dict, list[list]]: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: SQLReturnType = ...,
        branch: Ref = ...,
        sql_dialect: SqlDialect = ...,
        timeout: int = ...,
    ) -> tuple[dict, list[list]] | pd.core.frame.DataFrame | pa.Table | pyspark.sql.DataFrame: ...

    def query_foundry_sql(
        self,
        query: str,
        return_type: SQLReturnType = "pandas",
        branch: Ref = "master",
        sql_dialect: SqlDialect = "SPARK",
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
        if return_type != "raw":
            try:
                response_json = self.api_queries_execute(
                    query,
                    branch=branch,
                    dialect=sql_dialect,
                    timeout=timeout,
                ).json()
                query_id = response_json["queryId"]
                status = response_json["status"]

                if status != {"ready": {}, "type": "ready"}:
                    start_time = time.time()
                    query_id = response_json["queryId"]
                    while response_json["status"]["type"] == "running":
                        response = self.api_queries_status(query_id)
                        response_json = response.json()
                        if response_json["status"]["type"] == "failed":
                            raise FoundrySqlQueryFailedError(response)
                        if time.time() > start_time + timeout:
                            raise FoundrySqlQueryClientTimedOutError(response, timeout=timeout)
                        time.sleep(0.2)

                arrow_stream_reader = self.read_fsql_query_results_arrow(query_id=query_id)
                if return_type == "pandas":
                    return arrow_stream_reader.read_pandas()

                if return_type == "spark":
                    from foundry_dev_tools.utils.converter.foundry_spark import (
                        arrow_stream_to_spark_dataframe,
                    )

                    return arrow_stream_to_spark_dataframe(arrow_stream_reader)
                return arrow_stream_reader.read_all()

                return self._query_fsql(
                    query=query,
                    branch=branch,
                    return_type=return_type,
                )
            except (
                FoundrySqlSerializationFormatNotImplementedError,
                ImportError,
            ) as exc:
                if return_type == "arrow":
                    msg = (
                        "Only direct read eligible queries can be returned as arrow Table. Consider using setting"
                        " return_type to 'pandas'."
                    )
                    raise ValueError(
                        msg,
                    ) from exc

        warnings.warn("Falling back to query_foundry_sql_legacy!")
        return self.context.data_proxy.query_foundry_sql_legacy(
            query=query,
            return_type=return_type,
            branch=branch,
            sql_dialect=sql_dialect,
            timeout=timeout,
        )

    def read_fsql_query_results_arrow(self, query_id: str) -> pa.ipc.RecordBatchStreamReader:
        """Create a bytes io reader if query returned arrow."""
        from foundry_dev_tools._optional.pyarrow import pa

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
        response = self.api_queries_results(query_id)
        if response.content[0] != 65:  # 65 = "A"
            #     # Queries are direct read eligible when:
            #     # The dataset files are in a supported format.
            #     # The formats currently supported by direct read are Parquet, CSV, Avro, and Soho.

            #     # The query does not require SQL compute. Queries which contain aggregate, join, order by,
            #     # and filter predicates are not direct read eligible.

            #     # The query does not select from a column with a type that is ineligible for direct read.
            #     # Ineligible types are array, map, and struct.

            #     # May 2023: ARROW_V1 seems to consistently return ARROW format and not fallback to JSON.

            raise FoundrySqlSerializationFormatNotImplementedError(response)

        return pa.ipc.RecordBatchStreamReader(response.content[1:])

    def api_queries_execute(
        self,
        query: str,
        branch: Ref,
        dialect: SqlDialect = "SPARK",
        timeout: int = 600,
        **kwargs,
    ) -> requests.Response:
        """Queries the foundry sql server.

        Args:
            query: the SQL query
            branch: the dataset branch
            dialect: see :py:class:`foundry_dev_tools.utils.api_types.SqlDialect`
            timeout: the query timeout
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        assert_in_literal(dialect, SqlDialect, "dialect")

        return self.api_request(
            "POST",
            "queries/execute",
            json={
                "dialect": dialect,
                "fallbackBranchIds": [branch],
                "parameters": {"type": "unnamedParameterValues", "unnamedParameterValues": []},
                "query": query,
                "serializationProtocol": "ARROW_V1",
                "timeout": timeout,
            },
            error_handling=ErrorHandlingConfig(branch=branch, dialect=dialect, timeout=timeout),
            **kwargs,
        )

    def api_queries_status(
        self,
        query_id: str,
        **kwargs,
    ) -> requests.Response:
        """Get the foundry sql query status.

        Args:
            query_id: query id to get status
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "GET",
            f"queries/{query_id}/status",
            json={},
            **kwargs,
        )

    def api_queries_results(
        self,
        query_id: str,
        **kwargs,
    ) -> requests.Response:
        """Get query results.

        Args:
            query_id: query id to get results
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "GET",
            f"queries/{query_id}/results",
            headers={
                "Accept": "application/octet-stream",
            },
            **kwargs,
        )
