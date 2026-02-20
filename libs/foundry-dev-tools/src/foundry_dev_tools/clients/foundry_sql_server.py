"""Implementation of the foundry-sql-server API."""

from __future__ import annotations

import time
import warnings
from typing import TYPE_CHECKING, Any, Literal, overload

from foundry_dev_tools.clients.api_client import APIClient
from foundry_dev_tools.errors.handling import ErrorHandlingConfig
from foundry_dev_tools.errors.sql import (
    FoundrySqlQueryClientTimedOutError,
    FoundrySqlQueryFailedError,
    FoundrySqlSerializationFormatNotImplementedError,
)
from foundry_dev_tools.utils.api_types import (
    ArrowCompressionCodec,
    FurnaceSqlDialect,
    Ref,
    SqlDialect,
    SQLReturnType,
    assert_in_literal,
)

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
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
    ) -> pd.DataFrame: ...

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
        return_type: Literal["polars"],
        branch: Ref = ...,
        sql_dialect: SqlDialect = ...,
        timeout: int = ...,
    ) -> pl.DataFrame: ...

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
    ) -> tuple[dict, list[list]] | pd.DataFrame | pl.DataFrame | pa.Table | pyspark.sql.DataFrame: ...

    def query_foundry_sql(  # noqa: C901
        self,
        query: str,
        return_type: SQLReturnType = "pandas",
        branch: Ref = "master",
        sql_dialect: SqlDialect = "SPARK",
        timeout: int = 600,
    ) -> tuple[dict, list[list]] | pd.DataFrame | pl.DataFrame | pa.Table | pyspark.sql.DataFrame:
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
            :external+pandas:py:class:`~pandas.DataFrame` | :external+polars:py:class:`~polars.DataFrame` | :external+pyarrow:py:class:`~pyarrow.Table` | :external+spark:py:class:`~pyspark.sql.DataFrame`:

            A pandas, polars, Spark DataFrame or pyarrow.Table with the result.

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
                if return_type == "polars":
                    # The FakeModule implementation used in the _optional packages
                    # throws an ImportError when trying to access attributes of the module.
                    # This ImportError is caught below to fall back to query_foundry_sql_legacy
                    # which will again raise an ImportError when polars is not installed.
                    from foundry_dev_tools._optional.polars import pl

                    arrow_table = arrow_stream_reader.read_all()
                    return pl.from_arrow(arrow_table)

                if return_type == "spark":
                    from foundry_dev_tools.utils.converter.foundry_spark import (
                        arrow_stream_to_spark_dataframe,
                    )

                    return arrow_stream_to_spark_dataframe(arrow_stream_reader)
                return arrow_stream_reader.read_all()
            except (
                FoundrySqlSerializationFormatNotImplementedError,
                ImportError,
            ) as exc:
                # Swallow exception when return_type != 'arrow'
                # to fall back to query_foundry_sql_legacy
                if return_type == "arrow":
                    msg = (
                        "Only direct read eligible queries can be returned as arrow Table. Consider using setting"
                        " return_type to 'pandas'."
                    )
                    raise ValueError(
                        msg,
                    ) from exc

        # this fallback is not only used if return_type is 'raw', but also when one of
        # the above exceptions is caught and return_type != 'arrow'
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


class FoundrySqlServerClientV2(APIClient):
    """FoundrySqlServerClientV2 implements the newer foundry-sql-server API.

    This client uses a different API flow compared to V1:
    - Executes queries via POST to /api/ with applicationId and sql
    - Polls POST to /api/status for query completion
    - Retrieves results via POST to /api/stream with tickets
    """

    api_name = "foundry-sql-server"

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal["pandas"],
        branch: Ref = ...,
        sql_dialect: FurnaceSqlDialect = ...,
        arrow_compression_codec: ArrowCompressionCodec = ...,
        timeout: int = ...,
        experimental_use_trino: bool = ...,
    ) -> pd.core.frame.DataFrame: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal["polars"],
        branch: Ref = ...,
        sql_dialect: FurnaceSqlDialect = ...,
        arrow_compression_codec: ArrowCompressionCodec = ...,
        timeout: int = ...,
        experimental_use_trino: bool = ...,
    ) -> pl.DataFrame: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal["spark"],
        branch: Ref = ...,
        sql_dialect: FurnaceSqlDialect = ...,
        arrow_compression_codec: ArrowCompressionCodec = ...,
        timeout: int = ...,
        experimental_use_trino: bool = ...,
    ) -> pyspark.sql.DataFrame: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: Literal["arrow"],
        branch: Ref = ...,
        sql_dialect: FurnaceSqlDialect = ...,
        arrow_compression_codec: ArrowCompressionCodec = ...,
        timeout: int = ...,
        experimental_use_trino: bool = ...,
    ) -> pa.Table: ...

    @overload
    def query_foundry_sql(
        self,
        query: str,
        return_type: SQLReturnType = ...,
        branch: Ref = ...,
        sql_dialect: FurnaceSqlDialect = ...,
        arrow_compression_codec: ArrowCompressionCodec = ...,
        timeout: int = ...,
        experimental_use_trino: bool = ...,
    ) -> tuple[dict, list[list]] | pd.core.frame.DataFrame | pl.DataFrame | pa.Table | pyspark.sql.DataFrame: ...

    def query_foundry_sql(
        self,
        query: str,
        return_type: SQLReturnType = "pandas",
        branch: Ref = "master",
        sql_dialect: FurnaceSqlDialect = "SPARK",
        arrow_compression_codec: ArrowCompressionCodec = "NONE",
        timeout: int = 600,
        experimental_use_trino: bool = False,
    ) -> tuple[dict, list[list]] | pd.core.frame.DataFrame | pl.DataFrame | pa.Table | pyspark.sql.DataFrame:
        """Queries the Foundry SQL server using the V2 API.

        Uses Arrow IPC to communicate with the Foundry SQL Server Endpoint.

        Example:
            df = client.query_foundry_sql(
                query="SELECT * FROM `ri.foundry.main.dataset.abc` LIMIT 10"
            )

        Args:
            query: The SQL Query
            return_type: See :py:class:foundry_dev_tools.foundry_api_client.SQLReturnType
            branch: The dataset branch to query
            sql_dialect: The SQL dialect to use (only SPARK is supported for V2)
            arrow_compression_codec: Arrow compression codec (NONE, LZ4, ZSTD)
            timeout: Query timeout in seconds
            experimental_use_trino: If True, modifies the query to use Trino backend by adding /*+ backend(trino) */ hint

        Returns:
            :external+pandas:py:class:`~pandas.DataFrame` | :external+polars:py:class:`~polars.DataFrame` | :external+pyarrow:py:class:`~pyarrow.Table` | :external+spark:py:class:`~pyspark.sql.DataFrame`:

            A pandas DataFrame, polars, Spark DataFrame or pyarrow.Table with the result.

        Raises:
            FoundrySqlQueryFailedError: If the query fails
            FoundrySqlQueryClientTimedOutError: If the query times out

        """  # noqa: E501
        assert_in_literal(sql_dialect, FurnaceSqlDialect, "sql_dialect")

        if experimental_use_trino:
            query = query.replace("SELECT ", "SELECT /*+ backend(trino) */ ", 1)

        response_json = self.api_query(
            query=query, dialect=sql_dialect, branch=branch, arrow_compression_codec=arrow_compression_codec
        ).json()

        query_handle = self._extract_query_handle(response_json)
        start_time = time.time()

        # Poll for completion
        while response_json.get("status", {}).get("type") != "ready":
            time.sleep(0.2)
            response = self.api_status(query_handle)
            response_json = response.json()

            if response_json.get("status", {}).get("type") == "failed":
                raise FoundrySqlQueryFailedError(response)
            if time.time() > start_time + timeout:
                raise FoundrySqlQueryClientTimedOutError(response, timeout=timeout)

        # Extract tickets from successful response
        ticket = self._extract_ticket(response_json)

        # Fetch Arrow data using tickets
        arrow_stream_reader = self.read_stream_results_arrow(ticket)

        if return_type == "pandas":
            return arrow_stream_reader.read_pandas()

        if return_type == "polars":
            # The FakeModule implementation used in the _optional packages
            # throws an ImportError when trying to access attributes of the module.
            # This ImportError is caught below to fall back to query_foundry_sql_legacy
            # which will again raise an ImportError when polars is not installed.
            from foundry_dev_tools._optional.polars import pl

            arrow_table = arrow_stream_reader.read_all()
            return pl.from_arrow(arrow_table)

        if return_type == "spark":
            from foundry_dev_tools.utils.converter.foundry_spark import (
                arrow_stream_to_spark_dataframe,
            )

            return arrow_stream_to_spark_dataframe(arrow_stream_reader)

        if return_type == "arrow":
            return arrow_stream_reader.read_all()

        raise ValueError("The following return_type is not supported: " + return_type)

    def _extract_query_handle(self, response_json: dict[str, Any]) -> dict[str, Any]:
        """Extract query handle from execute response.

        Args:
            response_json: Response JSON from execute API


        Returns:
            Query handle dict

        """
        return response_json[response_json["type"]]["queryHandle"]

    def _extract_ticket(self, response_json: dict[str, Any]) -> dict[str, Any]:
        """Extract tickets from success response.

        Args:
            response_json: Success response JSON from status API

        Returns:
            List of tickets for fetching results

        """
        # we combine all tickets into one to get the full data
        # if performance is a concern this should be done in parallel
        return {
            "id": 0,
            "tickets": [
                ticket
                for ticket_group in response_json["status"]["ready"]["tickets"]
                for ticket in ticket_group["tickets"]
            ],
            "type": "furnace",
        }

    def read_stream_results_arrow(self, ticket: dict[str, Any]) -> pa.ipc.RecordBatchStreamReader:
        """Fetch query results using tickets and return Arrow stream reader.

        Args:
            ticket: dict of tickets e.g. { "id": 0, "tickets": ["ey...", ...], "type": "furnace", }

        Returns:
            Arrow RecordBatchStreamReader

        """
        from foundry_dev_tools._optional.pyarrow import pa

        response = self.api_stream_ticket(ticket)
        response.raw.decode_content = True

        return pa.ipc.RecordBatchStreamReader(response.raw)

    def api_query(
        self,
        query: str,
        dialect: FurnaceSqlDialect,
        branch: Ref,
        arrow_compression_codec: ArrowCompressionCodec = "NONE",
        **kwargs,
    ) -> requests.Response:
        """Execute a SQL query via the V2 API.

        Args:
            query: The SQL query string
            dialect: The SQL dialect to use (only SPARK is supported)
            branch: The dataset branch to query
            arrow_compression_codec: Arrow compression codec (NONE, LZ4, ZSTD)
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            Response with query handle and initial status

        """
        return self.api_request(
            "POST",
            "sql-endpoint/v1/queries/query",
            json={
                "querySpec": {
                    "query": query,
                    "tableProviders": {},
                    "dialect": dialect,
                    "options": {"options": [{"option": "arrowCompressionCodec", "value": arrow_compression_codec}]},
                },
                "executionParams": {
                    "defaultBranchIds": [{"type": "datasetBranch", "datasetBranch": branch}],
                    "resultFormat": "ARROW",
                    "resultMode": "AUTO",
                },
            },
            **kwargs,
        )

    def api_status(
        self,
        query_handle: dict[str, Any],
        **kwargs,
    ) -> requests.Response:
        """Get the status of a SQL query via the V2 API.

        Args:
            query_handle: Query handle dict from execute response
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            Response with query status

        """
        return self.api_request(
            "POST",
            "sql-endpoint/v1/queries/status",
            json=query_handle,
            **kwargs,
        )

    def api_stream_ticket(
        self,
        ticket: dict,
        **kwargs,
    ) -> requests.Response:
        """Stream query results using a ticket via the V2 API.

        Args:
            ticket: Ticket dict containing id, tickets list, and type.
                Example: {"id": 0, "tickets": ["eyJhbGc...", "eyJhbGc..."], "type": "furnace"}
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            Response with streaming Arrow data

        """
        return self.api_request(
            "POST",
            "sql-endpoint/v1/queries/stream",
            json=ticket,
            headers={
                "Accept": "application/octet-stream",
            },
            stream=True,
            **kwargs,
        )
