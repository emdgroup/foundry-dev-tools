import polars as pl
import pytest

from foundry_dev_tools.errors.dataset import BranchNotFoundError, DatasetHasNoSchemaError, DatasetNotFoundError
from foundry_dev_tools.errors.sql import (
    FoundrySqlQueryFailedError,
    FoundrySqlSerializationFormatNotImplementedError,
    FurnaceSqlSqlParseError,
)
from tests.integration.conftest import TEST_SINGLETON


def test_smoke():
    one_row_one_column = TEST_SINGLETON.ctx.foundry_sql_server.query_foundry_sql(
        f"SELECT sepal_width FROM `{TEST_SINGLETON.iris_new.rid}` LIMIT 1",
    )
    assert one_row_one_column.shape == (1, 1)


def test_polars_return_type():
    polars_df = TEST_SINGLETON.ctx.foundry_sql_server.query_foundry_sql(
        f"SELECT sepal_length FROM `{TEST_SINGLETON.iris_new.rid}` LIMIT 2",
        return_type="polars",
    )
    assert isinstance(polars_df, pl.DataFrame)
    assert polars_df.height == 2
    assert polars_df.width == 1


def test_exceptions():
    with pytest.raises(BranchNotFoundError) as exc:
        TEST_SINGLETON.ctx.foundry_sql_server.query_foundry_sql(
            f"SELECT * FROM `{TEST_SINGLETON.iris_new.rid}` LIMIT 100",
            branch="doesNotExist",
        )
    assert exc.value.dataset_rid == TEST_SINGLETON.iris_new.rid
    assert exc.value.branch == "doesNotExist"

    with pytest.raises(DatasetHasNoSchemaError):
        TEST_SINGLETON.ctx.foundry_sql_server.query_foundry_sql(
            f"SELECT * FROM `{TEST_SINGLETON.iris_no_schema.rid}` LIMIT 100",
        )

    with pytest.raises(DatasetNotFoundError) as exc:
        TEST_SINGLETON.ctx.foundry_sql_server.query_foundry_sql(
            "SELECT * FROM `/namespace1/does-not_exists/` LIMIT 100",
        )
    assert exc.value.path == "/namespace1/does-not_exists/"

    with pytest.raises(BranchNotFoundError) as exc:
        TEST_SINGLETON.ctx.foundry_sql_server.query_foundry_sql(
            "SELECT * FROM `ri.foundry.main.dataset.1337fb9d-1234-43c7-b83f-768f2b843b94` LIMIT 100",
        )
    assert exc.value.dataset_rid == "ri.foundry.main.dataset.1337fb9d-1234-43c7-b83f-768f2b843b94"
    assert exc.value.branch == "master"

    with pytest.raises(FoundrySqlQueryFailedError):
        TEST_SINGLETON.ctx.foundry_sql_server.query_foundry_sql(
            f"SELECT foo, bar, FROM `{TEST_SINGLETON.iris_new.rid}` LIMIT 100",
        )


def test_legacy_fallback(mocker):
    mocker.patch(
        "foundry_dev_tools.clients.foundry_sql_server.FoundrySqlServerClient.read_fsql_query_results_arrow",
    ).side_effect = FoundrySqlSerializationFormatNotImplementedError()
    query_foundry_sql_legacy_spy = mocker.patch(
        "foundry_dev_tools.clients.data_proxy.DataProxyClient.query_foundry_sql_legacy",
    )

    TEST_SINGLETON.ctx.foundry_sql_server.query_foundry_sql(f"SELECT * FROM `{TEST_SINGLETON.iris_new.rid}`")

    query_foundry_sql_legacy_spy.assert_called()


# V2 Client Tests


def test_v2_smoke():
    """Test basic V2 client functionality with a simple query."""
    one_row_one_column = TEST_SINGLETON.ctx.foundry_sql_server_v2.query_foundry_sql(
        query=f"SELECT sepal_width FROM `{TEST_SINGLETON.iris_new.rid}` LIMIT 1",
        application_id=TEST_SINGLETON.iris_new.rid,
    )
    assert one_row_one_column.shape == (1, 1)

    one_row_one_column = TEST_SINGLETON.ctx.foundry_sql_server_v2.query_foundry_sql(
        query=f"SELECT sepal_width FROM `{TEST_SINGLETON.iris_new.rid}` LIMIT 1",
        application_id=TEST_SINGLETON.iris_new.rid,
        return_type="arrow",
    )
    assert one_row_one_column.num_columns == 1
    assert one_row_one_column.num_rows == 1
    assert one_row_one_column.column_names == ["sepal_width"]


def test_v2_multiple_rows():
    """Test V2 client with multiple rows."""
    result = TEST_SINGLETON.ctx.foundry_sql_server_v2.query_foundry_sql(
        query=f"SELECT * FROM `{TEST_SINGLETON.iris_new.rid}` LIMIT 10",
        application_id=TEST_SINGLETON.iris_new.rid,
    )
    assert result.shape[0] == 10
    assert result.shape[1] == 5  # iris dataset has 5 columns


def test_v2_return_type_arrow():
    """Test V2 client with Arrow return type."""
    import pyarrow as pa

    result = TEST_SINGLETON.ctx.foundry_sql_server_v2.query_foundry_sql(
        query=f"SELECT * FROM `{TEST_SINGLETON.iris_new.rid}` LIMIT 5",
        application_id=TEST_SINGLETON.iris_new.rid,
        return_type="arrow",
    )
    assert isinstance(result, pa.Table)
    assert result.num_rows == 5


def test_v2_return_type_raw_not_supported():
    """Test V2 client with raw return type."""
    with pytest.raises(ValueError, match="The following return_type is not supported: .+"):
        schema, rows = TEST_SINGLETON.ctx.foundry_sql_server_v2.query_foundry_sql(
            query=f"SELECT sepal_width, sepal_length FROM `{TEST_SINGLETON.iris_new.rid}` LIMIT 3",
            application_id=TEST_SINGLETON.iris_new.rid,
            return_type="raw",
        )


def test_v2_aggregation_query():
    """Test V2 client with aggregation query."""
    result = TEST_SINGLETON.ctx.foundry_sql_server_v2.query_foundry_sql(
        query=f"""
        SELECT
            COUNT(*) as total_count,
            AVG(sepal_width) as avg_sepal_width
        FROM `{TEST_SINGLETON.iris_new.rid}`
        """,
        application_id=TEST_SINGLETON.iris_new.rid,
    )
    assert result.shape == (1, 2)
    assert "total_count" in result.columns
    assert "avg_sepal_width" in result.columns


def test_v2_query_failed():
    """Test V2 client with invalid SQL query."""
    with pytest.raises(FurnaceSqlSqlParseError):
        TEST_SINGLETON.ctx.foundry_sql_server_v2.query_foundry_sql(
            query=f"SELECT foo, bar, FROM `{TEST_SINGLETON.iris_new.rid}` LIMIT 100",
            application_id=TEST_SINGLETON.iris_new.rid,
        )


def test_v2_disable_arrow_compression():
    """Test V2 client with arrow compression disabled."""
    result = TEST_SINGLETON.ctx.foundry_sql_server_v2.query_foundry_sql(
        query=f"SELECT * FROM `{TEST_SINGLETON.iris_new.rid}` LIMIT 5",
        application_id=TEST_SINGLETON.iris_new.rid,
        disable_arrow_compression=True,
    )
    assert result.shape[0] == 5


def test_v2_with_where_clause():
    """Test V2 client with WHERE clause."""
    result = TEST_SINGLETON.ctx.foundry_sql_server_v2.query_foundry_sql(
        query=f"""
        SELECT * FROM `{TEST_SINGLETON.iris_new.rid}`
        WHERE is_setosa = 'setosa'
        LIMIT 20
        """,
        application_id=TEST_SINGLETON.iris_new.rid,
    )
    assert result.shape[0] <= 20
    # Verify all returned rows have is_setosa = 'setosa'
    if result.shape[0] > 0:
        assert all(result["is_setosa"] == "setosa")
