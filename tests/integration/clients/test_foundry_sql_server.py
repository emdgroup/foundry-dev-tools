import pytest

from foundry_dev_tools.errors.dataset import BranchNotFoundError, DatasetHasNoSchemaError, DatasetNotFoundError
from foundry_dev_tools.errors.sql import FoundrySqlQueryFailedError, FoundrySqlSerializationFormatNotImplementedError
from tests.integration.conftest import TEST_SINGLETON


def test_smoke():
    one_row_one_column = TEST_SINGLETON.ctx.foundry_sql_server.query_foundry_sql(
        f"SELECT sepal_width FROM `{TEST_SINGLETON.iris_new.rid}` LIMIT 1",
    )
    assert one_row_one_column.shape == (1, 1)


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
