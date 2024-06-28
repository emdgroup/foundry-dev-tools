import warnings
from typing import TYPE_CHECKING

import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.sql import SparkSession

from foundry_dev_tools.utils.caches.spark_caches import DiskPersistenceBackedSparkCache
from foundry_dev_tools.utils.converter.foundry_spark import (
    foundry_schema_to_spark_schema,
)
from tests.conftest import TEST_FOLDER
from tests.integration.conftest import TEST_SINGLETON

if TYPE_CHECKING:
    import transforms


@pytest.fixture()
def iris_integration_test_csv_rid():
    return TEST_SINGLETON.iris_new.rid


@pytest.fixture()
def iris_integration_test_csv_path():
    return TEST_SINGLETON.iris_new.path


@pytest.fixture()
def complex_dataset_fixture():
    return TEST_SINGLETON.complex_dataset.rid


@pytest.mark.parametrize(
    "input_dataset_fixture",
    [
        "complex_dataset_fixture",
        # "ri.foundry.main.dataset.4be02f70-e771-4914-b6d6-8781179ae6b9",  # array, struct types test
        "iris_integration_test_csv_rid",
        "iris_integration_test_csv_path",
    ],
)
def test_sql_with_limit(mocker, input_dataset_fixture, request):
    from transforms.api import Input, Output, TransformContext, transform_df

    input_dataset = request.getfixturevalue(input_dataset_fixture)
    dataset_identity = TEST_SINGLETON.ctx.foundry_rest_client.get_dataset_identity(input_dataset, "master")
    schema = foundry_schema_to_spark_schema(
        TEST_SINGLETON.ctx.foundry_rest_client.get_dataset_schema(
            dataset_identity["dataset_rid"],
            dataset_identity["last_transaction_rid"],
            branch="master",
        ),
    )

    from_foundry_and_cache = mocker.spy(Input, "_retrieve_from_foundry_and_cache")
    from_cache = mocker.spy(Input, "_retrieve_from_cache")
    with_sql_query = mocker.spy(Input, "_read_spark_df_with_sql_query")

    prev_tresh = TEST_SINGLETON.ctx.config.transforms_sql_dataset_size_threshold
    TEST_SINGLETON.ctx.config.transforms_sql_dataset_size_threshold = 0

    @transform_df(
        Output("/path/to/output1"),
        input1=Input(input_dataset, branch="master"),
    )
    def transform_me(ctx, input1):
        assert schema == input1.schema
        assert isinstance(ctx, TransformContext)  # ctx is our TransformContext
        assert isinstance(ctx.spark_session, SparkSession)  # ctx.spark_session is a SparkSession
        return input1.withColumn("col1", F.lit("replaced")).select("col1")

    df = transform_me.compute(TEST_SINGLETON.ctx)
    assert df.schema.names[0] == "col1"
    from_foundry_and_cache.assert_called()
    with_sql_query.assert_called()
    from_cache.assert_not_called()

    from_foundry_and_cache.reset_mock()
    from_cache.reset_mock()

    # second time should be loaded from cache, schema should be same!
    input1 = Input(input_dataset, branch="master")
    df = input1.init_input(TEST_SINGLETON.ctx).dataframe()
    assert schema == df.schema

    from_foundry_and_cache.assert_not_called()
    from_cache.assert_called()

    # Check that offline functions of cache work
    cache = DiskPersistenceBackedSparkCache(ctx=TEST_SINGLETON.ctx)
    ds_identity = cache.get_dataset_identity_not_branch_aware(input_dataset)
    assert cache.dataset_has_schema(ds_identity) is True
    TEST_SINGLETON.ctx.config.transforms_sql_dataset_size_threshold = prev_tresh


@pytest.mark.parametrize(
    "input_dataset_fixture",
    [
        "complex_dataset_fixture",
        "iris_integration_test_csv_rid",
        "iris_integration_test_csv_path",
    ],
)
def test_file_download(mocker, input_dataset_fixture, request):
    from transforms.api import Input, Output, TransformContext, transform_df

    input_dataset = request.getfixturevalue(input_dataset_fixture)

    dataset_identity = TEST_SINGLETON.ctx.foundry_rest_client.get_dataset_identity(input_dataset, "master")
    schema = foundry_schema_to_spark_schema(
        TEST_SINGLETON.ctx.foundry_rest_client.get_dataset_schema(
            dataset_identity["dataset_rid"],
            dataset_identity["last_transaction_rid"],
            branch="master",
        ),
    )

    from_foundry_and_cache = mocker.spy(Input, "_retrieve_from_foundry_and_cache")
    from_cache = mocker.spy(Input, "_retrieve_from_cache")

    with warnings.catch_warnings():
        # we expect no sql fallback warnings
        warnings.simplefilter("error")
        prev_config = TEST_SINGLETON.ctx.config
        TEST_SINGLETON.ctx.config.transforms_force_full_dataset_download = True
        TEST_SINGLETON.ctx.config.transforms_sql_dataset_size_threshold = 1

        @transform_df(
            Output("/path/to/output1"),
            input1=Input(input_dataset, branch="master"),
        )
        def transform_me(ctx, input1):
            assert schema == input1.schema
            assert isinstance(ctx, TransformContext)  # ctx is our TransformContext
            assert isinstance(ctx.spark_session, SparkSession)  # ctx.spark_session is a SparkSession
            return input1.withColumn("col1", F.lit("replaced")).select("col1")

        df = transform_me.compute(TEST_SINGLETON.ctx)
        assert df.schema.names[0] == "col1"
        from_foundry_and_cache.assert_called()
        from_cache.assert_not_called()

        from_foundry_and_cache.reset_mock()
        from_cache.reset_mock()

        # second time should be loaded from cache, schema should be same!
        input1 = Input(input_dataset, branch="master")
        df = input1.init_input(TEST_SINGLETON.ctx).dataframe()
        assert schema == df.schema

        from_foundry_and_cache.assert_not_called()
        from_cache.assert_called()

        # Check that offline functions of cache work
        cache = DiskPersistenceBackedSparkCache(ctx=TEST_SINGLETON.ctx)
        ds_identity = cache.get_dataset_identity_not_branch_aware(input_dataset)
        assert cache.dataset_has_schema(ds_identity) is True

        TEST_SINGLETON.ctx.config = prev_config


def test_binary_dataset(mocker):
    from transforms.api import Input, Output, transform

    binary_dataset = TEST_FOLDER.joinpath("test_data", "binary_dataset")
    ds, transaction = TEST_SINGLETON.generic_upload_dataset_if_not_exists("bin", binary_dataset)
    with binary_dataset.joinpath("bin").open(mode="rb") as uploaded_file:
        uploaded_binary = uploaded_file.read()

    online = mocker.spy(Input, "_online")
    offline = mocker.spy(Input, "_offline")

    @transform(
        output=Output("/path/to/output1"),
        input1=Input(ds.rid, branch="master"),
    )
    def transform_me_online(output, input1: "transforms.api._transform.TransformInput"):
        assert input1.filesystem() is not None
        assert input1.dataframe() is None
        with input1.filesystem().open("bin", "rb") as bin_fd:
            assert uploaded_binary == bin_fd.read()

    result = transform_me_online.compute(TEST_SINGLETON.ctx)
    assert "output" in result

    online.assert_called()
    offline.assert_not_called()

    online.reset_mock()
    offline.reset_mock()

    # Check that offline functions of cache work
    cache = TEST_SINGLETON.ctx.cached_foundry_client.cache
    ds_identity = cache.get_dataset_identity_not_branch_aware(ds.rid)
    assert cache.dataset_has_schema(ds_identity) is False

    TEST_SINGLETON.ctx.config.transforms_freeze_cache = True

    @transform(
        output=Output("/path/to/output1"),
        input1=Input(ds.rid),
    )
    def transform_me_from_offline_cache(output, input1):
        assert input1.filesystem() is not None
        assert input1.dataframe() is None
        with input1.filesystem().open("bin", "rb") as bin_fd:
            assert uploaded_binary == bin_fd.read()

    result = transform_me_from_offline_cache.compute(TEST_SINGLETON.ctx)
    assert "output" in result

    online.assert_not_called()
    offline.assert_called()
    TEST_SINGLETON.ctx.config.transforms_freeze_cache = False
