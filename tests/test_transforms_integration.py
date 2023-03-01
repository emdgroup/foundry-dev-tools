import warnings
from unittest import mock

import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession
from transforms.api import Input, Output, transform, transform_df, TransformContext
from transforms.api._transform import TransformInput

import foundry_dev_tools

import tests.utils
from foundry_dev_tools import FoundryRestClient
from foundry_dev_tools.utils.caches.spark_caches import DiskPersistenceBackedSparkCache
from foundry_dev_tools.utils.converter.foundry_spark import (
    foundry_schema_to_spark_schema,
)

from tests.conftest import PatchConfig

from tests.utils import generic_upload_dataset_if_not_exists, TEST_FOLDER


@pytest.fixture()
def iris_integration_test_csv_rid(iris_dataset):
    yield iris_dataset[0]


@pytest.fixture()
def iris_integration_test_csv_path(iris_dataset):
    yield iris_dataset[1]


@pytest.mark.parametrize(
    "input_dataset_fixture",
    [
        "complex_dataset_fixture",
        # "ri.foundry.main.dataset.4be02f70-e771-4914-b6d6-8781179ae6b9",  # array, struct types test
        "iris_integration_test_csv_rid",
        "iris_integration_test_csv_path",
    ],
)
def test_sql_with_limit(mocker, input_dataset_fixture, request, client):
    input_dataset = request.getfixturevalue(input_dataset_fixture)
    dataset_identity = client.get_dataset_identity(input_dataset, "master")
    schema = foundry_schema_to_spark_schema(
        client.get_dataset_schema(
            dataset_identity["dataset_rid"],
            dataset_identity["last_transaction_rid"],
            branch="master",
        )
    )

    from_foundry_and_cache = mocker.spy(Input, "_retrieve_from_foundry_and_cache")
    from_cache = mocker.spy(Input, "_retrieve_from_cache")
    with_sql_query = mocker.spy(Input, "_read_spark_df_with_sql_query")

    with mock.patch("foundry_dev_tools.CachedFoundryClient.api", client), PatchConfig(
        config_overwrite={"transforms_sql_dataset_size_threshold": 0}
    ) as pc:

        @transform_df(
            Output("/path/to/output1"),
            input1=Input(input_dataset, branch="master"),
        )
        def transform_me(ctx, input1):
            assert schema == input1.schema
            assert isinstance(ctx, TransformContext)  # ctx is our TransformContext
            assert isinstance(
                ctx.spark_session, SparkSession
            )  # ctx.spark_session is a SparkSession
            return input1.withColumn("col1", F.lit("replaced")).select("col1")

        df = transform_me.compute()
        assert df.schema.names[0] == "col1"
        from_foundry_and_cache.assert_called()
        with_sql_query.assert_called()
        from_cache.assert_not_called()

        from_foundry_and_cache.reset_mock()
        from_cache.reset_mock()

        # second time should be loaded from cache, schema should be same!
        input1 = Input(input_dataset, branch="master")
        df = input1.dataframe()
        assert schema == df.schema

        from_foundry_and_cache.assert_not_called()
        from_cache.assert_called()

        # Check that offline functions of cache work
        cache = DiskPersistenceBackedSparkCache(**foundry_dev_tools.Configuration)
        ds_identity = cache.get_dataset_identity_not_branch_aware(input_dataset)
        assert cache.dataset_has_schema(ds_identity) is True


@pytest.mark.parametrize(
    "input_dataset_fixture",
    [
        "complex_dataset_fixture",
        "iris_integration_test_csv_rid",
        "iris_integration_test_csv_path",
    ],
)
def test_file_download(mocker, input_dataset_fixture, request, client):
    input_dataset = request.getfixturevalue(input_dataset_fixture)

    dataset_identity = client.get_dataset_identity(input_dataset, "master")
    schema = foundry_schema_to_spark_schema(
        client.get_dataset_schema(
            dataset_identity["dataset_rid"],
            dataset_identity["last_transaction_rid"],
            branch="master",
        )
    )

    from_foundry_and_cache = mocker.spy(Input, "_retrieve_from_foundry_and_cache")
    from_cache = mocker.spy(Input, "_retrieve_from_cache")

    with warnings.catch_warnings():
        # we expect no sql fallback warnings
        warnings.simplefilter("error")
        with PatchConfig(
            initial_config_overwrite={
                "transforms_force_full_dataset_download": True,
                "transforms_sql_dataset_size_threshold": 1,
            }
        ), mock.patch("foundry_dev_tools.CachedFoundryClient.api", client):

            @transform_df(
                Output("/path/to/output1"),
                input1=Input(input_dataset, branch="master"),
            )
            def transform_me(ctx, input1):
                assert schema == input1.schema
                assert isinstance(ctx, TransformContext)  # ctx is our TransformContext
                assert isinstance(
                    ctx.spark_session, SparkSession
                )  # ctx.spark_session is a SparkSession
                return input1.withColumn("col1", F.lit("replaced")).select("col1")

            df = transform_me.compute()
            assert df.schema.names[0] == "col1"
            from_foundry_and_cache.assert_called()
            from_cache.assert_not_called()

            from_foundry_and_cache.reset_mock()
            from_cache.reset_mock()

            # second time should be loaded from cache, schema should be same!
            input1 = Input(input_dataset, branch="master")
            df = input1.dataframe()
            assert schema == df.schema

            from_foundry_and_cache.assert_not_called()
            from_cache.assert_called()

            # Check that offline functions of cache work
            cache = DiskPersistenceBackedSparkCache(**foundry_dev_tools.Configuration)
            ds_identity = cache.get_dataset_identity_not_branch_aware(input_dataset)
            assert cache.dataset_has_schema(ds_identity) is True


@pytest.mark.integration
def test_binary_dataset(mocker):
    upload_client = FoundryRestClient()
    uploaded_dataset = generic_upload_dataset_if_not_exists(
        upload_client, "bin", TEST_FOLDER / "test_data" / "binary_dataset"
    )
    with open(
        TEST_FOLDER / "test_data" / "binary_dataset" / "bin", "rb"
    ) as uploaded_file:
        uploaded_binary = uploaded_file.read()

    online = mocker.spy(Input, "_online")
    offline = mocker.spy(Input, "_offline")

    @transform(
        output=Output("/path/to/output1"),
        input1=Input(uploaded_dataset[0], branch="master"),
    )
    def transform_me_online(output, input1: TransformInput):
        assert input1.filesystem() is not None
        assert input1.dataframe() is None
        with input1.filesystem().open("bin", "rb") as bin_fd:
            assert uploaded_binary == bin_fd.read()

    result = transform_me_online.compute()
    assert "output" in result

    online.assert_called()
    offline.assert_not_called()

    online.reset_mock()
    offline.reset_mock()

    # Check that offline functions of cache work
    cache = DiskPersistenceBackedSparkCache(**foundry_dev_tools.Configuration)
    ds_identity = cache.get_dataset_identity_not_branch_aware(uploaded_dataset[0])
    assert cache.dataset_has_schema(ds_identity) is False

    with PatchConfig(config_overwrite={"transforms_freeze_cache": True}):

        @transform(
            output=Output("/path/to/output1"),
            input1=Input(uploaded_dataset[0]),
        )
        def transform_me_from_offline_cache(output, input1):
            assert input1.filesystem() is not None
            assert input1.dataframe() is None
            with input1.filesystem().open("bin", "rb") as bin_fd:
                assert uploaded_binary == bin_fd.read()

        result = transform_me_from_offline_cache.compute()
        assert "output" in result

        online.assert_not_called()
        offline.assert_called()
