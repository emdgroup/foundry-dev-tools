import io
import json
import pathlib
from unittest import mock

import fs
import pandas as pd
import pyspark.sql.functions as F
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from transforms.api import (
    configure,
    incremental,
    Input,
    Markings,
    OrgMarkings,
    Output,
    transform,
    transform_df,
    transform_pandas,
    TransformContext,
)
from transforms.api._transform import TransformInput, TransformOutput

import foundry_dev_tools
from foundry_dev_tools.utils.caches.spark_caches import DiskPersistenceBackedSparkCache

from tests.conftest import PatchConfig
from tests.test_foundry_mock import MockFoundryRestClient


def get_dataset_identity_mock(self, dataset_path: str, branch="master"):
    return {
        "dataset_path": dataset_path,
        "dataset_rid": dataset_path.replace("/", "") + "rid1",
        "last_transaction_rid": dataset_path.replace("/", "") + "rid1" + "t1",
    }


def get_dataset_stats_mock(self, dataset_rid, branch_or_transaction_rid):
    # we return a dataset that is of size 1 mb bigger than the limit to force sql execution and not file download
    return {
        "sizeInBytes": (
            foundry_dev_tools.Configuration["transforms_sql_dataset_size_threshold"] + 1
        )
        * 1024
        * 1024,
        "numFiles": 1,
        "hiddenFilesSizeInBytes": 0,
        "numHiddenFiles": 0,
        "numTransactions": 1,
    }


spark_df_return_data_one = (
    SparkSession.builder.master("local[*]")
    .getOrCreate()
    .createDataFrame([[1, 2]], "a:string, b: string")
)
spark_df_return_data_two = (
    SparkSession.builder.master("local[*]")
    .getOrCreate()
    .createDataFrame([[3, 4]], "a: string, b: string")
)
spark_df_return_data_timestamp_date = (
    SparkSession.builder.master("local[*]")
    .getOrCreate()
    .createDataFrame(
        [["2019-12-06 10:26:52.602000", "2019-12-06"]],
        "_importedAt: string, created_at: string",
    )
    .withColumn("_importedAt", F.to_timestamp("_importedAt"))
    .withColumn("created_at", F.to_date("created_at"))
)


def return_df(one, dataset_path, branch):
    if dataset_path == "/input1":
        return spark_df_return_data_one
    elif dataset_path == "/input2":
        return spark_df_return_data_two
    elif dataset_path == "/tsdate":
        return spark_df_return_data_timestamp_date


def get_spark_schema_mock(one, dataset_rid, last_transaction_rid, branch="master"):
    return return_df(one, dataset_rid.replace("rid1", ""), None).schema


def dataset_has_schema_mock(one, two, three):
    return True


@pytest.fixture()
def run_around_tests(tmpdir):
    with mock.patch(
        "transforms.api.Input._read_spark_df_with_sql_query", return_df
    ), mock.patch(
        "transforms.api.Input._dataset_has_schema", dataset_has_schema_mock
    ), mock.patch(
        "foundry_dev_tools.FoundryRestClient.get_dataset_identity",
        get_dataset_identity_mock,
    ), mock.patch(
        "foundry_dev_tools.FoundryRestClient.get_dataset_stats",
        get_dataset_stats_mock,
    ), mock.patch(
        "foundry_dev_tools.utils.converter.foundry_spark.foundry_schema_to_spark_schema",
        get_spark_schema_mock,
    ):
        yield


def test_transform_one_input(run_around_tests):
    @transform(output1=Output("/output/to/dataset"), input1=Input("/input1"))
    def transform_me(output1, input1):
        assert isinstance(output1, TransformOutput)
        assert isinstance(input1, TransformInput)
        assert_frame_equal(
            input1.dataframe().toPandas(), spark_df_return_data_one.toPandas()
        )
        output1.write_dataframe(
            input1.dataframe().withColumn("col1", F.lit("replaced")).select("col1")
        )

        files = [file.path for file in input1.filesystem().ls()]
        assert "_schema.json" in files
        assert "_SUCCESS" in files
        with input1.filesystem().open("_schema.json", "rb") as f:
            schema = json.load(f)
        assert schema == {
            "type": "struct",
            "fields": [
                {"name": "a", "type": "string", "nullable": True, "metadata": {}},
                {"name": "b", "type": "string", "nullable": True, "metadata": {}},
            ],
        }

        with output1.filesystem().open("output.json", "w") as f:
            f.write("test")
        assert "output.json" in [file.path for file in output1.filesystem().ls()]
        assert "output.json" in [
            file.path for file in output1.filesystem().ls(glob="*/**", regex=".*")
        ]
        assert len([file.path for file in output1.filesystem().ls(glob="**/*.py")]) is 0
        assert (
            len(
                [
                    file.path
                    for file in output1.filesystem().ls(
                        glob="**/*.json", regex=".*.csv"
                    )
                ]
            )
            is 0
        )
        with output1.filesystem().open("output.json", "r") as f:
            content = f.read()
        assert content == "test"

        assert input1.path == "/input1"
        assert input1.rid == "input1" + "rid1"
        assert input1.branch is not None
        assert output1.path == "/output/to/dataset"
        assert output1.branch is not None
        assert output1.rid is not None

        output1.filesystem()._fs.makedir("subfolder")
        with output1.filesystem().open("subfolder/test.txt", "w") as f:
            f.write("test")
        assert "subfolder/test.txt" in [file.path for file in output1.filesystem().ls()]

    result = transform_me.compute()
    assert "output1" in result
    assert isinstance(result["output1"], DataFrame)
    assert result["output1"].schema.names[0] == "col1"


def test_transform_df_one_input(mocker, run_around_tests):
    from_foundry_and_cache = mocker.spy(Input, "_retrieve_from_foundry_and_cache")
    from_cache = mocker.spy(Input, "_retrieve_from_cache")

    with pytest.warns(UserWarning) as record:

        @transform_df(Output("/output/to/dataset"), input1=Input("/input1"))
        def transform_me(input1: DataFrame) -> DataFrame:
            assert isinstance(input1, DataFrame)
            assert_frame_equal(input1.toPandas(), spark_df_return_data_one.toPandas())
            return input1.withColumn("col1", F.lit("replaced")).select("col1")

    # check that only one warning was raised
    assert len(record) == 1
    # sql sampling triggers warning
    assert "Retrieving subset" in record[0].message.args[0]

    df = transform_me.compute()
    assert df.schema.names[0] == "col1"

    from_foundry_and_cache.assert_called()
    from_cache.assert_not_called()

    from_foundry_and_cache.reset_mock()
    from_cache.reset_mock()

    _ = Input("/input1")

    from_foundry_and_cache.assert_not_called()
    from_cache.assert_called()


def test_transform_df_one_input_with_ctx(run_around_tests):
    @transform_df(Output("/output/to/dataset"), input1=Input("/input1"))
    def transform_me(ctx, input1: DataFrame) -> DataFrame:
        assert isinstance(input1, DataFrame)
        assert isinstance(ctx, TransformContext)  # ctx is our TransformContext
        assert isinstance(
            ctx.spark_session, SparkSession
        )  # ctx.spark_session is a SparkSession
        assert_frame_equal(input1.toPandas(), spark_df_return_data_one.toPandas())
        return input1.withColumn("col1", F.lit("replaced")).select("col1")

    df = transform_me.compute()
    assert df.schema.names[0] == "col1"


def test_transform_df_two_inputs(run_around_tests):
    @transform_df(
        Output("/output/to/dataset"),
        input1=Input("/input1"),
        input2=Input("/input2"),
    )
    def transform_me(ctx, input1: DataFrame, input2: DataFrame) -> DataFrame:
        assert isinstance(input1, DataFrame)
        assert isinstance(input2, DataFrame)
        assert isinstance(ctx, TransformContext)
        assert isinstance(ctx.spark_session, SparkSession)
        assert_frame_equal(input1.toPandas(), spark_df_return_data_one.toPandas())
        assert_frame_equal(input2.toPandas(), spark_df_return_data_two.toPandas())
        return input1.union(input2)

    df = transform_me.compute()
    assert df.columns == ["a", "b"]
    assert df.count() == 2


def test_transform_df_date_and_timestamp(run_around_tests):
    def mock_schema(one):
        return StructType.fromJson(
            {
                "type": "struct",
                "fields": [
                    {
                        "name": "_importedAt",
                        "type": "timestamp",
                        "nullable": False,
                        "metadata": {},
                    },
                    {
                        "name": "created_at",
                        "type": "date",
                        "nullable": False,
                        "metadata": {},
                    },
                ],
            }
        )

    foundry_dev_tools.utils.converter.foundry_spark.foundry_schema_to_spark_schema = (
        mock_schema
    )

    @transform_df(Output("/output/to/dataset"), input1=Input("/tsdate"))
    def transform_me(input1: DataFrame) -> DataFrame:
        assert isinstance(input1, DataFrame)
        assert input1.dtypes[0][1] == "timestamp"
        assert input1.dtypes[1][1] == "date"
        return input1

    df = transform_me.compute()
    assert df.columns == ["_importedAt", "created_at"]
    assert df.count() == 1


def test_transform_pandas_one_input(mocker, run_around_tests):
    from_foundry_and_cache = mocker.spy(Input, "_retrieve_from_foundry_and_cache")
    from_cache = mocker.spy(Input, "_retrieve_from_cache")

    @transform_pandas(Output("/output/to/dataset"), input1=Input("/input1"))
    def transform_me(input1: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(input1, pd.DataFrame)
        assert_frame_equal(input1, spark_df_return_data_one.toPandas())
        return input1

    df = transform_me.compute()
    assert isinstance(df, pd.DataFrame)

    from_foundry_and_cache.assert_called()
    from_cache.assert_not_called()

    from_foundry_and_cache.reset_mock()
    from_cache.reset_mock()

    input1 = Input("/input1")

    from_foundry_and_cache.assert_not_called()
    from_cache.assert_called()


def test_transform_pandas_one_input_with_ctx(run_around_tests):
    @transform_pandas(Output("/output/to/dataset"), input1=Input("/input1"))
    def transform_me(ctx, input1: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(input1, pd.DataFrame)
        assert isinstance(ctx, TransformContext)  # ctx is our TransformContext
        assert isinstance(
            ctx.spark_session, SparkSession
        )  # ctx.spark_session is a SparkSession
        assert_frame_equal(input1, spark_df_return_data_one.toPandas())
        return input1

    df = transform_me.compute()
    assert isinstance(df, pd.DataFrame)
    assert_frame_equal(df, spark_df_return_data_one.toPandas())


def test_transform_pandas_two_inputs(run_around_tests):
    @transform_pandas(
        Output("/output/to/dataset"),
        input1=Input("/input1"),
        input2=Input("/input2"),
    )
    def transform_me(ctx, input1: pd.DataFrame, input2: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(input1, pd.DataFrame)
        assert isinstance(input2, pd.DataFrame)
        assert isinstance(ctx, TransformContext)
        assert isinstance(ctx.spark_session, SparkSession)
        assert_frame_equal(input1, spark_df_return_data_one.toPandas())
        assert_frame_equal(input2, spark_df_return_data_two.toPandas())
        return input2

    df = transform_me.compute()
    assert isinstance(df, pd.DataFrame)
    assert_frame_equal(df, spark_df_return_data_two.toPandas())


def test_transform_pandas_date_and_timestamp(run_around_tests):
    def mock_schema(one):
        return StructType.fromJson(
            {
                "type": "struct",
                "fields": [
                    {
                        "name": "_importedAt",
                        "type": "timestamp",
                        "nullable": False,
                        "metadata": {},
                    },
                    {
                        "name": "created_at",
                        "type": "date",
                        "nullable": False,
                        "metadata": {},
                    },
                ],
            }
        )

    foundry_dev_tools.utils.converter.foundry_spark.foundry_schema_to_spark_schema = (
        mock_schema
    )

    @transform_pandas(Output("/output/to/dataset"), input1=Input("/tsdate"))
    def transform_me(input1: pd.DataFrame) -> pd.DataFrame:
        assert isinstance(input1, pd.DataFrame)
        assert input1.dtypes[0] == "datetime64[ns]"
        assert input1.dtypes[1] == "object"
        return input1

    df = transform_me.compute()
    assert list(df.columns) == ["_importedAt", "created_at"]
    assert df.shape[0] == 1


def test_transform_freeze_cache(mocker, tmpdir, run_around_tests):
    online = mocker.spy(Input, "_online")
    offline = mocker.spy(Input, "_offline")

    @transform(output1=Output("/output/to/dataset"), input1=Input("/input1"))
    def transform_me_data_from_online_cache(output1, input1):
        pass

    online.assert_called()
    offline.assert_not_called()

    online.reset_mock()
    offline.reset_mock()
    with PatchConfig(config_overwrite={"transforms_freeze_cache": True}):

        @transform(output1=Output("/output/to/dataset"), input1=Input("/input1"))
        def transform_me_data_from_offline_cache(output1, input1):
            assert isinstance(output1, TransformOutput)
            assert isinstance(input1, TransformInput)
            assert_frame_equal(
                input1.dataframe().toPandas(), spark_df_return_data_one.toPandas()
            )
            output1.write_dataframe(
                input1.dataframe().withColumn("col1", F.lit("replaced")).select("col1")
            )

        result = transform_me_data_from_offline_cache.compute()
        assert "output1" in result
        assert isinstance(result["output1"], DataFrame)
        assert result["output1"].schema.names[0] == "col1"

        online.assert_not_called()
        offline.assert_called()


def test_transforms_with_configure(run_around_tests):
    with pytest.warns(UserWarning):

        @configure(someArg=1234)
        @transform(
            output1=Output("/output/to/dataset"),
            input1=Input("/input1"),
        )
        def transform_me_data_from_online_cache(output1, input1):
            pass


def test_transforms_with_incremental(run_around_tests):
    with pytest.warns(UserWarning):

        @incremental()
        @transform(
            output1=Output("/output/to/dataset"),
            input1=Input("/input1"),
        )
        def transform_me_data_from_online_cache(output1, input1):
            pass


def test_transform_output_write_to_folder(tmp_path_factory, run_around_tests):
    transforms_output_folder = pathlib.Path(
        tmp_path_factory.mktemp("transforms_output_folder")
    )
    with PatchConfig(
        initial_config_overwrite={
            "transforms_output_folder": str(transforms_output_folder)
        }
    ):

        @transform(
            output1=Output("/output/to/dataset"),
            # folders are only created if filesystem() is called at least once
            output2=Output("/output/to/dataset2"),
        )
        def transform_me(output1, output2):
            assert isinstance(output1, TransformOutput)

            with output1.filesystem().open("output.json", "w") as f:
                f.write("test")

            with output1.filesystem().open("output.json", "r") as f:
                content = f.read()
            assert content == "test"

            assert output1.path == "/output/to/dataset"
            assert output1.branch is not None
            assert output1.rid is not None
            assert output1.rid is not None

        result = transform_me.compute()
        assert "output1" in result
        assert "output2" in result

        with open(transforms_output_folder / "output1" / "output.json", "r") as f:
            assert f.read() == "test"

        assert pathlib.Path(transforms_output_folder / "output2").is_dir() is False


def test_transform_works_in_no_git_repository(mocker, run_around_tests):
    with pytest.warns(UserWarning):
        import subprocess

        run_mock = mocker.Mock()
        run_mock.side_effect = subprocess.CalledProcessError(
            128, ["git", "rev-parse", "--show-toplevel"]
        )
        mocker.patch.object(subprocess, "run", new=run_mock)

        @transform(
            output1=Output("/output/to/dataset"),
            input1=Input("/input1"),
        )
        def transform_me_data_from_online_cache(output1, input1):
            pass


def test_transform_markings(run_around_tests):
    @transform_df(
        Output("output1"),
        input1=Input(
            "/input1",
            stop_propagating=Markings(["a-b-c"], on_branches=["master"]),
            stop_requiring=OrgMarkings("a-b-c", on_branches=None),
        ),
    )
    def transform_me(input1):
        return input1

    transform_me.compute()


def test_binary_dataset_with_empty_folders(tmpdir):
    input_dataset = "/namespace/dataset1"

    root = str(tmpdir)
    filesystem = fs.open_fs(root)
    client = MockFoundryRestClient(fs=filesystem)

    branch = "master"

    ds = client.create_dataset("/namespace/dataset1")
    client.create_branch(ds["rid"], branch)

    transaction_rid = client.open_transaction(ds["rid"], "SNAPSHOT", branch)

    client.upload_dataset_file(
        dataset_rid=ds["rid"],
        transaction_rid=transaction_rid,
        path_or_buf=io.BytesIO(),
        path_in_foundry_dataset="models",
    )
    client.upload_dataset_file(
        dataset_rid=ds["rid"],
        transaction_rid=transaction_rid,
        path_or_buf=io.BytesIO(),
        path_in_foundry_dataset="models/lda",
    )
    client.upload_dataset_file(
        dataset_rid=ds["rid"],
        transaction_rid=transaction_rid,
        path_or_buf=io.BytesIO(b"aa"),
        path_in_foundry_dataset="models/lda/model.joblib",
    )
    client.upload_dataset_file(
        dataset_rid=ds["rid"],
        transaction_rid=transaction_rid,
        path_or_buf=io.BytesIO(b"aa"),
        path_in_foundry_dataset="poseidon/features",
    )
    client.commit_transaction(dataset_rid=ds["rid"], transaction_id=transaction_rid)

    with mock.patch("foundry_dev_tools.CachedFoundryClient.api", client):

        @transform(
            output=Output("/path/to/output1"),
            input1=Input(input_dataset, branch="master"),
        )
        def transform_me(output, input1):
            assert input1.filesystem() is not None
            assert "models/lda/model.joblib" in [
                file.path for file in input1.filesystem().ls()
            ]
            assert "poseidon/features" in [
                file.path for file in input1.filesystem().ls()
            ]
            assert input1.dataframe() is None

        result = transform_me.compute()
        assert "output" in result

        # Check that _offline functions of cache work
        cache = DiskPersistenceBackedSparkCache(**foundry_dev_tools.Configuration)
        ds_identity = cache.get_dataset_identity_not_branch_aware(input_dataset)
        assert cache.dataset_has_schema(ds_identity) is False
