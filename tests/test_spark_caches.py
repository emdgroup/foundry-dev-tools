import datetime
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from foundry_dev_tools.utils.caches.spark_caches import DiskPersistenceBackedSparkCache


def to_dict(dataset_rid, last_transaction_rid, dataset_path):
    return {
        "dataset_rid": dataset_rid,
        "last_transaction_rid": last_transaction_rid,
        "dataset_path": dataset_path,
    }


@pytest.fixture(autouse=True)
def provide_config():
    from foundry_dev_tools import Configuration

    yield Configuration


def test_add_get_delete(spark_session, provide_config):
    pc = DiskPersistenceBackedSparkCache(**provide_config)
    assert len(pc) == 0
    with pytest.raises(KeyError):
        pc.get_dataset_identity_not_branch_aware("/d1")
    insert = spark_session.createDataFrame(
        data=[[1, 2], [3, 4]], schema="a: int, b: int"
    )
    pc[to_dict("d1", "t11", "/d1")] = insert
    retrieve = pc[to_dict("d1", "t11", "/d1")]
    assert_frame_equal(insert.toPandas(), retrieve.toPandas())
    assert to_dict("d1", "t11", "/d1") in pc
    assert pc.get_dataset_identity_not_branch_aware("/d1") == to_dict(
        "d1", "t11", "/d1"
    )
    assert pc.dataset_has_schema(to_dict("d1", "t11", "/d1")) is True
    assert len(pc) == 1
    del pc[to_dict("d1", "t11", "/d1")]
    assert len(pc) == 0


def test_iterate(spark_session, provide_config):
    pc = DiskPersistenceBackedSparkCache(**provide_config)
    pc[to_dict("d1", "t11", "/d1")] = spark_session.createDataFrame(
        data=[[1, 2]], schema="a: int, b: int"
    )
    pc[to_dict("d2", "t21", "/d2")] = spark_session.createDataFrame(
        data=[[1, 2]], schema="a: int, b: int"
    )
    pc[to_dict("d3", "t31", "/d3")] = spark_session.createDataFrame(
        data=[[3, 4]], schema="c: int, d: int"
    )

    for key, value in pc.items():
        assert_frame_equal(pc[key].toPandas(), value.toPandas())

    assert len(pc) == 3

    del pc[to_dict("d1", "t11", "/d1")]
    del pc[to_dict("d2", "t21", "/d2")]
    del pc[to_dict("d3", "t31", "/d3")]


def test_key_not_exists_throws_key_error(provide_config):
    pc = DiskPersistenceBackedSparkCache(**provide_config)
    assert len(pc) == 0
    with pytest.raises(KeyError):
        pc[to_dict("doesnotExist", "dne", "/ddoesnotExist")]
    with pytest.raises(KeyError):
        del pc[to_dict("doesnotExist", "dne", "/ddoesnotExist")]


# Fix for:
# E   UnicodeEncodeError: 'utf-8' codec can't encode character '\ud800' in position 0: surrogates not allowed
#
def test_unicode(spark_session, provide_config):
    pc = DiskPersistenceBackedSparkCache(**provide_config)
    data = spark_session.createDataFrame(
        data=[["\ud800foo", 2]], schema="a: string, b: string"
    )
    pc[to_dict("d1", "t11", "/d1")] = data

    for key, value in pc.items():
        assert_frame_equal(data.toPandas(), pc[key].toPandas())

    del pc[to_dict("d1", "t11", "/d1")]


def test_reads_csv_files_format(spark_session, provide_config):
    pc = DiskPersistenceBackedSparkCache(**provide_config)
    df = spark_session.createDataFrame(data=[[1, 2]], schema="a: int, b: int")
    spark_schema = df.schema.jsonValue()
    dataset_directory = os.sep.join([provide_config["cache_dir"], "d1", "t1.csv"])
    df.write.format("csv").option("header", "true").save(
        os.sep.join([dataset_directory])
    )
    pc.set_item_metadata(dataset_directory, to_dict("d1", "t1", "/d1"), spark_schema)

    assert_frame_equal(df.toPandas(), pc[to_dict("d1", "t1", "/d1")].toPandas())
    del pc[to_dict("d1", "t1", "/d1")]
    assert len(pc) == 0


def test_supports_foundry_schema(spark_session, provide_config):
    pc = DiskPersistenceBackedSparkCache(**provide_config)
    df = spark_session.createDataFrame(data=[[1, 2]], schema="a: int, b: int")

    foundry_schema = {
        "fieldSchemaList": [
            {
                "type": "INTEGER",
                "name": "a",
                "nullable": None,
                "userDefinedTypeClass": None,
                "customMetadata": {},
                "arraySubtype": None,
                "precision": None,
                "scale": None,
                "mapKeyType": None,
                "mapValueType": None,
                "subSchemas": None,
            },
            {
                "type": "INTEGER",
                "name": "b",
                "nullable": None,
                "userDefinedTypeClass": None,
                "customMetadata": {},
                "arraySubtype": None,
                "precision": None,
                "scale": None,
                "mapKeyType": None,
                "mapValueType": None,
                "subSchemas": None,
            },
        ],
        "primaryKey": None,
        "dataFrameReaderClass": "com.palantir.foundry.spark.input.TextDataFrameReader",
        "customMetadata": {
            "textParserParams": {
                "parser": "CSV_PARSER",
                "nullValues": None,
                "nullValuesPerColumn": None,
                "charsetName": "UTF-8",
                "fieldDelimiter": ",",
                "recordDelimiter": "\n",
                "quoteCharacter": '"',
                "dateFormat": {},
                "skipLines": 1,
                "jaggedRowBehavior": "THROW_EXCEPTION",
                "parseErrorBehavior": "THROW_EXCEPTION",
                "addFilePath": False,
                "addFilePathInsteadOfUri": False,
                "addByteOffset": False,
                "addImportedAt": False,
            }
        },
    }
    dataset_directory = os.sep.join([provide_config["cache_dir"], "d1", "t1.csv"])
    df.write.format("csv").option("header", "true").save(
        os.sep.join([dataset_directory])
    )
    pc.set_item_metadata(dataset_directory, to_dict("d1", "t1", "/d1"), foundry_schema)

    assert_frame_equal(df.toPandas(), pc[to_dict("d1", "t1", "/d1")].toPandas())
    del pc[to_dict("d1", "t1", "/d1")]
    assert len(pc) == 0


def test_multiline_csv(spark_session, provide_config):
    pc = DiskPersistenceBackedSparkCache(**provide_config)
    dataset_directory = os.sep.join(
        [provide_config["cache_dir"], "ds_multiline", "t1.csv"]
    )
    csv_content = """,Name,"Test Status (Current) 
(Company :Bar/Territory : Status : Indication : Date)",Report_date
0,"12345 asdf 1234 (asdf), Ba1234","Bawefawef Pharma International Ltd: Country: No Development Reported: Aram foo bar asdf: 16-Feb-2011
",2020-01-28"""

    os.makedirs(dataset_directory)
    with open(dataset_directory + "/manual.csv", "w") as f:
        f.write(csv_content)

    foundry_schema = {
        "fieldSchemaList": [
            {
                "type": "INTEGER",
                "name": "untitled_column",
                "nullable": None,
                "userDefinedTypeClass": None,
                "customMetadata": {},
                "arraySubtype": None,
                "precision": None,
                "scale": None,
                "mapKeyType": None,
                "mapValueType": None,
                "subSchemas": None,
            },
            {
                "type": "STRING",
                "name": "Name",
                "nullable": None,
                "userDefinedTypeClass": None,
                "customMetadata": {},
                "arraySubtype": None,
                "precision": None,
                "scale": None,
                "mapKeyType": None,
                "mapValueType": None,
                "subSchemas": None,
            },
            {
                "type": "STRING",
                "name": "Test_Status_Current_Bar__CountryTerritory__Status__Indication__Date",
                "nullable": None,
                "userDefinedTypeClass": None,
                "customMetadata": {},
                "arraySubtype": None,
                "precision": None,
                "scale": None,
                "mapKeyType": None,
                "mapValueType": None,
                "subSchemas": None,
            },
            {
                "type": "DATE",
                "name": "Report_date",
                "nullable": None,
                "userDefinedTypeClass": None,
                "customMetadata": {},
                "arraySubtype": None,
                "precision": None,
                "scale": None,
                "mapKeyType": None,
                "mapValueType": None,
                "subSchemas": None,
            },
        ],
        "dataFrameReaderClass": "com.palantir.foundry.spark.input.TextDataFrameReader",
        "customMetadata": {
            "textParserParams": {
                "parser": "MULTILINE_CSV_PARSER",
                "nullValues": None,
                "nullValuesPerColumn": None,
                "charsetName": "UTF-8",
                "fieldDelimiter": ",",
                "recordDelimiter": "\n",
                "quoteCharacter": '"',
                "dateFormat": {"Report_date": ["yyyy-MM-dd"]},
                "skipLines": 2,
                "jaggedRowBehavior": "THROW_EXCEPTION",
                "parseErrorBehavior": "THROW_EXCEPTION",
                "addFilePath": False,
                "addFilePathInsteadOfUri": False,
                "addByteOffset": False,
                "addImportedAt": False,
            }
        },
    }

    pc.set_item_metadata(
        dataset_directory,
        to_dict("ds_multiline", "t1", "/ds_multiline"),
        foundry_schema,
    )

    from_cache = pc[to_dict("ds_multiline", "t1", "/ds_multiline")].toPandas()
    assert from_cache.shape == (1, 4)
    assert type(from_cache["Report_date"].values[0]) == datetime.date
    assert isinstance(from_cache["untitled_column"].values[0], np.int32)
    assert isinstance(from_cache["Name"].values[0], str)
    assert isinstance(
        from_cache[
            "Test_Status_Current_Bar__CountryTerritory__Status__Indication__Date"
        ].values[0],
        str,
    )

    del pc[to_dict("ds_multiline", "t1", "/ds_multiline")]
    assert len(pc) == 0


def test_multiple_csvs_with_header(spark_session, provide_config):
    pc = DiskPersistenceBackedSparkCache(**provide_config)
    dataset_directory = os.sep.join(
        [provide_config["cache_dir"], "ds_multiline", "t1.csv"]
    )
    csv_content_1 = """given_name,family_name
Hans,Doe"""
    csv_content_2 = """given_name,family_name
Max,Mustermann"""

    os.makedirs(dataset_directory)
    with open(os.sep.join([dataset_directory, "1"]), "w") as f:
        f.write(csv_content_1)
    with open(os.sep.join([dataset_directory, "2"]), "w") as f:
        f.write(csv_content_2)

    foundry_schema = {
        "fieldSchemaList": [
            {
                "type": "STRING",
                "name": "given_name",
                "nullable": None,
                "userDefinedTypeClass": None,
                "customMetadata": {},
                "arraySubtype": None,
                "precision": None,
                "scale": None,
                "mapKeyType": None,
                "mapValueType": None,
                "subSchemas": None,
            },
            {
                "type": "STRING",
                "name": "family_name",
                "nullable": None,
                "userDefinedTypeClass": None,
                "customMetadata": {},
                "arraySubtype": None,
                "precision": None,
                "scale": None,
                "mapKeyType": None,
                "mapValueType": None,
                "subSchemas": None,
            },
        ],
        "primaryKey": None,
        "dataFrameReaderClass": "com.palantir.foundry.spark.input.TextDataFrameReader",
        "customMetadata": {
            "textParserParams": {
                "parser": "CSV_PARSER",
                "nullValues": None,
                "nullValuesPerColumn": None,
                "charsetName": "UTF-8",
                "fieldDelimiter": ",",
                "recordDelimiter": "\n",
                "quoteCharacter": '"',
                "dateFormat": {},
                "skipLines": 1,
                "jaggedRowBehavior": "THROW_EXCEPTION",
                "parseErrorBehavior": "THROW_EXCEPTION",
                "addFilePath": False,
                "addFilePathInsteadOfUri": False,
                "addByteOffset": False,
                "addImportedAt": False,
            }
        },
    }

    pc.set_item_metadata(
        dataset_directory,
        to_dict("ds_multiline", "t1", "/ds_multiline"),
        foundry_schema,
    )

    from_cache = pc[to_dict("ds_multiline", "t1", "/ds_multiline")].toPandas()
    assert from_cache.shape == (2, 2)
    assert not from_cache[from_cache.columns[0]].isin([from_cache.columns[0]]).any()
    assert_frame_equal(
        from_cache.sort_values(by=["given_name"]).reset_index(drop=True),
        pd.DataFrame(
            data={"given_name": ["Max", "Hans"], "family_name": ["Mustermann", "Doe"]}
        )
        .sort_values(by=["given_name"])
        .reset_index(drop=True),
    )
    assert list(from_cache.columns.values) == ["given_name", "family_name"]

    del pc[to_dict("ds_multiline", "t1", "/ds_multiline")]
    assert len(pc) == 0


def test_cache_keeps_two_old_transactions(spark_session, provide_config):
    pc = DiskPersistenceBackedSparkCache(**provide_config)

    insert = spark_session.createDataFrame(
        data=[[1, 2], [3, 4]], schema="a: int, b: int"
    )
    pc[
        to_dict(
            "d1",
            "ri.foundry.main.transaction.00000083-d2e4-ac6e-b00c-29965c42029e",
            "/d1",
        )
    ] = insert
    path_to_be_deleted = pc.get_path_to_local_dataset(
        to_dict(
            "d1",
            "ri.foundry.main.transaction.00000083-d2e4-ac6e-b00c-29965c42029e",
            "/d1",
        )
    )
    pc[
        to_dict(
            "d1",
            "ri.foundry.main.transaction.00000084-4242-3e20-8046-ef50abc1c09a",
            "/d1",
        )
    ] = insert
    path_not_to_be_deleted = pc.get_path_to_local_dataset(
        to_dict(
            "d1",
            "ri.foundry.main.transaction.00000084-4242-3e20-8046-ef50abc1c09a",
            "/d1",
        )
    )

    pc[
        to_dict(
            "d1",
            "ri.foundry.main.transaction.00000084-44c1-e115-8b84-9e282e000a09",
            "/d1",
        )
    ] = insert
    path_not_to_be_deleted_two = pc.get_path_to_local_dataset(
        to_dict(
            "d1",
            "ri.foundry.main.transaction.00000084-44c1-e115-8b84-9e282e000a09",
            "/d1",
        )
    )

    assert Path(path_to_be_deleted).exists() is False
    assert Path(path_not_to_be_deleted).exists() is True
    assert Path(path_not_to_be_deleted_two).exists() is True
