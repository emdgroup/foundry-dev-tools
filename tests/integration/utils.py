from __future__ import annotations

import datetime
import os
import random
import time
from decimal import Decimal
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np

from foundry_dev_tools.config.context import FoundryContext
from foundry_dev_tools.foundry_api_client import FoundryRestClient
from foundry_dev_tools.utils.spark import get_spark_session

if TYPE_CHECKING:
    import pyspark.sql

    from foundry_dev_tools.resources.dataset import Dataset
    from foundry_dev_tools.utils import api_types


INTEGRATION_TEST_COMPASS_ROOT_PATH = os.getenv(
    "INTEGRATION_TEST_COMPASS_ROOT_PATH",
    "/Global/global-use-case-public/Developer Experience/integration-test-folder-v2",
)
INTEGRATION_TEST_COMPASS_ROOT_RID = os.getenv(
    "INTEGRATION_TEST_COMPASS_ROOT_RID",
    "ri.compass.main.folder.abd54c7b-7e3f-4653-89fc-b25988ea3f71",
)

TEST_FOLDER = Path(__file__).parent.parent.resolve()

IRIS_SCHEMA = {
    "fieldSchemaList": [
        {
            "type": "DOUBLE",
            "name": "sepal_width",
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
            "type": "DOUBLE",
            "name": "sepal_length",
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
            "type": "DOUBLE",
            "name": "petal_width",
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
            "type": "DOUBLE",
            "name": "petal_length",
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
            "name": "is_setosa",
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
            "initialReadTimeout": "1 hour",
        },
    },
}

FOUNDRY_SCHEMA_COMPLEX_DATASET = {
    "fieldSchemaList": [
        {
            "type": "STRUCT",
            "name": "struct_column",
            "nullable": None,
            "userDefinedTypeClass": None,
            "customMetadata": {},
            "arraySubtype": None,
            "precision": None,
            "scale": None,
            "mapKeyType": None,
            "mapValueType": None,
            "subSchemas": [
                {
                    "type": "STRING",
                    "name": "value",
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
        },
        {
            "type": "STRING",
            "name": "string_column",
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
            "type": "LONG",
            "name": "long_column",
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
            "type": "DOUBLE",
            "name": "double_column",
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
            "type": "DECIMAL",
            "name": "decimal_column",
            "nullable": None,
            "userDefinedTypeClass": None,
            "customMetadata": {},
            "arraySubtype": None,
            "precision": 5,
            "scale": 2,
            "mapKeyType": None,
            "mapValueType": None,
            "subSchemas": None,
        },
        {
            "type": "DATE",
            "name": "date_column",
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
            "type": "TIMESTAMP",
            "name": "timestamp_column",
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
            "type": "ARRAY",
            "name": "struct_within_array_column",
            "nullable": True,
            "userDefinedTypeClass": None,
            "customMetadata": {},
            "arraySubtype": {
                "type": "STRUCT",
                "name": None,
                "nullable": True,
                "userDefinedTypeClass": None,
                "customMetadata": {},
                "arraySubtype": None,
                "precision": None,
                "scale": None,
                "mapKeyType": None,
                "mapValueType": None,
                "subSchemas": [
                    {
                        "type": "STRING",
                        "name": "value",
                        "nullable": True,
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
            },
            "precision": None,
            "scale": None,
            "mapKeyType": None,
            "mapValueType": None,
            "subSchemas": None,
        },
    ],
    "primaryKey": None,
    "dataFrameReaderClass": "com.palantir.foundry.spark.input.ParquetDataFrameReader",
    "customMetadata": {"format": "parquet"},
}

rng = np.random.default_rng()


def random_string_array(array_length: int, str_length: int = 20):
    return rng.integers(
        low=65,  # A
        high=90,  # Z
        size=array_length * str_length,
        dtype="int32",
    ).view(f"U{str_length}")


def random_struct_array(length: int) -> list[dict[str, str]]:
    strings = random_string_array(length)
    return [{"value": str(string)} for string in strings]


def random_struct_with_array_array(length: int) -> list[list[dict[str, str]]]:
    strings = random_string_array(length)
    return [[{"value": str(string)}] for string in strings]


def random_decimal_array(length: int) -> list[Decimal]:
    np_type = (
        rng.random(
            length,
        ).round(2)
        * 999
    )
    return [Decimal(value) for value in np_type]


def random_date_array(length: int) -> list[datetime.date]:
    # 596761200 -> 1988-11-29
    return [
        datetime.datetime.fromtimestamp(random.uniform(596761200, time.time()), tz=datetime.timezone.utc).date()
        for _ in range(length)
    ]


def random_datetime_array(length: int):
    # 596761200 -> 1988-11-29
    return [
        datetime.datetime.fromtimestamp(random.uniform(596761200, time.time()), tz=datetime.timezone.utc)
        for _ in range(length)
    ]


def generate_test_dataset(spark_session: pyspark.sql.SparkSession, n_rows: int = 50000) -> pyspark.sql.DataFrame:
    import pandas as pd
    import pyspark.sql.types as ps_types

    schema = ps_types.StructType(
        [
            ps_types.StructField(
                "struct_column",
                ps_types.StructType([ps_types.StructField("value", ps_types.StringType())]),
            ),
            ps_types.StructField("string_column", ps_types.StringType()),
            ps_types.StructField("long_column", ps_types.LongType()),
            ps_types.StructField("double_column", ps_types.DoubleType()),
            ps_types.StructField("decimal_column", ps_types.DecimalType(precision=5, scale=2)),
            # The DecimalType must have fixed precision (the maximum total number of digits)
            # and scale (the number of digits on the right of dot). For example, (5, 2) can
            # support the value from [-999.99 to 999.99].
            # For example, (5, 2) can support the value from [-999.99 to 999.99].
            ps_types.StructField("date_column", ps_types.DateType()),
            ps_types.StructField("timestamp_column", ps_types.TimestampType()),
            ps_types.StructField(
                "struct_within_array_column",
                ps_types.ArrayType(
                    ps_types.StructType([ps_types.StructField("value", ps_types.StringType())]),
                ),
            ),
        ],
    )
    pdf = pd.DataFrame(
        data={
            "struct_column": random_struct_array(n_rows),
            "string_column": random_string_array(n_rows),
            "long_column": rng.integers(1337, size=n_rows),
            "double_column": (
                rng.random(
                    n_rows,
                )
                * 1337
            ),
            "decimal_column": random_decimal_array(n_rows),
            "date_column": random_date_array(n_rows),
            "timestamp_column": random_datetime_array(n_rows),
            "struct_within_array_column": random_struct_with_array_array(n_rows),
        },
    )
    return spark_session.createDataFrame(pdf, schema=schema).repartition(10)


class TestSingleton:
    iris_new_transaction: api_types.Transaction | None = None

    @cached_property
    def ctx(self) -> FoundryContext:
        return FoundryContext()

    @cached_property
    def v1_client(self) -> FoundryRestClient:
        return FoundryRestClient(ctx=self.ctx)

    @cached_property
    def iris_csv_content(self):
        return TEST_FOLDER.joinpath("test_data", "iris", "iris.csv").read_bytes()

    @cached_property
    def iris_new(self) -> Dataset:
        _iris_new = self.ctx.get_dataset_by_path(
            INTEGRATION_TEST_COMPASS_ROOT_PATH + "/iris_new",
            create_if_not_exist=True,
        )
        if _iris_new.__created__:
            self.iris_new_transaction = transaction = _iris_new.put_file(
                "iris.csv",
                self.iris_csv_content,
            )
            _iris_new.upload_schema(transaction["rid"], schema=IRIS_SCHEMA)
        else:
            self.iris_new_transaction = _iris_new.get_last_transaction()

        return _iris_new

    @cached_property
    def iris_no_schema(self) -> Dataset:
        _iris_no_schema = self.ctx.get_dataset_by_path(
            INTEGRATION_TEST_COMPASS_ROOT_PATH + "/iris_new_no_schema_v1",
            create_if_not_exist=True,
        )
        if _iris_no_schema.__created__:
            _iris_no_schema.put_file(
                "iris.csv",
                self.iris_csv_content,
            )
        return _iris_no_schema

    @cached_property
    def empty_dataset(self) -> Dataset:
        return self.ctx.get_dataset_by_path(
            INTEGRATION_TEST_COMPASS_ROOT_PATH + "/empty_v1",
            create_if_not_exist=True,
        )

    @cached_property
    def complex_dataset(self) -> Dataset:
        _complex_dataset = self.ctx.get_dataset_by_path(
            INTEGRATION_TEST_COMPASS_ROOT_PATH + "/many_types_v3",
            create_if_not_exist=True,
        )

        if _complex_dataset.__created__:
            sdf = generate_test_dataset(self.spark_session)
            _complex_dataset.save_dataframe(sdf, foundry_schema=FOUNDRY_SCHEMA_COMPLEX_DATASET)
        return _complex_dataset

    @cached_property
    def spark_session(self):
        return get_spark_session()

    def generic_upload_dataset_if_not_exists(
        self,
        name: str,
        upload_folder: Path | None = None,
        foundry_schema: api_types.FoundrySchema | None = None,
    ) -> tuple[Dataset, api_types.Transaction | None]:
        ds = self.ctx.get_dataset_by_path(INTEGRATION_TEST_COMPASS_ROOT_PATH + "/" + name, create_if_not_exist=True)
        if ds.__created__ is True and upload_folder is not None:
            transaction = ds.upload_files(
                path_file_dict={
                    file.relative_to(upload_folder).as_posix().lstrip("/"): file
                    for file in upload_folder.rglob("*")
                    if not file.is_dir()
                },
            )
            if foundry_schema is not None:
                ds.upload_schema(transaction["rid"], foundry_schema)
            return ds, transaction
        return ds, None
