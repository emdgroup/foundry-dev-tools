import datetime
import os
import os.path
import pathlib
import random
from decimal import Decimal
from typing import Tuple

import numpy as np

from foundry_dev_tools.foundry_api_client import DatasetNotFoundError

INTEGRATION_TEST_COMPASS_ROOT_PATH = pathlib.PurePosixPath(
    os.getenv(
        "INTEGRATION_TEST_COMPASS_ROOT_PATH",
        "/Global/global-use-case-public/Developer Experience/integration-test-folder",
    )
)
INTEGRATION_TEST_COMPASS_ROOT_RID = os.getenv(
    "INTEGRATION_TEST_COMPASS_ROOT_RID",
    "ri.compass.main.folder.c733ffcc-5a13-461e-8bdc-d1be925a0646",
)

TEST_FOLDER = pathlib.Path(__file__).parent.resolve()

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
        }
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
                }
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
                    }
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


def generic_upload_dataset_if_not_exists(
    client: "foundry_dev_tools.FoundryRestClient",
    name="iris_new",
    upload_folder=None,
    foundry_schema=None,
) -> Tuple[str, str, str, str, bool]:
    ds_path = f"{INTEGRATION_TEST_COMPASS_ROOT_PATH}/{name}"
    ds_branch = "master"
    newly_created = False
    try:
        identity = client.get_dataset_identity(
            dataset_path_or_rid=ds_path, branch=ds_branch
        )
        rid = identity["dataset_rid"]
        transaction_rid = identity["last_transaction_rid"]
    except DatasetNotFoundError:
        rid = client.create_dataset(ds_path)["rid"]
        _ = client.create_branch(rid, ds_branch)
        newly_created = True
        if upload_folder:
            recursive_listing = pathlib.Path(upload_folder).rglob("*")
            filenames_with_dirs = [
                path.as_posix()
                for path in recursive_listing
                if not any(part.startswith(".") for part in path.parts)
            ]
            filepaths = [
                str(file) for file in filenames_with_dirs if not os.path.isdir(file)
            ]
            dataset_paths_in_foundry = [
                path.replace(pathlib.Path(upload_folder).as_posix(), "")[1:]
                for path in filepaths
            ]
            path_file_dict = dict(zip(dataset_paths_in_foundry, filepaths))
            transaction_rid = client.open_transaction(dataset_rid=rid, mode="SNAPSHOT")
            client.upload_dataset_files(
                dataset_rid=rid,
                transaction_rid=transaction_rid,
                path_file_dict=path_file_dict,
            )
            client.commit_transaction(dataset_rid=rid, transaction_id=transaction_rid)
            if foundry_schema:
                client.upload_dataset_schema(
                    rid, transaction_rid, foundry_schema, ds_branch
                )
        else:
            transaction_rid = None
    return rid, ds_path, transaction_rid, ds_branch, newly_created


def random_string_array(length):
    A, Z = np.array(["A", "Z"]).view("int32")
    NO_CODES = length
    LEN = 20
    return np.random.randint(low=A, high=Z, size=NO_CODES * LEN, dtype="int32").view(
        f"U{LEN}"
    )


def random_struct_array(length):
    strings = random_string_array(length)
    return [{"value": str(string)} for string in strings]


def random_struct_with_array_array(length):
    strings = random_string_array(length)
    return [[{"value": str(string)}] for string in strings]


def random_decimal_array(length):
    np_type = (
        np.random.rand(
            length,
        ).round(2)
        * 999
    )
    return [Decimal(value) for value in np_type]


def random_date_array(length):
    start = datetime.date(1988, 11, 29)
    end = datetime.date(2022, 11, 30)
    return [random.random() * (end - start) + start for _ in range(length)]


def random_datetime_array(length):
    start = datetime.datetime(1988, 11, 29, tzinfo=datetime.timezone.utc)
    end = datetime.datetime(2022, 11, 30, tzinfo=datetime.timezone.utc)
    return [random.random() * (end - start) + start for _ in range(length)]


def generate_test_dataset(spark_session, output_folder, n_rows=50000):
    import numpy as np
    import pandas as pd
    import pyspark.sql.types as T

    schema = T.StructType(
        [
            T.StructField(
                "struct_column", T.StructType([T.StructField("value", T.StringType())])
            ),
            T.StructField("string_column", T.StringType()),
            T.StructField("long_column", T.LongType()),
            T.StructField("double_column", T.DoubleType()),
            T.StructField("decimal_column", T.DecimalType(precision=5, scale=2)),
            # The DecimalType must have fixed precision (the maximum total number of digits)
            # and scale (the number of digits on the right of dot). For example, (5, 2) can
            # support the value from [-999.99 to 999.99].
            # For example, (5, 2) can support the value from [-999.99 to 999.99].
            T.StructField("date_column", T.DateType()),
            T.StructField("timestamp_column", T.TimestampType()),
            T.StructField(
                "struct_within_array_column",
                T.ArrayType(T.StructType([T.StructField("value", T.StringType())])),
            ),
        ]
    )
    pdf = pd.DataFrame(
        data={
            "struct_column": random_struct_array(n_rows),
            "string_column": random_string_array(n_rows),
            "long_column": np.random.randint(1337, size=n_rows),
            "double_column": np.random.rand(
                n_rows,
            )
            * 1337,
            "decimal_column": random_decimal_array(n_rows),
            "date_column": random_date_array(n_rows),
            "timestamp_column": random_datetime_array(n_rows),
            "struct_within_array_column": random_struct_with_array_array(n_rows),
        }
    )
    df = spark_session.createDataFrame(pdf, schema=schema).repartition(10)
    spark_write_path = f"{output_folder}/spark"
    df.write.format("parquet").save(path=spark_write_path)
