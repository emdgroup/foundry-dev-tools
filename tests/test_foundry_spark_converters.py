from pandas.testing import assert_frame_equal
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from foundry_dev_tools.utils.converter.foundry_spark import (
    foundry_schema_to_spark_schema,
    foundry_sql_data_to_spark_dataframe,
    infer_dataset_format_from_foundry_schema,
    spark_schema_to_foundry_schema,
)


def test_valid_foundry_schema_to_spark():
    spark_schema = foundry_schema_to_spark_schema(
        {
            "fieldSchemaList": [
                {
                    "type": "STRING",
                    "name": "Company",
                    "nullable": True,
                    "customMetadata": {},
                },
                {
                    "type": "DATE",
                    "name": "Established_Date",
                    "nullable": False,
                    "customMetadata": {},
                },
            ]
        }
    )

    assert spark_schema == StructType(
        [
            StructField("Company", StringType(), True, {}),
            StructField("Established_Date", DateType(), False, {}),
        ]
    )


def test_valid_spark_schema_to_foundry():
    spark_schema = StructType(
        [
            StructField("Company", StringType(), True, {}),
            StructField("Established_Date", DateType(), False, {}),
        ]
    )

    foundry_schema = spark_schema_to_foundry_schema(spark_schema, "parquet")

    assert foundry_schema == {
        "fieldSchemaList": [
            {
                "type": "STRING",
                "name": "Company",
                "nullable": True,
                "customMetadata": {},
            },
            {
                "type": "DATE",
                "name": "Established_Date",
                "nullable": False,
                "customMetadata": {},
            },
        ],
        "dataFrameReaderClass": "com.palantir.foundry.spark.input.ParquetDataFrameReader",
        "customMetadata": {"format": "parquet"},
    }


def test_valid_schema_nullable():
    spark_schema = foundry_schema_to_spark_schema(
        {
            "fieldSchemaList": [
                {
                    "type": "STRING",
                    "name": "roles",
                    "nullable": None,
                    "customMetadata": {},
                }
            ]
        }
    )

    assert spark_schema == StructType([StructField("roles", StringType(), True, {})])

    spark_schema = foundry_schema_to_spark_schema(
        {
            "fieldSchemaList": [
                {
                    "type": "STRING",
                    "name": "roles",
                    "customMetadata": {},
                }
            ]
        }
    )

    assert spark_schema == StructType([StructField("roles", StringType(), True, {})])

    spark_schema = foundry_schema_to_spark_schema(
        {
            "fieldSchemaList": [
                {
                    "type": "STRING",
                    "name": "roles",
                    "nullable": False,
                    "customMetadata": {},
                }
            ]
        }
    )

    assert spark_schema == StructType([StructField("roles", StringType(), False, {})])

    spark_schema = foundry_schema_to_spark_schema(
        {
            "fieldSchemaList": [
                {
                    "type": "STRING",
                    "name": "roles",
                    "nullable": True,
                    "customMetadata": {},
                }
            ]
        }
    )

    assert spark_schema == StructType([StructField("roles", StringType(), True, {})])


def test_valid_schema_decimal():
    spark_schema = foundry_schema_to_spark_schema(
        {
            "fieldSchemaList": [
                {
                    "type": "DECIMAL",
                    "name": "price",
                    "nullable": None,
                    "userDefinedTypeClass": None,
                    "customMetadata": {},
                    "arraySubtype": None,
                    "precision": 17,
                    "scale": 2,
                    "mapKeyType": None,
                    "mapValueType": None,
                    "subSchemas": None,
                }
            ]
        }
    )

    assert spark_schema == StructType(
        [StructField("price", DecimalType(17, 2), True, {})]
    )


def test_valid_schema_spark_to_foundry_decimal():
    foundry_schema = spark_schema_to_foundry_schema(
        StructType([StructField("price", DecimalType(17, 2), True, {})])
    )

    assert foundry_schema == {
        "fieldSchemaList": [
            {
                "type": "DECIMAL",
                "name": "price",
                "nullable": True,
                "customMetadata": {},
                "precision": 17,
                "scale": 2,
            }
        ],
        "dataFrameReaderClass": "com.palantir.foundry.spark.input.ParquetDataFrameReader",
        "customMetadata": {"format": "parquet"},
    }


# TypeError: field double_col: DoubleType can not accept object 'NaN' in type <class 'str'>
def test_float_nan_values(spark_session):
    schema = StructType([StructField("double_col", DoubleType(), True, {})])
    spark_df = foundry_sql_data_to_spark_dataframe([["NaN"], [1.0]], schema)

    assert_frame_equal(
        spark_df.toPandas(),
        spark_session.createDataFrame([[float("NaN")], [1.0]], schema).toPandas(),
    )


def test_array_type_foundry_to_spark():
    spark_schema = foundry_schema_to_spark_schema(
        {
            "fieldSchemaList": [
                {
                    "type": "ARRAY",
                    "name": "purple_alias",
                    "nullable": True,
                    "customMetadata": {},
                    "arraySubtype": {
                        "type": "STRING",
                        "name": None,
                        "nullable": True,
                        "userDefinedTypeClass": None,
                        "customMetadata": {},
                    },
                }
            ]
        }
    )

    assert spark_schema == StructType(
        [StructField("purple_alias", ArrayType(StringType()), True, {})]
    )


def test_array_type_spark_to_foundry(spark_session):
    foundry_schema = spark_schema_to_foundry_schema(
        StructType([StructField("purple_alias", ArrayType(StringType()), True, {})])
    )

    assert foundry_schema == {
        "fieldSchemaList": [
            {
                "type": "ARRAY",
                "name": "purple_alias",
                "nullable": True,
                "customMetadata": {},
                "arraySubtype": {
                    "type": "STRING",
                    "nullable": True,
                    "customMetadata": {},
                },
            }
        ],
        "dataFrameReaderClass": "com.palantir.foundry.spark.input.ParquetDataFrameReader",
        "customMetadata": {"format": "parquet"},
    }


def test_infer_csv_format():
    foundry_schema = {
        "fieldSchemaList": [
            {
                "type": "STRING",
                "name": "given_name",
                "nullable": True,
                "customMetadata": {},
            }
        ],
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

    assert infer_dataset_format_from_foundry_schema(foundry_schema, []) == "csv"


def test_infer_parquet_format():
    foundry_schema = {
        "fieldSchemaList": [
            {
                "type": "STRING",
                "name": "given_name",
                "nullable": True,
                "customMetadata": {},
            }
        ],
        "dataFrameReaderClass": "com.palantir.foundry.spark.input.ParquetDataFrameReader",
        "customMetadata": {"format": "parquet", "options": {}},
    }

    assert infer_dataset_format_from_foundry_schema(foundry_schema, []) == "parquet"


def test_infer_unknown_fallback_csv():
    assert infer_dataset_format_from_foundry_schema(None, ["test.csv"]) == "csv"


def test_infer_unknown():
    assert infer_dataset_format_from_foundry_schema(None, []) == "unknown"


def test_infer_unknown_fallback_parquet():
    assert (
        infer_dataset_format_from_foundry_schema(
            {
                "dataFrameReaderClass": "com.palantir.foundry.spark.input.SomethingDifferent"
            },
            ["jop.parquet"],
        )
        == "parquet"
    )


def test_complicated_schema():
    foundry_schema = {
        "fieldSchemaList": [
            {
                "type": "STRUCT",
                "name": "url",
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
            {
                "type": "STRING",
                "name": "entity_def_id",
                "nullable": True,
                "userDefinedTypeClass": None,
                "customMetadata": {},
                "arraySubtype": None,
                "precision": None,
                "scale": None,
                "mapKeyType": None,
                "mapValueType": None,
                "subSchemas": [],
            },
            {
                "type": "MAP",
                "name": "map_column",
                "nullable": True,
                "userDefinedTypeClass": None,
                "customMetadata": {},
                "arraySubtype": None,
                "precision": None,
                "scale": None,
                "mapKeyType": {
                    "type": "STRING",
                    "name": "map_key",
                    "nullable": True,
                    "userDefinedTypeClass": None,
                    "customMetadata": {},
                    "arraySubtype": None,
                    "precision": None,
                    "scale": None,
                    "mapKeyType": None,
                    "mapValueType": None,
                    "subSchemas": [],
                },
                "mapValueType": {
                    "type": "INTEGER",
                    "name": "map_value",
                    "nullable": False,
                    "userDefinedTypeClass": None,
                    "customMetadata": {},
                    "arraySubtype": None,
                    "precision": None,
                    "scale": None,
                    "mapKeyType": None,
                    "mapValueType": None,
                    "subSchemas": [],
                },
                "subSchemas": [],
            },
            {
                "type": "ARRAY",
                "name": "activity_entities",
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
                            "name": "permalink",
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
        "customMetadata": {"format": "parquet", "options": {}},
    }
    expected_spark_schema = StructType(
        [
            StructField(
                "url",
                StructType(
                    [
                        StructField("value", StringType(), True),
                    ]
                ),
            ),
            StructField("entity_def_id", StringType(), True),
            StructField(
                "map_column",
                MapType(StringType(), IntegerType(), valueContainsNull=False),
                True,
            ),
            StructField(
                "activity_entities",
                ArrayType(StructType([StructField("permalink", StringType(), True)])),
                True,
            ),
        ]
    )

    result = foundry_schema_to_spark_schema(foundry_schema)
    assert result == expected_spark_schema


def test_complicated_schema_null_values_not_returned():
    foundry_schema = {
        "fieldSchemaList": [
            {
                "type": "STRUCT",
                "name": "url",
                "nullable": True,
                "customMetadata": {},
                "subSchemas": [
                    {
                        "type": "STRING",
                        "name": "value",
                        "nullable": True,
                        "customMetadata": {},
                    }
                ],
            },
            {
                "type": "STRING",
                "name": "entity_def_id",
                "nullable": True,
                "customMetadata": {},
                "subSchemas": [],
            },
            {
                "type": "MAP",
                "name": "map_column",
                "nullable": True,
                "customMetadata": {},
                "mapKeyType": {
                    "type": "STRING",
                    "name": "map_key",
                    "nullable": True,
                    "customMetadata": {},
                    "subSchemas": [],
                },
                "mapValueType": {
                    "type": "INTEGER",
                    "name": "map_value",
                    "nullable": False,
                    "customMetadata": {},
                    "subSchemas": [],
                },
                "subSchemas": [],
            },
            {
                "type": "ARRAY",
                "name": "activity_entities",
                "nullable": True,
                "customMetadata": {},
                "arraySubtype": {
                    "type": "STRUCT",
                    "nullable": True,
                    "customMetadata": {},
                    "subSchemas": [
                        {
                            "type": "STRING",
                            "name": "permalink",
                            "nullable": True,
                            "customMetadata": {},
                        }
                    ],
                },
            },
        ],
        "dataFrameReaderClass": "com.palantir.foundry.spark.input.ParquetDataFrameReader",
        "customMetadata": {"format": "parquet", "options": {}},
    }
    expected_spark_schema = StructType(
        [
            StructField(
                "url",
                StructType(
                    [
                        StructField("value", StringType(), True),
                    ]
                ),
            ),
            StructField("entity_def_id", StringType(), True),
            StructField(
                "map_column",
                MapType(StringType(), IntegerType(), valueContainsNull=False),
                True,
            ),
            StructField(
                "activity_entities",
                ArrayType(StructType([StructField("permalink", StringType(), True)])),
                True,
            ),
        ]
    )

    result = foundry_schema_to_spark_schema(foundry_schema)
    assert result == expected_spark_schema
