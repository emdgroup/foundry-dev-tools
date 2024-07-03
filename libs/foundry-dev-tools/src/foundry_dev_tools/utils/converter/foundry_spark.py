"""Helper function for conversion of data structures."""

from __future__ import annotations

import tempfile
from typing import TYPE_CHECKING

from foundry_dev_tools.utils.spark import get_spark_session

if TYPE_CHECKING:
    import pyarrow as pa
    import pyspark.sql

    from foundry_dev_tools.utils import api_types


def foundry_schema_to_spark_schema(
    foundry_schema: dict,
) -> pyspark.sql.types.StructType:
    """Converts foundry json schema format to spark StructType schema.

        See the table below for supported field types:

    .. csv-table::
        :header: "Type";"FieldType";"Python type";"Aliases"
        :delim: ;
        :class: longtable

         Array; :class:`ArrayFieldType`; list, tuple, or array ;
         Boolean; :class:`BooleanFieldType`; bool; bool, boolean
         Binary; :class:`BinaryFieldType`; bytearray; binary, bytes
         Byte; :class:`ByteFieldType`; int or long; byte, int8
         Date; :class:`DateFieldType`; datetime.date; date
         Decimal; :class:`DecimalFieldType`; decimal.Decimal; decimal
         Double; :class:`DoubleFieldType`; float; double, float64
         Float; :class:`FloatFieldType`; float; float, float32
         Integer; :class:`IntegerFieldType`; int or long; integer, int, int32
         Long; :class:`LongFieldType`; long; long, int64
         Map; :class:`MapFieldType`; dict;
         Short; :class:`ShortFieldType`; int or long; short, int16
         String; :class:`StringFieldType`; string; string, str
         Struct; :class:`StructFieldType`; list or tuple ;
         Timestamp; :class:`TimestampFieldType`; datetime.timestamp; timestamp datetime

    Args:
        foundry_schema: output from foundry's schema API

    Returns:
        :external+spark:py:class:`~pyspark.sql.types.StructType`:
            Spark schema from foundry schema

    """
    from foundry_dev_tools._optional.pyspark import pyspark_sql_types

    spark_schema_json = _parse_fields(foundry_schema["fieldSchemaList"])
    return pyspark_sql_types.StructType.fromJson(spark_schema_json)


def _parse_fields(fields: list) -> dict:
    return {"type": "struct", "fields": [_parse_field(field) for field in fields]}


def _parse_field(field: dict) -> dict:
    spark_field = {
        "metadata": field["customMetadata"],
        "nullable": True,
        "type": field["type"].lower(),
    }
    if "name" in field and field["name"] is not None:
        spark_field["name"] = field["name"]
    if "nullable" in field and field["nullable"] is not None:
        spark_field["nullable"] = field["nullable"]
    if "Noneable" in field and field["noneable"] is not None:
        spark_field["nullable"] = field["Noneable"]
    if field["type"] == "DECIMAL":
        spark_field["type"] = f"decimal({field['precision']}, {field['scale']})"
    elif field["type"] == "STRING":
        spark_field["type"] = "string"
    elif field["type"] == "DATE":
        spark_field["type"] = "date"
    elif field["type"] == "STRUCT":
        spark_field["type"] = _parse_fields(field["subSchemas"])
    elif field["type"] == "ARRAY":
        element_type = _parse_field(field["arraySubtype"])
        spark_field["type"] = {
            "type": "array",
            "elementType": element_type["type"],
            "containsNull": field["nullable"],
        }
    elif field["type"] == "MAP":
        map_value_type = _parse_field(field["mapValueType"])
        spark_field["type"] = {
            "type": "map",
            "keyType": _parse_field(field["mapKeyType"])["type"],
            "valueType": map_value_type["type"],
            "valueContainsNull": map_value_type["nullable"],
        }
    return spark_field


def spark_schema_to_foundry_schema(
    spark_schema: pyspark.sql.types.StructType | dict,
    file_format: str = "parquet",
) -> dict:
    """Converts spark_schema to foundry schema API compatible payload.

    Args:
        spark_schema: output from foundry's schema API
        file_format: currently only parquet supported

    Returns:
        :py:class:`dict`:
            foundry schema from spark schema

    """
    if file_format != "parquet":
        raise NotImplementedError

    if not isinstance(spark_schema, dict):
        spark_schema = spark_schema.jsonValue()
    foundry_schema = {
        "fieldSchemaList": [],
    }
    for struct_field in spark_schema["fields"]:
        if isinstance(struct_field["type"], dict):
            new_field = _parse_complex_type(struct_field)
        else:
            new_field = _parse_simple_type(struct_field)
        foundry_schema["fieldSchemaList"].append(new_field)

    if file_format == "parquet":
        foundry_schema["dataFrameReaderClass"] = "com.palantir.foundry.spark.input.ParquetDataFrameReader"
        foundry_schema["customMetadata"] = {"format": "parquet"}

    return foundry_schema


def infer_dataset_format_from_foundry_schema(
    foundry_schema: api_types.FoundrySchema | None,
    list_of_files: list,
) -> str | None:
    """Infers dataset format from Foundry Schema dict, looking at key dataFrameReaderClass.

    Args:
        foundry_schema: Schema from foundry schema API
        list_of_files: files of dataset, as fallback option, first file will be checked
            for file ending

    Returns:
        str | None:
            parquet, csv or unknown

    """
    default_format = "unknown"
    if foundry_schema is not None:
        if "ParquetDataFrameReader" in foundry_schema["dataFrameReaderClass"]:
            default_format = "parquet"
        elif "TextDataFrameReader" in foundry_schema["dataFrameReaderClass"]:
            default_format = "csv"

    if default_format == "unknown" and len(list_of_files) > 0 and "csv" in list_of_files[0]:
        default_format = "csv"
    elif default_format == "unknown" and len(list_of_files) > 0 and "parquet" in list_of_files[0]:
        default_format = "parquet"

    return default_format


def _parse_simple_type(struct_field: dict) -> dict:
    new_field = {"type": struct_field["type"].upper().split("(")[0]}
    if "name" in struct_field:
        new_field["name"] = struct_field["name"]
    if "metadata" in struct_field:
        new_field["customMetadata"] = struct_field["metadata"]
    if "nullable" in struct_field:
        new_field["nullable"] = struct_field["nullable"]

    if "decimal" in struct_field["type"]:
        new_field["precision"] = int(struct_field["type"].split("(")[1].split(",")[0])
        new_field["scale"] = int(struct_field["type"].split("(")[1].split(",")[1].split(")")[0])
    return new_field


def _parse_complex_type(field: dict) -> dict:
    field["type"]["type"] = field["type"]["elementType"]
    new_field = {
        "type": "ARRAY",
        "name": field["name"],
        "nullable": field["nullable"],
        "customMetadata": field["metadata"],
        "arraySubtype": _parse_simple_type(field["type"]),
    }
    new_field["arraySubtype"]["nullable"] = field["type"]["containsNull"]
    if "customMetadata" not in new_field["arraySubtype"]:
        new_field["arraySubtype"]["customMetadata"] = {}
    return new_field


def foundry_sql_data_to_spark_dataframe(  # noqa: C901
    data: list[list],
    spark_schema: pyspark.sql.types.StructType,
) -> pyspark.sql.DataFrame:
    """Converts the result of a foundry sql API query to a spark dataframe.

    Args:
        data: list of list of data
        spark_schema: the spark schema to apply

    Returns:
        :external+spark:py:class:`~pyspark.sql.DataFrame`:
            spark dataframe from foundry sql data

    """
    from foundry_dev_tools._optional.pyspark import pyspark_sql, pyspark_sql_types

    timestamp_columns = []
    date_columns = []
    decimal_columns = {}
    for field in spark_schema:
        if field.dataType == pyspark_sql_types.TimestampType():
            timestamp_columns.append(field.name)
            field.dataType = pyspark_sql_types.StringType()
        if field.dataType == pyspark_sql_types.DateType():
            date_columns.append(field.name)
            field.dataType = pyspark_sql_types.StringType()
        if "decimal" in field.dataType.jsonValue():
            decimal_columns[field.name] = field.dataType.jsonValue()
            field.dataType = pyspark_sql_types.StringType()

    for i, row in enumerate(data):
        for j, col in enumerate(row):
            if spark_schema[j].dataType == pyspark_sql_types.DoubleType() and isinstance(col, str):
                data[i][j] = float(col)

    spark_df = get_spark_session().createDataFrame(data, spark_schema)
    for col in timestamp_columns:
        spark_df = spark_df.withColumn(col, pyspark_sql.functions.to_timestamp(col))
    for col in date_columns:
        spark_df = spark_df.withColumn(col, pyspark_sql.functions.to_date(col))
    for col, dtype in decimal_columns.items():
        spark_df = spark_df.withColumn(col, pyspark_sql.functions.col(col).cast(dtype))
    return spark_df


def foundry_schema_to_read_options(
    foundry_schema: dict,
) -> dict:
    """Converts Foundry Schema Metadata to Spark Read Options.

    Args:
        foundry_schema: output from foundry's schema API

    Returns:
        :py:class:`dict`:
            Key, values that can be passed to the 'options' call of a pyspark reader

    """
    read_options = {}
    if (
        "textParserParams" in foundry_schema["customMetadata"]
        and "parser" in foundry_schema["customMetadata"]["textParserParams"]
        and foundry_schema["customMetadata"]["textParserParams"]["parser"] == "MULTILINE_CSV_PARSER"
    ):
        read_options["multiline"] = "true"
    if (
        "textParserParams" in foundry_schema["customMetadata"]
        and "skipLines" in foundry_schema["customMetadata"]["textParserParams"]
        and foundry_schema["customMetadata"]["textParserParams"]["skipLines"] > 0
    ):
        read_options["header"] = "true"
    return read_options


def foundry_schema_to_dataset_format(
    foundry_schema: dict,
) -> str:
    """Infers from Foundry Schema Metadata one of 'parquet', 'avro', 'csv', 'json'.

    Args:
        foundry_schema: output from foundry's schema API

    Returns:
        :py:class:`str`:
            value indicating spark reader required

    Raises:
        ValueError: If the dataset format can't be inferred from the schema
    """
    if "ParquetDataFrameReader" in foundry_schema["dataFrameReaderClass"]:
        return "parquet"
    if "TextDataFrameReader" in foundry_schema["dataFrameReaderClass"]:
        return "csv"
    if "AvroDataFrameReader" in foundry_schema["dataFrameReaderClass"]:
        return "avro"
    if (
        "DataSourceDataFrameReader" in foundry_schema["dataFrameReaderClass"]
        and foundry_schema["customMetadata"]["format"] == "avro"
    ):
        return "avro"
    if (
        "DataSourceDataFrameReader" in foundry_schema["dataFrameReaderClass"]
        and foundry_schema["customMetadata"]["format"] == "json"
    ):
        return "json"
    msg = f"Can not infer dataset format for schema {foundry_schema=}"
    raise ValueError(msg)


def arrow_stream_to_spark_dataframe(
    stream_reader: pa.ipc.RecordBatchStreamReader,
) -> pyspark.sql.DataFrame:
    """Dumps an arrow stream to a parquet file in a temporary directory.

    And reads the parquet file with spark.

    Args:
        stream_reader: Arrow Stream

    Returns:
        :external+spark:py:class:`~pyspark.sql.DataFrame`:
            converted to a Spark DataFrame

    """
    from foundry_dev_tools._optional.pyarrow import pq

    temporary_parquet_file = f"{tempfile.mkdtemp(suffix='foundry_dev_tools_sql_temp_result_set')}/query-result.parquet"

    pq.write_table(stream_reader.read_all(), temporary_parquet_file, flavor="spark")
    return get_spark_session().read.format("parquet").load(temporary_parquet_file)
