from __future__ import annotations

import datetime
import os
import random
import time
from decimal import Decimal
from functools import cached_property
from pathlib import Path
from random import choice
from string import ascii_uppercase
from typing import TYPE_CHECKING, TypeVar

import numpy as np
import pytest

from foundry_dev_tools.config.context import FoundryContext
from foundry_dev_tools.errors.meta import FoundryAPIError
from foundry_dev_tools.foundry_api_client import FoundryRestClient
from foundry_dev_tools.utils.spark import get_spark_session

if TYPE_CHECKING:
    from collections.abc import Callable

    import pyspark.sql

    from foundry_dev_tools.helpers.multipass import Group, User
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

INTEGRATION_TEST_PROJECT_RID = "ri.compass.main.folder.f94fc087-6068-4e66-ac27-978fce7b9d9d"
"""The rid of `ls-use-case-foundry-devtools-dev-workspace` project."""

MARKING_ID = "3abebc75-4915-4b5c-8749-9ce06726b6e1"
"""The id of `ls-use-case-foundry-devtools-dev-workspace` marking.

To carry out tests depending on this marking, the user must be member of
`ls-use-case-foundry-devtools-dev-workspace-editor` group (63d61c77-59e3-4f47-b16d-297996fc4c2d).
"""

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

PROJECT_GROUP_ROLE_VIEWER = "viewer"
PROJECT_GROUP_ROLE_EDITOR = "editor"
PROJECT_GROUP_ROLE_OWNER = "owner"

DEFAULT_PROJECT_GROUP_ROLES = [PROJECT_GROUP_ROLE_VIEWER, PROJECT_GROUP_ROLE_EDITOR, PROJECT_GROUP_ROLE_OWNER]
"""Default project roles for groups that are assigned to and created for groups of a project on Foundry."""

rng = np.random.default_rng()

T = TypeVar("T")

INITIAL_SLEEP_TIME = 1.0
MAX_RETRIES = 3


def backoff(
    func: Callable[[], T],
    predicate: Callable[[T], bool],
    sleep: float = INITIAL_SLEEP_TIME,
    num_retries: int = MAX_RETRIES,
) -> tuple[bool, T, int]:
    """Utility function to wrap a function which should be repeatedly called until a certain condition is satisfied.

    It implements the exponential backoff algorithm and repeats the function call until the condition is met
    or the maximum number of retries has exceeded. An application would be API calls that are exposed to delay,
    e.g. when requested changes become effective only after a short time delay. In this case it may require
    to wait until the changes are applied before proceeding.

    Args:
        func: The actual function call to be executed and whose result is passed to the 'predicate' function to check
        predicate: Predicate function that checks whether the result from 'func' satisfies a particular condition
        sleep: The duration to wait until repeating 'func' again. The sleep time increases exponentially.
            Defaults to :py:const:`INITIAL_SLEEP_TIME` if not provided.
        num_retries: The maximum number of times to retry 'func' and check for satisfaction of the condition
            before returning. Defaults to :py:const:`MAX_RETRIES` if not provided.

    Returns:
        tuple[bool, T, int]:
            the function returns a bool indicating whether the condition has been finally satisfied,
            second the result from the call to 'func', and the number of required retries at last

    """
    result = func()

    retries = 0
    while not predicate(result):
        time.sleep(sleep)
        result = func()
        retries += 1
        if retries == num_retries:
            break
        sleep += sleep
    return predicate(result), result, retries


def skip_test_on_error(status_code_to_skip: int, skip_message: str) -> Callable[[Callable], None]:
    """Decorator function to catch errors and skip the test for the specified status code. Otherwise it will raise the errors.

    Args:
        status_code_to_skip: The response status code for which to catch the error and skip the test
        skip_message: The message to pass on to :py:meth:`pytest.skip` when skipping a function
    Returns:
        Callable:
            the decorator function
    """  # noqa: E501

    def decorator(test_function):
        def wrapper(*args, **kwargs):
            try:
                test_function(*args, **kwargs)
            except FoundryAPIError as err:
                if err.response.status_code == status_code_to_skip:
                    pytest.skip(skip_message)
                else:
                    raise

        return wrapper

    return decorator


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

    @cached_property
    def user(self) -> User:
        return self.ctx.get_user_info()

    @cached_property
    def simple_group(self) -> Group:
        rnd = "".join(choice(ascii_uppercase) for _ in range(5))
        name = "test-group_" + rnd

        organization_rid = self.user.attributes["multipass:organization-rid"][0]

        return self.ctx.create_group(name, {organization_rid})

    @cached_property
    def project_groups(self) -> dict[str, Group]:
        rnd = "".join(choice(ascii_uppercase) for _ in range(5))
        base_group_name = "test-group_" + rnd
        organization_rid = self.user.attributes["multipass:organization-rid"][0]

        role_group_mapping = {}

        for role in DEFAULT_PROJECT_GROUP_ROLES:
            name = base_group_name + "-" + role

            group = self.ctx.create_group(name, {organization_rid})
            role_group_mapping[role] = group

        owner_group = role_group_mapping[PROJECT_GROUP_ROLE_OWNER]
        owner_group.add_members({self.user.id})

        role_group_mapping[PROJECT_GROUP_ROLE_EDITOR].update_managers(
            deleted_manager_managers={self.user.id}, new_manager_managers={owner_group.id}
        )
        role_group_mapping[PROJECT_GROUP_ROLE_VIEWER].update_managers(
            deleted_manager_managers={self.user.id}, new_manager_managers={owner_group.id}
        )

        return role_group_mapping
