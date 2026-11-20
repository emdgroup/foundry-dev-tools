# Fast Snowflake Ingestion

`FastSnowflakeIngestion` syncs data from Snowflake into a Foundry dataset from inside a Foundry **lightweight transform**.
It focuses on efficient, parallel file export/upload, without Spark and without a Data Connection agent.
It can be used to migrate existing Data Connection syncs to in-code solutions, but also for any new syncs, as long as the desired data is present on Snowflake.

The helper does not include data transformation utilities and expects users to prepare the data using a Snowpark DataFrame.

FSI requires `snowflake-snowpark-python` as an optional dependency, which is not installed with Foundry DevTools.
Add it to the dependencies of your lightweight transforms repository next to `foundry-dev-tools`.

**Contents**

- [Process overview](#process-overview)
- [Limitations](#limitations)
- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [Shared vs non-shared database](#shared-vs-non-shared-database)
- [Stage fallback](#stage-fallback)
- [S3 regional endpoint workaround](#s3-regional-endpoint-workaround)
  - [Problem](#problem)
  - [What the patch does](#what-the-patch-does)
  - [Alternative without the patch](#alternative-without-the-patch)
- [Session query tags](#session-query-tags)
- [Storing the session ID on Foundry](#storing-the-session-id-on-foundry)
- [Re-using the session for Foundry-to-Snowflake temporary tables](#re-using-the-session-for-foundry-to-snowflake-temporary-tables)
- [More examples](#more-examples)
  - [Full table sync with timestamp-to-date casting (shared database)](#full-table-sync-with-timestamp-to-date-casting-shared-database)
  - [Filtered sync with custom file size and parallelism (non-shared database)](#filtered-sync-with-custom-file-size-and-parallelism-non-shared-database)
- [Snowflake permissions: shared vs non-shared database](#snowflake-permissions-shared-vs-non-shared-database)

## Process overview

1. Unloads a Snowpark DataFrame to Parquet files in a Snowflake stage.
2. Downloads the files to a local temporary directory.
3. Uploads files to a Foundry output via the filesystem in parallel.
4. Commits the dataset transaction.
5. Removes the local temporary directory.

The most important requirement is a Snowpark Session object, which can be obtained via `transforms.tables.TableInput/TableLightweightInput.snowpark().session`.
After obtaining a session and deciding whether the Snowflake database is shared or non-shared, the `FastSnowflakeIngestion` object can be created.
This object either runs the whole sync via `save_snowpark_dataframe_to_foundry_output`, or the single steps can be called one by one to customize the pipeline.
The recommended approach is `save_snowpark_dataframe_to_foundry_output`, which runs everything in a single call.

## Limitations

- No built-in incremental ingestion.
- No automatic type conversions; operates on the Snowpark DataFrame types you provide.
- `TIMESTAMP_TZ` and `TIMESTAMP_LTZ` columns cannot be unloaded to Parquet, as Parquet has no type that stores a
  time zone (offset) per value. Cast these columns to another type before calling FSI, see
  [Full table sync with timestamp-to-date casting](#full-table-sync-with-timestamp-to-date-casting-shared-database)
  and the Snowflake [data unloading considerations](https://docs.snowflake.com/en/user-guide/data-unload-considerations).
- Intended for full or filtered table exports; does not transform data beyond what you do within Snowpark.
- Only works inside a Foundry lightweight transform, as it uses the Foundry transforms runtime to commit the transaction.

## Prerequisites

- A Snowpark session (e.g., via `transforms.tables.TableInput.snowpark().session`).
- A Foundry transform Output.
- If using a shared database stage:
  - Database name
  - Schema name
  - Permission to create a temporary stage in that schema

## Quick start

```python
import snowflake.snowpark as snow
from transforms.api import LightweightContext, Output, lightweight, transform
from transforms.tables import TableInput

from foundry_dev_tools.helpers.fast_snowflake_ingestion import FastSnowflakeIngestion


@lightweight(cpu_cores=4, memory_gb=4)
@transform(
    output=Output("foundry_output_path"),
    virtual_table=TableInput("virtual_table_rid"),
)
def run_sync(ctx: LightweightContext, output: Output, virtual_table: TableInput):

    session: snow.Session = virtual_table.snowpark().session

    fsi = FastSnowflakeIngestion(
        snowpark_session=session,
        output=output,
        provisioned_vcpus=4,
    )

    df = virtual_table.snowpark().dataframe()

    # <optional dataframe transformations>

    fsi.save_snowpark_dataframe_to_foundry_output(df)
```

## Shared vs non-shared database

- Non-shared database:
  - Uses the session's default stage via `get_session_stage()`.
  - Simpler setup, no additional permissions required.
- Shared database (`use_shared_db=True`, `db_name` and `schema_name` required):
  - Switches the session context to the provided database and schema.
  - Creates a temporary stage at `DB.SCHEMA.TEMP_EXPORT_<random_name>`.
  - Uses a fully qualified stage path.

## Stage fallback

When the primary stage setup fails (e.g., due to permission issues or stage unavailability), a fallback shared database stage can be used.
The fallback is only used when both `fallback_db_name` and `fallback_schema_name` are provided.
The stage is resolved during object initialization, so by the time the pipeline runs, a working stage is already in place.

The strategies are tried in order:

1. If `use_shared_db=True`, try the user-provided DB/schema.
2. If `use_shared_db=False`, try the session's default stage.
3. If the above fails, `enable_automatic_stage_fallback=True` (the default) and the fallback DB/schema are provided, try `fallback_db_name.fallback_schema_name`.

Each strategy catches `snowflake.connector.errors.Error`.
A new random stage name is generated for the fallback to avoid collisions with the failed attempt.
If all attempted strategies fail, the `snowflake.connector.errors.Error` from the last attempt is raised during initialization.

```python
# Fallback stage configured
fsi = FastSnowflakeIngestion(
    snowpark_session=session,
    output=output,
    provisioned_vcpus=4,
    fallback_db_name="FALLBACK_DB",
    fallback_schema_name="STAGING",
)

# Fallback explicitly disabled, any stage setup failure raises immediately
fsi = FastSnowflakeIngestion(
    snowpark_session=session,
    output=output,
    provisioned_vcpus=4,
    fallback_db_name="FALLBACK_DB",
    fallback_schema_name="STAGING",
    enable_automatic_stage_fallback=False,
)
```

You can check whether the fallback stage is active via `get_database_and_staging_info()`:

```python
info = fsi.get_database_and_staging_info()
print(info["is_using_fallback"])  # True if fallback was activated
```

When the fallback activates, it creates a temporary stage in the fallback schema. The Snowflake role used by your session must have:

- `USAGE` on the fallback database
- `USAGE` on the fallback schema
- `CREATE STAGE` on the fallback schema

## S3 regional endpoint workaround

By default, the constructor calls `disable_s3_transfer_acceleration()`, which monkeypatches the Snowflake connector's `SnowflakeS3RestClient`.
This works around [snowflake-connector-python#2317](https://github.com/snowflakedb/snowflake-connector-python/issues/2317).

### Problem

Downloading the unloaded files (`session.file.get`) talks directly to the S3 bucket behind the internal stage.
Unless the Snowflake server tells the connector to use the regional endpoint (`useS3RegionalUrl` in the stage info),
the connector uses the **global** bucket endpoint `https://<bucket>.s3.amazonaws.com`.
Connector versions before 4.0.0 additionally call `https://<bucket>.s3.amazonaws.com/?accelerate`
to check whether S3 transfer acceleration is enabled. Version 4.0.0 skips this check for internal stages,
but still uses the global endpoint.

If the network only allows the Snowflake account and the **regional** bucket endpoint
`https://<bucket>.s3.<region>.amazonaws.com`, for example a Foundry network egress policy allowing only these hosts,
the download fails with an error like:

```text
RequestExceedMaxRetryError: GET with url b'https://<bucket>.s3.amazonaws.com/?accelerate' failed for exceeding maximum retries
```

### What the patch does

- `_get_bucket_accelerate_config` always returns `False`, so the `?accelerate` call is never made.
- `transfer_accelerate_config` always sets the regional endpoint and reports acceleration as disabled.

The patch applies process-wide to every `SnowflakeS3RestClient`. Because `transfer_accelerate_config` is replaced entirely,
it also overrides endpoints sent by the server (e.g. FIPS endpoints) and the connector's China region handling.
To disable the patch, pass `enable_s3_monkeypatch=False` to the constructor.

### Alternative without the patch

In the issue above, setting the following account parameter made the server return the regional endpoint,
after which downloads worked without the patch.
Note that Snowflake does not document this parameter for this purpose,
and the related [private connectivity feature](https://docs.snowflake.com/en/user-guide/private-internal-stages-aws) requires the Business Critical edition.

```sql
use role accountadmin;
alter account set ENABLE_INTERNAL_STAGES_PRIVATELINK = true;
```

## Session query tags

When a `FastSnowflakeIngestion` object is created, it sets the Snowflake session's query tag to Foundry job telemetry data.
Additional custom tags can be passed via `custom_tags` as a JSON-like dictionary, but they cannot overwrite the default telemetry tags.
Snowflake limits query tags to 2000 characters.

```python
fsi = FastSnowflakeIngestion(
    snowpark_session=session,
    output=output,
    custom_tags={
        "custom_snowflake_tag_1": "my telemetry tag value 1",
        "custom_snowflake_tag_2": {
            "custom_nested_snowflake_tag_1": 123,
            "custom_nested_snowflake_tag_2": ["A", "B", "C"],
        },
    },
)
```

Example of the default telemetry tags:

```python
{
    "output_dataset_rid": "ri.foundry.main.dataset.00000-00005",
    "output_transaction_rid": "ri.foundry.main.transaction.00000-00123",
    "branch": "master",
    "job_rid": "ri.foundry.main.job.00000-00010",
    "sourceProvenance_details_code": {
        "repositoryRid": "ri.stemma.main.repository.00000-00000",
        "versionId": "84e49aff909709b121250a682193328020f37435",
        "file": "transforms-python/src/myproject/hello_world.py",
        "lineNumber": 2137,
    },
    "inputs": {
        "input_1": "ri.foundry.main.dataset.00000-00001",
        "virtual_table_1": "ri.tables.main.table.00000-00002",
        "input_2": "ri.foundry.main.dataset.00000-00004",
    },
    "outputs": {
        "out": "ri.foundry.main.dataset.00000-00005",
    },
    **custom_tags,
}
```

## Storing the session ID on Foundry

`save_snowpark_dataframe_to_foundry_output` has an optional, enabled by default, `save_session_id` parameter.
If set to True, it saves a 0-byte file to the output, with the Snowflake session ID as part of the file name, e.g.:

- `_snowflake_session_metadata/788272638628742.txt`

The session IDs can be retrieved with Foundry DevTools:

```python
import re
import sys

import foundry_dev_tools

DATASET_RIDS = """
Place any text here, only valid dataset RIDs will be extracted.

  ri.foundry.main.dataset.00000000-0000-0000-0000-000000000001
  ri.foundry.main.dataset.00000000-0000-0000-0000-000000000002
"""

SESSION_METADATA_PREFIX = "_snowflake_session_metadata/"
DATASET_RID_PATTERN = re.compile(r"ri\.foundry\.main\.dataset\.[0-9a-f-]{36}")


def get_session_id(ctx: foundry_dev_tools.FoundryContext, dataset_rid: str) -> str | None:
    try:
        files = ctx.catalog.list_dataset_files(dataset_rid, logical_path=SESSION_METADATA_PREFIX)
    except Exception as e:
        print(f"{dataset_rid}: error listing files - {e}", file=sys.stderr)
        return None

    for entry in files:
        path: str = entry.get("logicalPath", "")
        if path.startswith(SESSION_METADATA_PREFIX) and path.endswith(".txt"):
            return path.removeprefix(SESSION_METADATA_PREFIX).removesuffix(".txt")
    return None


ctx = foundry_dev_tools.FoundryContext()
for rid in dict.fromkeys(DATASET_RID_PATTERN.findall(DATASET_RIDS)):
    session_id = get_session_id(ctx, rid)
    if session_id is None:
        print(f"{rid}: no session ID found", file=sys.stderr)
    else:
        print(session_id)
```

## Re-using the session for Foundry-to-Snowflake temporary tables

The same `Session` can be used to create a temporary table on Snowflake within the same Foundry transaction.
This requires a shared database with schema and database names provided, as the stage setup calls `use_database()` and `use_schema()` on the session.

```python
session: snow.Session = virtual_table.snowpark().session
fsi = FastSnowflakeIngestion(
    snowpark_session=session,
    output=out,
    provisioned_vcpus=1,
    use_shared_db=True,
    db_name="DB_NAME",
    schema_name="SCHEMA_NAME",
)
sample_pl: pl.DataFrame = sample.polars()
sample_snow_df: snow.DataFrame = session.create_dataframe(data=sample_pl.to_dicts(), schema=sample_pl.columns)
sample_snow_df.show(10)
```

This Snowpark DataFrame can be used within the same Foundry transaction until the session gets closed.

If creating the DataFrame fails with an error like the one below, the Snowflake role of your source most likely lacks the privilege to create temporary tables:

```text
SQL compilation error:
Table 'SNOWPARK_TEMP_TABLE_XXXXXXXXXX' does not exist
```

Grant it with:

```sql
grant create temporary table on schema DB_NAME.SCHEMA_NAME to role "<SOURCE_ROLE>";
```

## More examples

### Full table sync with timestamp-to-date casting (shared database)

```python
import snowflake.snowpark as snow
import snowflake.snowpark.functions as SF
import snowflake.snowpark.types as ST
from transforms.api import LightweightContext, Output, lightweight, transform
from transforms.tables import TableInput

from foundry_dev_tools.helpers.fast_snowflake_ingestion import FastSnowflakeIngestion


@lightweight(cpu_cores=4, memory_gb=4)
@transform(
    output=Output("output_path"),
    virtual_table=TableInput("virtual_table_rid"),
)
def run_sync(ctx: LightweightContext, output: Output, virtual_table: TableInput):
    session: snow.Session = virtual_table.snowpark().session

    fsi = FastSnowflakeIngestion(
        snowpark_session=session,
        output=output,
        provisioned_vcpus=4,
        use_shared_db=True,
        db_name="SHARED_DB",
        schema_name="EXPORT_SCHEMA",
    )

    df = virtual_table.snowpark().dataframe()

    # TIMESTAMP_TZ/TIMESTAMP_LTZ cannot be unloaded to Parquet, so timestamp columns are cast before the sync.
    # df.dtypes reports every timestamp variant (NTZ, LTZ, TZ) as "timestamp", so all of them are cast.
    # Either cast to ST.TimestampType(ST.TimestampTimeZone.NTZ), which drops the time zone offset and keeps
    # the local date and time, or to ST.DateType(), which also drops the time of day (used below).
    for name, dtype in df.dtypes:
        tname = str(dtype).upper()
        if "TIMESTAMP" in tname or tname == "DATETIME":
            df = df.with_column(name, SF.col(name).cast(ST.DateType()))

    fsi.save_snowpark_dataframe_to_foundry_output(df)
```

### Filtered sync with custom file size and parallelism (non-shared database)

```python
import snowflake.snowpark as snow
import snowflake.snowpark.functions as SF
from transforms.api import LightweightContext, Output, lightweight, transform
from transforms.tables import TableInput

from foundry_dev_tools.helpers.fast_snowflake_ingestion import FastSnowflakeIngestion


@lightweight(cpu_cores=4, memory_gb=4)
@transform(
    output=Output("output_path"),
    virtual_table=TableInput("virtual_table_rid"),
)
def run_sync(ctx: LightweightContext, output: Output, virtual_table: TableInput):
    session: snow.Session = virtual_table.snowpark().session

    fsi = FastSnowflakeIngestion(
        snowpark_session=session,
        output=output,
        provisioned_vcpus=4,
    )

    df = virtual_table.snowpark().dataframe().filter(SF.col("sale_date") >= SF.to_date(SF.lit("2025-01-01")))

    fsi.save_snowpark_dataframe_to_foundry_output(
        df,
        cpu_cores=2,  # parallel upload threads will be max(1, cpu_cores - 1)
        copy_options={
            "MAX_FILE_SIZE": 200 * 1024 * 1024,  # 200 MB
            "OVERWRITE": True,
            "SINGLE": False,
            "INCLUDE_QUERY_ID": False,
            "DETAILED_OUTPUT": True,
        },
        format_type_options={"COMPRESSION": "AUTO"},
    )
```

## Snowflake permissions: shared vs non-shared database

The key difference between the two modes are the permissions required for the temporary stage.
Non-shared database mode uses the session's built-in `get_session_stage()`, which does not require additional permissions.
Shared database mode creates a temporary stage via `CREATE OR REPLACE TEMPORARY STAGE ...`, so the role used by your Foundry source needs the privilege to create stages in that schema.

- Granting object privileges typically requires SECURITYADMIN or ACCOUNTADMIN, or the object owner role.
- To operate on schema objects, the grantee must have USAGE on both the parent database and schema.
- Unquoted identifiers are stored/resolved in uppercase; quoted identifiers are case-sensitive. Be consistent with quoting in grants.

Example grants, where `<SOURCE_ROLE>` is the Snowflake role your Foundry source uses:

```sql
GRANT USAGE ON WAREHOUSE EXPORT_WH TO ROLE "<SOURCE_ROLE>";
GRANT USAGE ON DATABASE EXPORT_DB TO ROLE "<SOURCE_ROLE>";
GRANT USAGE ON SCHEMA EXPORT_DB.EXPORT TO ROLE "<SOURCE_ROLE>";
GRANT CREATE STAGE ON SCHEMA EXPORT_DB.EXPORT TO ROLE "<SOURCE_ROLE>";
```
