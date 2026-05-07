```{warning}
The FoundryRestClient and CachedFoundryClient are deprecated in the v2, it should work the same as the one in v1. But now it acts only as a wrapper around [the new v2 clients](#foundry_dev_tools.clients).

The following samples will include the way the new clients and classes can be used in v2, and the old way how it was done in the v1.
```

# API clients

## Pandas DataFrame from spark SQL dialect

Queries the Foundry SQL server with spark SQL dialect.


````{tab} v2
```python
from foundry_dev_tools import FoundryContext

ctx = FoundryContext()
df = ctx.foundry_sql_server.query_foundry_sql(
    "SELECT * FROM `/path/to/test_dataset`", branch="master"
)  # returns pandas dataframe by default, can be changed by setting the return_type parameter
print(df.shape)
```
````

````{tab} v1

```python
from foundry_dev_tools import FoundryRestClient

rest_client = FoundryRestClient()
df = rest_client.query_foundry_sql("SELECT * FROM `/path/to/test_dataset`", branch='master')
df.to_string()
```
````

:::{seealso}
[With the `Dataset` class](./dataset.md#polars-dataframe-from-spark-sql-dialect)
:::

## Download a dataset to local cache

If dataset isn't already in the cache, download it to the cache and returns a PySpark DataFrame. Useful when reusing datasets.

```python
from foundry_dev_tools import CachedFoundryClient

cached_client = CachedFoundryClient()
df = cached_client.load_dataset('/path/to/test_dataset', branch='master')
df.toPandas().to_string()
```

## foundry-platform-sdk and OSDK integration

FDT can act as the auth provider for the [foundry-platform-sdk](https://pypi.org/project/foundry-platform-sdk/) and any TPA-specific OSDK package generated from a Foundry third-party application (TPA).

Install the optional dependency first:

```shell
pip install 'foundry-dev-tools[public]'
```

### Using the built-in foundry-platform-sdk client

`ctx.public_client_v2` returns a pre-configured `foundry_sdk.v2.FoundryClient`:

```python
import pyarrow as pa
import polars as pl
from foundry_dev_tools import FoundryContext

ctx = FoundryContext()
client = ctx.public_client_v2
ds = client.datasets.Dataset.read_table("<dataset-rid>", format="ARROW")
df = pl.from_arrow(pa.ipc.open_stream(ds).read_all())
```

### Using a TPA OSDK package

`ctx.public_auth` returns a `FoundryDevToolsAuth` adapter that can be passed directly to any OSDK client:

```python
from foundry_dev_tools import FoundryContext
from my_tpa_sdk import FoundryClient  # your generated OSDK package

ctx = FoundryContext()
client = FoundryClient(auth=ctx.public_auth, hostname=ctx.host.domain)
```
