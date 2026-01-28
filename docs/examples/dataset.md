# Dataset

## Info

The dataset class is an object-oriented way to use the Foundry DevTools API clients.

All methods which require a transaction to work will create one automatically and commit it automatically.
This works via the [`transaction_context`](#foundry_dev_tools.resources.dataset.Dataset.transaction_context) context manager (click the link to see the documentation for it).

Examples:
```python
with ds.transaction_context():
    # will start the transaction
    print(ds.transaction) # will print the dictionary of the transaction object
    ds.put_file(...)
    ds.remove_file(...)
    # will commit the transaction unless an error happened

print(ds.transaction) # will throw an error as there is currently no open transaction
```

You can also chain multiple actions together.
Only works with methods that return the dataset, e.g. `list_files` obviously does not return the Dataset class but the list of files and so on.

```python
ds = ctx.get_dataset(...)
ds.start_transaction().put_file(...).upload_schema(...).commit_transaction()
```

And if you create a transaction manually before using the context it won't do anything.

```python
ds.start_transaction()
with ds.transaction_context():
    # will not start a new transaction
    print(ds.transaction) # will print the transaction started earlier
    # will not commit or abort the transaction

print(ds.transaction) # still accessible and open

# you'll need to close it manually 
ds.commit_transaction()
```

The state/attributes of resources like the [`Dataset`](#foundry_dev_tools.resources.dataset.Dataset) class may get out of "sync" if you have multiple instances for the same dataset, or modify the dataset in other ways than through the Dataset object.

```python
ds.sync()
```

### Uploading a dataset to Foundry

Saves a Pandas or PySpark dataframe to Foundry.

````{tab} v2
```python
from foundry_dev_tools import FoundryContext
import pandas as pd

df = pd.DataFrame({"a": [0, 1, 2], "b": [1, 2, 3]})
ctx = FoundryContext()
dataset = ctx.get_dataset_by_path("/path/to/test_output_dataset", create_if_not_exist=True)
dataset.save_dataframe(df)
```
````

````{tab} v1
```python
from foundry_dev_tools import CachedFoundryClient

cached_client = CachedFoundryClient()
cached_client.save_dataset(df, '/path/to/test_output_dataset',
                           branch='master', exists_ok=True, mode='SNAPSHOT')
```
````
### Uploading a folder to a dataset in Foundry

Upload the complete content of a local folder to a dataset in Foundry
````{tab} v2
```python
from foundry_dev_tools import FoundryContext
from pathlib import Path

ctx = FoundryContext()
dataset = ctx.get_dataset_by_path("/path/to/test_folder_upload", create_if_not_exist=True)
dataset.upload_folder(Path("/path/to/folder-to-upload"))
```
````

````{tab} v1
```python
import os
from foundry_dev_tools import FoundryRestClient

upload_folder = "/path/to/folder-to-upload"
target_dataset_path = "/path/to/test_folder_upload"

file_paths = [file for file in Path(upload_folder).rglob("*") if file.is_file() and not file.name.startswith(".")]
dataset_paths_in_foundry = [str(file_path.relative_to(upload_folder)) for file_path in file_paths]
path_file_dict = dict(zip(dataset_paths_in_foundry, file_paths))

rest_client = FoundryRestClient()
dataset_rid = rest_client.get_dataset_rid(dataset_path=target_dataset_path)
transaction_rid = rest_client.open_transaction(dataset_rid=dataset_rid,
                                               mode='UPDATE',
                                               branch='master')
rest_client.upload_dataset_files(dataset_rid=dataset_rid,
                                 transaction_rid=transaction_rid,
                                 path_file_dict=path_file_dict)
rest_client.commit_transaction(dataset_rid, transaction_rid)
```
````
### Save model or other type of python object

````{tab} v2
```python
import pickle

from foundry_dev_tools import FoundryContext

model_obj = """<PMML xmlns="http://www.dmg.org/PMML-4_1" version="4.1"></PMML>"""  # can be any python object that can be pickled
ctx = FoundryContext()
dataset = ctx.get_dataset_by_path("/path/to/playground/model1", create_if_not_exist=True)
pickled_model = pickle.dumps(model_obj)
dataset.put_file("model.pickle", file_data=pickled_model)

```
````

````{tab} v1
```python
from foundry_dev_tools import CachedFoundryClient
import pickle

model_obj = """<PMML xmlns="http://www.dmg.org/PMML-4_1" version="4.1"></PMML>"""
cached_client = CachedFoundryClient()
cached_client.save_model(model_obj, dataset_path_or_rid='/path/to/playground/model1',
                         branch='master', exists_ok=True, mode='SNAPSHOT')
```
````

### Load model or other type of file (using temporary file)
````{tab} v1
```python
from foundry_dev_tools import FoundryContext
from pathlib import Path
import pickle

ctx = FoundryContext()
dataset = ctx.get_dataset_by_path("/path/to/playground/model1")
model_file = dataset.download_file(output_directory=Path("/tmp/model"),path_in_dataset="model.pickle")

with model_file.open("rb") as model:
    print(pickle.load(model))

```
````

````{tab} v2
```python
from foundry_dev_tools import FoundryRestClient
import pickle

rest_client = FoundryRestClient()
rid = rest_client.get_dataset_rid('/path/to/playground/model1')
model_file = rest_client.download_dataset_files(dataset_rid=rid, output_directory='/tmp/model', branch='master')[0]

with open(model_file, 'rb') as file:
    print(pickle.load(file))
```
````

### Load model or other type of file (in-memory)

````{tab} v2
```python
from foundry_dev_tools import FoundryContext
import pickle

ctx = FoundryContext()
dataset = ctx.get_dataset_by_path("/path/to/playground/model1")
model_file_bytes = dataset.get_file("model.pickle")
print(pickle.loads(model_file_bytes))

```
````

````{tab} v1
```python
from foundry_dev_tools import FoundryRestClient
import pickle

rest_client = FoundryRestClient()
rid = rest_client.get_dataset_rid('/path/to/playground/model1')
model_file_bytes = rest_client.download_dataset_file(dataset_rid=rid,
                                                     output_directory=None,
                                                     foundry_file_path='model.pickle',
                                                     view='master')
print(pickle.loads(model_file_bytes))
```
````
### Download a dataset to a temporary folder

Downloads to a temporary folder and reading parquet dataset with pandas/pyarrow. When exiting the context, the temp files are automatically deleted.

````{tab} v2
```python
from foundry_dev_tools import FoundryContext
import pandas as pd

ctx = FoundryContext()
dataset = ctx.get_dataset("ri.foundry.main.dataset...")
with dataset.download_files_temporary() as tmp_dir:
    df = pd.read_parquet(tmp_dir)

print(df.shape)
```
````

````{tab} v1
```python
from foundry_dev_tools import FoundryRestClient
import pandas as pd

rest_client = FoundryRestClient()
rid = "ri.foundry.main.dataset.xxxxxxx-xxxx-xxx-xx-xxxxxxxxxx"
with rest_client.download_dataset_files_temporary(dataset_rid=rid, view='master') as temp_folder:
    df = pd.read_parquet(temp_folder)

print(df.shape)
```
````

### Download only few files from dataset

You can simply specify the list of files you want to download in download_dataset_files
````{tab} v2
```python
rid = "ri.foundry.main.dataset.xxxxxxx-xxxx-xxx-xxx-xxxxxxxxx"
ds = ctx.get_dataset(rid)
ds.download_files(output_directory=Path("/path/to/only_few_files"),paths_in_dataset={"file1.png","file2.png"})
```
````

````{tab} v1
```python
rid = "ri.foundry.main.dataset.xxxxxxx-xxxx-xxx-xxx-xxxxxxxxx"
rest_client.download_dataset_files(dataset_rid=rid, output_directory='/paht/to/only_few_files', files=['file1.png', 'file2.png'], branch='master')
```
````

### Polars DataFrame from Spark SQL dialect

Queries the Foundry SQL server with Spark SQL dialect, load arrow stream using [polars](https://www.pola.rs/).

````{tab} v2
```python
from foundry_dev_tools import FoundryContext
import polars as pl

ctx = FoundryContext()
ds = ctx.get_dataset_by_path("/path/to/test_dataset")
df = ds.query_foundry_sql("SELECT *", return_type="polars")
print(df)
```
````

````{tab} v1
```python
from foundry_dev_tools import FoundryRestClient

rest_client = FoundryRestClient()
arrow_table = rest_client.query_foundry_sql(
    "SELECT * FROM `/path/to/test_dataset`",
    branch="master",
    return_type="arrow",
)
import polars as pl

df = pl.from_arrow(arrow_table)
print(df)
```
````

### DuckDB Table from Spark SQL dialect

Queries the Foundry SQL server with Spark SQL dialect, load arrow stream using [duckdb](https://duckdb.org/).

````{tab} v2
```python
from foundry_dev_tools import FoundryContext

ctx = FoundryContext()
ds = ctx.get_dataset_by_path("/path/to/test_dataset")
arrow_table = ds.to_arrow()

import duckdb

# Get an in-memory DuckDB database and create a new table from the result arrow table.
# Note that the python variable is automatically determined from the query string.
con = duckdb.connect()
con.execute("CREATE TABLE my_table AS SELECT * FROM arrow_table")
```
````

````{tab} v1
```python
from foundry_dev_tools import FoundryRestClient

rest_client = FoundryRestClient()
arrow_table = rest_client.query_foundry_sql(
    "SELECT * FROM `/path/to/test_dataset`",
    branch="master",
    return_type="arrow",
)
import duckdb

# Get an in-memory DuckDB database and create a new table from the result arrow table.
# Note that the python variable is automatically determined from the query string.
con = duckdb.connect()
con.execute("CREATE TABLE my_table AS SELECT * FROM arrow_table")
```
````
