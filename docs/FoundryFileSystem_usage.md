# Foundry DevTools fsspec implementation

## In combination with fsspec:
```pycon
>>> import fsspec
>>> dataset_rid = 'ri.foundry.main.dataset.15461bb5-be92-4ad9-aa3e-07c16175e713'
>>> with fsspec.open(f"foundry://{dataset_rid}/test.txt", "r") as f:
>>>    print(f.read())
>>> 'content'
```

## Use in combination with pandas:
```pycon
>>> import pandas as pd
>>> df = pd.read_csv(f"foundry://{dataset_rid}/test1.csv")
>>> df.shape
(1, 1)
```

## Use in combination with dask:

```pycon
>>> import dask.dataframe as dd
>>> df = dd.read_csv(f"foundry://{dataset_rid}/test1.csv")
>>> df.head()
     header
0  content1
```

## Pass custom branch, token or both:

```pycon
>>> branch = 'custom-branch'
>>> token = 'super-secret-foundry-token'
>>> df = pd.read_csv(f"foundry://{branch}:{token}@{dataset_rid}/test1.csv")
>>> df.shape
(1, 1)
```

## Direct instantiation with dataset_rid:

```pycon
>>> fs = fsspec.filesystem('foundry', dataset=dataset_rid)
>>> with fs.open("example.txt", 'w') as f:
>>>     f.write("test2")
>>> with fs.open("example.txt", 'r') as f:
>>>     print(f.read())
test2
```

## Upload multiple files in the same foundry transaction:

```pycon
>>> with fs.transaction:
>>>     with fs.open("example1.txt", 'w') as f:
>>>        f.write("test1")
>>>     with fs.open("example2.txt", 'w') as f:
>>>        f.write("test2")
```
