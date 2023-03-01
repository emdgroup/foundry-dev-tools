# Usage

## Usage with foundry repositories

### Git clone the Foundry repository

Open your Code Repository in Foundry and use the Clone Button in the top right corner to retrieve the git link.
Now you can use your local git installation to clone the repository. Please create a feature branch before starting your
work.

### Run the transformations locally

The disadvantage is that the rendering of the result dataframe is not of comparable
visual quality as in the Foundry environment. But if you use your IDE for running the transforms code,
you have the full debugging support, which is a great productivity boost.

Let's say you have a file with the following transformation:

```python
from transforms.api import transform_df, Input, Output
from pyspark.sql import DataFrame


@transform_df(
    Output("/PathTo/Compass/OutputDataset"),
    df=Input("/Global/Foundry Operations/Foundry Support/iris")
)
def apply_the_schema(df: DataFrame) -> DataFrame:
    # Your transformations
    # ...
    return df
```

Now to run this compute function locally you just need to add the following line
to get the results of this transformation:

```python
if __name__ == "__main__":
    print(apply_the_schema.compute().toPandas().to_string())
```

After adding this, you just need to run the file.

#### Using Foundry DevTools inside a Jupyter Notebook

You can run your transform code from jupyter with very minimal, non-breaking changes to your repository.

Open the `transforms-python/src/setup.py` file of your repository and change line 8 and 9 by adding a default value 
for package name and package version:

```diff
-      name=os.environ['PKG_NAME'],
+      name=os.getenv('PKG_NAME', 'your-package-name'),
-      name=os.environ['PKG_VERSION'],
+      name=os.getenv('PKG_VERSION', 'your-package-version'),
```

Now you can install the repository into your local environment, when in directory `transform-python/src`:

```shell
pip install -e .
```

Point your notebook to the same python kernel, you can import your transform function and compute it:

```python
from myproject.datasets import apply_the_schema

# run the function by calling compute
output = apply_the_schema.compute()
# output in pandas format
output.toPandas()
```


## More specific Foundry DevTools usage

```{toctree}
The FoundryRestClient <FoundryRestClient_usage>
The fsspec implementation <FoundryFileSystem_usage>
Configuration <Configuration_usage>
SSO <SSO_usage>
```