# Transforms

## Git clone the Foundry repository

Open your Code Repository in Foundry and use the Clone Button in the top right corner to retrieve the git link.
Now you can use your local git installation to clone the repository. Please create a feature branch before starting your
work.

## Run the transformations locally

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

### Using Foundry DevTools inside a Jupyter Notebook

You can run your transform code from jupyter with very minimal, non-breaking changes to your repository.

Open the `transforms-python/src/setup.py` file of your repository and change line 8 and 9 by adding a default value
for package name and package version.
The name should be unique among your projects, so it won't conflict with another, you can use for example your Foundry project name.
It will be the name of the **locally** installed package, so it isn't that important.
For the version you can use a version number like '1.0.0', it is also not really important, it should be just a valid Python Version specification.

:::seealso
[Python Packaging Version Specifier Documentation](https://packaging.python.org/en/latest/specifications/version-specifiers/)

[Python Packaging Naming Documentation](https://packaging.python.org/en/latest/specifications/name-normalization/)
:::

```diff
-      name=os.environ['PKG_NAME'],
+      name=os.getenv('PKG_NAME', 'your-project-name'),
-      version=os.environ['PKG_VERSION'],
+      version=os.getenv('PKG_VERSION', '1.2.3'),
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

:::{seealso}
[Configuration for Transforms](/docs/configuration.md#configuration-for-transforms)
:::
