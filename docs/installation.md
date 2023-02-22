# Installation

## Prerequisites

* Python >=3.8
* Git
* Apache Spark (optional)

## Get the Foundry JSON Web Token

Go to https://\<foundrystack\>/workspace/settings/tokens and generate a new token.
Copy it for later use in the config.

## Create Basic Config File

Open your terminal and create a new folder `.foundry-dev-tools` in your home directory.

```shell
mkdir ~/.foundry-dev-tools
cd ~/.foundry-dev-tools
```

Create a file called `config` with the following content:

```ini
[default]
foundry_url=<your foundry_url>
jwt=<paste your foundry token here>
```

## Install the latest version of Foundry DevTools

We recommend using a new [conda environment] or [python environment],
after you activated it, just run:

```bash
pip install foundry-dev-tools
```

or, if you have the repository locally available and want to tinker with the source code
you could install it in "editable"/"develop mode". Run the following command in
the Foundry DevTools root directory:

```bash
pip install -e .
```

## Pyspark

`pyspark>=3.0.0` is an optional dependency of Foundry DevTools.
It is required to run `transforms` and the `CachedFoundryClient` locally.

If you install `foundry-dev-tools[tranforms]`, it will install pyspark as well:

```bash
pip install ... foundry-dev-tools[transforms]
```

You can run the following code in your python environment, to verify if `pyspark` is installed correctly:

```python
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.master("local[*]").appName("foundry-dev-tools").getOrCreate()
spark.createDataFrame(data=[[1, 2]]).collect()
```

This should not fail and return `[Row(_1=1, _2=2)]`. If it does not work check your Spark installation and pyspark
installation.

## Install with SSO and native Foundry SQL support

foundry-dev-tools comes with built-in support for Single-Sign-On (SSO) with Foundry
using
the [Third Party Applications](https://www.palantir.com/docs/foundry/platform-security-third-party/third-party-apps-overview/)
feature of Foundry.
Internally, this uses the Palantir provided library `palantir-oauth-client` to handle the authentication flows.

It is recommended to set up foundry-dev-tools with the dependency, using `foundry-libs`:

```bash
pip install foundry-dev-tools[foundry-libs]
```

[conda environment]: https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html
[python environment]: https://docs.python.org/3/library/venv.html