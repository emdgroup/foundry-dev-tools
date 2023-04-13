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

## PySpark

The python package [PySpark](https://pypi.org/project/pyspark/) is an optional dependency of Foundry DevTools.
It is required to run `transforms` and the `CachedFoundryClient`, the PySpark version needs to be at least 3.0.

If you install `foundry-dev-tools[tranforms]`, it will install pyspark as well:

```bash
pip install foundry-dev-tools[transforms]
```

Alternative installation methods, or more information on how to get Spark running,
can be found in the [PySpark Documenation] and the [Spark Documentation].

[PySpark Documentation]: https://spark.apache.org/docs/latest/api/python/getting_started/install.html
[Spark Documentation]: https://spark.apache.org/docs/latest/
[conda environment]: https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html
[python environment]: https://docs.python.org/3/library/venv.html