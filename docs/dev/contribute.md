# Contribute to the library

Code contributions or forks of our project are welcome
and to keep our codebase consistent we use the workflow described below.
As our base we did use [pyscaffold].
We use [PDM] as our build system instead of setuptools.

To install and manage your dependencies you should use `pdm`.
PDM automatically installs the needed dependencies, the same that will be used in the CI.

## Clone the repository

```shell
git clone https://github.com/emdgroup/foundry-dev-tools
cd foundry-dev-tools
```

## Setup PDM and install the Project

````{tab} Conda/Mamba
First create an environment specifically for foundry-dev-tools with pdm and activate it
```shell
mamba create -n foundry-dev-tools pdm openjdk===17
mamba activate foundry-dev-tools
```
````
````{tab} Without Conda/Mamba
If you don't want to use conda or mamba, you'll need to install pdm through other means.
It is available in most Linux package managers, Homebrew, Scoop and also via pip.
Or through other methods mentioned in the [official docs](https://pdm-project.org/en/latest/#installation).
You'll also need to install Java 17 for PySpark.

If you don't have a python virtual environment activated, PDM will create one for you automatically.
````

Then install foundry-dev-tools and the dev dependencies via:
```shell
pdm install
```


This command will link the local version of this package into your pip package folder.
Every change you make in the code is instantly applied.

## Pre-Commit hooks & formatting

To format the code and make it ready for a commit we use pre-commit.
Currently, we run [ruff] and force the line endings to linux/mac ones.
After a commit gets pushed it will automatically check if it is correctly formatted.
If not, the checks will fail, and we will not be able to merge your changes.

To set up [pre-commit] hooks run:

```shell
# after this everytime you commit in this repo, it will run the hooks
# and if it needs to reformat, your commit gets aborted
# and you will need to readd the reformatted files
pre-commit install
```

To run [pre-commit] hooks:

```shell
pre-commit run --all-files
```

To check if it is correctly formatted according
to the pre-commit hooks, we use [PDM]
which runs the pre-commit hooks:

```shell
pdm run lint
```


## Run unit tests

```shell
pdm run unit
```

## Run integration test

To run the integration tests, make sure to have a valid `config` file and have the environment variables `INTEGRATION_TEST_COMPASS_ROOT_PATH` and `INTEGRATION_TEST_COMPASS_ROOT_RID` set.
These environment variables should point to an empty folder on palantir foundry you have permissions to,
the tests will create the datasets automatically.

```shell
pdm run integration
```


## Documentation

The documentation uses [sphinx] and [myst-parser],
have a look at their documentation for more information about writing documentation.
The API references are generated with `sphinx-apidoc`, which parses the docstrings in the python code.

## Build

Builds the documentation into html format.

Use [PDM]:
```shell
cd docs
pdm install
pdm run build
```

## Serve

This will build the docs and serve them via http and update them when you change something.

Use [PDM]:
```shell
cd docs
pdm install
pdm run build
```


[pyscaffold]: https://pyscaffold.org/en/stable/
[PDM]: https://pdm-project.org/
[pre-commit]: https://pre-commit.com/
[black]: https://github.com/psf/black
[ruff]: https://github.com/astral-sh/ruff
[sphinx]: https://www.sphinx-doc.org
[myst-parser]: https://myst-parser.readthedocs.io
