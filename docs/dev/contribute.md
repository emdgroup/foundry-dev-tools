# Contribute to the library

Code contributions or forks of our project are welcome
and to keep our codebase consistent we use the workflow described below.
As our base we did use [pyscaffold].
We use [hatch] as our build system instead of setuptools.

To install and manage your dependencies you can either use pip or hatch.
Hatch automatically installs the needed dependencies and should be used to run tests, as this is the environment that will be used in the CI, otherwise other dependencies could interfere with tests.

## Install in Development Mode

```shell
hatch shell
```
or
```shell
pip install -e .
```

### with all test/dev dependencies

```shell
hatch -e dev shell
```
or
```shell
pip install -e '.[dev]'
```

This command will link the local version of this package into your pip package folder.
Every change you make in the code is instantly applied.

## Pre-Commit hooks & formatting

To format the code and make it ready for a commit we use pre-commit.
Currently, we run [black], [ruff] and force the line endings to linux/mac ones.
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
to the pre-commit hooks, we use [hatch]
which runs the pre-commit hooks:

```shell
hatch run test:lint
```


## Run unit tests

````{tab} hatch
```shell
hatch run test:unit
```
````

````{tab} without hatch
```shell
pytest tests/unit
```
````

## Run integration test

To run the integration tests, make sure to have a valid `config` file and have the environment variables `INTEGRATION_TEST_COMPASS_ROOT_PATH` and `INTEGRATION_TEST_COMPASS_ROOT_RID` set.
These environment variables should point to an empty folder on palantir foundry you have permissions to,
the tests will create the datasets automatically.

````{tab} hatch
```shell
hatch run test:integration
```
````

````{tab} without hatch
```shell
pytest tests/integration
```
````


## Documentation

The documentation uses [sphinx] and [myst-parser],
have a look at their documentation for more information about writing documentation.
The API references are generated with `sphinx-apidoc`, which parses the docstrings in the python code.

## Build

Builds the documentation into html format.

Use [hatch]:
```shell
hatch run docs:build
```

## Serve

This will build the docs and serve them via http and update them when you change something.

Use [hatch]:
```shell
hatch run docs:live
```


[pyscaffold]: https://pyscaffold.org/en/stable/
[hatch]: https://hatch.pypa.io/latest/
[pre-commit]: https://pre-commit.com/
[black]: https://github.com/psf/black
[ruff]: https://github.com/astral-sh/ruff
[sphinx]: https://www.sphinx-doc.org
[myst-parser]: https://myst-parser.readthedocs.io
