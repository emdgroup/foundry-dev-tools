# Development of this library

Code contributions or forks of our project are welcome
and to keep our codebase consistent we use the workflow described below.
As our base we use [pyscaffold] and some parts of our workflow
are inherited from their recommendations.

## Install in Development Mode

```shell
pip install -e .
```

This command will link the local version of this package into your pip package folder.
Every change you make in the code is instantly applied.

## Run unit tests

Install the test dependencies and execute pytest.

```shell
pip install -e ".[testing,transforms]"
pytest
```

or use [tox]

```shell
tox
```

## Run integration test

To run the integration tests, make sure to have a valid `config` file in your `~/.foundry_dev_tools/` folder
and have the environment variables `INTEGRATION_TEST_COMPASS_ROOT_PATH` and `INTEGRATION_TEST_COMPASS_ROOT_RID` set.
These environment variables should point to an empty folder on palantir foundry you have permissions to,
the tests will create the datasets automatically.

```shell
pip install -e ".[integration,testing,transforms]"
pytest --integration
```

or use [tox]

```shell
tox -- --integration
```

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
to the pre-commit hooks, we run in our pipeline:

```shell
tox -e lint
```

[pyscaffold]: https://pyscaffold.org/en/stable/
[tox]: https://tox.wiki/en/latest/
[pre-commit]: https://pre-commit.com/
[black]: https://github.com/psf/black
[ruff]: https://github.com/charliermarsh/ruff
