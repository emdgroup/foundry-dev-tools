# Contribute to the library

Code contributions or forks of our project are welcome
and to keep our codebase consistent we use the workflow described below.
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
mamba create -n foundry-dev-tools python=3.12 pdm openjdk=17
mamba activate foundry-dev-tools
```
Note that python>3.12 throws an error when running `pdm install`: `configured Python interpreter version (3.14) is newer than PyO3's maximum supported version (3.12)`.

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
pdm run live
```

# Release procedure

1. When creating a release the first thing to do is create a changelog entry, preferably in the same PR as the feature that was added.
   The changelog follows the [keep a changelog](https://keepachangelog.com/en/1.0.0/) format. (Don't forget to add the tag URL at the bottom.)
   Commit the changelog with the following commit message `feat: release v9.9.9`, this should be ideally the last commit before the tag is created.
   For example: https://github.com/emdgroup/foundry-dev-tools/commit/5e7a9c217c5784c9ae3b127357216282f4ef5d9a

2. After the PR has been merged, wait for all tests to pass on the main branch.

3. Locally pull the latest main branch, and then create a git tag with the version e.g. `git tag v9.9.9`.
   ```{note}
   Only users with the roles "Repository admin" or "Maintain" can push/create these tags.
   ``` 

   We follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html) and prefix it with a `v`
   ```text
   Given a version number MAJOR.MINOR.PATCH, increment the:
      1. MAJOR version when you make incompatible API changes
      2. MINOR version when you add functionality in a backward compatible manner
      3. PATCH version when you make backward compatible bug fixes
   ```

4. Push the tag via `git push --tags`

5. Wait for the release pipeline to finish, check that the newest version is available on pypi. [foundry-dev-tools](https://pypi.org/project/foundry-dev-tools) and [foundry-dev-tools-transforms](https://pypi.org/project/foundry-dev-tools-transforms)

6. Then create a 'release' on GitHub (https://github.com/emdgroup/foundry-dev-tools/releases) "Draft a new release", choose your tag, use the tag as a title, and copy the contents of the changelog for this version into the description, for example: https://github.com/emdgroup/foundry-dev-tools/releases/tag/v2.0.1

7. We also publish packages on conda-forge, we have one feedstock for each package [foundry-dev-tools-feedstock](https://github.com/conda-forge/foundry-dev-tools-feedstock) and [foundry-dev-tools-transforms-feedstock](https://github.com/conda-forge/foundry-dev-tools-transforms-feedstock)
   Normally a GitHub bot automatically checks for new releases on PyPI, creates a PR and if everything passes the tests, it will be automatically merged.
   But in the case that the release needs to be done ASAP, you can manually create a PR, by copying the SHA256 sum for the `.tar.gz` file from the pypi files page e.g. https://pypi.org/project/foundry-dev-tools/#files
   Then replace the version and the sha256sum in the conda recipe for both packages (both packages have different hashes), open a PR and wait for the Pipelines to run, if you already have permissions for the feedstock then you can directly merge it, otherwise you will need to contact one of maintainers listed in the conda recipe yaml.

   
[PDM]: https://pdm-project.org/
[pre-commit]: https://pre-commit.com/
[black]: https://github.com/psf/black
[ruff]: https://github.com/astral-sh/ruff
[sphinx]: https://www.sphinx-doc.org
[myst-parser]: https://myst-parser.readthedocs.io
