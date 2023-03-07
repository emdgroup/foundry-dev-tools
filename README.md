<div align="center">
  <br/>

  <a href="https://github.com/emdgroup/foundry-dev-tools/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/emdgroup/foundry-dev-tools/ci.yml?style=flat-square"/></img>
  <a href="https://github.com/emdgroup/foundry-dev-tools/actions/workflows/docs.yml"><img src="https://img.shields.io/github/actions/workflow/status/emdgroup/foundry-dev-tools/docs.yml?style=flat-square"/></img>
  <a href="https://pypi.org/project/foundry-dev-tools/"><img src="https://img.shields.io/pypi/v/foundry-dev-tools.svg?style=flat-square"/></a>
  <a href="https://pypi.org/project/foundry-dev-tools/"><img src="https://img.shields.io/pypi/pyversions/foundry-dev-tools?style=flat-square"/></a>
  <a href="http://www.apache.org/licenses/LICENSE-2.0"><img src="https://shields.io/badge/License-Apache%202.0-green.svg?style=flat-square"/></a>
  <a href="https://github.com/emdgroup/foundry-dev-tools/issues"><img src="https://img.shields.io/github/issues/emdgroup/foundry-dev-tools?color=important&style=flat-square"/></a>
  <a href="https://github.com/emdgroup/foundry-dev-tools/pulls"><img src="https://img.shields.io/github/issues-pr/emdgroup/foundry-dev-tools?color=blueviolet&style=flat-square"/></a>

  <p><a href="https://emdgroup.github.io/foundry-dev-tools">Documentation</a></p>

  <a href="https://emdgroup.github.io/foundry-dev-tools/installation.html">Installation<a/>
  &nbsp;•&nbsp;
  <a href="https://emdgroup.github.io/foundry-dev-tools/usage_and_examples.html">Usage<a/>
  &nbsp;•&nbsp;
  <a href="https://emdgroup.github.io/foundry-dev-tools/develop.html">Development<a/>

</div>

# Foundry DevTools

Seamlessly run your Palantir Foundry Repository transforms code and more on your local machine.
Foundry DevTools is a set of useful libraries to interact with the Foundry APIs. There are currently three
high level entrypoints to Foundry DevTools:

* A [transforms](https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api/) implementation

  * An implementation of the Foundry `transforms` package that internally uses the `CachedFoundryClient`.
    This allows you to seamlessly run your Palantir Foundry Code Repository transforms code on your local machine.
    Foundry DevTools does not cover all of Foundry's features, more on this [here](https://emdgroup.github.io/foundry-dev-tools/architecture.html#known-limitations).

* [FoundryRestClient](https://emdgroup.github.io/foundry-dev-tools/FoundryRestClient_usage.html)

  * An API client that contains an opinionated client implementation to some of Foundry's APIs.

  * For example:

    ```python
    from foundry_dev_tools import FoundryRestClient

    # Queries the Foundry SQL Server with spark SQL dialect
    rest_client = FoundryRestClient()
    df = rest_client.query_foundry_sql("SELECT * FROM `/Global/Foundry Training and Resources/Foundry Reference Project/Ontology Project: Aviation/airlines`", branch='master')
    df.shape
    # Out[2]: (17, 10)
    ```

* [FoundryFileSystem](https://emdgroup.github.io/foundry-dev-tools/FoundryFileSystem_usage.html)

  * An implementation of `fsspec` for Foundry. Useful to interact with Foundry from popular data science libraries such as
  `pandas` or `dask`.

  * For example:

    ```python
    import pandas as pd
    # /Global/Foundry Training and Resources/Foundry Reference Project/Ontology Project: Aviation/airlines
    df = pd.read_parquet("foundry://ri.foundry.main.dataset.5d78f3ae-a588-4fd8-9ba2-66827808c85f")
    df.shape
    # Out[2]: (17, 10)
    ```


## Quickstart

```shell
pip install foundry-dev-tools
```

[Further instructions](https://emdgroup.github.io/foundry-dev-tools/installation.html) can be found in our documentation.

## Why did we build this?

* Local development experience in your favourite IDE (PyCharm, VSCode, ...)
    * Access to modern developer tools and workflows such as pylint, black, isort, pre-commit hooks etc.
* Quicker turnaround time when making changes
    * Debug, change code and run in a matter of seconds instead of minutes
* No accidental or auto commits
    * Keep your git history clean

# License
Copyright (c) 2023 Merck KGaA, Darmstadt, Germany

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

The full text of the license can be found in the [LICENSE](https://github.com/emdgroup/foundry-dev-tools/blob/main/LICENSE) file in the repository root directory.
