<div align="center">
  <br/>

<a href="https://github.com/emdgroup/foundry-dev-tools/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/emdgroup/foundry-dev-tools/ci.yml?style=flat-square"/></img>
<a href="https://github.com/emdgroup/foundry-dev-tools/actions/workflows/docs.yml"><img src="https://img.shields.io/github/actions/workflow/status/emdgroup/foundry-dev-tools/docs.yml?style=flat-square"/></img>
<a href="https://pypi.org/project/foundry-dev-tools/"><img src="https://img.shields.io/pypi/pyversions/foundry-dev-tools?style=flat-square&label=Supported%20Python%20versions&color=%23ffb86c"/></a>
<a href="https://pypi.org/project/foundry-dev-tools/"><img src="https://img.shields.io/pypi/v/foundry-dev-tools.svg?style=flat-square&label=PyPI%20version&color=%23bd93f9"/></a>
<a href="https://anaconda.org/conda-forge/foundry-dev-tools"><img src="https://img.shields.io/conda/vn/conda-forge/foundry-dev-tools.svg?style=flat-square&label=Conda%20Forge%20Version&color=%23bd93f9" alt="Conda Version"/></a>
<a href="https://pypi.org/project/foundry-dev-tools/"><img src="https://img.shields.io/pypi/dm/foundry-dev-tools?label=PyPI%20Downloads&style=flat-square&color=%236272a4"/></a>
<a href="https://anaconda.org/conda-forge/foundry-dev-tools"><img src="https://img.shields.io/conda/dn/conda-forge/foundry-dev-tools.svg?style=flat-square&label=Conda%20Forge%20Downloads&color=%236272a4" alt="Conda Downloads"/></a>
<a href="https://github.com/emdgroup/foundry-dev-tools/issues"><img src="https://img.shields.io/github/issues/emdgroup/foundry-dev-tools?style=flat-square&color=%23ff79c6"/></a>
<a href="https://github.com/emdgroup/foundry-dev-tools/pulls"><img src="https://img.shields.io/github/issues-pr/emdgroup/foundry-dev-tools?style=flat-square&color=%23ff79c6"/></a>
<a href="http://www.apache.org/licenses/LICENSE-2.0"><img src="https://shields.io/badge/License-Apache%202.0-green.svg?style=flat-square&color=%234c1"/></a>

  <p><a href="https://emdgroup.github.io/foundry-dev-tools">Documentation</a></p>

<a href="https://emdgroup.github.io/foundry-dev-tools/getting_started/index.html">Getting Started / Usage<a/>
&nbsp;•&nbsp;
<a href="https://emdgroup.github.io/foundry-dev-tools/examples/api.html">Examples<a/>
&nbsp;•&nbsp;
<a href="https://emdgroup.github.io/foundry-dev-tools/dev/contribute.html">Development/Contribute<a/>

</div>

# Foundry DevTools

Seamlessly run your Palantir Foundry Repository transforms code and more on your local machine.
Foundry DevTools is a set of useful libraries to interact with the Foundry APIs. 
It consists of two parts:

- The [transforms](https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api/) implementation

  - An implementation of the Foundry `transforms` package that internally uses the `CachedFoundryClient`.
    This allows you to seamlessly run your Palantir Foundry Code Repository transforms code on your local machine.
    Foundry DevTools does not cover all of Foundry's features, more on this [here](https://emdgroup.github.io/foundry-dev-tools/dev/architecture.html#known-limitations-contributions-welcome).

- API clients

    We implemented multiple clients for many foundry APIs like compass, catalog or foundry-sql-server.

  - For example:

    ```python
    from foundry_dev_tools import FoundryContext

    # the context, that contains your credentials and configuration
    ctx = FoundryContext()

    df = ctx.foundry_sql_server.query_foundry_sql("SELECT * FROM `/Global/Foundry Training and Resources/Example Data/Aviation Ontology/airlines`", branch='master')
    df.shape
    # Out[2]: (17, 10)
    ```

## Quickstart

With pip:

```shell
pip install foundry-dev-tools
```

With conda or mamba on the conda-forge channel:

```shell
conda install -c conda-forge foundry-dev-tools
```

[Further instructions](https://emdgroup.github.io/foundry-dev-tools/getting_started/installation.html) can be found in our documentation.

## Why did we build this?

- Local development experience in your favorite IDE (PyCharm, VSCode, ...)
  - Access to modern developer tools and workflows such as ruff, mypy, pylint, black, pre-commit hooks etc.
- Quicker turnaround time when making changes
  - Debug, change code and run in a matter of seconds instead of minutes
- No accidental or auto commits
  - Keep your git history clean

# License

Copyright (c) 2024 Merck KGaA, Darmstadt, Germany

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
