<div align="center">
  <br/>

  <a href="https://github.com/emdgroup/foundry-dev-tools/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/emdgroup/foundry-dev-tools/ci.yml?style=flat-square"/></img>
  <a href="https://github.com/emdgroup/foundry-dev-tools/actions/workflows/docs.yml"><img src="https://img.shields.io/github/actions/workflow/status/emdgroup/foundry-dev-tools/docs.yml?style=flat-square"/></img>
  <a href="http://www.apache.org/licenses/LICENSE-2.0"><img src="https://shields.io/badge/License-Apache%202.0-green.svg?style=flat-square"/></a>
  <a href="https://github.com/emdgroup/foundry-dev-tools/issues"><img src="https://img.shields.io/github/issues/emdgroup/foundry-dev-tools?color=important&style=flat-square"/></a>
  <a href="https://github.com/emdgroup/foundry-dev-tools/pulls"><img src="https://img.shields.io/github/issues-pr/emdgroup/foundry-dev-tools?color=blueviolet&style=flat-square"/></a>

  <br/>
    <img width="55%" src="https://github.com/emdgroup/foundry-dev-tools/raw/main/docs/pictures/foundry-dev-tools_connections.drawio.svg" alt="DrawIO Diagram with connection possibilities for Foundry DevTools"/>
  <br/>

  <p><a href="https://emdgroup.github.io/foundry-dev-tools">Documentation</a></p>

  <a href="https://emdgroup.github.io/foundry-dev-tools/installation.html">Installation<a/>
  &nbsp;•&nbsp;
  <a href="https://emdgroup.github.io/foundry-dev-tools/usage.html">Usage<a/>
  &nbsp;•&nbsp;
  <a href="https://emdgroup.github.io/foundry-dev-tools/develop.html">Development<a/>

</div>

# Foundry DevTools

Seamlessly run your Palantir Foundry Repository transforms code and more on your local machine.
Foundry DevTools is a set of useful libraries to interact with the Foundry APIs. There are currently three
high level entrypoints to Foundry DevTools:

* [FoundryRestClient](docs/FoundryRestClient_usage.md)

  * An API client that contains an opinionated client implementation to some of Foundry's APIs.

* [FoundryFileSystem](docs/FoundryFileSystem_usage.md)

  * An implementation of `fsspec` for Foundry. Useful to interact with Foundry from popular data science libraries such as
  `pandas` or `dask`.

* A [transforms](https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api/) implementation

  * An implementation of the Foundry `transforms` package that internally uses the `CachedFoundryClient`.
    This allows you to seamlessly run your Palantir Foundry Code Repository transforms code on your local machine.
    Foundry DevTools does not cover all of Foundry's features, more on this [here](https://emdgroup.github.io/foundry-dev-tools/architecture.html#known-limitations).

## Quickstart

```shell
pip install foundry-dev-tools
```

[Further instructions](https://emdgroup.github.io/foundry-dev-tools/installation.html) are in the documentation.

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

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

The full text of the license can be found in the file [LICENSE](LICENSE) in the repository root directory.
