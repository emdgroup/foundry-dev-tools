# Installation

## Prerequisites

- Python >=3.8

## Get Necessary Credentials

> Either JWT or OAuth

```{tab} JWT
Go to https://your-stack.palantirfoundry.com/workspace/settings/tokens and generate a new token.
Copy it for later use in the config.
```

```{tab} OAuth
OAuth credentials, can only be generated on Foundry if you have the necessary privileges.
OAuth credentials consist of a `client_id` and optionally a `client_secret`, keep these ready for the next steps.

:::{seealso}
See also [`OAuthTokenProvider`](#foundry_dev_tools.config.token_provider.OAuthTokenProvider) and [Registering third-party applications](https://www.palantir.com/docs/foundry/platform-security-third-party/register-3pa/)
:::
```

## Install the Latest Version of Foundry DevTools

We recommend using a new [conda environment] or [python environment],
after you activated it, just run:

````{tab} pip
```shell
pip install 'foundry-dev-tools[cli]'
```
[CLI](cli) is an optional dependency but can be helpful for configuration and environment setup.

If you want to use everything provided by FoundryDevTools (running transforms locally, s3 compatible dataset api and the cli) you can use the following command:

```shell
pip install 'foundry-dev-tools[all]'
```

````

````{tab} conda
```shell
conda install -c conda-forge foundry-dev-tools
```

To also install the CLI, you need to install these optional dependencies:

```shell
conda install -c conda-forge click inquirer websockets rich packaging
```

````

:::{seealso}
The installation guide for contributors can be found [here](/dev/contribute.md).
:::

## Check Your Installation

With the [`fdt info`](/getting_started/cli.md#the-info-command) command you can check if everything is correctly installed.

## Create a Basic Configuration File

After you've installed foundry-dev-tools successfully, run in a shell `fdt config -e`.
Select the config file you want to edit. You probably want to edit one of the **user** configuration files.

:::{seealso}
Foundry DevTools CLI [config command](/getting_started/cli.md#the-config-command)
:::

### Without the CLI

#### Config Paths

- On all operating systems:
  - \~/.foundry-dev-tools/config.toml (user)
  - \~/.config/foundry-dev-tools/config.toml (user)
- Linux:
  - /etc/foundry-dev-tools/config.toml (system-wide)
- macOS:
  - /Library/Application Support/foundry-dev-tools/config.toml (system-wide)
  - ~/Library/Application Suppport/foundry-dev-tools/config.toml (user)
- Windows:
  - C:\\ProgramData\\foundry-dev-tools\\config.toml (system-wide)
  - \%USERPROFILE\%\\AppData\\Local\\foundry-dev-tools\\config.toml (user)
  - \%USERPROFILE\%\\AppData\\Roaming\\foundry-dev-tools\\config.toml (user)

If you don't want to install the CLI dependencies, you can run the following code, to list the possible config file locations:
```python
from foundry_dev_tools.utils.config import cfg_files

for x in cfg_files().keys():
    print(x)
```

Or in one line: `python -c 'from foundry_dev_tools.utils.config import cfg_files; [print(x) for x in cfg_files().keys()]'`

:::{seealso}
If you've never used TOML, [here](/configuration.md#quick-toml-overview) is a quick overview.
:::

## Credentials Configuration

<!-- includes are for including that part in configuration.md -->
<!-- include -->
````{tab} JWT
The following contents are needed for the JWT authentication:
```toml
[credentials]
domain = "palantir foundry domain"
token_provider.config.jwt = "jwt token you generated"
```
````

`````{tab} OAuth
The following contents are needed for the OAuth authentication.

````{tab} Authorization Code Grant
The client secret is optional with the authorization code grant.

```toml
[credentials]
domain = "palantir foundry domain"
token_provider.name = "oauth"

[credentials.token_provider.config]
client_id = "client_id"
client_secret = "client_secret" # optional with authorization code grant
```
````

````{tab} Client Credentials Grant
```toml
[credentials]
domain = "palantir foundry domain"
token_provider.name = "oauth"

[credentials.token_provider.config]
client_id = "your client_id"
client_secret = "your client secret" # required with the client credentials grant
grant_type = "client_credentials"
```
````

`````
<!-- include_end -->

:::{seealso}
For more info about the configuration: [](/configuration.md)
:::

## PySpark

The python package [PySpark](https://pypi.org/project/pyspark/) is an optional dependency of Foundry DevTools.
It is required to run `transforms` and the `CachedFoundryClient`, the PySpark version needs to be at least 3.0.

For PySpark to work, you'll also need to install Java, but PySpark is currently only compatible with Java 8/11/17.

````{tab} pip
If you use `foundry-dev-tools[transforms]` to install, it will install the PySpark dependency as well:

```shell
pip install 'foundry-dev-tools[transforms]'
```
````

````{tab} conda
```shell
conda install -c conda-forge 'pyspark>3' 'openjdk==17.*'
```
````

Alternative installation methods, or more information on how to get Spark running,
can be found in the [PySpark Documentation] and the [Spark Documentation].

[PySpark Documentation]: https://spark.apache.org/docs/latest/api/python/getting_started/install.html
[Spark Documentation]: https://spark.apache.org/docs/latest/
[conda environment]: https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html
[python environment]: https://docs.python.org/3/library/venv.html
