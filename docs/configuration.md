# Configuration

The configuration gets loaded from multiple configuration files and environment variables.

To see the configuration paths for your system and easily launch a config file in your editor, use the [`fdt config`](/getting_started/cli.md#view-the-config-and-config-files) CLI.

:::{note}
The configuration files get merged into one configuration. The environment variables take precedence above all files. See [below](#how-the-configuration-gets-loaded-and-merged).
:::

## Credentials Config

The credentials config consists of these keys:
```toml
[credentials]
domain = "your foundry domain"
scheme = "scheme" # http/https

[credentials.token_provider]
name = "token provider implementation name"
config = {} # token provider implementation specific configuration
```

- The `domain` and `scheme` get converted to a [Host](#foundry_dev_tools.config.config_types.Host), which is the first parameter of each [token provider](/dev/architecture/token_provider_implementation.md) implementation.
  - The default for scheme is 'https' defined in [DEFAULT_SCHEME](#foundry_dev_tools.config.config_types)
- The `token_provider` table configures the token provider which gets used for authentication to Foundry.
  - The `name` value lets you select the token provider implemention, per default, this is "jwt"
  - The `config` [table] options depends on the [`token_provider`](#foundry_dev_tools.config.token_provider) implementation.

Examples from [Getting Started - Credentials Configuration](/getting_started/installation.md#credentials-configuration):

```{include} /getting_started/installation.md
:start-after: "<!-- include -->"
:end-before: "<!-- include_end -->"
```

## Configuration Options

- The current available configuration options and their description can be found [here](#foundry_dev_tools.config.config.Config).
- These options can be set in the `config` [table] in the config.toml
 - Or via environment variables: `FDT_CONFIG__<config_name>`/`FDT_CREDENTIALS__<config_name>`


For example the `cache_dir` config option can be set in these three different ways:

````{tab} Config File
```toml
[config]
cache_dir = "/home/USER/.cache/foundry-dev-tools"
```
or
```toml
config.cache_dir = "/home/USER/.cache/foundry-dev-tools"
```
````

````{tab} Environment Variables
For example in Linux/macOS (bash/zsh/sh/...):

```shell
export FDT_CONFIG__CACHE_DIR="/home/USER/.cache/foundry-dev-tools"
```
````

````{tab} In Python
```python
from foundry_dev_tools import Config
from pathlib import Path

cache_dir = Path.home().joinpath("/.cache/foundry-dev-tools")
conf = Config(cache_dir=cache_dir)
ctx = FoundryContext(config=conf)

# or after creating the context

ctx.config.cache_dir = cache_dir
```
````


## How the Configuration Gets Loaded and Merged

For example if there are the files /etc/foundry-dev-tools/config.toml: 
```toml
[config]
key = 123
key2 = "foobar"

[credentials.token_provider]
name = "oauth"

[credentials.token_provider.config]
client_id = "topsecret"
```
And the configuration file /home/user/.config/foundry-dev-tools/config.toml
```toml
[config]
key = 987
key3 = "baz"

[credentials.token_provider.config]
client_secret = "top_client_secret"
````
And the environment variable `FDT_CONFIG__KEY3=asdf`
The resulting config would theoretically look like:
```toml
[config]
key = 987
key2 = "foobar"
key3 = "asdf"

[credentials]
token_provider.name = "oauth"

[credentials.token_provider.config]
client_id = "topsecret"
client_secret = "top_client_secret"
```

### Project Specific Configuration

A Project specific configuration file called `.foundry_dev_tools.toml` placed at the root of your git repository
takes precedence over the other configuration files, but still will be overwritten by environment variables.

:::{warning}
If you are using git and add your credentials to the project specific config, please add the `.foundry_dev_tools.toml` file to the `.gitignore` file, so your credentials won't be committed.
:::

## Configuration Profiles

You can use profiles to have multiple configurations, which you can choose from when creating a [FoundryContext](/getting_started/foundry_dev_tools.md#foundrycontext).

This feature enables you to segregate different configurations under unique profiles, providing a way to manage multiple environments or setups.

Instead of defining your configuration in the standard format:

```toml
[config]
cache_dir = "/tmp/cache"
```

You can choose an arbitrary prefix/profile name (note: `config` and `credentials` are reserved and cannot be used as prefixes) and define your configuration under this prefix:

```toml
[dev.config]
cache_dir = "/tmp/cache"
```

In this example, `dev` is the chosen profile name. The configuration under this profile can be accessed when initializing a `FoundryContext` by specifying the prefix:

```python
from foundry_dev_tools import FoundryContext

ctx = FoundryContext(profile="dev")
```

In this instance, `FoundryContext` will only consider the configuration options prefixed with `dev` and merge them with the non-prefixed configuration. This allows for easy switching between different configurations by simply changing the profile when initializing `FoundryContext`.

You can also set the top level variable `profile` to set a specific prefix as the default.

This way you can do the following:

Define multiple profiles in your user/system configuration:

:::{code-block}
:caption: ~/.config/foundry-dev-tools/config.toml
[one.config]
requests_ca_bundle = '/path/to/bundle/for/one'

[one.credentials]
domain = "one.plntr-domain"
scheme = "http"
token_provider.config = {"jwt"="eyJ..1"}

[two.config]
requests_ca_bundle = '/path/to/bundle/for/two'

[two.credentials]
domain = "two.plntr-domain"
token_provider.config = {"jwt"="eyJ..2"}

:::

And then in your projects, you can use a project specific configuration with the following content:

:::{code-block}
:caption: /path/to/your/project/.foundry_dev_tools.toml
profile = "one"
:::

Now you'll use "one.plntr-domain" as your foundry host, and you will authenticate with the JWT "eyJ..1"



## Quick TOML Overview

:::{seealso}
More detailed info can be found in the [official spec](https://toml.io/en/latest).
:::

TOML is relatively simple but the same data can be written in multiple ways:

Let's assume we want to represent this json in toml:

```json
{
  "credentials": {
    "domain": "example.com",
    "token_provider": {
      "name": "oauth",
      "config": {
        "client_id": "client id",
        "scopes": [
          "scope1",
          "scope2"
        ]
      }
    }
  }
}
```

We can present them (at least, there are more, but these could make sense for this specific json object) in these 3 ways

````{tab} Similar to the JSON
```toml
[credentials]
domain = "example.com"
token_provider = { "name" = "oauth", config = { client_id = "client id", scopes = [
  "scope1",
  "scope2",
] } }
```
````

````{tab} Flattened Out
```toml
[credentials]
domain = "example.com"
token_provider.name = "oauth"
token_provider.config.client_id = "client id"
token_provider.config.scopes = ["scope1", "scope2"]
```
````

````{tab} As a Seperate Table
```toml
[credentials]
domain = "example.com"

[credentials.token_provider]
name = "oauth"

[credentials.token_provider.config]
client_id = "client id"
scopes = ["scope1", "scope2"]
```
````

[table]: https://toml.io/en/v1.0.0#table
