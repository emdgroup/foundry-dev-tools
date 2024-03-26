# Extending Foundry DevTools

Both API clients and token providers can be provided by external packages. These external packages can then register their api clients / token providers via entry points.

First follow the setuptools quickstart guide on how to create a python package: https://setuptools.pypa.io/en/latest/userguide/quickstart.html

## Adding Token Providers

On how to implement a token provider take a look at the [Token Provider Implementation](/dev/architecture/token_provider_implementation.md) documentation.

And then in your `pyproject.toml` add the following:

```toml
[project.entry-points."fdt_token_provider"]
my_token_provider = "your.module.name:MyTokenProvider"
```

Replace `my_token_provider` with something more fitting, this will be the name used in the [credentials config](/configuration.md#credentials-config) as the token provider name.
Also change "your.module.name" to the module name you chose, and "MyTokenProvider" to the class name of your token provider.

Now If you install the package, you will be able to use the token provider in Foundry DevTools. 

:::{seealso}
https://setuptools.pypa.io/en/latest/userguide/entry_point.html#entry-points-for-plugins
:::

### Without entry points

If you don't want to create a new package, you can still use your own TokenProvider, but you won't be able to use it in your configuration files.

```python
from foundry_dev_tools import FoundryContext, Host
from your.module.name import MyTokenProvider

ctx = FoundryContext(token_provider=MyTokenProvider(Host("your-stack.palantirfoundry.com"),...))

```

## Adding API clients

On how to implement a token provider take a look at the [API client Implementation](/dev/architecture/api_client_implementation.md) documentation.

And then in your `pyproject.toml` add the following:

(I'll use the InfoClient created as an example in the API client implementation documentation as an example here)

```toml
[project.entry-points."fdt_api_client"]
info = "foundry_dev_tools_info.client:InfoClient"
```

Change "info", "foundry_dev_tools_info.client" and "InfoClient" respectively to the `api_name`, your module name and your class name.

Now it can be used as an API client from the FoundryContext via:

```python
from foundry_dev_tools import FoundryContext

ctx = FoundryContext()
ctx.info.api_list_users()
```

### Without entry points

If you don't want to create a new package, you can still use your own API clients, but not in the way above.

```python
from foundry_dev_tools import FoundryContext
from foundry_dev_tools_info.client import InfoClient

ctx = FoundryContext()
info_client = InfoClient(ctx)
```
