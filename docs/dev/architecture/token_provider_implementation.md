# Token provider implementation

A token provider always needs to have the attributes [`host`](#foundry_dev_tools.config.token_provider.TokenProvider.host) and [`token`](#foundry_dev_tools.config.token_provider.TokenProvider.token) and implement the class [`TokenProvider`]
And a token provider always has a [`requests_auth_handler`](#requests-auth-handler) method.

The simplest implementation of a token provider is the [`JWTTokenProvider`]:

:::{literalinclude} ../../../libs/foundry-dev-tools/src/foundry_dev_tools/config/token_provider.py
:pyobject: JWTTokenProvider
:::

:::{note}
Every variable in the token provider that does not start with a `_` will be displayed in the [config cli](/getting_started/cli.md#the-config-command) in plaintext.
For secrets like a jwt token, prefix it with a `_`, these will be shown as not set if they are None or otherwise shown as set, but the value will not be displayed
If it shouldn't be displayed at all prefix it with `__`, for example like `__cached` in the [`OAuthTokenProvider`].
:::

The `token` property just returns the supplied jwt token via the `jwt` parameter.
And `host` is set to the `host` parameter from the constructor.

## requests auth handler

The requests auth handler is a method that takes a [`PreparedRequest`](#requests.PreparedRequest) as an argument, modifies it and returns it.
:::{seealso}
Requests documentation: https://requests.readthedocs.io/en/latest/user/authentication/#new-forms-of-authentication
:::

This method will be set as the `auth` attribute by default in the [ContextHTTPClient](/dev/architecture/foundry_context_implementation.md#client)

## Cached token provider

There is also a [`CachedTokenProvider`] implementation, which does not work on its own but is a building block for other token providers.
The builtin [`OAuthTokenProvider`] is based on it.

:::{literalinclude} ../../../libs/foundry-dev-tools/src/foundry_dev_tools/config/token_provider.py
:pyobject: CachedTokenProvider
:::

A simple example implementation could look like this:

```python
import time

from foundry_dev_tools.config.token_provider import CachedTokenProvider


class ExampleCachedTokenProvider(CachedTokenProvider):
    def _request_token(self):
        # expire in 100 seconds
        return "token", time.time() + 100
```

To implement from [`CachedTokenProvider`] you only need to change the [_request_token](#foundry_dev_tools.config.token_provider._request_token) method.
The method only needs to return a tuple with a token and the timestamp when the token will expire.

## More configuration details

:::{seealso}
See [Credentials Configuration](/configuration.md#credentials-config).
:::

These steps happen when the [credentials configuration](configuration.md#credentials-config) gets parsed by the [`parse_credentials_config`](#foundry_dev_tools.config.config.parse_credentials_config) method:

:::{literalinclude} ../../../libs/foundry-dev-tools/src/foundry_dev_tools/config/config.py
:pyobject: parse_credentials_config
:::

[`JWTTokenProvider`]: #foundry_dev_tools.config.token_provider.JWTTokenProvider
[`TokenProvider`]: #foundry_dev_tools.config.token_provider.TokenProvider
[`CachedTokenProvider`]: #foundry_dev_tools.config.token_provider.CachedTokenProvider
[`OAuthTokenProvider`]: #foundry_dev_tools.config.token_provider.OAuthTokenProvider
[`requests_auth_handler`]: #foundry_dev_tools.config.token_provider.TokenProvider.requests_auth_handler
