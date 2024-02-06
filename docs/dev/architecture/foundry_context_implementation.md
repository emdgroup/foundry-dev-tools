# FoundryContext

The [FoundryContext] is the central object that loads and holds your configuration and token provider.
The [client] is a [ContextHTTPClient] that gets shared by all [APIClients](api_client.md) that use the FoundryContext instance.

When initializing a [FoundryContext] without any parameters, the [configuration](configuration.md) gets loaded in the constructor from the config files and environment variables.
If only the `config` parameter is set, only the `token_provider` will be loaded from files and environment variables and vice versa.

:::{seealso}
[Getting Started - FoundryContext](/getting_started/foundry_dev_tools.md#foundrycontext)
:::

## Client

The [ContextHTTPClient] is based on the [](#requests.Session) class, the only difference is the auth_handler and [request](#foundry_dev_tools.clients.context_client.ContextHTTPClient.request) function.

The client automatically raises Foundry API errors, more information about error handling and configuration can be found [here](/dev/architecture/errors.md)

The request method has the extra keyword argument `error_handling`, which can be either an [`ErrorHandlingConfig`](#foundry_dev_tools.errors.handling.ErrorHandlingConfig), `False` or `None`. The default is `None`, which uses an ErrorHandlingConfig created without arguments.
When set to `False`, no automatic error handling will be done, and it's up to you what you'll do when the Response returns an error.

'Low level' API client methods also pass their `**kwargs` to the `request` method, so they can also use the `error_handling`.

### auth handler

The `requests` session has an [`auth`](#requests.Session.auth) parameter which is per default set to the [requests_auth_handler](#foundry_dev_tools.config.token_provider.TokenProvider.requests_auth_handler) method of the context token provider.

[FoundryContext]: #foundry_dev_tools.config.context.FoundryContext
[ContextHTTPClient]: #foundry_dev_tools.clients.context_client.ContextHTTPClient
[client]: #foundry_dev_tools.config.context.FoundryContext.client

