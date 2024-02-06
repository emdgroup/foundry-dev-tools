# Foundry DevTools

This getting started guide provides an overview of the Foundry DevTools Python library, 
introducing its key components, such as the [FoundryContext] and the [API clients]
and explaining the [basic concepts] to quickly get started using the Foundry APIs in your projects.

## Basic Concepts

The [FoundryContext] is the core object in the FoundryDevTools library, 
responsible for managing the configuration and credentials required for interacting with Foundry. 
It retrieves the necessary configuration from config files or accepts it through parameters during initialization.
Through the [FoundryContext] you will access all the [API clients] and [Resources].

:::{seealso}
If specific API endpoints are missing they can be easily implemented: [Implement an API Client]
:::

Best practice is to use only one [FoundryContext] instance throughout your program.
This leverages connection pooling through the requests library and avoids the overhead of repeatedly parsing your configuration files.

A [`TokenProvider`] is used to authenticate the requests made by [API clients].
It can be configured through the [credentials config].

:::{seealso}
If you want to provide your credentials through another way, you can easily implement your own token provider: [Implement a Token Provider]
:::

## FoundryContext

The context can be created like this:

```python
from foundry_dev_tools import FoundryContext

# this way it will take the configuration and credentials
# from your configuration files and environment variables
ctx = FoundryContext()

# or if you don't want to use configuration files,
# you can supply the configuration and credentials as parameters
# either jwt token provider or oauth token provider is needed,
# or another custom token provider
from foundry_dev_tools import Config, JWTTokenProvider, OAuthTokenProvider

# note: credentials shouldn't be stored directly in your code, this is just an example
# jwt:
ctx = FoundryContext(config=Config(), token_provider=JWTTokenProvider(jwt="..."))
# oauth:
ctx = FoundryContext(config=Config(), token_provider=OAuthTokenProvider(client_id="..."))
```

The configuration can be accessed and changed through `ctx.config`. For all configuration options see [`Config`]

To get your authentication token use `ctx.token`, for the [`JWTTokenProvider`] this will be the `jwt` value, and for the [`OAuthTokenProvider`] this will be a jwt token that has been obtained through OAuth, it is automatically cached and will renew itself automatically.

For all attributes/methods see [`FoundryContext`].

## API Clients

The [APIClient](#foundry_dev_tools.clients.api_client.APIClient) class is the base for all the API clients.

The API clients are all in the [](#foundry_dev_tools.clients) module.

### Naming Conventions

Each API client is seperate foundry api endpoint.
For example the [`CompassClient`](#foundry_dev_tools.clients.compass.CompassClient) is at the URL `https://your-stack.palantirfoundry.com/compass/api`.
Or the [`FoundrySqlServerClient`](#foundry_dev_tools.clients.foundry_sql_server.FoundrySqlServerClient) is at the URL `https://your-stack.palantirfoundry.com/foundry-sql-server/api`.

API client methods that start with `api_` are 'lower level' methods, they only wrap around a single API call.
For example [`CompassClient.api_create_folder`](#foundry_dev_tools.clients.compass.CompassClient.api_create_folder) makes only a request to `https://your-stack.palantirfoundry.com/compass/api/folders`.
This means these methods don't parse the response (except for [Error Handling](#error-handling)) and just return a [`requests.Response`](#requests.Response) object.

API client methods that don't have this prefix may make multiple requests (by calling 'lower level' methods) or do further parsing.
For example [`CompassClient.get_child_objects_of_folder`](#foundry_dev_tools.clients.compass.CompassClient.api_create_folder) yields all children,
it does so by calling the [`CompassClient.api_get_children`](#foundry_dev_tools.clients.compass.CompassClient.api_get_children) method until there are no pages left.


### Usage

To access the APIClients use the [FoundryContext] you created before.

For example listing all children of a folder:

```python
# list all children of the folder with the rid 'ri.compass.main.folder....'
children = list(ctx.compass.get_child_objects_of_folder("ri.compass.main.folder...."))
```

When accessing an API client through the context it automatically creates a new client, and then caches it, so the next time you'll use `ctx.compass` it will be the same instance.

```{literalinclude} ../../src/foundry_dev_tools/config/context.py
:pyobject: FoundryContext.compass
```

To see all API clients and the methods they provide see [`here`](#foundry_dev_tools.clients).

## Resources

Resources are object oriented classes for Foundry Resources like Datasets or Folders.
This can prove useful when needing to do multiple operations on the same Resource, or when using a REPL.

They can be created using the [`FoundryContext`]

```python
res = ctx.get_resource("rid")  # depending on the resource, this will return a different class
# for example if the RID starts with ri.main.foundry.dataset it will return a Dataset class
ds = ctx.get_resource("ri.main.foundry.dataset...")
ds = ctx.get_resource_by_path("/path/to/dataset")

# datasets have their own methods, they basically do the same thing,
# except that you can provide a different branch name
ds = ctx.get_resource("ri.main.foundry.dataset...", branch="dev/feature1")
# and can create a dataset via the get_dataset_by_path method
ds = ctx.get_resource_by_path("/path/to/new_dataset", branch="branch_name", create_if_not_exists=True)

# each resource has the same attributes like rid, path, modified or created
print(ds.rid, ds.path, ds.modified, ds.created)

# but for example the dataset class also has the branch attribute additionally
print(ds.branch)
```

:::{seealso}
To see all methods and attributes, take a look at the API documentation:

- [`Resource`] 
- [`Dataset`] 
- [`Folder`] 
:::

## Error Handling

Error Handling happens automatically when using the [API clients].
This means that errors from the Foundry API will be raised as custom python exceptions.

For example:
```python
from foundry_dev_tools.errors.dataset import DatasetNotFoundError

try:
    ctx.catalog.api_get_dataset("rid.foundry.main.dataset.non-existent-rid")
except DatasetNotFoundError as dnfe:
    print(dnfe.dataset_rid)  # will print rid.foundry.main.dataset.non-existent-rid
```

Each of these exceptions is based on the [`FoundryAPIError`](#foundry_dev_tools.errors.meta.FoundryAPIError).
[`FoundryAPIError`](#foundry_dev_tools.errors.meta.FoundryAPIError) itself is based on the [`FoundryDevToolsError`](#foundry_dev_tools.errors.meta.FoundryDevToolsError).

:::{seealso}
More indepth information can be found here: [Errors](/dev/architecture/errors.md) and [ContextHTTPClient](/dev/architecture/foundry_context_implementation.md#client)
:::

[FoundryContext]: #foundrycontext
[API clients]: #api-clients
[basic concepts]: #basic-concepts
[Resources]: #resources
[`Config`]: #foundry_dev_tools.config.config.Config
[`TokenProvider`]: #foundry_dev_tools.config.token_provider
[credentials config]: ../configuration.md#credentials-configuration
[Implement an API Client]: /dev/architecture/api_client_implementation.md 
[Implement a Token Provider]: /dev/architecture/token_provider_implementation.md
[`JWTTokenProvider`]: #foundry_dev_tools.config.token_provider.JWTTokenProvider
[`OAuthTokenProvider`]: #foundry_dev_tools.config.token_provider.OAuthTokenProvider
[`FoundryContext`]: #foundry_dev_tools.config.context.FoundryContext
[`Resource`]: #foundry_dev_tools.resources.resource.Resource
[`Dataset`]: #foundry_dev_tools.resources.dataset.Dataset
[`Folder`]: #foundry_dev_tools.resources.folder.Folder
