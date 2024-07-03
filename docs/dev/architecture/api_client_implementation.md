# API client implementation

This guide describes the conventions for implementing a Foundry DevTools [`APIClient`].

## The api_name attribute

Every [`APIClient`] implementation needs to set the class attribute [`api_name`].

## The api_request method

The [`api_request`] method is a special request function, that can be used to easily make API requests to Foundry.

:::{literalinclude} ../../../libs/foundry-dev-tools/src/foundry_dev_tools/clients/api_client.py
:pyobject: APIClient.api_request

:::

The `method` parameter gets passed to the [`ContextHTTPClient`] of the [`FoundryContext`] associated with the [`APIClient`].
The `api_path` parameter builds the correct URL for that API with the [`build_api_url`](#foundry_dev_tools.utils.clients.build_api_url) method.

:::{literalinclude} ../../../libs/foundry-dev-tools/src/foundry_dev_tools/utils/clients.py
:pyobject: build_api_url
:::

This convenience function just appends the Foundry `url` to the `api_name` of the client and appends the `api_path`.

:::{warning}
The 'Content-Type' is per default 'application/json', if you want another 'Content-Type' you need to set it in the `headers` parameter.
:::

## Naming conventions

### Classes

The [`APIClient`] implementations should (not a must) be called `<Api_name>Client`, omitting "Foundry" from the class name is okay.
For example the API client for "foundry-catalog" is called [`CatalogClient`](#foundry_dev_tools.clients.catalog.CatalogClient) instead of `FoundryCatalogClient`.

### Methods

#### Direct API calls

If it is just a wrapper around an API and does nothing else than calling the [`api_request`] method, the method's name should begin with 'api_' followed by the name of the api.

For example the [`DataProxyClient.api_get_file`](#foundry_dev_tools.clients.data_proxy.DataProxyClient.api_get_file) method:

:::{literalinclude} ../../../libs/foundry-dev-tools/src/foundry_dev_tools/clients/data_proxy.py
:pyobject: DataProxyClient.api_get_file
:::

The parameters of the method should be very similar to the original parameters name.
The `**kwargs` should be passed to the `api_request` method, for [custom error handling](/dev/architecture/errors.md).
These methods always return the [`requests.Response`](#requests.Response) of the API call.

#### Higher level methods

Methods which do more than just calling the API like [`CatalogClient.list_dataset_files`](#foundry_dev_tools.clients.catalog.CatalogClient.list_dataset_files):

:::{literalinclude} ../../../libs/foundry-dev-tools/src/foundry_dev_tools/clients/catalog.py
:pyobject: CatalogClient.list_dataset_files
:::

These methods don't have specific naming convention and can return anything they want.

## API typing

As you may have seen in the examples above, there are types like [`DatasetRid`](#foundry_dev_tools.utils.api_types.DatasetRid), [`PathInDataset`](#foundry_dev_tools.utils.api_types.PathInDataset), [`and more`](#foundry_dev_tools.utils.api_types).
These are not "real" types. They are all exactly the same as the `str` class. These are encouraged to use, for better readability.

Here `dataset` could be anything:
```python
def api_xyz(self, dataset:str): ...
```

Now we know, it is a path on foundry:
```python
def api_xyz(self,dataset:FoundryPath): ...
```

Both of these methods can be called via `api_xyz("/xyz")`.
You don't need to convert strings to this type, calling e.g. `PathInDataset("/xyz")` is exactly the same as `str("/xyz")`, which just returns `"/xyz"` again.

## Example

Let's imagine there is an api called "info". This does not exist and is completely for illustrative purposes, to show what goes into writing an API client for Foundry with Foundry DevTools.

We start of by creating the class `InfoClient`, and set the `api_name` attribute to `info`.

```python
from __future__ import annotations  # this way types don't get 'executed' at runtime
from foundry_dev_tools.clients.api_client import APIClient
from typing import TYPE_CHECKING  # special variable that is only `True` if a type checker is 'looking' at the code
from foundry_dev_tools.errors.handling import ErrorHandlingConfig  # we will need this later for error handling
from foundry_dev_tools.errors.info import (
    UserNotFoundError,
    UserAlreadyExistsError,
    InsufficientPermissionsError,
)  # the errors we will create in the next step

if TYPE_CHECKING:
    # will only be imported if type checking, and not at runtime
    import requests


class InfoClient(APIClient):
    api_name = "info"
```

### Errors

First of we will define the Exceptions that the API provides, in a seperate file.

#### UserNotFound

```python
from foundry_dev_tools.errors.meta import FoundryAPIError

class UserNotFoundError(FoundryAPIError):
    message = "The username provided does not exist."
```
#### UserAlreadyExists

```python
class UserNotFoundError(FoundryAPIError):
    message = "The username already exists."
```

#### InsufficientPermissions


```python
class InsufficientPermissionsError(FoundryAPIError):
    message = "You don't have sufficient permissions to use this API."
```

### API Methods

Each API method on the `info` endpoint, will be its own low level method.

#### Get User Info

URL: /user

HTTP Method: GET

Query Parameters:

- username: for which user to get information

```python
    def api_get_user_info(self, username: str, **kwargs) -> requests.Response:
        """Returns information about the user.

        Args:
            username: for which user to get information
        """
        return self.api_request(
            "GET",
            "user",
            params={"username": username},
            error_handling=ErrorHandlingConfig(
                {
                    "Info:UserNotFoundError": UserNotFoundError,
                    "Info:InsufficientPermissions": InsufficientPermissionsError,
                }
            ),
            **kwargs,
        )
```

#### Change User Info

URL: /user

HTTP Method: POST

Query Parameters:

- username: the name of the user where the information should be changed

Body:

JSON Body with the following definition:

Key Value object, where the key is the information name, and the value is the information value.

Following information names exist:

- org: organization name
- isAdmin: controls if the user is an admin
- enabled: controls if the users account is enabled

If a key is not in the post body it will not be changed.

```python
    def api_change_user_info(
        self,
        username: str,
        org: str | None = None,
        is_admin: bool | None = None,
        enabled: bool | None = None,
        **kwargs,
    ) -> requests.Response:
        """Change user information.

        Args:
            username: the name of the user where the information should be changed
            org: organization name
            is_admin: controls if the user is an admin
            enabled: controls if the users account is enabled
        """
        body = {}
        # We only want to change provided values
        if org is not None:
            body["org"] = org
        if is_admin is not None:
            body["isAdmin"] = is_admin
        if enabled is not None:
            body["enabled"] = enabled
        return self.api_request(
            "POST",
            "user",
            params={"username": username},
            json=body,
            error_handling=ErrorHandlingConfig(
                {
                    "Info:UserNotFoundError": UserNotFoundError,
                    "Info:InsufficientPermissions": InsufficientPermissionsError,
                }
            ),
            **kwargs,
        )
```
#### List users

URL: /users

HTTP Method: GET

```python
    def api_list_users(self, **kwargs) -> requests.Response:
        """List all users."""
        return self.api_request(
            "GET",
            "users",
            error_handling=ErrorHandlingConfig({"Info:InsufficientPermissions": InsufficientPermissionsError}),
            **kwargs,
        )
```

#### Create User

URL: /users

HTTP Method: POST

Body and Query with the same definition as [](#change-user-info), except that `org` is mandatory, and `isAdmin` is per default false, and `enabled` per default true if omitted.

```python
    def api_create_user(
        self,
        username: str,
        org: str,
        is_admin: bool | None = None,
        enabled: bool | None = None,
        **kwargs,
    ) -> requests.Response:
        """Create a user.

        Args:
            username: name of the user to be created
            org: organization name
            is_admin: controls if the user is an admin, defaults to false
            enabled: controls if the users account is enabled, defaults to true
        """
        body = {"org": org}
        # We only want to set provided values
        if is_admin is not None:
            body["isAdmin"] = is_admin
        if enabled is not None:
            body["enabled"] = enabled
        return self.api_request(
            "POST",
            "users",
            params={"username": username},
            json=body,
            error_handling=ErrorHandlingConfig(
                {
                    "Info:UserAlreadyExistsError": UserAlreadyExistsError,
                    "Info:InsufficientPermissions": InsufficientPermissionsError,
                }
            ),
            **kwargs,
        )
```

[`ContextHTTPClient`]: #foundry_dev_tools.clients.context_client.ContextHTTPClient
[`APIClient`]: #foundry_dev_tools.clients.api_client.APIClient
[`api_request`]: #foundry_dev_tools.clients.api_client.APIClient.api_request
[`api_name`]: #foundry_dev_tools.clients.api_client.APIClient.api_name
[`FoundryContext`]: #foundry_dev_tools.config.context.FoundryContext
