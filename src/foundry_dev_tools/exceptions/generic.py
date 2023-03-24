"""
Module for generic exceptions.
"""


class FoundryDevToolsError(Exception):
    """Metaclass for :class:`FoundryAPIError` and :class:`foundry_dev_tools.fsspec_impl.FoundryFileSystemError`.

    Catch all foundry_dev_tools errors:

    >>> try:
    >>>     fun() # raise DatasetNotFoundError or any other
    >>> except FoundryDevToolsError:
    >>>     print("Some foundry_dev_tools error")

    """


class FoundryAPIError(FoundryDevToolsError):
    """Parent class for all foundry api errors.

    Some children of this Error can take arguments
    which can later be used in an except block e.g.:

    >>> try:
    >>>     abcd() # raises DatasetNotFoundError
    >>> except DatasetNotFoundError as e:
    >>>     print(e.dataset_rid)

    Also, all "child" errors can be catched with this parent class e.g.:

    >>> try:
    >>>     abcd() # could raise DatasetHasNoSchemaError or DatasetNotFoundError
    >>> except FoundryAPIError as e:
    >>>     print(e.dataset_rid)

    """


class FoundryFileSystemError(FoundryDevToolsError):
    """Parent class for alle FoundryFileSystem errors.

    See also :class:`foundry_dev_tools.foundry_api_client.FoundryDevToolsError` and
        :class:`foundry_dev_tools.foundry_api_client.FoundryAPIError`
    """
