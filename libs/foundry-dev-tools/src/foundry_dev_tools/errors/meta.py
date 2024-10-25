"""Metaclasses for all Foundry DevTools exceptions/errors."""

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import requests


class FoundryDevToolsError(Exception):
    """Metaclass for :class:`FoundryAPIError`.

    Catch all foundry_dev_tools errors:

    .. code-block:: python

        try:
            fun()  # raise DatasetNotFoundError or any other
        except FoundryDevToolsError:
            print("Some foundry_dev_tools error")

    """


class FoundryAPIError(FoundryDevToolsError):
    """Parent class for all foundry api errors.

    All "child" errors can be caught with this parent class e.g.:

    .. code-block:: python

        try:
            abcd()  # could raise DatasetHasNoSchemaError or DatasetNotFoundError
        except FoundryAPIError as e:
            print(e)

    """

    message = "Details about the Foundry API error:\n"

    def __init__(self, response: requests.Response | None = None, info: str | None = None, **kwargs) -> None:
        """Initialize a Foundry API error.

        Args:
            response: requests Response where the API error occured
            info: add additional information to this error
            kwargs: error specific parameters which may contain more information about the error
        """
        self.response = response
        self.kwargs = kwargs
        self.info = info
        if api_message := self.kwargs.get("message"):
            # don't show in the list of parameters anymore
            # otherwise this message will be shown twice
            self.message = api_message
            del self.kwargs["message"]
        term_size = shutil.get_terminal_size().columns
        msg = self.message
        if self.info:
            msg += f"\n{self.info}\n"
        else:
            msg += "\n"
        request_sep = "-" * int((term_size - 7) / 2)
        msg += request_sep + "REQUEST" + request_sep + "\n"
        if self.response is not None:
            if self.response.request.method:
                msg += "METHOD = " + self.response.request.method + "\n"
            if ct := self.response.request.headers.get("content-type"):
                msg += "CONTENT-TYPE = " + ct + "\n"
            msg += "ENDPOINT = " + self.response.request.path_url + "\n"

        if len(self.kwargs) > 0:
            param_sep = "-" * int((term_size - 10) / 2)
            msg += param_sep + "PARAMETERS" + param_sep + "\n"
            for k, v in self.kwargs.items():
                msg += str(k) + " = " + str(v) + "\n"

        response_sep = "-" * int((term_size - 8) / 2)
        msg += response_sep + "RESPONSE" + response_sep + "\n"

        if (
            (en := self.kwargs.get("error_name"))
            and (ec := self.kwargs.get("error_code"))
            and (eid := self.kwargs.get("error_instance_id"))
        ):
            msg += f"ERROR_NAME = {en}\nERROR_CODE = {ec}\nERROR_INSTANCE_ID = {eid}\n"

        if self.response is not None:
            msg += f"STATUS = {self.response.status_code}\n"

        super().__init__(msg)

    def __dir__(self):
        yield from super().__dir__()
        yield from self.kwargs.keys()

    def __getattr__(self, name: str):
        return self.kwargs.get(name) or super().__getattribute__(name)
