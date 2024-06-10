"""HTTP client implementations, for the context and the base for API clients."""

from __future__ import annotations

import logging
import numbers
import os
from typing import TYPE_CHECKING, Literal

import requests

from foundry_dev_tools.__about__ import __version__
from foundry_dev_tools.errors.handling import ErrorHandlingConfig, raise_foundry_api_error

if TYPE_CHECKING:
    from requests import Response
    from requests.cookies import RequestsCookieJar
    from requests.sessions import (  # type: ignore[attr-defined]
        Incomplete,
        _Auth,
        _Cert,
        _Data,
        _Files,
        _HeadersUpdateMapping,
        _HooksInput,
        _Params,
        _TextMapping,
        _Timeout,
        _Verify,
    )

    from foundry_dev_tools.config.context import FoundryContext

DEFAULT_TIMEOUT = (60, None)


class ContextHTTPClient(requests.Session):
    """Requests Session with config and authentication applied."""

    def __init__(self, context: FoundryContext) -> None:
        self.context = context
        super().__init__()
        if self.context.config.requests_ca_bundle:
            self.verify = os.fspath(self.context.config.requests_ca_bundle)
        self.auth = lambda r: self.context.token_provider.requests_auth_handler(r)
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.DEBUG)
        self._counter = 0
        self.headers = {"User-Agent": f"foundry-dev-tools/{__version__}/python-requests"}

    def request(
        self,
        method: str | bytes,
        url: str | bytes,
        params: _Params | None = None,
        data: _Data | None = None,
        headers: _HeadersUpdateMapping | None = None,
        cookies: None | RequestsCookieJar | _TextMapping = None,
        files: _Files | None = None,
        auth: _Auth | None = None,
        timeout: _Timeout | None = None,
        allow_redirects: bool = True,
        proxies: _TextMapping | None = None,
        hooks: _HooksInput | None = None,
        stream: bool | None = None,
        verify: _Verify | None = None,
        cert: _Cert | None = None,
        json: Incomplete | None = None,
        error_handling: ErrorHandlingConfig | Literal[False] | None = None,
    ) -> Response:
        """Make an authenticated HTTP request.

        Args:
            method: see :py:meth:`requests.Session.request`
            url: see :py:meth:`requests.Session.request`
            params: see :py:meth:`requests.Session.request`
            data: see :py:meth:`requests.Session.request`
            headers: see :py:meth:`requests.Session.request`
            cookies: see :py:meth:`requests.Session.request`
            files: see :py:meth:`requests.Session.request`
            auth: see :py:meth:`foundry_dev_tools.clients.context_client.ContextHTTPClient.auth_handler`
            timeout: see :py:meth:`requests.Session.request`
            allow_redirects: see :py:meth:`requests.Session.request`
            proxies: see :py:meth:`requests.Session.request`
            hooks: see :py:meth:`requests.Session.request`
            stream: see :py:meth:`requests.Session.request`
            verify: see :py:meth:`requests.Session.request`
            cert: see :py:meth:`requests.Session.request`
            json: see :py:meth:`requests.Session.request`
            error_handling: error handling config; if set to False, errors won't be automatically handled
        """
        if verify is None and (rcab := self.context.config.requests_ca_bundle) is not None:
            verify = rcab
        if self.context.config.debug:
            self._counter = count = self._counter + 1
            self._logger.debug(f"(r{count}) Making {method!s} request to {url!s}")  # noqa: G004

        if isinstance(timeout, numbers.Number):
            # add default connect timeout if timeout is a number
            timeout = (DEFAULT_TIMEOUT[0], timeout)
        elif timeout is None:
            # set to the default connect timeout if it is none
            timeout = DEFAULT_TIMEOUT

        response = super().request(
            method,
            url,
            params=params,
            data=data,
            headers=headers,
            cookies=cookies,
            files=files,
            auth=auth,
            timeout=timeout,
            allow_redirects=allow_redirects,
            proxies=proxies,
            hooks=hooks,
            stream=stream,
            verify=verify,
            cert=cert,
            json=json,
        )
        if self.context.config.debug:
            self._logger.debug(
                f"(r{count}) Got response status={response.status_code}, "  # noqa: G004
                f"content_type={response.headers.get('content-type')}, "
                f"content_length={response.headers.get('content-length')}",
            )
        raise_foundry_api_error(response, error_handling)
        return response