"""API client parent class."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Literal

from foundry_dev_tools.utils.clients import build_api_url

if TYPE_CHECKING:
    from requests import Response
    from requests.cookies import RequestsCookieJar
    from requests.sessions import (  # type: ignore[attr-defined]
        Incomplete,
        _Auth,
        _Cert,
        _Data,
        _Files,
        _HooksInput,
        _Params,
        _TextMapping,
        _Timeout,
        _Verify,
    )

    from foundry_dev_tools.config.context import FoundryContext
    from foundry_dev_tools.errors.handling import ErrorHandlingConfig


class APIClient:
    """Base class for API clients."""

    """The name of the API, it is used to build the api url, the api_name is the part after '/api/' `https://foundry/api/<api_name>/...`."""
    api_name: ClassVar[str]

    def __init__(self, context: FoundryContext) -> None:
        self.context = context

    def api_url(self, api_path: str) -> str:
        """Returns the API URL for the specified parameters."""
        return build_api_url(
            self.context.token_provider.host.url,
            self.api_name,
            api_path,
        )

    def api_request(
        self,
        method: str | bytes,
        api_path: str,
        params: _Params | None = None,
        data: _Data | None = None,
        headers: dict | None = None,
        cookies: RequestsCookieJar | _TextMapping | None = None,
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
        """Make an authenticated request to the Foundry API.

        The `api_path` argument is only the api path and not the full URL.
        For https://foundry/example/api/method/... this would be only,
        "method/...".

        Args:
            method: see :py:meth:`requests.Session.request`
            api_path: **only** the api path
            params: see :py:meth:`requests.Session.request`
            data: see :py:meth:`requests.Session.request`
            headers: see :py:meth:`requests.Session.request`, content-type defaults to application/json if not set
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
        if headers:
            headers["content-type"] = headers.get("content-type") or headers.get("Content-Type") or "application/json"
        else:
            headers = {"content-type": "application/json"}

        return self.context.client.request(
            method=method,
            url=self.api_url(api_path),
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
            error_handling=error_handling,
        )
