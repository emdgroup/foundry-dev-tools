"""The Foundry DevTools Token providers."""

from __future__ import annotations

import base64
import time
from functools import cached_property
from typing import TYPE_CHECKING, ClassVar

import palantir_oauth_client
import requests
from requests.structures import CaseInsensitiveDict

from foundry_dev_tools.config.config_types import Host
from foundry_dev_tools.errors.config import TokenProviderConfigError
from foundry_dev_tools.errors.handling import ErrorHandlingConfig, raise_foundry_api_error
from foundry_dev_tools.errors.multipass import ClientAuthenticationFailedError
from foundry_dev_tools.utils.config import entry_point_fdt_token_provider

if TYPE_CHECKING:
    from foundry_dev_tools.config.config_types import FoundryOAuthGrantType, Token


class TokenProvider:
    """Parent class for all TokenProviders.

    TokenProvider implementations always need to have these properties:
        host: the foundry host, needs to be implemented
        token: the token from the token provider, needs to be implemented
    """

    def __init__(self, host: Host | str):
        """The TokenProvider base class.

        Args:
            host: the foundry host
        """
        if isinstance(host, str):
            host = Host(host)
        self.host = host

    @property
    def token(self):
        """Returns the token from the provider."""
        msg = "This is only the base TokenProvider class and does not implement getting a token."
        raise NotImplementedError(msg)

    def requests_auth_handler(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
        """Sets bearer authentication header on PreparedRequest object.

        Does not overwrite authorization header if present.
        """
        r.headers.setdefault("authorization", f"Bearer {self.token}")
        return r

    def set_requests_session(self, session: requests.Session) -> None:
        """No-op by default."""


class JWTTokenProvider(TokenProvider):
    """Provides Host and Token."""

    def __init__(self, host: Host | str, jwt: Token) -> None:
        """Initialize the JWTTokenProvider.

        Args:
            host: the foundry host
            jwt: the jwt token
        """
        super().__init__(host)
        self._jwt = jwt

    @cached_property
    def token(self) -> Token:
        """Returns the token supplied when creating this Provider."""
        return self._jwt


class CachedTokenProvider(TokenProvider):
    """Parent class for token providers which get their token dynamically and need caching."""

    _cached: Token | None = None
    _valid_until: float = -1
    # time to remove from expiry
    # e.g. it will request a new token if your token expires in 5 seconds
    _clock_skew: int = 10

    def invalidate_cache(self):
        """Invalidates the token cache."""
        self._cached = None
        self._valid_until = -1

    def _request_token(self) -> tuple[Token, float]:
        """Requests the token from the dynamic source."""
        msg = "This needs to be implemented by a class, this is just the meta class."
        raise NotImplementedError(msg)

    @property
    def token(self) -> Token:
        """Returns the token from a dynamic source and caches it."""
        if not self._cached or self._valid_until < time.time() + 10:
            self._cached, self._valid_until = self._request_token()
        return self._cached


DEFAULT_OAUTH_SCOPES = [
    "offline_access",
    "compass:view",
    "compass:edit",
    "compass:discover",
    "api:write-data",
    "api:read-data",
    "build2:run-build-using-service",
]


class OAuthTokenProvider(CachedTokenProvider):
    """Provides the hostname and tokens obtained from Palantir OAuth."""

    def __init__(
        self,
        host: Host | str,
        client_id: str,
        client_secret: str | None = None,
        grant_type: FoundryOAuthGrantType | None = None,
        scopes: list[str] | str | None = None,
    ) -> None:
        """Provides tokens via the OAuth authentication.

        Args:
            host: the foundry host
            client_id: the client ID
            client_secret: the client secret, optional if the `grant_type` is `authorization_code`,
                and mandatory if the `grant_type` is `client_credentials`
            grant_type: the OAuth grant type,
                see :py:class:`~foundry_dev_tools.config.config_types.FoundryOAuthGrantType`
            scopes: if the `grant_type` is `authorization_code` and not set
                it will default to :py:attr:`~foundry_dev_tools.config.token_provider.DEFAULT_OAUTH_SCOPES`,
                if the `grant_type` is `client_credentials`
                the scopes provided will be used, per default these are null
        """
        super().__init__(host)
        self.grant_type = grant_type or "authorization_code"
        self._client_id = client_id
        self._client_secret = client_secret
        if self.grant_type == "client_credentials" and self._client_secret is None:
            msg = "You need to provide a client secret for the client credentials grant type."
            raise TokenProviderConfigError(msg)
        scopes = self._scopes_to_list(scopes)
        if self.grant_type == "authorization_code":
            if scopes is not None:
                self.scopes = scopes
            else:
                self.scopes = DEFAULT_OAUTH_SCOPES
        else:
            self.scopes = scopes
        self._requests_session = requests.Session()

    def _scopes_to_list(self, scopes: list[str] | str | None) -> list[str] | None:
        if scopes is not None and isinstance(scopes, str):
            splitted = scopes.split(",")
            return [scope for scope in splitted if len(scope) > 0]
        return scopes

    def _request_token(self) -> tuple[Token, float]:
        if self.grant_type == "authorization_code":
            credentials = palantir_oauth_client.get_user_credentials(
                scopes=self.scopes,
                hostname=self.host.domain,
                client_id=self._client_id,
                client_secret=self._client_secret,
                use_local_webserver=False,
            )
            return credentials.token, credentials.expiry.timestamp()
        if self.grant_type == "client_credentials" and self._client_secret is not None:
            # since we share the same requests session everywhere and on this session
            # the auth is set to a lambda function we need to manually disable this
            # for the single token call
            auth_handler = self._requests_session.auth
            self._requests_session.auth = None
            try:
                resp = self._requests_session.request(
                    "POST",
                    f"{self.host.url}/multipass/api/oauth2/token",
                    data={"grant_type": "client_credentials", "scope": " ".join(self.scopes)}
                    if self.scopes
                    else {"grant_type": "client_credentials"},
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Authorization": "Basic "
                        + base64.b64encode(
                            bytes(
                                self._client_id + ":" + self._client_secret,
                                "ISO-8859-1",
                            ),
                        ).decode("ascii"),
                    },
                    timeout=30,
                )
            finally:
                # add original auth handler again
                self._requests_session.auth = auth_handler
            raise_foundry_api_error(
                resp,
                error_handling=ErrorHandlingConfig({401: ClientAuthenticationFailedError}, client_id=self._client_id),
            )
            credentials = resp.json()
            return credentials["access_token"], credentials["expires_in"] + time.time()
        if self._client_secret is None:
            msg = f"For grant type {self.grant_type} you need to set a client_secret."
            raise AttributeError(msg)

        msg = f"Grant type {self.grant_type} is not implemented."
        raise NotImplementedError(msg)

    def set_requests_session(self, session: requests.Session) -> None:
        """Sets request session used for client credentials grant.."""
        self._requests_session = session


class AppServiceTokenProvider(CachedTokenProvider):
    """Token Provider for the AppService, which gets the token via a header from flask/dash/streamlit."""

    header: ClassVar[str] = "X-Foundry-AccessToken"

    def _streamlit(self) -> Token | None:
        try:
            from streamlit import context
        except ImportError:
            pass
        else:
            if context and (token := context.headers.get(self.header)):
                return token
        return None

    def _deprecated_streamlit(self) -> Token | None:
        try:
            from streamlit.web.server.websocket_headers import _get_websocket_headers
        except ImportError:
            pass
        else:
            if (headers := _get_websocket_headers()) and (token := CaseInsensitiveDict(headers).get(self.header)):
                return token
        return None

    def _flask(self) -> Token | None:
        try:
            from flask import request
        except ImportError:
            pass
        else:
            try:
                if request is not None and (token := request.headers.get(self.header)):
                    return token
            except RuntimeError:
                pass
        return None

    def __init__(self, host: Host | str):
        super().__init__(host)

        token = self._streamlit() or self._deprecated_streamlit() or self._flask()

        if token is not None:
            self._cached = token
            self._valid_until = time.time() + 3600
            return
        msg = "Could not get Foundry token from flask/dash/streamlit headers."
        raise TokenProviderConfigError(msg)

    def _request_token(self) -> tuple[Token, float]:
        msg = "Token is expired. Please refresh the web page."
        raise TokenProviderConfigError(msg)


# markers for documentation
# [begin token_provider mapping]
TOKEN_PROVIDER_MAPPING = {
    "jwt": JWTTokenProvider,
    "oauth": OAuthTokenProvider,
    "app_service": AppServiceTokenProvider,
    **entry_point_fdt_token_provider(),
}
# [end token_provider mapping]
