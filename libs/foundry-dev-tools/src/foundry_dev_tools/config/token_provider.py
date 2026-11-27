"""The Foundry DevTools Token providers."""

from __future__ import annotations

import base64
import time
from datetime import datetime, timezone
from functools import cached_property
from typing import TYPE_CHECKING, ClassVar

import palantir_oauth_client
import requests
from requests.structures import CaseInsensitiveDict

from foundry_dev_tools.config.config_types import Host
from foundry_dev_tools.config.palantir_mcp import (
    REAUTHORIZE_HINT,
    PalantirMCPCredential,
    palantir_mcp_config_path,
    read_palantir_mcp_credentials,
    select_palantir_mcp_credential,
    token_expiry,
)
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

    def token_expires_at(self) -> float | None:
        """Return when the current token expires as a Unix timestamp, if it is known.

        Implementations must not trigger a token refresh or any network request;
        this is used for error diagnostics.
        """
        return None

    def refresh_if_possible(self) -> None:
        """Refresh the current token when the provider supports it.

        Providers with a fixed token do nothing, so callers can request a
        refresh without checking the provider type. A provider may do this
        lazily: drop its cache here and fetch the new token on the next
        :attr:`token` access.
        """

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

    def token_expires_at(self) -> float | None:
        """Return the expiry encoded in the JWT, if it can be decoded."""
        return token_expiry(self._jwt)


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

    def token_expires_at(self) -> float | None:
        """Return the expiry of the cached token without triggering a refresh."""
        return token_expiry(self._cached) if self._cached else None

    def refresh_if_possible(self) -> None:
        """Drop the cached token so the next access requests a new one."""
        self.invalidate_cache()


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


class PalantirMCPTokenProvider(TokenProvider):
    """Uses the token that Palantir MCP cached in ``~/.palantir/mcp-config.json``.

    Palantir MCP owns the re-authorization flow. This provider reads the cached
    token and fails with a clear message when the token is missing or expired.
    The credential is kept in memory until its token is about to expire, using
    the expiry decoded by :py:func:`~foundry_dev_tools.config.palantir_mcp.token_expiry`,
    so a re-authorization performed through the Palantir MCP client is picked up
    without restarting the process while routine API calls do not read the file.
    The token must carry an expiry; a missing or unreadable cache fails right away
    instead of guessing how long a credential stays valid.

    .. code-block:: toml

        [credentials]
        domain = "foundry.example.com"
        palantir_mcp = {}
    """

    supports_scope_diagnostics: ClassVar[bool] = True

    _credential_refresh_skew = 60
    """Re-read the MCP config this many seconds before the cached token expires."""

    def __init__(self, host: Host | str) -> None:
        """Initialize the Palantir MCP token provider.

        Args:
            host: the Foundry host whose cached MCP token should be used
        """
        super().__init__(host)
        self._credential: PalantirMCPCredential = self._read_credential()
        self._credential_stale_after = self._stale_after(self._credential)

    def _read_credential(self) -> PalantirMCPCredential:
        credential = select_palantir_mcp_credential(read_palantir_mcp_credentials(), host=self.host)
        if credential is None:
            msg = (
                f"No Palantir MCP token found for {self.host.url} in {palantir_mcp_config_path()}. "
                f"{REAUTHORIZE_HINT}"
            )
            raise TokenProviderConfigError(msg)
        return credential

    def _stale_after(self, credential: PalantirMCPCredential) -> float:
        """Return when the credential should be read from disk again."""
        if credential.expires_at is None:
            msg = (
                f"The Palantir MCP token for {credential.host.url} has no decodable expiry, "
                "so Foundry DevTools cannot tell when to refresh it. "
                f"{REAUTHORIZE_HINT}"
            )
            raise TokenProviderConfigError(msg)
        return credential.expires_at - self._credential_refresh_skew

    def _cached_credential(self) -> PalantirMCPCredential:
        """Return the cached credential, re-reading the MCP config when it is stale."""
        if time.time() < self._credential_stale_after:
            return self._credential
        self._credential = self._read_credential()
        self._credential_stale_after = self._stale_after(self._credential)
        return self._credential

    def token_expires_at(self) -> float | None:
        """Return the cached MCP token's expiry, even if it has already expired."""
        return self._credential.expires_at

    def refresh_if_possible(self) -> None:
        """Re-read the Palantir MCP config on the next token access."""
        self._credential_stale_after = 0

    @property
    def token(self) -> Token:
        """Return the currently cached MCP token without reading the config on every access."""
        credential = self._cached_credential()
        if credential.is_expired():
            # The credential cannot exist without an expiry: `_stale_after` rejects
            # tokens without one when the provider is created or refreshed.
            expires_at = datetime.fromtimestamp(credential.expires_at, tz=timezone.utc).isoformat()
            msg = f"The Palantir MCP token for {credential.host.url} expired at {expires_at}. {REAUTHORIZE_HINT}"
            raise TokenProviderConfigError(msg)
        return credential.token


# markers for documentation
# [begin token_provider mapping]
TOKEN_PROVIDER_MAPPING = {
    "jwt": JWTTokenProvider,
    "oauth": OAuthTokenProvider,
    "app_service": AppServiceTokenProvider,
    "palantir_mcp": PalantirMCPTokenProvider,
    **entry_point_fdt_token_provider(),
}
# [end token_provider mapping]
