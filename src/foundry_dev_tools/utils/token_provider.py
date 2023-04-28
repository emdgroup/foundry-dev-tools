"""Token Provider for common UPTIMIZE AppService."""
from abc import abstractmethod

from requests.structures import CaseInsensitiveDict

APP_SERVICE_ACCESS_TOKEN_HEADER = "X-Foundry-AccessToken"  # noqa: S105


class AbstractTokenProvider:
    """An abstract class for tokenprovider implementations."""

    @property
    @abstractmethod
    def config_name(self) -> str:
        """The config name to select a token provider.

        This is an abstract property and needs to be overriden
        by a token provider implementation.

        Returns:
            str:
                a unique name for this token provider implementation
        """
        raise NotImplementedError

    @abstractmethod
    def get_token(self) -> "str | None":
        """This method returns the token from the token provider.

        This is an abstract method and needs to be overriden
        by a token provider implementation

        Returns:
            str | None:
                the token from the token provider implementation
        """
        raise NotImplementedError


class AppServiceStreamlitTokenProvider(AbstractTokenProvider):
    """The streamlit token provider."""

    config_name = "streamlit"

    def get_token(self) -> "str | None":
        """Retrieves foundry user token from UPTIMIZE App Service header.

        Returns:
            str | None:
                Foundry token or None
        """
        try:
            return self.get_streamlit_request_headers()[APP_SERVICE_ACCESS_TOKEN_HEADER]
        except (ImportError, KeyError, RuntimeError, ModuleNotFoundError, TypeError):
            try:
                return self.get_streamlit_request_headers_1_14_0()[
                    APP_SERVICE_ACCESS_TOKEN_HEADER
                ]
            except (
                ImportError,
                KeyError,
                RuntimeError,
                ModuleNotFoundError,
                TypeError,
            ):
                return None

    def get_streamlit_request_headers(self):
        """Helper function that returns streamlit request headers.

        Current implementation works starting with streamlit>=1.8.0
        and stopped working <=1.11.1

        Returns:
            :external+tornado:py:class:`tornado.httputil.HTTPHeaders`:
                request headers

        """
        from streamlit.scriptrunner.script_run_context import get_script_run_ctx
        from streamlit.server.server import Server

        session_info = Server.get_current()._get_session_info(
            get_script_run_ctx().session_id
        )
        return session_info.ws.request.headers

    def get_streamlit_request_headers_1_14_0(self) -> CaseInsensitiveDict:
        """Helper function that returns streamlit request headers.

        This implementation works starting with streamlit>=1.14.0

        Returns:
            :py:class:`~requests.structures.CaseInsensitiveDict`:
                case-insensitive dict with request headers
        """
        from streamlit.web.server.websocket_headers import _get_websocket_headers

        return CaseInsensitiveDict(_get_websocket_headers())


class AppServiceDashTokenProvider(AbstractTokenProvider):
    """The flask/dash tokenprovider."""

    config_name = "dash"

    def get_token(self) -> "str | None":
        """Retrieves foundry user token from UPTIMIZE App Service header.

        Returns:
            str | None:
                Foundry token or None

        """
        try:
            return self.get_flask_request_headers()[APP_SERVICE_ACCESS_TOKEN_HEADER]
        except (ImportError, KeyError, RuntimeError):
            return None

    def get_flask_request_headers(self):
        """Helper function for the request headers.

        Returns:
            :external+werkzeug:py:class:`~werkzeug.datastructures.EnvironHeaders`:
                flask/dash request headers for the current request

        """
        from flask import request

        return request.headers


TOKEN_PROVIDERS = [AppServiceStreamlitTokenProvider, AppServiceDashTokenProvider]
__all__ = [
    "TOKEN_PROVIDERS",
    "AppServiceStreamlitTokenProvider",
    "AppServiceDashTokenProvider",
]
