"""Auth adapters for integrating FoundryDevTools with foundry-platform-sdk."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, TypeVar

from foundry_dev_tools._optional.foundry_platform_sdk import foundry_sdk

if TYPE_CHECKING:
    from collections.abc import Callable

    from foundry_dev_tools.config.context import FoundryContext


class Token(ABC):
    """Reimplementation of the private foundry_sdk Token interface."""

    @property
    @abstractmethod
    def access_token(self) -> str:  # noqa: D102
        pass


T = TypeVar("T")


class FoundryDevToolsToken(Token):
    """Token adapter that implements the foundry_sdk Token interface."""

    def __init__(self, token: str) -> None:
        """Initialize the token wrapper.

        Args:
            token (str): The access token string
        """
        self._token = token

    @property
    def access_token(self) -> str:
        """Return the access token.

        Returns:
            str: The access token
        """
        return self._token


class FoundryDevToolsAuth(foundry_sdk.Auth):
    """Auth adapter that bridges FoundryContext with foundry-platform-sdk."""

    def __init__(self, ctx: FoundryContext) -> None:
        """Initialize the auth adapter.

        Args:
            ctx: The FoundryContext instance to use for authentication
        """
        self._ctx: FoundryContext = ctx

    def get_token(self) -> FoundryDevToolsToken:
        """Get the current token from the FoundryContext.

        Returns:
            FoundryDevToolsToken: The wrapped access token
        """
        return FoundryDevToolsToken(self._ctx.token)

    def execute_with_token(self, func: Callable[[FoundryDevToolsToken], T]) -> T:
        """Execute a function with the current token.

        Args:
            func: Function to execute with the token

        Returns:
            T: The return value of the function
        """
        return func(self.get_token())

    def run_with_token(self, func: Callable[[FoundryDevToolsToken], T]) -> None:
        """Run a function with the current token without returning a value.

        Args:
            func: Function to run with the token
        """
        func(self.get_token())
