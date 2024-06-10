"""Foundry DevTools configuration custom exceptions."""

from __future__ import annotations

from foundry_dev_tools.errors.meta import FoundryDevToolsError


class FoundryConfigError(FoundryDevToolsError):
    """Error meta class for all config related Errors."""

    def __init__(self, *args) -> None:
        super().__init__(
            *args,
            "For more information see: https://emdgroup.github.io/foundry-dev-tools/getting_started/installation.html",
        )


class MissingCredentialsConfigError(FoundryConfigError):
    """Error if no credentials config is available."""

    def __init__(self) -> None:
        super().__init__(
            "To create a FoundryContext you need to provide a credentials config.\n"
            "Either through the token_provider parameter or through the config file.",
        )


class TokenProviderConfigError(FoundryConfigError):
    """Error if the credentials config is invalid."""

    def __init__(self, *args) -> None:
        super().__init__(*args)


class MissingFoundryHostError(TokenProviderConfigError):
    """Raised when the domain is not in the config."""

    def __init__(self) -> None:
        super().__init__("A domain is missing in your credentials configuration.")
