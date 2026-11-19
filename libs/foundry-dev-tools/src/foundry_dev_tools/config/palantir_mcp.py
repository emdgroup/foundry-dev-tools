"""Read the token cache maintained by Palantir MCP.

Palantir MCP stores Foundry tokens in ``~/.palantir/mcp-config.json``:

.. code-block:: json

   {
     "hosts": {
       "https://foundry.example.com": {
         "token": "eyJ..."
       }
     }
   }

This module only reads that cache. Triggering the MCP re-authorization flow and
changing the token scope is deliberately out of scope; re-authorize the token
through your Palantir MCP client to refresh or expand its scope.
"""

from __future__ import annotations

import base64
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from foundry_dev_tools.config.config_types import Host
from foundry_dev_tools.errors.config import TokenProviderConfigError

if TYPE_CHECKING:
    from foundry_dev_tools.config.config_types import Token

PALANTIR_MCP_CONFIG_DIR = ".palantir"
PALANTIR_MCP_CONFIG_FILE = "mcp-config.json"
"""The config file Palantir MCP writes, relative to the user's home directory."""

TOKEN_CLOCK_SKEW = 30
"""Treat tokens that expire within this many seconds as already expired."""

REAUTHORIZE_HINT = "Re-authorize the Palantir MCP token to refresh or expand its scope."


@dataclass(frozen=True)
class PalantirMCPCredential:
    """A host/token pair cached by Palantir MCP."""

    host: Host
    token: Token
    expires_at: float | None = None

    def is_expired(self, now: float | None = None) -> bool:
        """Return whether the token is expired (or expires within the clock skew)."""
        if self.expires_at is None:
            return False
        return self.expires_at <= (time.time() if now is None else now) + TOKEN_CLOCK_SKEW


def palantir_mcp_config_path() -> Path:
    """Return the default Palantir MCP config path."""
    return Path.home().joinpath(PALANTIR_MCP_CONFIG_DIR, PALANTIR_MCP_CONFIG_FILE)


def token_expiry(token: str) -> float | None:
    """Return the JWT ``exp`` claim as a Unix timestamp, if it can be decoded."""
    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
    except (TypeError, ValueError):
        return None
    exp = claims.get("exp") if isinstance(claims, dict) else None
    if isinstance(exp, int | float):
        return float(exp)
    return None


def host_from_origin(origin: str) -> Host:
    """Return the :class:`Host` for an MCP origin like ``https://foundry.example.com``.

    A missing scheme defaults to HTTPS. Raises :class:`ValueError` for anything
    that is not an HTTP(S) origin with a netloc.
    """
    value = origin.strip()
    if "://" not in value:
        value = f"https://{value}"
    parsed = urlsplit(value)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        msg = f"Invalid Palantir MCP host origin: {origin!r}"
        raise ValueError(msg)
    return Host(parsed.netloc, parsed.scheme)


def read_palantir_mcp_credentials() -> list[PalantirMCPCredential]:
    """Read all usable host/token pairs from the Palantir MCP config.

    A missing, unreadable, or malformed file is treated as an empty cache. Invalid
    host entries are skipped so that one bad entry cannot break the others.
    """
    path = palantir_mcp_config_path()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    if not isinstance(data, dict):
        return []
    hosts = data.get("hosts")
    if not isinstance(hosts, dict):
        return []

    credentials: list[PalantirMCPCredential] = []
    for origin, entry in hosts.items():
        if not isinstance(origin, str) or not isinstance(entry, dict):
            continue
        token = entry.get("token")
        if not isinstance(token, str) or not token.strip():
            continue
        token = token.strip()
        try:
            host = host_from_origin(origin)
        except ValueError:
            continue
        credentials.append(
            PalantirMCPCredential(host=host, token=token, expires_at=token_expiry(token)),
        )
    return credentials


def select_palantir_mcp_credential(
    credentials: list[PalantirMCPCredential],
    host: Host | str | None = None,
) -> PalantirMCPCredential | None:
    """Select the credential for ``host``, or the only unambiguous cached credential.

    When ``host`` is provided, the matching entry is returned even if it is expired;
    :class:`~foundry_dev_tools.config.token_provider.PalantirMCPTokenProvider` turns
    that into a clear error on first use.

    When ``host`` is not provided, a single unexpired credential is used. If several
    hosts have unexpired tokens, the choice is ambiguous and a ``TokenProviderConfigError``
    is raised instead of silently picking one.
    """
    if not credentials:
        return None

    source = palantir_mcp_config_path()

    if host is not None:
        selected_host = host_from_origin(host) if isinstance(host, str) else host
        matches = [credential for credential in credentials if credential.host.domain == selected_host.domain]
        if not matches:
            msg = f"No Palantir MCP token found for {selected_host.url} in {source}. {REAUTHORIZE_HINT}"
            raise TokenProviderConfigError(msg)
        exact_scheme = [credential for credential in matches if credential.host.scheme == selected_host.scheme]
        if not exact_scheme:
            # The scheme is part of the credential's origin: never send a token cached
            # for HTTPS to an HTTP host (or vice versa).
            msg = f"No Palantir MCP token found for {selected_host.url} in {source}. {REAUTHORIZE_HINT}"
            raise TokenProviderConfigError(msg)
        return exact_scheme[0]

    valid = [credential for credential in credentials if not credential.is_expired()]
    if len(valid) == 1:
        return valid[0]
    if len(valid) > 1:
        hosts = ", ".join(sorted({credential.host.url for credential in valid}))
        msg = (
            f"Found Palantir MCP tokens for multiple Foundry hosts ({hosts}). "
            "Set credentials.domain in your Foundry DevTools configuration to choose one."
        )
        raise TokenProviderConfigError(msg)
    if len(credentials) == 1:
        return credentials[0]
    hosts = ", ".join(sorted({credential.host.url for credential in credentials}))
    msg = f"All Palantir MCP tokens are expired ({hosts}). {REAUTHORIZE_HINT}"
    raise TokenProviderConfigError(msg)


def discover_palantir_mcp_credential(
    host: Host | str | None = None,
) -> PalantirMCPCredential | None:
    """Return the automatically selectable Palantir MCP credential, if any.

    When ``host`` is provided, only a credential cached for that exact origin is
    selected. Otherwise a single unambiguous unexpired credential is used.
    """
    return select_palantir_mcp_credential(read_palantir_mcp_credentials(), host=host)


class _AutoPickupNotice:
    """Tracks whether the one-time auto-pickup notice has been printed."""

    printed = False

    @classmethod
    def print_once(cls, credential: PalantirMCPCredential) -> None:
        """Print the notice for ``credential`` unless it was already printed."""
        if cls.printed:
            return
        cls.printed = True
        # stderr keeps machine-readable stdout, such as the git credential helper, clean.
        sys.stderr.write(
            f"Foundry DevTools is using the Palantir MCP token for {credential.host.url} "
            f"from {palantir_mcp_config_path()}. "
            "Create an explicit Foundry DevTools configuration to silence this message.\n",
        )

    @classmethod
    def reset(cls) -> None:
        """Allow the notice to be printed again (used by tests)."""
        cls.printed = False


def print_auto_pickup_notice(credential: PalantirMCPCredential) -> None:
    """Print one process-wide notice that the MCP credential was auto-detected."""
    _AutoPickupNotice.print_once(credential)


def reset_auto_pickup_notice() -> None:
    """Reset the one-time auto-pickup notice (used by tests)."""
    _AutoPickupNotice.reset()
