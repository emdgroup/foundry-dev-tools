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
changing the token scope is deliberately out of scope; users do that through
``tux palantir-mcp reauthorize``.
"""

from __future__ import annotations

import base64
import json
import time
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from foundry_dev_tools.config.config_types import Host
from foundry_dev_tools.errors.config import TokenProviderConfigError

if TYPE_CHECKING:
    from os import PathLike

    from foundry_dev_tools.config.config_types import Token

PALANTIR_MCP_CONFIG_DIR = ".palantir"
PALANTIR_MCP_CONFIG_FILE = "mcp-config.json"
"""The config file Palantir MCP writes, relative to the user's home directory."""

TOKEN_CLOCK_SKEW = 30
"""Treat tokens that expire within this many seconds as already expired."""

REAUTHORIZE_HINT = "Run `tux palantir-mcp reauthorize` to refresh or expand the Palantir MCP token."

_AUTO_PICKUP_WARNING_EMITTED = False


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


def _host_from_origin(origin: str) -> Host:
    value = origin.strip()
    if "://" not in value:
        value = f"https://{value}"
    parsed = urlsplit(value)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        msg = f"Invalid Palantir MCP host origin: {origin!r}"
        raise ValueError(msg)
    return Host(parsed.netloc, parsed.scheme)


def read_palantir_mcp_credentials(
    config_path: PathLike[str] | str | None = None,
) -> list[PalantirMCPCredential]:
    """Read all usable host/token pairs from the Palantir MCP config.

    A missing, unreadable, or malformed file is treated as an empty cache. Invalid
    host entries are skipped so that one bad entry cannot break the others.
    """
    path = Path(config_path).expanduser() if config_path is not None else palantir_mcp_config_path()
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
            host = _host_from_origin(origin)
        except ValueError:
            continue
        credentials.append(
            PalantirMCPCredential(host=host, token=token, expires_at=token_expiry(token)),
        )
    return credentials


def select_palantir_mcp_credential(
    credentials: list[PalantirMCPCredential],
    host: Host | str | None = None,
    config_path: PathLike[str] | str | None = None,
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

    source = Path(config_path).expanduser() if config_path is not None else palantir_mcp_config_path()

    if host is not None:
        selected_host = _host_from_origin(host) if isinstance(host, str) else host
        matches = [credential for credential in credentials if credential.host.domain == selected_host.domain]
        if not matches:
            msg = f"No Palantir MCP token found for {selected_host.url} in {source}. {REAUTHORIZE_HINT}"
            raise TokenProviderConfigError(msg)
        exact_scheme = [credential for credential in matches if credential.host.scheme == selected_host.scheme]
        return (exact_scheme or matches)[0]

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
    config_path: PathLike[str] | str | None = None,
) -> PalantirMCPCredential | None:
    """Return the automatically selectable Palantir MCP credential, if any."""
    return select_palantir_mcp_credential(
        read_palantir_mcp_credentials(config_path),
        config_path=config_path,
    )


def warn_auto_pickup(credential: PalantirMCPCredential) -> None:
    """Emit one process-wide warning that the MCP credential was auto-detected."""
    global _AUTO_PICKUP_WARNING_EMITTED  # noqa: PLW0603
    if _AUTO_PICKUP_WARNING_EMITTED:
        return
    _AUTO_PICKUP_WARNING_EMITTED = True
    warnings.warn(
        f"Foundry DevTools is using the Palantir MCP token for {credential.host.url} "
        f"from {palantir_mcp_config_path()}. Create a Foundry DevTools configuration to override this.",
        UserWarning,
        stacklevel=2,
    )


def reset_auto_pickup_warning() -> None:
    """Reset the one-time auto-pickup warning (used by tests)."""
    global _AUTO_PICKUP_WARNING_EMITTED  # noqa: PLW0603
    _AUTO_PICKUP_WARNING_EMITTED = False
