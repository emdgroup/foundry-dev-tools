"""Helpers for explaining the scope of the current Foundry token."""

from __future__ import annotations

import contextlib
import threading
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from foundry_dev_tools.config.palantir_mcp import REAUTHORIZE_HINT

if TYPE_CHECKING:
    from collections.abc import Iterator

    from foundry_dev_tools.config.context import FoundryContext

COMPASS_RID_PREFIX = "ri.compass."
"""Only Compass resources are reported; other resource types are ignored."""

COMPASS_NOT_FOUND_ERROR_NAME = "Compass:NotFound"
"""Foundry reports resources outside a token's scope as not found instead of forbidden."""

SCOPE_DIAGNOSTICS_TIMEOUT = (5, 10)
"""Connection/read timeout for the scope request so it cannot delay the original error."""

_DIAGNOSTICS = threading.local()


def _collect_compass_resources(node: object, resources: list[str]) -> None:
    """Collect Compass resource identifiers from a recursive scope node."""
    if isinstance(node, list):
        for child in node:
            _collect_compass_resources(child, resources)
        return
    if not isinstance(node, dict):
        return

    if node.get("type") == "resource":
        resources.extend(
            rid for rid in node.get("resourceIds") or [] if isinstance(rid, str) and rid.startswith(COMPASS_RID_PREFIX)
        )

    scopes = node.get("scopes")
    if isinstance(scopes, list):
        for child in scopes:
            _collect_compass_resources(child, resources)


def parse_compass_resources(payload: object) -> tuple[str, ...]:
    """Return the Compass resources referenced by a ``/multipass/api/me/scope`` response."""
    resources: list[str] = []
    _collect_compass_resources(payload, resources)
    return tuple(dict.fromkeys(resources))


@contextlib.contextmanager
def _scope_diagnostics_guard() -> Iterator[bool]:
    """Prevent recursive diagnostics when the diagnostic calls themselves fail."""
    if getattr(_DIAGNOSTICS, "active", False):
        yield False
        return
    _DIAGNOSTICS.active = True
    try:
        yield True
    finally:
        _DIAGNOSTICS.active = False


def describe_compass_resources(context: FoundryContext) -> tuple[str, ...] | None:
    """Return the Compass resources referenced by the token scope, if it can be read."""
    with _scope_diagnostics_guard() as allowed:
        if not allowed:
            return None
        payload: object = None
        # Diagnostics are best-effort; a scope lookup failure must not mask the original error.
        with contextlib.suppress(Exception):
            payload = context.multipass.api_me_scope(timeout=SCOPE_DIAGNOSTICS_TIMEOUT).json()
    if payload is None:
        return None
    return parse_compass_resources(payload)


def _token_expires_at(context: FoundryContext) -> float | None:
    getter = getattr(context.token_provider, "token_expires_at", None)
    if getter is None:
        return None
    expires_at: float | None = None
    with contextlib.suppress(Exception):
        expires_at = getter()
    return expires_at


def _format_expiry(expires_at: float) -> str:
    return datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat(timespec="seconds")


def _expiry_hint(context: FoundryContext, status_code: int) -> tuple[str | None, bool]:
    if status_code not in (401, 403):
        return None, False
    expires_at = _token_expires_at(context)
    if expires_at is None or expires_at > time.time():
        return None, False
    return f"The Foundry token expired at {_format_expiry(expires_at)} (UTC).", True


def _format_scope_hint(resources: tuple[str, ...], status_code: int) -> str:
    if status_code == 404:
        intro = (
            "Foundry reported this resource as not found (HTTP 404). The Palantir MCP token "
            "references these Compass resources (including their descendants), so a resource "
            "that is not covered by them may be reported as not found:"
        )
    else:
        intro = (
            "Foundry denied access (HTTP 403). The Palantir MCP token references these "
            "Compass resources (including their descendants):"
        )
    return "\n".join(
        [
            intro,
            *(f"  - {resource}" for resource in resources),
            f"Expand the token scope if you expected to access this resource. {REAUTHORIZE_HINT}",
        ],
    )


def build_auth_error_hint(
    context: FoundryContext,
    status_code: int,
    error_name: str | None = None,
) -> str | None:
    """Build a user-facing hint for authentication and permission responses.

    An expiry note is added for any token provider that knows when its current
    token expires. Providers that opt in via ``supports_scope_diagnostics``
    additionally get the Compass resources referenced by their token scope for
    ``403`` responses and ``404`` Compass responses, which Foundry uses to mask
    resources outside the token's scope. The original exception is never
    replaced; callers append the returned text to it.
    """
    if status_code not in (401, 403, 404):
        return None

    supports_scope = getattr(context.token_provider, "supports_scope_diagnostics", False)
    hints: list[str] = []

    expiry_hint, expired = _expiry_hint(context, status_code)
    if expiry_hint:
        hints.append(expiry_hint)

    if status_code == 401:
        if supports_scope:
            reason = "" if expired else "The token has probably expired. "
            hints.append(f"Foundry rejected the Palantir MCP token (HTTP 401). {reason}{REAUTHORIZE_HINT}")
        elif expired:
            hints.append("Obtain a new Foundry token or re-authenticate.")
        return "\n".join(hints) or None

    is_scope_denial = status_code == 403 or error_name == COMPASS_NOT_FOUND_ERROR_NAME
    if supports_scope and is_scope_denial:
        resources = describe_compass_resources(context)
        if resources:
            hints.append(_format_scope_hint(resources, status_code))

    return "\n".join(hints) or None
