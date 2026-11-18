"""Helpers for explaining the scope of the current Foundry token."""

from __future__ import annotations

import contextlib
import threading
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from foundry_dev_tools.config.palantir_mcp import REAUTHORIZE_HINT

if TYPE_CHECKING:
    from collections.abc import Iterator

    from foundry_dev_tools.config.context import FoundryContext

COMPASS_RID_PREFIX = "ri.compass."
"""Only Compass resources are resolved to human-readable names."""

SCOPE_DIAGNOSTICS_TIMEOUT = (5, 10)
"""Connection/read timeout for diagnostic requests so they cannot delay the original error."""

_DIAGNOSTICS = threading.local()


@dataclass(frozen=True)
class ScopeResource:
    """A resource that the current token is allowed to access."""

    rid: str
    name: str | None = None


@dataclass(frozen=True)
class TokenScope:
    """A parsed ``/multipass/api/me/scope`` response."""

    universal: bool
    operations: tuple[str, ...]
    resources: tuple[ScopeResource, ...]

    @property
    def compass_resources(self) -> tuple[ScopeResource, ...]:
        """Return only Compass resources, which are the ones that can be named via Compass."""
        return tuple(resource for resource in self.resources if resource.rid.startswith(COMPASS_RID_PREFIX))


def _collect_scope(node: object, operations: list[str], resources: list[str]) -> bool:
    """Collect operation and resource identifiers and report whether the node is unrestricted."""
    if isinstance(node, list):
        # A bare list combines its children like a union. Visit every child so
        # their operations and resources are collected too.
        child_universal = [_collect_scope(child, operations, resources) for child in node]
        return any(child_universal)
    if not isinstance(node, dict):
        return False

    node_type = node.get("type")
    if node_type == "operation":
        operations.extend(op for op in node.get("operations") or [] if isinstance(op, str))
    elif node_type == "resource":
        resources.extend(rid for rid in node.get("resourceIds") or [] if isinstance(rid, str))

    scopes = node.get("scopes")
    child_universal: list[bool] = []
    if isinstance(scopes, list):
        child_universal = [_collect_scope(child, operations, resources) for child in scopes]

    if node_type == "universal":
        return True

    if node_type == "intersection":
        # Every intersecting branch must be unrestricted; one restriction still applies.
        return bool(child_universal) and all(child_universal)
    return any(child_universal)


def parse_token_scope(payload: object) -> TokenScope:
    """Parse the recursive scope structure returned by ``/multipass/api/me/scope``."""
    operations: list[str] = []
    resource_ids: list[str] = []
    universal = _collect_scope(payload, operations, resource_ids)
    return TokenScope(
        universal=universal,
        operations=tuple(dict.fromkeys(operations)),
        resources=tuple(ScopeResource(rid) for rid in dict.fromkeys(resource_ids)),
    )


def _resolve_compass_names(context: FoundryContext, scope: TokenScope) -> TokenScope:
    compass_resources = scope.compass_resources
    if not compass_resources:
        return scope
    rids = {resource.rid for resource in compass_resources}
    try:
        details = context.compass.api_get_resources(rids, timeout=SCOPE_DIAGNOSTICS_TIMEOUT).json()
    except Exception:  # noqa: BLE001 - diagnostics are best-effort
        return scope
    if not isinstance(details, dict):
        return scope

    named = []
    for resource in scope.resources:
        detail = details.get(resource.rid)
        name = detail.get("name") if isinstance(detail, dict) else None
        named.append(replace(resource, name=name) if isinstance(name, str) and name else resource)
    return TokenScope(universal=scope.universal, operations=scope.operations, resources=tuple(named))


def describe_token_scope(context: FoundryContext) -> TokenScope:
    """Return the current token's operations and resources, resolving Compass names.

    Raises whatever the underlying API calls raise; callers that use this for
    error diagnostics should treat it as best-effort.
    """
    scope = parse_token_scope(context.multipass.api_me_scope(timeout=SCOPE_DIAGNOSTICS_TIMEOUT).json())
    return _resolve_compass_names(context, scope)


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


def _format_scope_hint(scope: TokenScope) -> str:
    if scope.universal:
        return (
            "Foundry denied access (HTTP 403), but the Palantir MCP token is not restricted to "
            "specific Compass resources. Check that the resource exists and that your user has "
            f"permission to access it. {REAUTHORIZE_HINT}"
        )

    compass_resources = scope.compass_resources
    if not compass_resources:
        return (
            "Foundry denied access (HTTP 403). The Palantir MCP token is scoped by operation, not "
            f"to specific Compass resources. {REAUTHORIZE_HINT}"
        )

    lines = [
        "Foundry denied access (HTTP 403). The Palantir MCP token is currently scoped to these " "Compass resources:",
    ]
    for resource in compass_resources:
        if resource.name:
            lines.append(f"  - {resource.name} ({resource.rid})")
        else:
            lines.append(f"  - {resource.rid}")
    lines.append(
        "The token is currently scoped to the resources listed above. Expand the token scope if "
        f"the resource you tried to access is not included. {REAUTHORIZE_HINT}"
    )
    return "\n".join(lines)


def build_scope_hint(context: FoundryContext, status_code: int) -> str | None:
    """Build a user-facing hint for 401/403 responses from a scoped MCP token.

    Only token providers that opt in via ``supports_scope_diagnostics`` are
    diagnosed. The original exception is never replaced; callers append the
    returned text to it.
    """
    if not getattr(context.token_provider, "supports_scope_diagnostics", False):
        return None

    if status_code == 401:
        return (
            "Foundry rejected the Palantir MCP token (HTTP 401). The token has probably expired. " f"{REAUTHORIZE_HINT}"
        )
    if status_code != 403:
        return None

    with _scope_diagnostics_guard() as allowed:
        if not allowed:
            return None
        try:
            scope = describe_token_scope(context)
        except Exception:  # noqa: BLE001 - diagnostics are best-effort
            return (
                "Foundry denied access (HTTP 403). The Palantir MCP token is scoped to specific "
                f"resources and this token cannot read the scope right now. {REAUTHORIZE_HINT}"
            )
    return _format_scope_hint(scope)
