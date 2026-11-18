from __future__ import annotations

import time
from types import SimpleNamespace

from foundry_dev_tools.utils.scope import build_auth_error_hint, parse_compass_resources

FOLDER_RID = "ri.compass.main.folder.1234"


def test_parse_compass_resources_collects_and_dedupes():
    other_rid = "ri.foundry.main.dataset.5678"

    resources = parse_compass_resources(
        {
            "type": "union",
            "scopes": [
                {"type": "resource", "resourceIds": [FOLDER_RID, other_rid], "includeChildren": True},
                {
                    "type": "intersection",
                    "scopes": [
                        {"type": "resource", "resourceIds": [FOLDER_RID]},
                        {"type": "operation", "operations": ["compass:read-resource"]},
                    ],
                },
            ],
        },
    )

    assert resources == (FOLDER_RID,)


def test_parse_compass_resources_handles_invalid_nodes():
    assert parse_compass_resources(None) == ()
    assert parse_compass_resources({"type": "operation", "operations": ["api:read-data"]}) == ()
    assert parse_compass_resources(
        {"type": "resource", "resourceIds": [None, 1, "ri.compass.main.folder.1"]},
    ) == ("ri.compass.main.folder.1",)


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def make_context(
    provider_supports_scope: bool = True,
    expires_at: float | None = None,
    scope_payload: object | None = None,
):
    calls = SimpleNamespace(scope=0)
    payload = (
        scope_payload
        if scope_payload is not None
        else {"type": "resource", "resourceIds": [FOLDER_RID], "includeChildren": True}
    )

    def api_me_scope(**_kwargs):
        calls.scope += 1
        return FakeResponse(payload)

    context = SimpleNamespace(
        token_provider=SimpleNamespace(
            supports_scope_diagnostics=provider_supports_scope,
            token_expires_at=lambda: expires_at,
        ),
        multipass=SimpleNamespace(api_me_scope=api_me_scope),
    )
    return context, calls


def test_build_auth_error_hint_lists_compass_resources_on_403():
    context, calls = make_context()

    hint = build_auth_error_hint(context, 403)

    assert hint is not None
    assert "HTTP 403" in hint
    assert FOLDER_RID in hint
    assert "Expand the token scope" in hint
    assert calls.scope == 1


def test_build_auth_error_hint_for_401_does_not_query_scope():
    context, calls = make_context()

    hint = build_auth_error_hint(context, 401)

    assert hint is not None
    assert "HTTP 401" in hint
    assert "Re-authorize the Palantir MCP token" in hint
    assert calls.scope == 0


def test_build_auth_error_hint_ignores_non_scoped_providers():
    context, calls = make_context(provider_supports_scope=False)

    assert build_auth_error_hint(context, 403) is None
    assert calls.scope == 0


def test_build_auth_error_hint_is_silent_when_scope_lookup_fails():
    context, _ = make_context()
    context.multipass.api_me_scope = lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("boom"))

    assert build_auth_error_hint(context, 403) is None


def test_build_auth_error_hint_is_silent_without_compass_resources():
    context, calls = make_context(scope_payload={"type": "operation", "operations": ["api:read-data"]})

    assert build_auth_error_hint(context, 403) is None
    assert calls.scope == 1


def test_build_auth_error_hint_reports_expired_token_for_any_provider():
    context, calls = make_context(provider_supports_scope=False, expires_at=1000.0)

    hint = build_auth_error_hint(context, 401)

    assert hint is not None
    assert "expired at 1970-01-01T00:16:40+00:00" in hint
    assert "re-authenticate" in hint
    assert calls.scope == 0


def test_build_auth_error_hint_reports_expiry_on_403():
    context, _ = make_context(provider_supports_scope=False, expires_at=1000.0)

    hint = build_auth_error_hint(context, 403)

    assert hint is not None
    assert "expired at" in hint


def test_build_auth_error_hint_is_silent_for_valid_token():
    context, _ = make_context(provider_supports_scope=False, expires_at=time.time() + 3600)

    assert build_auth_error_hint(context, 401) is None
    assert build_auth_error_hint(context, 403) is None


def test_build_auth_error_hint_ignores_unrelated_status_codes():
    context, _ = make_context(expires_at=1000.0)

    assert build_auth_error_hint(context, 404) is None


def test_build_auth_error_hint_explains_compass_404():
    context, calls = make_context()

    hint = build_auth_error_hint(context, 404, "Compass:NotFound")

    assert hint is not None
    assert "not found (HTTP 404)" in hint
    assert FOLDER_RID in hint
    assert calls.scope == 1


def test_build_auth_error_hint_ignores_other_404_error_names():
    context, calls = make_context()

    assert build_auth_error_hint(context, 404, "DataProxy:FileDoesNotExistInTransaction") is None
    assert calls.scope == 0


def test_build_auth_error_hint_ignores_compass_404_for_non_scoped_providers():
    context, calls = make_context(provider_supports_scope=False)

    assert build_auth_error_hint(context, 404, "Compass:NotFound") is None
    assert calls.scope == 0
