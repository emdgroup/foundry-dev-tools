from __future__ import annotations

import time
from types import SimpleNamespace

from foundry_dev_tools.utils.scope import build_auth_error_hint, parse_token_scope


def test_parse_token_scope_collects_operations_and_resources():
    folder_rid = "ri.compass.main.folder.1234"
    dataset_rid = "ri.foundry.main.dataset.5678"
    scope = parse_token_scope(
        {
            "type": "union",
            "scopes": [
                {"type": "operation", "operations": ["api:read-data", "api:read-data"]},
                {
                    "type": "intersection",
                    "scopes": [
                        {
                            "type": "resource",
                            "resourceIds": [folder_rid, dataset_rid],
                            "includeChildren": True,
                        },
                        {"type": "operation", "operations": ["compass:read-resource"]},
                    ],
                },
            ],
        },
    )

    assert scope.universal is False
    assert scope.operations == ("api:read-data", "compass:read-resource")
    assert [resource.rid for resource in scope.resources] == [folder_rid, dataset_rid]
    assert [resource.rid for resource in scope.compass_resources] == [folder_rid]


def test_parse_token_scope_universal():
    scope = parse_token_scope({"type": "universal"})
    assert scope.universal is True
    assert scope.operations == ()
    assert scope.resources == ()


def test_parse_token_scope_intersection_with_universal_branch_is_restricted():
    # A universal branch in an intersection does not lift the other restrictions.
    scope = parse_token_scope(
        {
            "type": "intersection",
            "scopes": [
                {"type": "universal"},
                {"type": "resource", "resourceIds": ["ri.compass.main.folder.1234"]},
            ],
        },
    )
    assert scope.universal is False
    assert [resource.rid for resource in scope.resources] == ["ri.compass.main.folder.1234"]


def test_parse_token_scope_union_with_universal_branch_is_universal():
    scope = parse_token_scope(
        {
            "type": "union",
            "scopes": [
                {"type": "universal"},
                {"type": "resource", "resourceIds": ["ri.compass.main.folder.1234"]},
            ],
        },
    )
    assert scope.universal is True


def test_parse_token_scope_ignores_invalid_nodes():
    assert parse_token_scope(None).universal is False
    assert parse_token_scope({"type": "operation", "operations": [None, 1, "api:read-data"]}).operations == (
        "api:read-data",
    )


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def make_context(provider_supports_scope: bool = True, expires_at: float | None = None):
    folder_rid = "ri.compass.main.folder.1234"
    calls = SimpleNamespace(scope=0, resources=0)

    def api_me_scope(**kwargs):
        calls.scope += 1
        return FakeResponse(
            {
                "type": "union",
                "scopes": [
                    {
                        "type": "intersection",
                        "scopes": [
                            {"type": "resource", "resourceIds": [folder_rid], "includeChildren": True},
                            {"type": "operation", "operations": ["compass:write-resource"]},
                        ],
                    },
                ],
            },
        )

    def api_get_resources(rids, **kwargs):
        calls.resources += 1
        assert rids == {folder_rid}
        return FakeResponse({folder_rid: {"rid": folder_rid, "name": "Project A"}})

    context = SimpleNamespace(
        token_provider=SimpleNamespace(
            supports_scope_diagnostics=provider_supports_scope,
            token_expires_at=lambda: expires_at,
        ),
        multipass=SimpleNamespace(api_me_scope=api_me_scope),
        compass=SimpleNamespace(api_get_resources=api_get_resources),
    )
    return context, calls


def test_build_auth_error_hint_lists_named_compass_resources():
    context, calls = make_context()
    hint = build_auth_error_hint(context, 403)

    assert hint is not None
    assert "Project A" in hint
    assert "ri.compass.main.folder.1234" in hint
    assert "Expand the token scope" in hint
    assert calls.scope == 1
    assert calls.resources == 1


def test_build_auth_error_hint_for_401_does_not_query_scope():
    context, calls = make_context()
    hint = build_auth_error_hint(context, 401)

    assert hint is not None
    assert "HTTP 401" in hint
    assert "Re-authorize the Palantir MCP token" in hint
    assert calls.scope == 0
    assert calls.resources == 0


def test_build_auth_error_hint_ignores_non_scoped_providers():
    context, calls = make_context(provider_supports_scope=False)
    assert build_auth_error_hint(context, 403) is None
    assert calls.scope == 0
    assert calls.resources == 0


def test_build_auth_error_hint_is_best_effort_when_scope_lookup_fails():
    context, _ = make_context()
    context.multipass.api_me_scope = lambda: (_ for _ in ()).throw(RuntimeError("boom"))

    hint = build_auth_error_hint(context, 403)

    assert hint is not None
    assert "HTTP 403" in hint
    assert "Re-authorize the Palantir MCP token" in hint


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


def test_build_auth_error_hint_explains_masked_scope_on_compass_404():
    context, calls = make_context()

    hint = build_auth_error_hint(context, 404, "Compass:NotFound")

    assert hint is not None
    assert "not found (HTTP 404)" in hint
    assert "Project A" in hint
    assert "ri.compass.main.folder.1234" in hint
    assert "Expand the token scope" in hint
    assert calls.scope == 1
    assert calls.resources == 1


def test_build_auth_error_hint_ignores_other_404_error_names():
    context, calls = make_context()

    hint = build_auth_error_hint(context, 404, "DataProxy:FileDoesNotExistInTransaction")

    assert hint is None
    assert calls.scope == 0


def test_build_auth_error_hint_ignores_compass_404_for_non_scoped_providers():
    context, calls = make_context(provider_supports_scope=False)

    hint = build_auth_error_hint(context, 404, "Compass:NotFound")

    assert hint is None
    assert calls.scope == 0


def test_build_auth_error_hint_ignores_compass_404_for_universal_scope():
    context, _ = make_context()
    context.multipass.api_me_scope = lambda **_kwargs: FakeResponse({"type": "universal"})

    assert build_auth_error_hint(context, 404, "Compass:NotFound") is None
