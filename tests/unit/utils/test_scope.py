from __future__ import annotations

from types import SimpleNamespace

from foundry_dev_tools.utils.scope import build_scope_hint, parse_token_scope


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


def make_context(provider_supports_scope: bool = True):
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
        token_provider=SimpleNamespace(supports_scope_diagnostics=provider_supports_scope),
        multipass=SimpleNamespace(api_me_scope=api_me_scope),
        compass=SimpleNamespace(api_get_resources=api_get_resources),
    )
    return context, calls


def test_build_scope_hint_lists_named_compass_resources():
    context, calls = make_context()
    hint = build_scope_hint(context, 403)

    assert hint is not None
    assert "Project A" in hint
    assert "ri.compass.main.folder.1234" in hint
    assert "Expand the token scope" in hint
    assert calls.scope == 1
    assert calls.resources == 1


def test_build_scope_hint_for_401_does_not_query_scope():
    context, calls = make_context()
    hint = build_scope_hint(context, 401)

    assert hint is not None
    assert "HTTP 401" in hint
    assert "Re-authorize the Palantir MCP token" in hint
    assert calls.scope == 0
    assert calls.resources == 0


def test_build_scope_hint_ignores_non_scoped_providers():
    context, calls = make_context(provider_supports_scope=False)
    assert build_scope_hint(context, 403) is None
    assert calls.scope == 0
    assert calls.resources == 0


def test_build_scope_hint_is_best_effort_when_scope_lookup_fails():
    context, _ = make_context()
    context.multipass.api_me_scope = lambda: (_ for _ in ()).throw(RuntimeError("boom"))

    hint = build_scope_hint(context, 403)

    assert hint is not None
    assert "HTTP 403" in hint
    assert "Re-authorize the Palantir MCP token" in hint
