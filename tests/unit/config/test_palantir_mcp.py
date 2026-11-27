from __future__ import annotations

import json
import os
import time
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from freezegun import freeze_time

from foundry_dev_tools.config import config as config_module
from foundry_dev_tools.config.config_types import Host
from foundry_dev_tools.config.context import FoundryContext
from foundry_dev_tools.config.palantir_mcp import (
    PalantirMCPCredential,
    discover_palantir_mcp_credential,
    read_palantir_mcp_credentials,
    select_palantir_mcp_credential,
)
from foundry_dev_tools.config.token_provider import JWTTokenProvider, PalantirMCPTokenProvider
from foundry_dev_tools.errors.config import MissingFoundryHostError, TokenProviderConfigError
from foundry_dev_tools.errors.meta import FoundryAPIError
from foundry_dev_tools.utils.compat import v1_to_v2_config
from tests.unit.mocks import (
    MOCK_ADAPTER,
    TEST_DOMAIN,
    TEST_HOST,
    FoundryMockContext,
    MockTokenProvider,
    make_jwt,
    write_mcp_config,
)

if TYPE_CHECKING:
    from pathlib import Path


class ScopeDiagnosticsMockTokenProvider(MockTokenProvider):
    """Mock provider that opts into the Palantir MCP scope diagnostics."""

    supports_scope_diagnostics = True


def test_read_palantir_mcp_credentials_skips_invalid_entries(isolated_palantir_mcp_config: Path):
    path = isolated_palantir_mcp_config
    path.write_text(
        json.dumps(
            {
                "hosts": {
                    "https://foundry.example.com": {"token": make_jwt(time.time() + 3600)},
                    "https://invalid.example.com": {"token": ""},
                    "https://wrong.example.com": "not-an-object",
                },
            },
        ),
    )

    credentials = read_palantir_mcp_credentials()

    assert len(credentials) == 1
    assert credentials[0].host == Host("foundry.example.com")
    assert credentials[0].expires_at is not None


def test_read_palantir_mcp_credentials_fails_for_token_without_an_expiry(isolated_palantir_mcp_config: Path):
    write_mcp_config(isolated_palantir_mcp_config, {"https://foundry-dev-tools.test": "not-a-jwt"})

    with pytest.raises(TokenProviderConfigError, match="no decodable expiry") as exc_info:
        read_palantir_mcp_credentials()

    assert "https://foundry-dev-tools.test" in str(exc_info.value)
    assert "Re-authorize the Palantir MCP token" in str(exc_info.value)


def test_read_palantir_mcp_credentials_tolerates_missing_or_broken_file(isolated_palantir_mcp_config: Path):
    assert read_palantir_mcp_credentials() == []
    isolated_palantir_mcp_config.write_text("{")
    assert read_palantir_mcp_credentials() == []


def test_select_palantir_mcp_credential():
    valid = PalantirMCPCredential(Host("one.example.com"), "one", time.time() + 3600)
    valid_two = PalantirMCPCredential(Host("two.example.com"), "two", time.time() + 3600)
    expired = PalantirMCPCredential(Host("one.example.com"), "expired", time.time() - 100)

    assert select_palantir_mcp_credential([valid]) == valid
    assert select_palantir_mcp_credential([expired]) == expired
    assert select_palantir_mcp_credential([valid], host=Host("one.example.com")) == valid

    with pytest.raises(TokenProviderConfigError, match="multiple Foundry hosts"):
        select_palantir_mcp_credential([valid, valid_two])
    with pytest.raises(TokenProviderConfigError, match="No Palantir MCP token found"):
        select_palantir_mcp_credential([valid], host=Host("other.example.com"))


def test_select_palantir_mcp_credential_requires_matching_scheme():
    cached = PalantirMCPCredential(Host("one.example.com", "https"), "one", time.time() + 3600)

    with pytest.raises(TokenProviderConfigError, match="No Palantir MCP token found"):
        select_palantir_mcp_credential([cached], host=Host("one.example.com", "http"))


def test_auto_pickup_when_no_foundry_dev_tools_config(
    mock_config_location,
    isolated_palantir_mcp_config: Path,
    capsys: pytest.CaptureFixture[str],
):
    write_mcp_config(isolated_palantir_mcp_config, {"https://foundry-dev-tools.test": make_jwt(time.time() + 3600)})

    with mock.patch.dict(os.environ, {}, clear=True):
        config = config_module.get_config_dict()

    stderr = capsys.readouterr().err
    assert "using the Palantir MCP token" in stderr
    assert "fdt config init" in stderr
    assert config == {
        "credentials": {
            "domain": "foundry-dev-tools.test",
            "scheme": "https",
            "palantir_mcp": {},
        },
    }
    # The notice is printed only once per process.
    with mock.patch.dict(os.environ, {}, clear=True):
        assert config_module.get_config_dict() == config
    assert capsys.readouterr().err == ""


def test_auto_pickup_when_config_only_has_general_settings(
    mock_config_location,
    isolated_palantir_mcp_config: Path,
    capsys: pytest.CaptureFixture[str],
):
    user_config = list(mock_config_location)[1]
    user_config.write_text("[config]\nrich_traceback = true\n")
    write_mcp_config(isolated_palantir_mcp_config, {"https://foundry-dev-tools.test": make_jwt(time.time() + 3600)})

    with mock.patch.dict(os.environ, {}, clear=True):
        config = config_module.get_config_dict()

    assert "using the Palantir MCP token" in capsys.readouterr().err
    assert config["config"] == {"rich_traceback": True}
    assert config["credentials"]["domain"] == "foundry-dev-tools.test"
    assert config["credentials"]["palantir_mcp"] == {}


def test_explicit_palantir_mcp_provider_can_omit_domain(
    mock_config_location,
    isolated_palantir_mcp_config: Path,
    capsys: pytest.CaptureFixture[str],
):
    user_config = list(mock_config_location)[1]
    user_config.write_text("[credentials]\npalantir_mcp = {}\n")
    write_mcp_config(isolated_palantir_mcp_config, {"https://foundry-dev-tools.test": make_jwt(time.time() + 3600)})

    with mock.patch.dict(os.environ, {}, clear=True):
        config = config_module.get_config_dict()
        domain = config["credentials"]["domain"]
        provider = config_module.parse_credentials_config(config)

    assert domain == "foundry-dev-tools.test"
    assert isinstance(provider, PalantirMCPTokenProvider)
    assert provider.token
    assert capsys.readouterr().err == ""


def test_app_service_environment_does_not_auto_pick(mock_config_location, isolated_palantir_mcp_config: Path):
    write_mcp_config(isolated_palantir_mcp_config, {"https://foundry-dev-tools.test": make_jwt(time.time() + 3600)})

    with mock.patch.dict(os.environ, {"APP_SERVICE_TS": "1"}, clear=True):
        assert config_module.get_config_dict() is None


def test_explicit_credentials_win_over_mcp_config(
    mock_config_location,
    isolated_palantir_mcp_config: Path,
    capsys: pytest.CaptureFixture[str],
):
    user_config = list(mock_config_location)[1]
    user_config.write_text('[credentials]\ndomain = "explicit.example.com"\njwt = "explicit-token"\n')
    write_mcp_config(
        isolated_palantir_mcp_config,
        {
            "https://one.example.com": make_jwt(time.time() + 3600),
            "https://two.example.com": make_jwt(time.time() + 3600),
        },
    )

    config = config_module.get_config_dict()

    assert config["credentials"]["domain"] == "explicit.example.com"
    assert config["credentials"]["jwt"] == "explicit-token"
    assert "palantir_mcp" not in config["credentials"]
    assert capsys.readouterr().err == ""


def test_profile_credentials_are_not_affected_by_multiple_cached_hosts(
    mock_config_location,
    isolated_palantir_mcp_config: Path,
    capsys: pytest.CaptureFixture[str],
):
    user_config = list(mock_config_location)[1]
    user_config.write_text('[one.credentials]\ndomain = "profile.example.com"\njwt = "profile-token"\n')
    write_mcp_config(
        isolated_palantir_mcp_config,
        {
            "https://one.example.com": make_jwt(time.time() + 3600),
            "https://two.example.com": make_jwt(time.time() + 3600),
        },
    )

    config = config_module.get_config_dict(profile="one")

    assert config["credentials"] == {"domain": "profile.example.com", "jwt": "profile-token"}
    assert capsys.readouterr().err == ""


def test_env_credentials_are_not_affected_by_multiple_cached_hosts(
    mock_config_location,
    isolated_palantir_mcp_config: Path,
    capsys: pytest.CaptureFixture[str],
):
    write_mcp_config(
        isolated_palantir_mcp_config,
        {
            "https://one.example.com": make_jwt(time.time() + 3600),
            "https://two.example.com": make_jwt(time.time() + 3600),
        },
    )

    with mock.patch.dict(
        os.environ,
        {"FDT_CREDENTIALS__DOMAIN": "env.example.com", "FDT_CREDENTIALS__JWT": "env-token"},
        clear=True,
    ):
        config = config_module.get_config_dict()

    assert config["credentials"]["domain"] == "env.example.com"
    assert config["credentials"]["jwt"] == "env-token"
    assert "palantir_mcp" not in config["credentials"]
    assert capsys.readouterr().err == ""


def test_v1_credentials_ignore_ambiguous_mcp_cache(
    mock_config_location,
    isolated_palantir_mcp_config: Path,
):
    write_mcp_config(
        isolated_palantir_mcp_config,
        {
            "https://one.example.com": make_jwt(time.time() + 3600),
            "https://two.example.com": make_jwt(time.time() + 3600),
        },
    )

    token_provider, _ = v1_to_v2_config({"jwt": "v1-token", "foundry_url": "https://v1.example.com"})

    assert isinstance(token_provider, JWTTokenProvider)


def test_v1_url_selects_matching_mcp_host(
    mock_config_location,
    isolated_palantir_mcp_config: Path,
):
    write_mcp_config(
        isolated_palantir_mcp_config,
        {
            "https://one.example.com": make_jwt(time.time() + 3600),
            "https://two.example.com": make_jwt(time.time() + 3600),
        },
    )

    token_provider, _ = v1_to_v2_config({"foundry_url": "https://one.example.com"})

    assert isinstance(token_provider, PalantirMCPTokenProvider)
    assert token_provider.host.url == "https://one.example.com"


def test_v1_url_without_matching_mcp_host_does_not_raise_ambiguity(
    mock_config_location,
    isolated_palantir_mcp_config: Path,
):
    write_mcp_config(
        isolated_palantir_mcp_config,
        {
            "https://one.example.com": make_jwt(time.time() + 3600),
            "https://two.example.com": make_jwt(time.time() + 3600),
        },
    )

    with pytest.raises(TokenProviderConfigError, match="To authenticate with Foundry"):
        v1_to_v2_config({"foundry_url": "https://three.example.com"})


def test_auto_pickup_errors_when_multiple_hosts_are_valid(mock_config_location, isolated_palantir_mcp_config: Path):
    write_mcp_config(
        isolated_palantir_mcp_config,
        {
            "https://one.example.com": make_jwt(time.time() + 3600),
            "https://two.example.com": make_jwt(time.time() + 3600),
        },
    )

    with mock.patch.dict(os.environ, {}, clear=True), pytest.raises(TokenProviderConfigError, match="multiple"):
        config_module.get_config_dict()


def test_configured_domain_selects_the_cached_host(
    mock_config_location,
    isolated_palantir_mcp_config: Path,
    capsys: pytest.CaptureFixture[str],
):
    user_config = list(mock_config_location)[1]
    user_config.write_text('[credentials]\ndomain = "one.example.com"\n')
    write_mcp_config(
        isolated_palantir_mcp_config,
        {
            "https://one.example.com": make_jwt(time.time() + 3600),
            "https://two.example.com": make_jwt(time.time() + 3600),
        },
    )

    with mock.patch.dict(os.environ, {}, clear=True):
        config = config_module.get_config_dict()

    assert "using the Palantir MCP token" in capsys.readouterr().err
    assert config["credentials"]["domain"] == "one.example.com"
    assert config["credentials"]["palantir_mcp"] == {}


def test_configured_scheme_selects_the_cached_entry(
    mock_config_location,
    isolated_palantir_mcp_config: Path,
    capsys: pytest.CaptureFixture[str],
):
    user_config = list(mock_config_location)[1]
    user_config.write_text('[credentials]\ndomain = "one.example.com"\nscheme = "http"\n')
    write_mcp_config(isolated_palantir_mcp_config, {"http://one.example.com": make_jwt(time.time() + 3600)})

    with mock.patch.dict(os.environ, {}, clear=True):
        config = config_module.get_config_dict()

    assert "using the Palantir MCP token" in capsys.readouterr().err
    assert config["credentials"] == {
        "domain": "one.example.com",
        "scheme": "http",
        "palantir_mcp": {},
    }


def test_explicit_non_mcp_provider_does_not_inherit_a_discovered_domain(
    mock_config_location,
    isolated_palantir_mcp_config: Path,
):
    user_config = list(mock_config_location)[1]
    user_config.write_text('[credentials]\njwt = "explicit-token"\n')
    write_mcp_config(isolated_palantir_mcp_config, {"https://foundry-dev-tools.test": make_jwt(time.time() + 3600)})

    with mock.patch.dict(os.environ, {}, clear=True), pytest.raises(MissingFoundryHostError):
        config_module.get_config_dict()


def test_explicit_mcp_provider_without_cached_token_reports_the_missing_token(
    mock_config_location,
    isolated_palantir_mcp_config: Path,
):
    user_config = list(mock_config_location)[1]
    user_config.write_text("[credentials]\npalantir_mcp = {}\n")

    with (
        mock.patch.dict(os.environ, {}, clear=True),
        pytest.raises(
            TokenProviderConfigError,
            match="No Palantir MCP token found",
        ) as exc_info,
    ):
        config_module.get_config_dict()

    assert "Re-authorize the Palantir MCP token" in str(exc_info.value)


def test_explicit_token_provider_does_not_require_mcp_discovery(
    mock_config_location,
    isolated_palantir_mcp_config: Path,
):
    write_mcp_config(
        isolated_palantir_mcp_config,
        {
            "https://one.example.com": make_jwt(time.time() + 3600),
            "https://two.example.com": make_jwt(time.time() + 3600),
        },
    )
    provider = MockTokenProvider(jwt="explicit-token")

    with mock.patch.dict(os.environ, {}, clear=True):
        context = FoundryContext(token_provider=provider)

    assert context.token_provider is provider


def test_discover_returns_none_without_mcp_config(isolated_palantir_mcp_config: Path):
    assert discover_palantir_mcp_credential() is None


def test_provider_reuses_the_cached_token(isolated_palantir_mcp_config: Path):
    first = make_jwt(time.time() + 3600)
    write_mcp_config(isolated_palantir_mcp_config, {"https://foundry-dev-tools.test": first})

    provider = PalantirMCPTokenProvider("foundry-dev-tools.test")
    # Removing the file makes any further read fail, so this proves the token is cached.
    isolated_palantir_mcp_config.unlink()

    assert provider.token == first
    assert provider.token == first
    assert provider.token_expires_at() is not None


def test_provider_refreshes_from_disk_when_the_token_is_about_to_expire(isolated_palantir_mcp_config: Path):
    with freeze_time("2020-01-01 00:00:00") as frozen:
        first = make_jwt(time.time() + 3600)
        second = make_jwt(time.time() + 7200)
        write_mcp_config(isolated_palantir_mcp_config, {"https://foundry-dev-tools.test": first})

        provider = PalantirMCPTokenProvider("foundry-dev-tools.test")
        assert provider.token == first

        write_mcp_config(isolated_palantir_mcp_config, {"https://foundry-dev-tools.test": second})
        # The cache follows the token's lifetime: no re-read while the token is fresh.
        frozen.tick(120)
        assert provider.token == first

        # Within the refresh window before the expiry the new token is picked up.
        frozen.tick(3500)
        assert provider.token == second


def test_provider_refresh_if_possible_rereads_the_config(isolated_palantir_mcp_config: Path):
    first = make_jwt(time.time() + 3600)
    second = make_jwt(time.time() + 7200)
    write_mcp_config(isolated_palantir_mcp_config, {"https://foundry-dev-tools.test": first})

    provider = PalantirMCPTokenProvider("foundry-dev-tools.test")
    assert provider.token == first

    write_mcp_config(isolated_palantir_mcp_config, {"https://foundry-dev-tools.test": second})
    assert provider.token == first

    provider.refresh_if_possible()
    assert provider.token == second


def test_provider_fails_for_token_without_an_expiry(isolated_palantir_mcp_config: Path):
    write_mcp_config(isolated_palantir_mcp_config, {"https://foundry-dev-tools.test": "opaque-token"})

    with pytest.raises(TokenProviderConfigError, match="no decodable expiry"):
        PalantirMCPTokenProvider("foundry-dev-tools.test")


def test_provider_fails_when_the_cache_file_becomes_unreadable(isolated_palantir_mcp_config: Path):
    with freeze_time("2020-01-01 00:00:00") as frozen:
        first = make_jwt(time.time() + 3600)
        write_mcp_config(isolated_palantir_mcp_config, {"https://foundry-dev-tools.test": first})

        provider = PalantirMCPTokenProvider("foundry-dev-tools.test")
        assert provider.token == first

        # Simulate a partial write by Palantir MCP: the stale cache is re-read and fails early.
        isolated_palantir_mcp_config.write_text("{")
        frozen.tick(3541)

        with pytest.raises(TokenProviderConfigError, match="No Palantir MCP token found"):
            _ = provider.token


def test_provider_raises_clear_error_for_expired_token(isolated_palantir_mcp_config: Path):
    write_mcp_config(isolated_palantir_mcp_config, {"https://foundry-dev-tools.test": make_jwt(time.time() - 100)})

    provider = PalantirMCPTokenProvider("foundry-dev-tools.test")
    with pytest.raises(TokenProviderConfigError, match="expired at") as exc_info:
        _ = provider.token
    assert "Re-authorize the Palantir MCP token" in str(exc_info.value)


def test_provider_raises_when_host_is_not_cached(isolated_palantir_mcp_config: Path):
    write_mcp_config(isolated_palantir_mcp_config, {"https://foundry-dev-tools.test": make_jwt(time.time() + 3600)})

    with pytest.raises(TokenProviderConfigError, match="No Palantir MCP token found"):
        PalantirMCPTokenProvider("other.example.com")


def test_jwt_provider_reports_token_expiry():
    expires_at = time.time() + 3600

    provider = JWTTokenProvider("foundry-dev-tools.test", make_jwt(expires_at))

    assert provider.token_expires_at() == pytest.approx(expires_at)
    with pytest.raises(TokenProviderConfigError, match="no decodable expiry"):
        JWTTokenProvider("foundry-dev-tools.test", "opaque").token_expires_at()


def test_mcp_provider_reports_expiry_without_refreshing(isolated_palantir_mcp_config: Path):
    expires_at = time.time() - 100
    write_mcp_config(isolated_palantir_mcp_config, {"https://foundry-dev-tools.test": make_jwt(expires_at)})

    provider = PalantirMCPTokenProvider("foundry-dev-tools.test")

    assert provider.token_expires_at() == pytest.approx(expires_at)
    with pytest.raises(TokenProviderConfigError, match="expired at"):
        _ = provider.token


def test_403_adds_compass_scope_hint():
    context = FoundryMockContext(token_provider=ScopeDiagnosticsMockTokenProvider(jwt="token"))
    folder_rid = "ri.compass.main.folder.1234"
    scope = {
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
    }
    MOCK_ADAPTER.register_uri(
        "GET",
        f"{TEST_HOST.url}/multipass/api/me",
        status_code=403,
        json={"errorCode": "PERMISSION_DENIED", "errorName": "test:PermissionDenied", "errorInstanceId": "id"},
    )
    MOCK_ADAPTER.register_uri("GET", f"{TEST_HOST.url}/multipass/api/me/scope", json=scope)
    try:
        with pytest.raises(FoundryAPIError) as exc_info:
            context.multipass.api_me()
        message = str(exc_info.value)
        assert "HTTP 403" in message
        assert folder_rid in message
        assert "Expand the token scope" in message
    finally:
        MOCK_ADAPTER.reset()


def test_404_compass_not_found_adds_scope_hint():
    context = FoundryMockContext(token_provider=ScopeDiagnosticsMockTokenProvider(jwt="token"))
    folder_rid = "ri.compass.main.folder.1234"
    project_rid = "ri.compass.main.folder.9999"
    scope = {
        "type": "union",
        "scopes": [
            {
                "type": "intersection",
                "scopes": [
                    {"type": "resource", "resourceIds": [folder_rid], "includeChildren": True},
                    {"type": "operation", "operations": ["compass:view-project-imports"]},
                ],
            },
        ],
    }
    MOCK_ADAPTER.register_uri(
        "POST",
        f"{TEST_HOST.url}/compass/api/projects/imports/{project_rid}",
        status_code=404,
        json={"errorCode": "NOT_FOUND", "errorName": "Compass:NotFound", "errorInstanceId": "id"},
    )
    MOCK_ADAPTER.register_uri("GET", f"{TEST_HOST.url}/multipass/api/me/scope", json=scope)
    try:
        with pytest.raises(FoundryAPIError) as exc_info:
            context.compass.api_get_imports(project_rid, page_size=1)
        message = str(exc_info.value)
        assert "not found (HTTP 404)" in message
        assert folder_rid in message
    finally:
        MOCK_ADAPTER.reset()


def test_auth_override_skips_token_hints():
    context = FoundryMockContext(token_provider=ScopeDiagnosticsMockTokenProvider(jwt="token"))
    MOCK_ADAPTER.register_uri("GET", f"{TEST_HOST.url}/multipass/api/me", status_code=403)
    try:
        with pytest.raises(FoundryAPIError) as exc_info:
            context.multipass.api_request("GET", "me", auth=lambda request: request)
        assert "Palantir MCP token" not in str(exc_info.value)
    finally:
        MOCK_ADAPTER.reset()


def test_401_adds_reauthorize_hint():
    context = FoundryMockContext(token_provider=ScopeDiagnosticsMockTokenProvider(jwt="token"))
    MOCK_ADAPTER.register_uri("GET", f"{TEST_HOST.url}/multipass/api/me", status_code=401)
    try:
        with pytest.raises(Exception, match="HTTP 401") as exc_info:
            context.multipass.api_me()
        assert "Re-authorize the Palantir MCP token" in str(exc_info.value)
    finally:
        MOCK_ADAPTER.reset()


def test_401_reports_jwt_expiry_for_non_mcp_provider():
    expires_at = time.time() - 100
    context = FoundryMockContext(token_provider=MockTokenProvider(jwt=make_jwt(expires_at)))
    MOCK_ADAPTER.register_uri("GET", f"{TEST_HOST.url}/multipass/api/me", status_code=401)
    try:
        with pytest.raises(FoundryAPIError) as exc_info:
            context.multipass.api_me()
    finally:
        MOCK_ADAPTER.reset()

    assert "expired at" in str(exc_info.value)
    assert "re-authenticate" in str(exc_info.value)


def test_non_mcp_provider_gets_no_scope_hint():
    context = FoundryMockContext(token_provider=MockTokenProvider(jwt="token"))
    MOCK_ADAPTER.register_uri("GET", f"{TEST_HOST.url}/multipass/api/me", status_code=403)
    try:
        with pytest.raises(FoundryAPIError) as exc_info:
            context.multipass.api_me()
        assert "Palantir MCP token" not in str(exc_info.value)
    finally:
        MOCK_ADAPTER.reset()


def test_provider_selects_matching_host_when_multiple_are_cached(isolated_palantir_mcp_config: Path):
    write_mcp_config(
        isolated_palantir_mcp_config,
        {
            "https://foundry-dev-tools.test": make_jwt(time.time() + 3600),
            "https://other.example.com": make_jwt(time.time() + 3600),
        },
    )
    provider = PalantirMCPTokenProvider(TEST_DOMAIN)
    assert provider.token
