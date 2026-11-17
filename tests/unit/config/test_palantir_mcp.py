from __future__ import annotations

import base64
import json
import os
import time
import warnings
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from foundry_dev_tools.config import config as config_module
from foundry_dev_tools.config import palantir_mcp
from foundry_dev_tools.config.config_types import Host
from foundry_dev_tools.config.palantir_mcp import (
    PalantirMCPCredential,
    discover_palantir_mcp_credential,
    read_palantir_mcp_credentials,
    select_palantir_mcp_credential,
)
from foundry_dev_tools.config.token_provider import PalantirMCPTokenProvider
from foundry_dev_tools.errors.config import TokenProviderConfigError
from foundry_dev_tools.errors.meta import FoundryAPIError
from tests.unit.mocks import MOCK_ADAPTER, TEST_DOMAIN, TEST_HOST, FoundryMockContext, MockTokenProvider

if TYPE_CHECKING:
    from pathlib import Path


def make_jwt(exp: float | None) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    payload = base64.urlsafe_b64encode(json.dumps({"exp": exp} if exp else {}).encode()).rstrip(b"=").decode()
    return f"{header}.{payload}.signature"


def write_mcp_config(path: Path, hosts: dict[str, str]) -> None:
    path.write_text(json.dumps({"hosts": {host: {"token": token} for host, token in hosts.items()}}))


@pytest.fixture()
def mcp_config_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path.joinpath("mcp-config.json")
    monkeypatch.setattr(palantir_mcp, "palantir_mcp_config_path", lambda: path)
    palantir_mcp.reset_auto_pickup_warning()
    yield path
    palantir_mcp.reset_auto_pickup_warning()


def test_read_palantir_mcp_credentials_skips_invalid_entries(tmp_path: Path):
    path = tmp_path.joinpath("mcp-config.json")
    path.write_text(
        json.dumps(
            {
                "hosts": {
                    "https://foundry.example.com": {"token": make_jwt(time.time() + 3600)},
                    "foundry-bare.example.com": {"token": "not-a-jwt"},
                    "https://invalid.example.com": {"token": ""},
                    "https://wrong.example.com": "not-an-object",
                },
            },
        ),
    )

    credentials = read_palantir_mcp_credentials(path)

    assert len(credentials) == 2
    assert credentials[0].host == Host("foundry.example.com")
    assert credentials[0].expires_at is not None
    assert credentials[1].host == Host("foundry-bare.example.com")
    assert credentials[1].expires_at is None


def test_read_palantir_mcp_credentials_tolerates_missing_or_broken_file(tmp_path: Path):
    assert read_palantir_mcp_credentials(tmp_path.joinpath("missing.json")) == []
    broken = tmp_path.joinpath("broken.json")
    broken.write_text("{")
    assert read_palantir_mcp_credentials(broken) == []


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


def test_auto_pickup_when_no_foundry_dev_tools_config(mock_config_location, mcp_config_path: Path):
    write_mcp_config(mcp_config_path, {"https://foundry-dev-tools.test": make_jwt(time.time() + 3600)})

    with (
        mock.patch.dict(os.environ, {}, clear=True),
        pytest.warns(UserWarning, match="using the Palantir MCP token"),
    ):
        config = config_module.get_config_dict()

    assert config == {
        "credentials": {
            "domain": "foundry-dev-tools.test",
            "scheme": "https",
            "palantir_mcp": {},
        },
    }
    # The warning is emitted only once per process.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert config_module.get_config_dict() == config
    assert not caught


def test_auto_pickup_when_config_only_has_general_settings(mock_config_location, mcp_config_path: Path):
    user_config = list(mock_config_location)[1]
    user_config.write_text("[config]\nrich_traceback = true\n")
    write_mcp_config(mcp_config_path, {"https://foundry-dev-tools.test": make_jwt(time.time() + 3600)})

    with mock.patch.dict(os.environ, {}, clear=True), pytest.warns(UserWarning):
        config = config_module.get_config_dict()

    assert config["config"] == {"rich_traceback": True}
    assert config["credentials"]["domain"] == "foundry-dev-tools.test"
    assert config["credentials"]["palantir_mcp"] == {}


def test_explicit_palantir_mcp_provider_can_omit_domain(mock_config_location, mcp_config_path: Path):
    user_config = list(mock_config_location)[1]
    user_config.write_text("[credentials]\npalantir_mcp = {}\n")
    write_mcp_config(mcp_config_path, {"https://foundry-dev-tools.test": make_jwt(time.time() + 3600)})

    with mock.patch.dict(os.environ, {}, clear=True), warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        config = config_module.get_config_dict()
        domain = config["credentials"]["domain"]
        provider = config_module.parse_credentials_config(config)

    assert domain == "foundry-dev-tools.test"
    assert isinstance(provider, PalantirMCPTokenProvider)
    assert provider.token
    assert not caught


def test_app_service_environment_does_not_auto_pick(mock_config_location, mcp_config_path: Path):
    write_mcp_config(mcp_config_path, {"https://foundry-dev-tools.test": make_jwt(time.time() + 3600)})

    with mock.patch.dict(os.environ, {"APP_SERVICE_TS": "1"}, clear=True):
        assert config_module.get_config_dict() is None


def test_explicit_credentials_win_over_mcp_config(mock_config_location, mcp_config_path: Path):
    user_config = list(mock_config_location)[1]
    user_config.write_text('[credentials]\ndomain = "explicit.example.com"\njwt = "explicit-token"\n')
    write_mcp_config(mcp_config_path, {"https://foundry-dev-tools.test": make_jwt(time.time() + 3600)})

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        config = config_module.get_config_dict()

    assert config["credentials"]["domain"] == "explicit.example.com"
    assert config["credentials"]["jwt"] == "explicit-token"
    assert "palantir_mcp" not in config["credentials"]
    assert not caught


def test_auto_pickup_errors_when_multiple_hosts_are_valid(mock_config_location, mcp_config_path: Path):
    write_mcp_config(
        mcp_config_path,
        {
            "https://one.example.com": make_jwt(time.time() + 3600),
            "https://two.example.com": make_jwt(time.time() + 3600),
        },
    )

    with mock.patch.dict(os.environ, {}, clear=True), pytest.raises(TokenProviderConfigError, match="multiple"):
        config_module.get_config_dict()


def test_discover_returns_none_without_mcp_config(mcp_config_path: Path):
    assert discover_palantir_mcp_credential() is None


def test_provider_reads_token_and_refreshes_from_disk(tmp_path: Path):
    path = tmp_path.joinpath("mcp-config.json")
    first = make_jwt(time.time() + 3600)
    second = make_jwt(time.time() + 7200)
    write_mcp_config(path, {"https://foundry-dev-tools.test": first})

    provider = PalantirMCPTokenProvider("foundry-dev-tools.test", config_path=path)
    assert provider.token == first

    write_mcp_config(path, {"https://foundry-dev-tools.test": second})
    assert provider.token == second


def test_provider_raises_clear_error_for_expired_token(tmp_path: Path):
    path = tmp_path.joinpath("mcp-config.json")
    write_mcp_config(path, {"https://foundry-dev-tools.test": make_jwt(time.time() - 100)})

    provider = PalantirMCPTokenProvider("foundry-dev-tools.test", config_path=path)
    with pytest.raises(TokenProviderConfigError, match="expired at") as exc_info:
        _ = provider.token
    assert "tux palantir-mcp reauthorize" in str(exc_info.value)


def test_provider_raises_when_host_is_not_cached(tmp_path: Path):
    path = tmp_path.joinpath("mcp-config.json")
    write_mcp_config(path, {"https://foundry-dev-tools.test": make_jwt(time.time() + 3600)})

    with pytest.raises(TokenProviderConfigError, match="No Palantir MCP token found"):
        PalantirMCPTokenProvider("other.example.com", config_path=path)


def test_403_adds_scoped_project_names_to_error(tmp_path: Path):
    path = tmp_path.joinpath("mcp-config.json")
    write_mcp_config(path, {"https://foundry-dev-tools.test": make_jwt(time.time() + 3600)})
    context = FoundryMockContext(token_provider=PalantirMCPTokenProvider(TEST_HOST, config_path=path))
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
    MOCK_ADAPTER.register_uri(
        "POST",
        f"{TEST_HOST.url}/compass/api/batch/resources",
        json={folder_rid: {"rid": folder_rid, "name": "Project A"}},
    )
    try:
        with pytest.raises(Exception, match="Project A") as exc_info:
            context.multipass.api_me()
        assert "Expand the token scope" in str(exc_info.value)
    finally:
        MOCK_ADAPTER.reset()


def test_401_adds_reauthorize_hint(tmp_path: Path):
    path = tmp_path.joinpath("mcp-config.json")
    write_mcp_config(path, {"https://foundry-dev-tools.test": make_jwt(time.time() + 3600)})
    context = FoundryMockContext(token_provider=PalantirMCPTokenProvider(TEST_HOST, config_path=path))
    MOCK_ADAPTER.register_uri("GET", f"{TEST_HOST.url}/multipass/api/me", status_code=401)
    try:
        with pytest.raises(Exception, match="HTTP 401") as exc_info:
            context.multipass.api_me()
        assert "tux palantir-mcp reauthorize" in str(exc_info.value)
    finally:
        MOCK_ADAPTER.reset()


def test_non_mcp_provider_gets_no_scope_hint():
    context = FoundryMockContext(token_provider=MockTokenProvider(jwt="token"))
    MOCK_ADAPTER.register_uri("GET", f"{TEST_HOST.url}/multipass/api/me", status_code=403)
    try:
        with pytest.raises(FoundryAPIError) as exc_info:
            context.multipass.api_me()
        assert "Palantir MCP token" not in str(exc_info.value)
    finally:
        MOCK_ADAPTER.reset()


def test_provider_selects_matching_host_when_multiple_are_cached(tmp_path: Path):
    path = tmp_path.joinpath("mcp-config.json")
    write_mcp_config(
        path,
        {
            "https://foundry-dev-tools.test": make_jwt(time.time() + 3600),
            "https://other.example.com": make_jwt(time.time() + 3600),
        },
    )
    provider = PalantirMCPTokenProvider(TEST_DOMAIN, config_path=path)
    assert provider.token
