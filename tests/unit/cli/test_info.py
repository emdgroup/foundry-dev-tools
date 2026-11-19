from __future__ import annotations

import json
import os
import time
from typing import TYPE_CHECKING
from unittest import mock

from click.testing import CliRunner

from foundry_dev_tools.cli.info import _format_token_ttl, info_cli
from foundry_dev_tools.config.token_provider import TokenProvider
from tests.unit.mocks import make_jwt

if TYPE_CHECKING:
    from pathlib import Path


def test_info_cli():
    cli_runner = CliRunner(mix_stderr=False)
    res = cli_runner.invoke(info_cli, catch_exceptions=False)
    assert res.exit_code == 0
    assert not res.stderr


def test_info_cli_reports_configuration_errors(mock_config_location: dict[Path, None]):
    user_config = list(mock_config_location)[1]
    user_config.write_text("[credentials]\npalantir_mcp = {}\n")

    with mock.patch.dict(os.environ, {}, clear=True):
        res = CliRunner(mix_stderr=False).invoke(info_cli, catch_exceptions=False)

    assert res.exit_code == 0
    assert "Could not load the Foundry DevTools configuration." in res.output
    assert "No Palantir MCP token found" in res.output


def test_info_cli_prints_the_token_ttl(mock_config_location: dict[Path, None]):
    user_config = list(mock_config_location)[1]
    jwt = make_jwt(time.time() + 3600)
    user_config.write_text(f'[credentials]\ndomain = "foundry.example.com"\njwt = "{jwt}"\n')

    with mock.patch.dict(os.environ, {}, clear=True):
        res = CliRunner(mix_stderr=False).invoke(info_cli, catch_exceptions=False)

    assert res.exit_code == 0
    assert "token_ttl" in res.output
    assert "expires in" in res.output


def test_token_ttl_is_omitted_when_the_provider_does_not_know_it():
    assert _format_token_ttl(TokenProvider("example.com")) is None


def test_info_cli_hides_mcp_provider_state(
    mock_config_location: dict[Path, None],
    isolated_palantir_mcp_config: Path,
):
    user_config = list(mock_config_location)[1]
    user_config.write_text('[credentials]\ndomain = "foundry-dev-tools.test"\npalantir_mcp = {}\n')
    isolated_palantir_mcp_config.write_text(
        json.dumps({"hosts": {"https://foundry-dev-tools.test": {"token": make_jwt(time.time() + 3600)}}}),
    )

    with mock.patch.dict(os.environ, {}, clear=True):
        res = CliRunner(mix_stderr=False).invoke(info_cli, catch_exceptions=False)

    assert res.exit_code == 0
    assert "token_ttl" in res.output
    # The MCP provider has no masked secret rows: its credential is internal state.
    assert "not shown for security reasons" not in res.output
    assert "credential_stale_after" not in res.output
