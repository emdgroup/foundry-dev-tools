import os
import sys
import time
from pathlib import Path

import pytest
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem
from pyfakefs.fake_filesystem_unittest import Patcher

from foundry_dev_tools.cli.config import init, migrate, migrate_project
from foundry_dev_tools.utils.config import cfg_files, site_cfg_file, user_cfg_file, user_cfg_files
from tests.unit.mocks import make_jwt, write_mcp_config

# compatibility for python version < 3.11
if sys.version_info < (3, 11):
    import tomli as tomllib
else:
    import tomllib


@pytest.fixture()
def fs():
    with Patcher() as p:
        yield p.fs
    # these cached values don't get teared down correctly from the patcher
    cfg_files.cache_clear()
    site_cfg_file.cache_clear()
    user_cfg_files.cache_clear()


def test_config_init_targets_the_dotdir_config():
    assert user_cfg_file() == Path.home().joinpath(".foundry-dev-tools", "config.toml")


def test_config_init_writes_minimal_mcp_config(
    tmp_path: Path,
    isolated_palantir_mcp_config: Path,
    mocker,
):
    write_mcp_config(isolated_palantir_mcp_config, {"https://foundry-dev-tools.test": make_jwt(time.time() + 3600)})
    target = tmp_path.joinpath("foundry-dev-tools", "config.toml")
    mocker.patch("foundry_dev_tools.cli.config.cfg_files", return_value={target: None})
    mocker.patch("foundry_dev_tools.cli.config.user_cfg_file", return_value=target)

    res = CliRunner().invoke(init)

    assert res.exit_code == 0
    assert tomllib.loads(target.read_text()) == {
        "credentials": {"domain": "foundry-dev-tools.test", "palantir_mcp": {}},
    }
    assert "will no longer be printed" in " ".join(res.stdout.split())


def test_config_init_writes_a_non_default_scheme(
    tmp_path: Path,
    isolated_palantir_mcp_config: Path,
    mocker,
):
    write_mcp_config(isolated_palantir_mcp_config, {"http://foundry-dev-tools.test": make_jwt(time.time() + 3600)})
    target = tmp_path.joinpath("config.toml")
    mocker.patch("foundry_dev_tools.cli.config.cfg_files", return_value={target: None})
    mocker.patch("foundry_dev_tools.cli.config.user_cfg_file", return_value=target)

    assert CliRunner().invoke(init).exit_code == 0

    assert tomllib.loads(target.read_text())["credentials"]["scheme"] == "http"


def test_config_init_does_not_overwrite_an_existing_config(
    tmp_path: Path,
    isolated_palantir_mcp_config: Path,
    mocker,
):
    write_mcp_config(isolated_palantir_mcp_config, {"https://foundry-dev-tools.test": make_jwt(time.time() + 3600)})
    target = tmp_path.joinpath("config.toml")
    target.write_text("[config]\nrich_traceback = true\n")
    mocker.patch("foundry_dev_tools.cli.config.cfg_files", return_value={target: None})
    mocker.patch("foundry_dev_tools.cli.config.user_cfg_file", return_value=target)

    res = CliRunner().invoke(init)

    assert res.exit_code == 1
    assert "already exists" in res.stdout
    assert target.read_text() == "[config]\nrich_traceback = true\n"


def test_config_init_requires_an_mcp_token(
    tmp_path: Path,
    isolated_palantir_mcp_config: Path,
    mocker,
):
    target = tmp_path.joinpath("config.toml")
    mocker.patch("foundry_dev_tools.cli.config.cfg_files", return_value={target: None})
    mocker.patch("foundry_dev_tools.cli.config.user_cfg_file", return_value=target)

    res = CliRunner().invoke(init)

    assert res.exit_code == 1
    assert "No Palantir MCP token found" in res.stdout
    assert not target.exists()


def test_migrate(fs: FakeFilesystem):
    v1_dir = Path("~/.foundry-dev-tools/").expanduser()
    v2_dir = Path("~/.config/foundry-dev-tools/").expanduser()
    v2_conf_path = v2_dir.joinpath("config.toml")
    runner = CliRunner()
    res = runner.invoke(migrate)
    assert "Can't find old configuration file, nothing to migrate." in res.stdout
    assert res.exit_code == 1

    fdt_dir = fs.create_dir(v1_dir)
    v1_config_file = fs.create_file(v1_dir.joinpath("config"))

    res = runner.invoke(migrate)
    assert "does not contain any configuration, nothing to migrate.".replace(" ", "") in res.stdout.replace(
        "\n", ""
    ).replace(" ", "")
    assert res.exit_code == 1
    v1_config_file.set_contents(
        """
[default]
jwt=123
foundry_url=https://example.com
                                """,
    )

    res = runner.invoke(migrate, input=b"2\ny\n")
    restdout = res.stdout.replace("\n", "")
    assert "Created new config file" in restdout
    assert res.exit_code == 0
    assert fs.exists(v2_conf_path)
    assert (
        v2_conf_path.read_text()
        == """[credentials]
domain = "example.com"
jwt = "123"
"""
    )
    fs.rename(fdt_dir.path, v1_dir)

    res = runner.invoke(migrate, input="2\ny\n")
    assert "Do you want to replace it?" in res.stdout
    assert res.exit_code == 0

    fs.rename(fdt_dir.path, v1_dir)
    v2_conf_path.expanduser().write_text("!invalid toml!")
    res = runner.invoke(migrate, input="2\ny\n")
    assert "Failed to parse existing v2 config at" in str(res.exc_info)


def test_migrate_project(fs: FakeFilesystem):
    proj_dir = fs.create_dir("/project/")
    cli_runner = CliRunner()
    res = cli_runner.invoke(migrate_project, args=[proj_dir.path])
    assert f"The given project path {proj_dir.path} is not a git repo" in res.stdout
    assert res.exit_code == 1

    fs.create_dir("/project/.git")
    res = cli_runner.invoke(migrate_project, args=[os.fspath(proj_dir.path)])
    assert f"The project {proj_dir.path} does not have a v1 project config" in res.stdout

    v1_proj_file = fs.create_file("/project/.foundry_dev_tools")
    v1_proj_file.set_contents(
        """
    [default]
    jwt=123
    """
    )
    res = cli_runner.invoke(migrate_project, args=[os.fspath(proj_dir.path)], input="y\n")
    restdout = res.stdout.replace("\n", "")
    v2_proj_file = fs.get_object("/project/.foundry_dev_tools.toml")
    assert f"Write the converted config to {v2_proj_file.path}?" in restdout
    assert res.exit_code == 0
    assert (
        v2_proj_file.contents.replace(os.linesep, "\n")
        == """[credentials]
jwt = "123"
"""
    )
