import os
from pathlib import Path

import pytest
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem
from pyfakefs.fake_filesystem_unittest import Patcher

from foundry_dev_tools.cli.config import migrate, migrate_project
from foundry_dev_tools.utils.config import cfg_files, site_cfg_file, user_cfg_files


@pytest.fixture()
def fs():
    with Patcher() as p:
        yield p.fs
    # these cached values don't get teared down correctly from the patcher
    cfg_files.cache_clear()
    site_cfg_file.cache_clear()
    user_cfg_files.cache_clear()


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
