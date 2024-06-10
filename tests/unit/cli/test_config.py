from pathlib import Path

from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem

from foundry_dev_tools.cli.config import migrate, migrate_project


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
    assert "does not contain any configuration, nothing to migrate." in res.stdout.replace("\n", "")
    assert res.exit_code == 1
    v1_config_file.set_contents(
        """
[default]
jwt=123
foundry_url=https://example.com
                                """,
    )

    fs.makedir(v1_dir.joinpath("cache"))
    res = runner.invoke(migrate, input=b"2\ny\n")
    restdout = res.stdout.replace("\n", "")
    assert "Created new config file" in restdout
    assert "Moving ~/.foundry-dev-tools to `~/.foundry-dev-tools.backup`" in restdout
    assert "Moving the foundry-dev-tools cache" in restdout
    assert res.exit_code == 0
    assert fs.exists(v2_conf_path)
    assert (
        v2_conf_path.read_text()
        == """[credentials]
domain = "example.com"
scheme = "https"

[credentials.token_provider]
name = "jwt"

[credentials.token_provider.config]
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


def test_migrate_project(fs):
    proj_dir = fs.create_dir("/project/")
    cli_runner = CliRunner()
    res = cli_runner.invoke(migrate_project, args=[proj_dir.path])
    assert "The given project path /project is not a git repo" in res.stdout
    assert res.exit_code == 1

    fs.create_dir("/project/.git")
    res = cli_runner.invoke(migrate_project, args=[proj_dir.path])
    assert "The project /project does not have a v1 project config" in res.stdout

    v1_proj_file = fs.create_file("/project/.foundry_dev_tools")
    v1_proj_file.set_contents(
        """
    [default]
    jwt=123
    """
    )
    res = cli_runner.invoke(migrate_project, args=[proj_dir.path], input="y\n")
    restdout = res.stdout.replace("\n", "")
    assert "Write the converted config to /project/.foundry_dev_tools.toml?" in restdout
    assert "Moving /project/.foundry_dev_tools to /project/.foundry_dev_tools.backup" in restdout
    assert res.exit_code == 0
    assert fs.exists("/project/.foundry_dev_tools.toml")
    assert fs.exists("/project/.foundry_dev_tools.backup")
    assert (
        Path("/project/.foundry_dev_tools.toml").read_text()
        == """[credentials.token_provider]
name = "jwt"

[credentials.token_provider.config]
jwt = "123"
"""
    )
    assert Path("/project/.foundry_dev_tools.backup").read_text() == v1_proj_file.contents
