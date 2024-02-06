from click.testing import CliRunner

from foundry_dev_tools.cli.info import info_cli


def test_info_cli():
    cli_runner = CliRunner(mix_stderr=False)
    res = cli_runner.invoke(info_cli, catch_exceptions=False)
    assert res.exit_code == 0
    assert not res.stderr
