"""This file implements the fdt cli.

It registers the subcommands of fdt.
"""

from __future__ import annotations

import click

from foundry_dev_tools.cli.build import build_cli
from foundry_dev_tools.cli.config import config_cli
from foundry_dev_tools.cli.git import git_cli
from foundry_dev_tools.cli.info import info_cli
from foundry_dev_tools.cli.s3 import s3_cli
from foundry_dev_tools.cli.stubs import stubs_cli


@click.group("fdt")
def cli():
    """Foundry DevTools CLI."""


cli.add_command(build_cli)
cli.add_command(info_cli)
cli.add_command(config_cli)
cli.add_command(git_cli)
cli.add_command(stubs_cli)
cli.add_command(s3_cli)
if __name__ == "__main__":
    cli()
