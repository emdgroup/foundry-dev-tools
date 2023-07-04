"""This file implements the fdt cli.

It registers the subcommands of fdt.
"""
import click

from .build import build_cli
from .info import info_cli


@click.group("fdt")
def cli():
    """Foundry DevTools CLI."""
    pass


cli.add_command(build_cli)
cli.add_command(info_cli)
if __name__ == "__main__":
    cli()
