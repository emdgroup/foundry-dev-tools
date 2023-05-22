"""This file implements the fdt cli.

It registers the subcommands of fdt.
"""
import click

from .build import build_cli


@click.group("fdt")
def cli():
    """Foundry DevTools CLI."""
    pass


cli.add_command(build_cli)
if __name__ == "__main__":
    cli()
