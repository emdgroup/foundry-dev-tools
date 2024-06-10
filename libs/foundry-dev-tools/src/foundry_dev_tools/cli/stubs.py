"""CLI for generating stubs, when using third party addons."""

from __future__ import annotations

from importlib.metadata import entry_points
from pathlib import Path

import click

import foundry_dev_tools.config.context

STUB_FILE = """
from functools import cached_property
from typing import Any

%s

class FoundryContext:
    def __getattr__(self, name: str) -> Any: ...  # incomplete
%s
"""


@click.command("stubs")
def stubs_cli():
    """Generates stubs for external Foundry DevTools addons."""
    imports = ""
    stub_methods = ""
    for ep in entry_points(group="fdt_api_client"):
        click.echo(f"Found '{ep.name}' addon.")
        cls = ep.load()
        imports += f"from {cls.__module__} import {cls.__name__}\n"
        stub_methods += f"""
    @cached_property
    def {ep.name}(self) -> {cls.__name__}: ...
        """
    Path(foundry_dev_tools.config.context.__file__ + "i").write_text(STUB_FILE % (imports, stub_methods))
