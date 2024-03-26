"""The configuration CLI.

Allows to view and edit the Foundry DevTools configuration.
"""

from __future__ import annotations

import os
import platform
import sys
from typing import TYPE_CHECKING

import click
from rich.console import Console
from rich.prompt import IntPrompt

from foundry_dev_tools.cli.info import _config_section
from foundry_dev_tools.utils.config import find_project_config_file, site_cfg_file, user_cfg_files

if TYPE_CHECKING:
    from pathlib import Path

if sys.version_info < (3, 11):
    pass
else:
    pass


@click.command("config")
@click.option("-e", "--edit", is_flag=True, help="If supplied, edit config files.")
def config_cli(edit: bool):  # noqa: C901, TODO
    """Prints the current config and the config file locations."""
    console = Console(markup=True)
    if edit:

        def _edit_or_create(f: Path) -> str:
            return "[bold green]edit[/bold green]" if f.exists() else "[bold yellow]create[/bold yellow]"

        site_cfg = site_cfg_file()
        user_cfgs = user_cfg_files()
        project_cfg = find_project_config_file()

        files = [site_cfg]
        console.print(
            "[bold][red]\\[0] Site config file[/bold]: may need admin rights to[/red]"
            f" {_edit_or_create(site_cfg)} {site_cfg!s}",
        )

        for u_cfg_file in user_cfgs:
            files.append(u_cfg_file)
            console.print(
                f"[bold cyan]\\[{len(files)-1}] User config file:[/cyan bold]"
                f" {_edit_or_create(u_cfg_file)} {u_cfg_file!s}",
            )

        if project_cfg:
            files.append(project_cfg)
            console.print(
                f"[bold][yellow]\\[{len(files)-1}] Project config file:[/yellow][/bold]"
                f" {_edit_or_create(project_cfg)} {project_cfg}",
            )
        console.print("This will launch the file in your default editor.")
        while True:
            file_num = IntPrompt.ask(
                f"Which file do you want to open? Enter a number between 0 and {len(files)-1}",
                console=console,
            )
            if file_num >= 0 and file_num < len(files):
                break
            console.print(f"[prompt.invalid]Number must be between 0 and {len(files)-1}")

        selected_config_file = files[file_num]
        if not selected_config_file.exists():
            selected_config_file.parent.mkdir(parents=True, exist_ok=True)
            selected_config_file.touch()

        if platform.system() == "Windows":
            os.startfile(os.fspath(selected_config_file))  # noqa: S606
        elif platform.system() == "Linux":
            editor = os.getenv("EDITOR", "xdg-open")
            os.execlp(editor, editor, os.fspath(selected_config_file))  # noqa: S606
        elif platform.system() == "Darwin":
            editor = os.getenv("EDITOR", "open")
            os.execlp(editor, editor, os.fspath(selected_config_file))  # noqa: S606
        else:
            msg = f"Can't find default editor to open config file {selected_config_file!s}."
            raise RuntimeError(msg)

    else:
        console.print(_config_section())
