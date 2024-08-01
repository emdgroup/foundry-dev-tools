"""The configuration CLI.

Allows to view and edit the Foundry DevTools configuration.
"""

from __future__ import annotations

import os
import platform
import sys

# compatibility for python version < 3.11
if sys.version_info < (3, 11):
    import tomli as tomllib
else:
    import tomllib
from configparser import ConfigParser
from pathlib import Path

import click
import tomli_w
from rich.console import Console
from rich.markdown import Markdown
from rich.prompt import IntPrompt

from foundry_dev_tools.cli.info import _config_section
from foundry_dev_tools.utils.compat import v1_to_v2_config_dict
from foundry_dev_tools.utils.config import (
    PROJECT_CFG_FILE_NAME,
    find_project_config_file,
    site_cfg_file,
    user_cfg_files,
)
from foundry_dev_tools.utils.repo import git_toplevel_dir


def _select_config_path(console: Console, create: bool = True) -> tuple[Path, bool]:
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
    while True:
        file_num = IntPrompt.ask(
            f"Which file do you want to open? Enter a number between 0 and {len(files)-1}",
            console=console,
        )
        if file_num >= 0 and file_num < len(files):
            break
        console.print(f"[prompt.invalid]Number must be between 0 and {len(files)-1}")

    selected_config_file = files[file_num]
    created = False
    if create and not selected_config_file.exists():
        selected_config_file.parent.mkdir(parents=True, exist_ok=True)
        selected_config_file.touch()
        created = True
        console.print(f"Created new config file {selected_config_file!s}")
    return selected_config_file, created


def _edit_file(f: Path):
    if platform.system() == "Windows":
        os.startfile(os.fspath(f))  # noqa: S606
    elif (is_linux := platform.system() == "Linux") or platform.system() == "Darwin":
        editor = os.getenv("EDITOR", "xdg-open" if is_linux else "open")
        os.execlp(editor, editor, os.fspath(f))  # noqa: S606
    else:
        msg = f"Can't find default editor to open config file {f!s}."
        raise RuntimeError(msg)


@click.group("config", invoke_without_command=True)
@click.pass_context
@click.option(
    "-p",
    "--profile",
    help=("The config profile to select.\n"),
)
def config_cli(ctx: click.Context, profile: str | None):
    """Prints the current config and the config file locations."""
    if ctx.invoked_subcommand is None:
        console = Console(markup=True)
        console.print(_config_section(profile=profile))


@config_cli.command("edit")
def edit():
    """Let's you edit your Foundry DevTools configuration file more easily."""
    console = Console(markup=True)

    console.print("This will launch the file in your default editor.")
    selected_config_file, _ = _select_config_path(console, create=True)
    _edit_file(selected_config_file)


@config_cli.command("migrate")
def migrate():
    """Migrate your configuration from Foundry DevTools v1 to the new v2 config file format."""
    p = Path("~/.foundry-dev-tools/config").expanduser()
    console = Console(markup=True)
    if not p.exists():
        console.print("Can't find old configuration file, nothing to migrate.")
        sys.exit(1)
    v2_config_toml = _convert_old_conf(console, p)
    f, created = _select_config_path(console)
    existing = None
    replace = False
    if not created:
        existing_v2_conf = f.read_text()
        if len(existing_v2_conf) > 0:
            try:
                existing = tomllib.loads(existing_v2_conf)
            except Exception as e:
                msg = "Failed to parse existing v2 config at {f!s}"
                raise AttributeError(msg) from e
            if len(existing) > 0:
                console.print(
                    Markdown(
                        f"There is already a v2 config with the following content:\n```toml\n{existing_v2_conf}```",
                    ),
                )
                if not click.confirm("Do you want to replace it?"):
                    console.print("Aborting config migration.")
                    sys.exit(0)

                replace = True
    if replace or click.confirm(f"Write the converted config to {f.absolute()!s}?"):
        f.write_text(v2_config_toml)
    console.print("Run `fdt config` to verify your configuration.")


@config_cli.command("migrate-project")
@click.argument(
    "project_directory",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, resolve_path=True),
    required=False,
)
def migrate_project(project_directory: str | None):
    """Migrates the project config file to the v2 project config."""
    project_dir = Path(project_directory) if project_directory else None
    console = Console(markup=True)
    d = git_toplevel_dir(project_dir)
    if d is None:
        console.print(
            f"The given project path {project_dir if project_dir else Path.cwd()!s}" " is not a git repository.",
        )
        sys.exit(1)
    v1_proj_conf = d.joinpath(".foundry_dev_tools")
    if not v1_proj_conf.exists():
        console.print(f"The project {d!s} does not have a v1 project configuration.")
        sys.exit(1)
    v2_config_toml = _convert_old_conf(console, v1_proj_conf)
    v2_conf_path = d.joinpath(PROJECT_CFG_FILE_NAME)
    if click.confirm(f"Write the converted config to {v2_conf_path!s}?"):
        v2_conf_path.write_text(v2_config_toml)


def _convert_old_conf(console: Console, path: Path) -> str:
    """Reads old config file and converts it to the v2 toml."""
    cp = ConfigParser()
    cp.read_string(path.read_text())
    if not cp.has_section("default"):
        console.print(f"The config file {path!s} does not contain any configuration, nothing to migrate.")
        sys.exit(1)

    v1_config = dict(cp.items("default"))
    v2_config = v1_to_v2_config_dict(v1_config, env=False, get_config=False)
    v2_config_toml = tomli_w.dumps(v2_config)
    console.print(
        Markdown(f"Your v1 project config converted to the v2 format looks like this:\n```toml\n{v2_config_toml}\n```"),
    )
    return v2_config_toml
