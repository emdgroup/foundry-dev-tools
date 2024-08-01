"""`fdt info` cli."""

from __future__ import annotations

import contextlib
import importlib.metadata
import os
import platform
import subprocess
import sys
from pathlib import Path

import click
from packaging.markers import _evaluate_markers, default_environment
from packaging.requirements import Requirement
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table, box
from rich.tree import Tree

from foundry_dev_tools.__about__ import __version__
from foundry_dev_tools.config.config import (
    _pure_config_dict,
    get_config_dict,
    parse_credentials_config,
    parse_general_config,
)
from foundry_dev_tools.config.context import FoundryContext
from foundry_dev_tools.utils.cli import _bool_color, _bool_icon
from foundry_dev_tools.utils.config import cfg_files

HIGHLIGHT_BEGIN = "[bold color(5)]"
HIGHLIGHT_END = "[/bold color(5)]"


def _python_section() -> Tree:
    using_conda = Path(sys.prefix).joinpath("conda-meta").is_dir()
    python_node = Tree("[bold]Python")
    python_node.add(
        Panel(
            # print the python version
            f"[bold]Python {HIGHLIGHT_BEGIN}v{sys.version}, {platform.python_implementation()}{HIGHLIGHT_END}"
            # print the foundry-dev-tools version
            f"\n[bold]Foundry DevTools {HIGHLIGHT_BEGIN}v{__version__}{HIGHLIGHT_END}[/bold]\n" +
            # check if using conda/mamba
            _bool_color(
                using_conda,
                f"[bold]{'' if using_conda else 'not '}using conda {_bool_icon(using_conda)} ",
            )
            +
            # display the prompt modifier if available
            # this should be the environment name, enclosed in parentheses
            (f"{os.getenv('CONDA_PROMPT_MODIFIER') or ''}[/bold]" if using_conda else ""),
            expand=False,
        ),
    )
    return python_node


def _spark_section() -> Tree:
    spark_node = Tree("[bold]Spark")
    try:
        pyspark_dist = importlib.metadata.distribution("pyspark")
    except importlib.metadata.PackageNotFoundError:
        pyspark_dist = None
    spark_node_lines = [
        (
            f"[bold]PySpark {HIGHLIGHT_BEGIN+'v'+pyspark_dist.version+HIGHLIGHT_END if pyspark_dist else 'not'}"
            f" installed {_bool_icon(bool(pyspark_dist))}[/bold]"
        ),
    ]

    spark_version = ""
    with contextlib.suppress(FileNotFoundError):
        spark_submit_version = subprocess.run(
            ["spark-submit", "--version"],
            capture_output=True,
            text=True,
            check=False,
        )
        spark_installed = spark_submit_version.returncode == 0
        if spark_installed and spark_submit_version.stderr:
            spark_ver_lines = [line for line in spark_submit_version.stderr.splitlines() if "version " in line]
            if len(spark_ver_lines) > 0:
                spark_version = spark_ver_lines[0][spark_ver_lines[0].index("version ") + 8 :].split(" ", maxsplit=1)[0]
    spark_node_lines.append(
        f"[bold]Spark {HIGHLIGHT_BEGIN+' v'+spark_version+HIGHLIGHT_END if spark_version else 'not'}"
        f" installed {_bool_icon(bool(spark_version))}",
    )
    if spark_version and pyspark_dist:
        ver_match = spark_version == pyspark_dist.version
        spark_node_lines.append(
            _bool_color(
                ver_match,
                "Spark and PySpark version " + ("match" if ver_match else "don't match"),
            ),
        )
        spark_success = ver_match
    else:
        spark_success = False

    spark_node.label = _bool_color(spark_success, f"{spark_node.label} {_bool_icon(spark_success)}")
    spark_node.add(Panel("\n".join(spark_node_lines), expand=False))
    return spark_node


def _java_section() -> Tree:
    java_node_lines = []
    java_installed = False
    with contextlib.suppress(FileNotFoundError):
        java_props = subprocess.run(
            ["java", "-XshowSettings:properties", "-version"],
            capture_output=True,
            text=True,
            check=False,
        )
        if java_props.returncode == 0:
            java_installed = True
            for line in java_props.stderr.splitlines():
                stripped_line = line.strip()
                if stripped_line.startswith("java.runtime.version"):
                    java_node_lines.append(
                        "[bold]Java Runtime Version: "
                        f"{HIGHLIGHT_BEGIN}{stripped_line.split('=', 1)[1].lstrip(' ')}{HIGHLIGHT_END}[/bold]",
                    )
                elif stripped_line.startswith("java.runtime.name"):
                    java_node_lines.append(
                        "[bold]Java Runtime Name: "
                        f"{HIGHLIGHT_BEGIN}{stripped_line.split('=', 1)[1].lstrip(' ')}{HIGHLIGHT_END}[/bold]",
                    )
    java_node = Tree(f"[bold] Java {_bool_icon(java_installed)}")
    if java_installed:
        java_node.add(Panel("\n".join(java_node_lines), expand=False))
    return java_node


def _sysinfo_section() -> Tree:
    sysinfo_node = Tree("[bold] System Information")
    sysinfo_table = Table(show_header=False, box=box.ROUNDED)
    sysinfo_table.add_row("[bold]OS", HIGHLIGHT_BEGIN + platform.system())
    sysinfo_table.add_row("[bold]OS release", HIGHLIGHT_BEGIN + platform.release())
    sysinfo_table.add_row("[bold]OS version", HIGHLIGHT_BEGIN + platform.version())
    sysinfo_table.add_row("[bold]Instruction set", HIGHLIGHT_BEGIN + platform.machine())
    sysinfo_node.add(sysinfo_table)

    return sysinfo_node


def _dependency_section() -> Tree:  # noqa: C901,PLR0912
    dependency_node = Tree("[bold] Dependencies")
    fdt_package = importlib.metadata.distribution("foundry_dev_tools")
    if fdt_package is None:
        return dependency_node
    # split the Foundry DevTools dependencies into the extras and "core"
    extras: dict[str, list[Requirement]] = {"core": []}
    if fdt_requires := fdt_package.requires:
        for req in fdt_requires:
            r = Requirement(req)

            if r.marker:
                markers = {}
                for m in r.marker._markers:  # noqa: SLF001
                    sm = str(m[0])
                    if sm not in markers:
                        markers[sm] = [m]
                    else:
                        markers[sm] += [m]
                if not markers:
                    extras["core"].append(r)
                    continue

                if "python_version" in markers and not _evaluate_markers(
                    markers["python_version"],
                    default_environment(),
                ):
                    continue

                if "extra" in markers:
                    for extra_marker in markers["extra"]:
                        extra = str(extra_marker[2])
                        if extra not in extras:
                            extras[extra] = []
                        extras[extra].append(r)
                else:
                    extras["core"].append(r)
            else:
                extras["core"].append(r)

        # For each extra display a separate list in the tree.
        for extra in extras:
            extra_installed = True
            extra_node = dependency_node.add(f"[bold]{extra}")
            for extra_req in extras[extra]:
                try:
                    dist = importlib.metadata.distribution(extra_req.name)
                except importlib.metadata.PackageNotFoundError:
                    dist = None
                extra_node.add(
                    f"{extra_req.name} {HIGHLIGHT_BEGIN+'v'+dist.version+HIGHLIGHT_END if dist else 'not'}"
                    f" installed {_bool_icon(bool(dist))}",
                )
                if not dist:
                    extra_installed = False
            extra_node.label = _bool_color(extra_installed, f"{extra_node.label} {_bool_icon(extra_installed)}")

    return dependency_node


def _config_section(profile: str | None) -> Tree:
    if profile is None:
        pure_config = _pure_config_dict()
        if pure_config is not None:
            profile = pure_config.get("profile")
    config_node = Tree("Configuration" + ((f"(Profile '{profile}')") if profile is not None else ""))
    config_table = Table("Config Name", "Value")
    config_file_table = Table("Config File Path", "Exists")

    config_dict = get_config_dict(profile=profile)
    config = parse_general_config(config_dict)
    for config_item, value in config.__dict__.items():
        if isinstance(value, bool):
            config_table.add_row(config_item, _bool_icon(value))
        else:
            config_table.add_row(config_item, str(value))
    config_node.add(config_table)
    try:
        cred_config = parse_credentials_config(config_dict)

        cred_table = Table(
            "Credential Configuration Name",
            "Value",
            title=f"Using the {cred_config.__class__.__name__} implementation for authentication.",
        )
        for cred_config_entry, value in cred_config.__dict__.items():
            # This check needs to happend before the single underscore check
            # Otherwise this check would be unecessary
            if cred_config_entry.startswith("__"):
                continue

            if cred_config_entry.startswith("_"):
                cred_table.add_row(
                    cred_config_entry[1:],
                    _bool_color(
                        value is not None,
                        f"Is {'not ' if value is None else ''}set"
                        f"{', but not shown for security reasons' if value is not None else ''}.",
                    ),
                )
            else:
                cred_table.add_row(
                    cred_config_entry,
                    str(value),
                )

        ctx = FoundryContext(config=config, token_provider=cred_config)
        config_node.add(cred_table)
        username = None
        with contextlib.suppress(Exception):
            username = ctx.multipass.api_me().json()["username"]
        config_node.add(
            _bool_color(
                bool(username),
                (f"Successfully authenticated as '{username}'" if username else "Failed to authenticate"),
            )
        )
    except Exception as e:  # noqa: BLE001
        config_node.add(
            (
                _bool_icon(False) + "Can't create a TokenProvider,"
                " please refer to the documentation how to configure FoundryDevTools for authentication.\n"
                f"Following Error was raised: {e}"
            ),
        )
    for cfg_file in cfg_files():
        config_file_table.add_row(os.fspath(cfg_file), _bool_icon(cfg_file.is_file()))
    config_node.add(config_file_table)
    return config_node


@click.command("info")
@click.option(
    "-p",
    "--profile",
    help=("The config profile to select.\n"),
)
def info_cli(profile: str | None):
    """Prints useful information about the Foundry DevTools installation."""
    # Create the rich console and tree
    # Each section will be a node in the tree
    # at the end the tree will be printed by `console`.
    console = Console(highlight=False, force_terminal=True)
    tree = Tree("[bold]Foundry DevTools Information")

    # Spark sometimes takes very lang to answer
    # This way the CLI doesn't feel 'stuck'
    with Live(tree, refresh_per_second=30, console=console):
        tree.add(_python_section())
        tree.add(_spark_section())
        tree.add(_java_section())
        tree.add(_sysinfo_section())
        tree.add(_dependency_section())
        tree.add(_config_section(profile=profile))
