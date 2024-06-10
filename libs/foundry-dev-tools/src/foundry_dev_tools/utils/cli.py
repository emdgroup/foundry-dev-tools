"""Utils functions for the CLI."""

from __future__ import annotations


def _bool_color(a: bool, s: str) -> str:
    c = "green" if a else "red"
    return f"[bold {c}]{s}[/bold {c}]"


def _bool_icon(a: bool) -> str:
    return _bool_color(a, "✔") if a else _bool_color(a, "x")
