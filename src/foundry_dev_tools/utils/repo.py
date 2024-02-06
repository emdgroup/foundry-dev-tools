"""This file provides helper function for git repos."""
from __future__ import annotations

import logging
import subprocess
from pathlib import Path

from foundry_dev_tools.errors.meta import FoundryDevToolsError

LOGGER = logging.getLogger(__name__)


def get_repo(repo_dir: Path | None = None) -> tuple[str, str, str, Path]:
    """Get the repository RID of the current working directory.

    Args:
        repo_dir: the path to a (sub)directory of a git repo,
            otherwise current working directory
    Returns:
        tuple[str,str,str]: repo_rid,git_ref,git_revision_hash
    """
    if git_dir := git_toplevel_dir(repo_dir, use_git=True):
        gradle_props = git_dir.joinpath("gradle.properties")
        if gradle_props.is_file():
            try:
                with gradle_props.open("r") as gpf:
                    for line in gpf.readlines():
                        if line.startswith("transformsRepoRid"):
                            return (
                                line.split("=")[1].strip(),
                                get_git_ref(git_dir),
                                get_git_revision_hash(git_dir),
                                git_dir,
                            )
            except Exception as e:  # noqa: BLE001
                msg = "Can't get repository RID from the gradle.properties file. Malformed file?"
                raise FoundryDevToolsError(msg) from e
            msg = "Can't get repository RID from the gradle.properties file. Is this really a foundry repository?"
            raise FoundryDevToolsError(
                msg,
            )
        msg = "There is no gradle.properties file at the top of the git repository, can't get repository RID."
        raise FoundryDevToolsError(
            msg,
        )
    msg = (
        "If you don't provide a repository RID you need to be in a repository directory to detect what you want to"
        " build."
    )
    raise FoundryDevToolsError(
        msg,
    )


def get_git_ref(git_dir: Path | None = None) -> str:
    """Get the branch ref in the supplied git directory.

    Args:
        git_dir: the path to a (sub)directory of a git repo,
            otherwise current working directory
    """
    return (
        subprocess.check_output(
            [
                "git",
                "symbolic-ref",
                "HEAD",
            ],
            cwd=git_dir,
        )
        .decode("ascii")
        .strip()
    )


def get_git_revision_hash(git_dir: Path | None = None) -> str:
    """Get the git revision hash.

    Args:
        git_dir: the path to a (sub)directory of a git repo,
            otherwise current working directory
    """
    return (
        subprocess.check_output(
            [
                "git",
                "rev-parse",
                "HEAD",
            ],
            cwd=git_dir,
        )
        .decode("ascii")
        .strip()
    )


def git_toplevel_dir(git_dir: Path | None = None, use_git: bool = False) -> Path | None:
    """Get git top level directory.

    Args:
        git_dir: the path to a (sub)directory of a git repo,
            otherwise current working directory
        use_git: if true call git executable with subprocess,
            otherwise use minimal python only implementation

    Returns:
        Path | None: the path to the toplevel git directory or
            None if nothing was found.

    """
    if use_git:
        try:
            return Path(
                subprocess.check_output(["git", "rev-parse", "--show-toplevel"], cwd=git_dir).decode("utf-8").strip(),
            )
        except subprocess.CalledProcessError:
            pass
    if git_dir is None:
        git_dir = Path.cwd()
    if git_dir.joinpath(".git").is_dir():
        return git_dir
    for p in git_dir.resolve().parents:
        if p.joinpath(".git").is_dir():
            return p
    return None
