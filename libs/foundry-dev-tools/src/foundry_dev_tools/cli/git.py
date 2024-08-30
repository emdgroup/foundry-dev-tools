"""Foundry DevTools git cli, clone foundry repositories with your Foundry DevTools credentials."""

import os
import re
import subprocess
from urllib.parse import quote, urlparse

import click
from rich.console import Console

from foundry_dev_tools.config.context import FoundryContext


@click.group("git")
def git_cli():
    """Foundry DevTools git cli."""


def _is_repo_id(maybe_repo_id: str) -> bool:
    if not maybe_repo_id.startswith("ri.stemma.main.repository."):
        return False
    uuid_pattern = r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
    match = re.match(uuid_pattern, maybe_repo_id[26:])
    return bool(match)


def _parse_repo(console: Console, ctx: FoundryContext, repo: str) -> str | None:
    if repo.startswith(("https://", "http://", "git://", "git+https://", "git+http://")):
        parsed = urlparse(repo)
        netloc = parsed.netloc
        # python uses the obsolete 'netloc' instead of authority defined in rfc3986 and does not split it up
        # remove user information
        netloc = netloc.split("@", maxsplit=1)[-1]
        # remove port
        netloc = netloc.split(":", maxsplit=1)[0]
        if netloc != ctx.host.domain:
            console.print(
                "The domain of the repo does not match the domain in your Foundry DevTools configuration "
                f"({ctx.host.domain})."
            )
            return None
        for part in parsed.path.split("/"):
            if _is_repo_id(part):
                return part
        console.print("Could not find a valid repository ID in the repository URL")
        return None

    if _is_repo_id(repo):
        return repo
    console.print("The repo argument should be either the repository ID or an  URL which contains the repository ID")
    return None


def _git_clone_cli(ctx: FoundryContext, console: Console, repo: str, path: str | None):
    try:
        subprocess.check_output(["git", "config", "--get", "credential." + ctx.host.url + ".helper"])
    except subprocess.CalledProcessError:
        console.print("git-credential-foundry not set up, please run the git-credential-foundry command to setup")
        return
    except FileNotFoundError:
        console.print("git not found, please install git")
        return

    repo_id = _parse_repo(console, ctx, repo)
    if repo_id is None:
        return
    repo_object = ctx.get_resource(repo_id)
    clone_url = f"{ctx.host.scheme}://{ctx.host.domain}" + quote(f"/stemma/git/{repo_id}/{repo_object.name}")

    _path = repo_object.name if path is None else path
    console.print(f"Executing git clone {clone_url} {path}")
    if _path:
        os.execvp("git", ["git", "clone", clone_url, _path])  # noqa: S606
    else:
        os.execvp("git", ["git", "clone", clone_url])  # noqa: S606


@git_cli.command("clone")
@click.argument("repo")
@click.argument("path", required=False, type=click.Path(resolve_path=True))
@click.option("--profile", "-p", required=False, help="The Foundry DevTools configuration profile to use")
def git_clone_cli(repo: str, profile: str | None, path: str | None):
    """Clone a the foundry repository REPO.

    This is a helper to clone a foundry repository without storing a foundry jwt in your local clone,
    which is normally the case when you use the clone URL from foundry, as it contains a jwt inside the URL.
    You need to set up the `git-credential-foundry` helper first to use this, as it will authenticate you
    on the fly when executing a `git clone`.

    REPO accepts a repository RID, a url which contains the repository id or the clone URL provided by foundry. This means you could just copy the URL from the address bar while viewing a repository on Foundry without using the dedicated clone button.


    PATH is optionally a path where the repository should be cloned to, it will be appended to `git clone`


    """  # noqa: E501
    # check if git credential foundry is setup with git config --get credential.$protocol$domain
    ctx = FoundryContext(profile=profile)
    console = Console(markup=True)
    _git_clone_cli(ctx, console, repo, path)
