import subprocess
import sys
from pathlib import Path


def remove_transforms_modules():
    # remove transform modules from sys.modules so mocking works
    transform_modules = [module for module in sys.modules if module.startswith("transforms.") or module == "transforms"]
    for tmod in transform_modules:
        del sys.modules[tmod]


def add_git_submodule(git_dir: Path, submodule_name: str, git_env: dict) -> None:
    # First create an empty local repository and initialize it
    subprocess.check_call(["git", "init", "dummy_repo"], cwd=git_dir, env=git_env)
    subprocess.check_call(
        [
            "git",
            "commit",
            "--allow-empty",
            "-m",
            "Initialize",
        ],
        cwd=git_dir.joinpath("dummy_repo"),
        env=git_env,
    )

    # Add local repository as submodule to toplevel repo
    subprocess.check_call(
        [
            "git",
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "./dummy_repo",
            submodule_name,
        ],
        cwd=git_dir,
        env=git_env,
    )
