import os
import subprocess
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from foundry_dev_tools.utils.repo import get_branch, get_repo, git_toplevel_dir
from tests.utils import add_git_submodule

if TYPE_CHECKING:
    import py.path


@pytest.mark.parametrize(
    "use_git", [False]
)  # only False, as we test use_git=True with test_get_repo
def test_git_toplevel_dir(use_git: bool, tmpdir: "py.path.LocalPath", git_env: dict):
    toplevel = Path(tmpdir)
    subprocess.check_call(["git", "init"], cwd=toplevel)
    git_dir = git_toplevel_dir(Path(tmpdir.mkdir("subdireectory")), use_git=use_git)
    assert git_dir == toplevel
    with tmpdir.mkdir("second_subdirectory").as_cwd():
        git_dir_cwd = git_toplevel_dir(use_git=use_git)
    assert git_dir_cwd == toplevel

    # Test if git submodules can be recognized as toplevel_dir
    submodule_name = "dummy_submodule"
    add_git_submodule(git_dir=toplevel, submodule_name=submodule_name, git_env=git_env)
    toplevel_submodule = toplevel.joinpath(submodule_name)
    git_dir_submodule = git_toplevel_dir(
        Path(tmpdir.mkdir(submodule_name, "submodule_subdirectory")), use_git=use_git
    )
    assert git_dir_submodule == toplevel_submodule


def test_get_branch(tmpdir: "py.path.LocalPath", git_env: dict):
    toplevel = Path(tmpdir)

    # Test without git submodule
    parent_repo_name = "parent_repo"
    target_branch_name_parent = "parent_repo_main"
    subprocess.check_call(
        [
            "git",
            "init",
            parent_repo_name,
            f"--initial-branch={target_branch_name_parent}",
        ],
        cwd=toplevel,
        env=git_env,
    )
    branch_name = get_branch(toplevel.joinpath(parent_repo_name))
    assert branch_name == target_branch_name_parent

    # Test for git submodule
    # Add an empty submodule
    submodule_name = "dummy_submodule"
    add_git_submodule(
        git_dir=toplevel.joinpath(parent_repo_name),
        submodule_name=submodule_name,
        git_env=git_env,
    )
    # Checkout and test new branch from submodule
    target_branch_name_sub = "submodule_repo_dev"
    git_dir_submodule = toplevel.joinpath(parent_repo_name, submodule_name)
    subprocess.check_call(
        [
            "git",
            "checkout",
            "-b",
            target_branch_name_sub,
        ],
        cwd=git_dir_submodule,
        env=git_env,
    )
    assert os.path.isfile(git_dir_submodule.joinpath(".git"))

    branch_name = get_branch(git_dir_submodule)
    assert branch_name == target_branch_name_sub


def test_get_repo(tmpdir: "py.path.LocalPath", git_env: dict):
    repo_rid = f"ri.stemma.main.repository{uuid.uuid4()}"
    git_dir = Path(tmpdir)
    subprocess.check_call(["git", "init"], cwd=git_dir, env=git_env)
    with git_dir.joinpath("gradle.properties").open("w+") as gradle_prop:
        gradle_prop.write(f"transformsRepoRid = {repo_rid}")

    subprocess.check_call(["git", "add", "gradle.properties"], cwd=git_dir, env=git_env)
    subprocess.check_call(
        ["git", "commit", "-m", "initial commit"], cwd=git_dir, env=git_env
    )
    assert get_repo(git_dir)[0] == repo_rid
    with tmpdir.as_cwd():
        get_repo_out = get_repo()
        assert get_repo_out[0] == repo_rid
        assert get_repo_out[3] == git_dir
