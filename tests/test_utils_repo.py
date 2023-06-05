import subprocess
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from foundry_dev_tools.utils.repo import get_repo, git_toplevel_dir

if TYPE_CHECKING:
    import py.path


@pytest.mark.parametrize(
    "use_git", [False]
)  # only False, as we test use_git=True with test_get_repo
def test_git_toplevel_dir(use_git: bool, tmpdir: "py.path.LocalPath"):
    toplevel = Path(tmpdir)
    subprocess.check_call(["git", "init"], cwd=toplevel)
    git_dir = git_toplevel_dir(Path(tmpdir.mkdir("subdireectory")), use_git=use_git)
    assert git_dir == toplevel
    with tmpdir.mkdir("second_subdirectory").as_cwd():
        git_dir_cwd = git_toplevel_dir(use_git=use_git)
    assert git_dir_cwd == toplevel


def test_get_repo(tmpdir: "py.path.LocalPath"):
    repo_rid = f"ri.stemma.main.repository{uuid.uuid4()}"
    git_dir = Path(tmpdir)
    subprocess.check_call(["git", "init"], cwd=git_dir)
    with git_dir.joinpath("gradle.properties").open("w+") as gradle_prop:
        gradle_prop.write(f"transformsRepoRid = {repo_rid}")
    GIT_ENV = {
        "HOME": str(git_dir),
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_COMMITTER_NAME": "pytest get_repo test",
        "GIT_COMMITTER_EMAIL": "pytest@get_repo.py",
        "GIT_AUTHOR_NAME": "pytest get_repo test",
        "GIT_AUTHOR_EMAIL": "pytest@get_repo.py",
    }  # should use default configs
    subprocess.check_call(["git", "add", "gradle.properties"], cwd=git_dir, env=GIT_ENV)
    subprocess.check_call(
        ["git", "commit", "-m", "initial commit"], cwd=git_dir, env=GIT_ENV
    )
    assert get_repo(git_dir)[0] == repo_rid
    with tmpdir.as_cwd():
        get_repo_out = get_repo()
        assert get_repo_out[0] == repo_rid
        assert get_repo_out[3] == git_dir
