import os
import shutil
import subprocess
from pathlib import Path
from unittest import mock

import py.path
import pytest

from foundry_dev_tools.config.config import get_config_dict
from foundry_dev_tools.errors.config import FoundryConfigError
from foundry_dev_tools.utils.config import PROJECT_CFG_FILE_NAME, check_init, find_project_config_file


def test_check_init():
    class X:
        def __init__(self, a: int, b: bool, c: str = "", *args, **kwargs):
            pass

    check_init(X, "mock", {"a": 1, "b": True, "c": "test"})
    with pytest.warns(
        UserWarning,
        match=f"mock.a was type {str!s} but has been cast to {int!s}, this was needed to instantiate {X!s}.",
    ):
        check_init(X, "mock", {"a": "1", "b": True})
    with pytest.warns(UserWarning, match=f"mock.d is not a valid config option for {X!s}"):
        check_init(X, "mock", {"a": 1, "b": False, "d": 1})
    with pytest.raises(FoundryConfigError, match=f"mock.b is missing to create {X!s}"):
        check_init(X, "mock", {"a": 1})

    with pytest.raises(
        FoundryConfigError,
        match=f"To initialize {X!s}, the config option mock.a needs to be of type {int!s}, but it is type {dict!s}",
    ):
        check_init(X, "mock", {"a": {}, "b": True})


# using fakefs, as we only care about the env variables and not config files
def test_get_environment_variable_config(fs):
    # Set up mock environment variables
    env = {
        "FDT_CREDENTIALS__DOMAIN": "domain",
        "FDT_CREDENTIALS__JWT": "jwt_value",
        "FDT_TEST": "invalid",
        "OTHER_VARIABLE": "othervalue",
    }
    v1_env = {
        "FOUNDRY_DEV_TOOLS_JWT": "jwt_value_v1",
        "FOUNDRY_DEV_TOOLS_FOUNDRY_URL": "https://domain_v1",
    }
    v1_oauth_env = {
        "FOUNDRY_DEV_TOOLS_CLIENT_ID": "client_id",
        "FOUNDRY_DEV_TOOLS_FOUNDRY_URL": "https://domain_v1",
    }

    # parses v1 env
    with mock.patch.dict(os.environ, v1_env):
        result = get_config_dict()
    assert result == {"credentials": {"domain": "domain_v1", "jwt": "jwt_value_v1"}}

    with (
        pytest.warns(
            UserWarning,
            match="FDT_TEST is not a valid Foundry DevTools configuration environment variable.",
        ),
        mock.patch.dict(os.environ, env),
    ):
        result = get_config_dict()
    assert result == {"credentials": {"domain": "domain", "jwt": "jwt_value"}}

    with mock.patch.dict(os.environ, v1_oauth_env):
        result = get_config_dict()
    assert result == {"credentials": {"domain": "domain_v1", "oauth": {"client_id": "client_id"}}}

    # v1 and v2 together
    env.update(v1_env)

    with (
        pytest.warns(
            UserWarning,
            match="FDT_TEST is not a valid Foundry DevTools configuration environment variable.",
        ),
        mock.patch.dict(os.environ, env),
    ):
        result = get_config_dict()
    # v2 takes precedence
    assert result == {"credentials": {"domain": "domain", "jwt": "jwt_value"}}


def test_find_project_config_file(tmpdir: py.path.LocalPath, git_env: dict, monkeypatch):
    toplevel = Path(tmpdir)
    monkeypatch.chdir(toplevel)
    toplevel_conf_path = toplevel.joinpath(PROJECT_CFG_FILE_NAME)
    subprocess.check_call(["git", "init"], cwd=toplevel)
    assert toplevel_conf_path == find_project_config_file(check_caller_file=False)
    sub = toplevel.joinpath("sub")
    sub.mkdir()
    monkeypatch.chdir(sub)
    assert toplevel_conf_path == find_project_config_file(check_caller_file=False)

    # remove git directory
    shutil.rmtree(toplevel.joinpath(".git"))

    assert (
        find_project_config_file(check_caller_file=False) is None
    )  # no project config file present, don't return anything
    cwd_conf_path = sub.joinpath(PROJECT_CFG_FILE_NAME)
    cwd_conf_path.touch()

    assert cwd_conf_path == find_project_config_file(check_caller_file=False)
