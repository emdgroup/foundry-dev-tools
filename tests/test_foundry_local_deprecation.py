import os
import pathlib
import shutil
import tempfile
from unittest import mock

import pytest

import foundry_dev_tools
import foundry_dev_tools.config
from foundry_dev_tools.config import execute_as_subprocess

from tests.conftest import PatchConfig

# fake home for full control over all config files and variables
# without modifying or deleting the users config files
FAKE_HOME = pathlib.Path(tempfile.mkdtemp())


@pytest.mark.no_patch_conf
@mock.patch("pathlib.Path.home", return_value=FAKE_HOME)
@mock.patch.dict(
    os.environ,
    {
        "FOUNDRY_LOCAL_TRANSFORMS_SQL_SAMPLE_ROW_LIMIT": "8888",
        "FOUNDRY_DEV_TOOLS_TRANSFORMS_SQL_SAMPLE_ROW_LIMIT": "9999",
    },
)
def test_new_env_variable_takes_precedence(tmp):
    with PatchConfig(
        initial_config_overwrite={
            "jwt": "whatever",
            "foundry_url": "foundry_local_deprecation",
            "disable_token_providers": True,
        },
        read_initial=True,
    ):
        assert (
            foundry_dev_tools.Configuration["transforms_sql_sample_row_limit"] == 9999
        )


@pytest.mark.no_patch_conf
@mock.patch("pathlib.Path.home", return_value=FAKE_HOME)
@mock.patch.dict(os.environ, {"FOUNDRY_LOCAL_TRANSFORMS_SQL_SAMPLE_ROW_LIMIT": "123"})
def test_old_env_variable_fallback(tmp):
    with pytest.deprecated_call():
        with PatchConfig(
            initial_config_overwrite={
                "jwt": "whatever",
                "foundry_url": "foundry_local_deprecation",
                "disable_token_providers": True,
            },
            read_initial=True,
        ):
            assert (
                foundry_dev_tools.Configuration["transforms_sql_sample_row_limit"]
                == 123
            )


@pytest.mark.no_patch_conf
@mock.patch("pathlib.Path.home", return_value=FAKE_HOME)
def test_old_config_directory_or_file_warning(tmp):
    FL_DIR = pathlib.Path.home() / ".foundry-local"
    os.mkdir(FL_DIR)
    with open(FL_DIR / "config", "w+") as fl_config:
        fl_config.write(
            "[default]\nfoundry_url=foundry_local_deprecation\njwt=old_config_directory\n"
        )

    with pytest.deprecated_call():
        with PatchConfig(
            initial_config_overwrite={"disable_token_providers": True},
            read_initial=True,
        ):
            assert foundry_dev_tools.Configuration["jwt"] == "old_config_directory"

    FDT_DIR = pathlib.Path.home() / ".foundry-dev-tools"
    os.mkdir(FDT_DIR)
    with open(FDT_DIR / "config", "w+") as fl_config:
        fl_config.write(
            "[default]\nfoundry_url=foundry_local_deprecation\njwt=new_config_directory\n"
        )

    with PatchConfig(
        initial_config_overwrite={"disable_token_providers": True}, read_initial=True
    ):
        assert foundry_dev_tools.Configuration["jwt"] == "new_config_directory"

    FAKE_GIT_DIR = pathlib.Path.home() / "supercool-transform-git-repo"
    os.mkdir(FAKE_GIT_DIR)
    with open(FAKE_GIT_DIR / ".foundry_local_config", "w+") as old_git_cfg:
        old_git_cfg.write("[default]\njwt=old_git_config_file\n")
    with mock.patch(
        "foundry_dev_tools.config._traverse_to_git_project_top_level_dir",
        return_value=str(FAKE_GIT_DIR),
    ):
        with pytest.deprecated_call():
            with PatchConfig(
                initial_config_overwrite={"disable_token_providers": True},
                read_initial=True,
            ):
                assert foundry_dev_tools.Configuration["jwt"] == "old_git_config_file"

        with open(FAKE_GIT_DIR / ".foundry_dev_tools", "w+") as old_git_cfg:
            old_git_cfg.write("[default]\njwt=new_git_config_file\n")

        with PatchConfig(
            initial_config_overwrite={"disable_token_providers": True},
            read_initial=True,
        ):
            assert foundry_dev_tools.Configuration["jwt"] == "new_git_config_file"
            # TODO: This breaks the global config that is present in integration tests and overwrites the real
            # jwt, after this test, all other tests fail.

    shutil.rmtree(FAKE_HOME)


@pytest.mark.no_patch_conf
@mock.patch("pathlib.Path.home", return_value=FAKE_HOME)
@mock.patch.dict(os.environ, {})
def test_old_set_method(tmp):
    with PatchConfig(
        initial_config_overwrite={
            "jwt": "whatever",
            "foundry_url": "https://test.com",
            "disable_token_providers": True,
        },
        read_initial=True,
    ):
        with pytest.warns(expected_warning=DeprecationWarning) as record1:
            foundry_dev_tools.Configuration.set("test_key", "value")
        assert "deprecated" in record1.pop(DeprecationWarning).message.args[0]

        assert foundry_dev_tools.Configuration.get("test_key") == "value"
        config = foundry_dev_tools.Configuration.get_config()
        assert "jwt" in config and config["jwt"] == "whatever"
        assert "foundry_url" in config and config["foundry_url"] == "https://test.com"
        assert (
            "disable_token_providers" in config
            and config["disable_token_providers"] is True
        )
        assert "test_key" in config and config["test_key"] == "value"

        with pytest.warns(expected_warning=DeprecationWarning) as record:
            foundry_dev_tools.Configuration.delete("test_key")

        assert "deprecated" in record.pop(DeprecationWarning).message.args[0]
        assert "Test" not in foundry_dev_tools.Configuration
