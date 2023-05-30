import os
import pathlib
import shutil
import tempfile
from unittest import mock

import pytest

import foundry_dev_tools
import foundry_dev_tools.config
from tests.conftest import PatchConfig

# fake home for full control over all config files and variables
# without modifying or deleting the users config files
FAKE_HOME = pathlib.Path(tempfile.mkdtemp())


@pytest.mark.no_patch_conf()
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
            "enable_runtime_token_providers": False,
        },
        read_initial=True,
    ):
        assert (
            foundry_dev_tools.config.Configuration["transforms_sql_sample_row_limit"]
            == 9999
        )


@pytest.mark.no_patch_conf()
@mock.patch("pathlib.Path.home", return_value=FAKE_HOME)
@mock.patch.dict(os.environ, {"FOUNDRY_LOCAL_TRANSFORMS_SQL_SAMPLE_ROW_LIMIT": "123"})
def test_old_env_variable_fallback(tmp):
    with pytest.deprecated_call(), PatchConfig(
        initial_config_overwrite={
            "jwt": "whatever",
            "foundry_url": "foundry_local_deprecation",
            "enable_runtime_token_providers": False,
        },
        read_initial=True,
    ):
        assert (
            foundry_dev_tools.config.Configuration["transforms_sql_sample_row_limit"]
            == 123
        )


@pytest.mark.no_patch_conf()
@mock.patch("pathlib.Path.home", return_value=FAKE_HOME)
def test_old_config_directory_or_file_warning(tmp):
    FL_DIR = pathlib.Path.home() / ".foundry-local"
    FL_DIR.mkdir()
    with FL_DIR.joinpath("config").open(mode="w+") as fl_config:
        fl_config.write(
            "[default]\nfoundry_url=foundry_local_deprecation\njwt=old_config_directory\n"
        )

    with pytest.deprecated_call(), PatchConfig(
        initial_config_overwrite={"enable_runtime_token_providers": False},
        read_initial=True,
    ):
        assert foundry_dev_tools.config.Configuration["jwt"] == "old_config_directory"

    FDT_DIR = pathlib.Path.home() / ".foundry-dev-tools"
    FDT_DIR.mkdir()
    with FDT_DIR.joinpath("config").open(mode="w+") as fl_config:
        fl_config.write(
            "[default]\nfoundry_url=foundry_local_deprecation\njwt=new_config_directory\n"
        )

    with PatchConfig(
        initial_config_overwrite={"enable_runtime_token_providers": False},
        read_initial=True,
    ):
        assert foundry_dev_tools.config.Configuration["jwt"] == "new_config_directory"

    FAKE_GIT_DIR = pathlib.Path.home() / "supercool-transform-git-repo"
    FAKE_GIT_DIR.mkdir()
    with FAKE_GIT_DIR.joinpath(".foundry_local_config").open(mode="w+") as old_git_cfg:
        old_git_cfg.write("[default]\njwt=old_git_config_file\n")
    with mock.patch(
        "foundry_dev_tools.utils.repo.git_toplevel_dir",
        return_value=FAKE_GIT_DIR,
    ):
        with pytest.deprecated_call(), PatchConfig(
            initial_config_overwrite={"enable_runtime_token_providers": False},
            read_initial=True,
        ):
            assert (
                foundry_dev_tools.config.Configuration["jwt"] == "old_git_config_file"
            )

        with FAKE_GIT_DIR.joinpath(".foundry_dev_tools").open(mode="w+") as old_git_cfg:
            old_git_cfg.write("[default]\njwt=new_git_config_file\n")

        with PatchConfig(
            initial_config_overwrite={"enable_runtime_token_providers": False},
            read_initial=True,
        ):
            assert (
                foundry_dev_tools.config.Configuration["jwt"] == "new_git_config_file"
            )
    shutil.rmtree(FAKE_HOME)


@pytest.mark.no_patch_conf()
@mock.patch("pathlib.Path.home", return_value=FAKE_HOME)
@mock.patch.dict(os.environ, {})
def test_old_set_method(tmp):
    with PatchConfig(
        initial_config_overwrite={
            "jwt": "whatever",
            "foundry_url": "https://test.com",
            "enable_runtime_token_providers": False,
        },
        read_initial=True,
    ):
        with pytest.warns(expected_warning=DeprecationWarning) as record1:
            foundry_dev_tools.config.Configuration.set("test_key", "value")
        assert "deprecated" in record1.pop(DeprecationWarning).message.args[0]

        assert foundry_dev_tools.config.Configuration.get("test_key") == "value"
        config = foundry_dev_tools.config.Configuration.get_config()
        assert "jwt" in config
        assert config["jwt"] == "whatever"
        assert "foundry_url" in config
        assert config["foundry_url"] == "https://test.com"
        assert "enable_runtime_token_providers" in config
        assert not config["enable_runtime_token_providers"]
        assert "test_key" in config
        assert config["test_key"] == "value"

        with pytest.warns(expected_warning=DeprecationWarning) as record:
            foundry_dev_tools.config.Configuration.delete("test_key")

        assert "deprecated" in record.pop(DeprecationWarning).message.args[0]
        assert "Test" not in foundry_dev_tools.config.Configuration
