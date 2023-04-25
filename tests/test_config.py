import os
import pathlib
import random
import tempfile
from unittest import mock

import foundry_dev_tools.config
from foundry_dev_tools.config import Config
from tests.conftest import PatchConfig

# fake home for full control over all config files and variables
# without modifying or deleting the users config files
FAKE_HOME = pathlib.Path(tempfile.mkdtemp())
RANDOM_NUMBER1 = random.randint(1, 12345)
RANDOM_NUMBER2 = random.randint(1, 12345)
RANDOM_NUMBER3 = random.randint(1, 12345)


@mock.patch("pathlib.Path.home", return_value=FAKE_HOME)
def test_config_precedence(tmp):
    MAGIC = 999
    with PatchConfig(
        initial_config_overwrite={
            "jwt": "123",
            "foundry_url": "precedence-test-url",
            "transforms_sql_dataset_size_threshold": str(MAGIC),
        }
    ):
        # test initial config overwrite
        assert (
            foundry_dev_tools.config.INITIAL_CONFIG["jwt"]
            == foundry_dev_tools.config.Configuration["jwt"]
            == "123"
        )

        # test type conversion
        assert (
            foundry_dev_tools.config.INITIAL_CONFIG[
                "transforms_sql_dataset_size_threshold"
            ]
            == foundry_dev_tools.config.Configuration[
                "transforms_sql_dataset_size_threshold"
            ]
            == MAGIC
        )

        # test dynamic overwrite config, not overwriting static configs
        config_overwrite = Config({"jwt": "456"})
        assert config_overwrite["jwt"] == "456"

        # initial configs and static configs stay untouched
        assert (
            foundry_dev_tools.config.INITIAL_CONFIG["jwt"]
            == foundry_dev_tools.config.Configuration["jwt"]
            == "123"
        )

        # change static config directly
        foundry_dev_tools.config.Configuration["jwt"] = "789"
        # initial config and the transforms config stay untouched
        assert foundry_dev_tools.config.INITIAL_CONFIG["jwt"] == "123"

        # the dynamic config overwrites with its own supplied value
        assert config_overwrite["jwt"] == "456"


@mock.patch("pathlib.Path.home", return_value=FAKE_HOME)
@mock.patch.dict(
    os.environ,
    {
        "FOUNDRY_DEV_TOOLS_TRANSFORMS_SQL_SAMPLE_ROW_LIMIT": f"{RANDOM_NUMBER1}",
    },
)
def test_env_variable_takes_precedence(tmp):
    with PatchConfig(
        config_overwrite={
            "jwt": "whatever",
            "foundry_url": "foundry_config_env_takes_precedence",
            "transforms_sql_sample_row_limit": RANDOM_NUMBER2,
        },
        read_initial=True,
    ):
        assert (
            foundry_dev_tools.config.INITIAL_CONFIG["transforms_sql_sample_row_limit"]
            == RANDOM_NUMBER1
        )

        # overwrite config supplied takes precedence over env variable
        assert (
            Config({"transforms_sql_sample_row_limit": RANDOM_NUMBER3})[
                "transforms_sql_sample_row_limit"
            ]
            == RANDOM_NUMBER3
        )


@mock.patch("pathlib.Path.home", return_value=FAKE_HOME)
@mock.patch.dict(
    os.environ, {"FOUNDRY_DEV_TOOLS_JWT": "env_jwt_takes_precedence_over_config_files"}
)
def test_env_varibale_takes_precedence_over_config_files(tmp):
    FDT_DIR = pathlib.Path.home().joinpath(".foundry-dev-tools")
    FDT_DIR.mkdir(parents=True)
    with FDT_DIR.joinpath("config").open(mode="w+") as fdt_conf_file:
        fdt_conf_file.write(
            "[default]\njwt=123456789\nfoundry_url=https://env_take_prec.lan/"
        )
    with PatchConfig(read_initial=True):
        assert (
            foundry_dev_tools.config.Configuration["jwt"]
            == "env_jwt_takes_precedence_over_config_files"
        )
        assert (
            foundry_dev_tools.config.INITIAL_CONFIG["jwt"]
            == "env_jwt_takes_precedence_over_config_files"
        )
