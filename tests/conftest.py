# -*- coding: utf-8 -*-
"""
    Dummy conftest.py for Foundry DevTools.

    If you don't know what this is for, just leave it empty.
    Read more about conftest.py under:
    https://pytest.org/latest/plugins.html
"""
import copy
import os
import pathlib
from random import choice
from string import ascii_uppercase

import fs
import pytest

import foundry_dev_tools
from foundry_dev_tools.foundry_api_client import DatasetNotFoundError, FoundryRestClient

from tests.test_foundry_mock import MockFoundryRestClient
from tests.utils import (
    FOUNDRY_SCHEMA_COMPLEX_DATASET,
    generate_test_dataset,
    generic_upload_dataset_if_not_exists,
    INTEGRATION_TEST_COMPASS_ROOT_PATH,
    IRIS_SCHEMA,
    TEST_FOLDER,
)


def pytest_addoption(parser):
    parser.addoption(
        "--integration",
        action="store_true",
        help="run the tests only in case of that command line (marked with marker @integration)",
    )
    parser.addoption(
        "--performance",
        action="store_true",
        help="run the tests only in case of that command line (marked with marker @performance)",
    )


def pytest_runtest_setup(item):
    if "integration" in item.keywords and not item.config.getoption("--integration"):
        pytest.skip("need --integration option to run this test")
    if "performance" in item.keywords and not item.config.getoption("--performance"):
        pytest.skip("need --performance option to run this test")


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: mark test to run only integration tests"
    )
    config.addinivalue_line(
        "markers", "performance: mark test to run only performance tests"
    )


def pytest_generate_tests(metafunc):
    if "is_integration_test" in metafunc.fixturenames:
        if metafunc.config.getoption("--integration"):
            is_integration_test = True
        else:
            is_integration_test = False
        metafunc.parametrize("is_integration_test", [is_integration_test])


class PatchConfig:
    def __init__(
        self,
        config_overwrite: dict = None,
        initial_config_overwrite: dict = None,
        read_initial: bool = False,
    ):
        self.initial_config_overwrite = initial_config_overwrite
        self.config_overwrite = config_overwrite
        self.read_initial = read_initial

    def __enter__(self):
        self.conf_save = override_config(
            initial_config_overwrite=self.initial_config_overwrite,
            config_overwrite=self.config_overwrite,
            read_initial=self.read_initial,
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        (
            foundry_dev_tools.INITIAL_CONFIG,
            foundry_dev_tools.FOUNDRY_DEV_TOOLS_DIRECTORY,
        ) = self.conf_save[0]
        foundry_dev_tools.Configuration = self.conf_save[1]


def override_config(
    initial_config_overwrite: dict = None,
    config_overwrite: dict = None,
    read_initial=False,
) -> (
    (dict, dict, pathlib.Path),
    foundry_dev_tools.config.Config,
    foundry_dev_tools.config.Config,
):
    save = (
        copy.deepcopy(foundry_dev_tools.INITIAL_CONFIG),
        copy.deepcopy(foundry_dev_tools.FOUNDRY_DEV_TOOLS_DIRECTORY),
    )
    if read_initial:
        (
            foundry_dev_tools.INITIAL_CONFIG,
            foundry_dev_tools.FOUNDRY_DEV_TOOLS_DIRECTORY,
        ) = foundry_dev_tools.config.initial_config()

    if initial_config_overwrite:
        foundry_dev_tools.INITIAL_CONFIG.update(
            foundry_dev_tools.config.type_convert(initial_config_overwrite)
        )

    config_save = copy.deepcopy(foundry_dev_tools.Configuration)
    if config_overwrite:
        foundry_dev_tools.Configuration = foundry_dev_tools.config.Config(
            config_overwrite
        )

    return save, config_save


@pytest.fixture(autouse=True)
def config_for_unit_tests(request, is_integration_test, tmp_path_factory):
    if "no_patch_conf" in request.keywords:
        yield
    else:
        rnd = "".join(choice(ascii_uppercase) for _ in range(5))
        temp_directory = tmp_path_factory.mktemp(f"foundry_dev_tools_test_{rnd}")

        if not is_integration_test:
            # Mandatory config keys
            with PatchConfig(
                initial_config_overwrite={
                    "cache_dir": str(temp_directory),
                    "jwt": "123",
                    "foundry_url": "https://stack.palantirfoundry.com",
                }
            ):
                yield
        else:
            with PatchConfig(
                initial_config_overwrite={"cache_dir": str(temp_directory)}
            ):
                yield


@pytest.fixture()
def client(is_integration_test):
    if is_integration_test:
        yield FoundryRestClient()
    else:
        test_folder = pathlib.Path(__file__).parent.resolve()
        root = f"{test_folder}/foundry_mock_root"
        os.makedirs(root, exist_ok=True)
        yield MockFoundryRestClient(fs=fs.open_fs(root))


@pytest.fixture()
def iris_dataset(client):
    iris_dataset = generic_upload_dataset_if_not_exists(
        client=client,
        name="iris_new",
        upload_folder=str(TEST_FOLDER / "test_data" / "iris"),
        foundry_schema=IRIS_SCHEMA,
    )
    yield iris_dataset


@pytest.fixture()
def iris_no_schema_dataset(client, is_integration_test):
    if is_integration_test:
        iris_dataset = generic_upload_dataset_if_not_exists(
            client=client,
            name="iris_new_no_schema_v1",
            upload_folder=str(TEST_FOLDER / "test_data" / "iris"),
            foundry_schema=None,
        )
        yield iris_dataset
    else:
        yield "iris-rid", "iris-path", "iris_transaction", "iris-branch", False


@pytest.fixture()
def empty_dataset(client, is_integration_test):
    if is_integration_test:
        empty_dataset = generic_upload_dataset_if_not_exists(
            client=client, name="empty_v1", upload_folder=None, foundry_schema=None
        )
        yield empty_dataset
    else:
        yield "empty-rid", "empty-path", None, "empty-branch", False


@pytest.fixture()
def complex_dataset_fixture(client, spark_session, tmpdir):
    DATASET_NAME = "many_types_v3"
    ds_path = f"{INTEGRATION_TEST_COMPASS_ROOT_PATH}/{DATASET_NAME}"
    try:
        identity = client.get_dataset_identity(
            dataset_path_or_rid=ds_path, branch="master"
        )
        yield identity["dataset_rid"]
    except DatasetNotFoundError:
        generate_test_dataset(spark_session, output_folder=tmpdir, n_rows=5000)
        (ds_rid, _, _, _, _,) = generic_upload_dataset_if_not_exists(
            client,
            name=DATASET_NAME,
            upload_folder=str(tmpdir),
            foundry_schema=FOUNDRY_SCHEMA_COMPLEX_DATASET,
        )
        yield ds_rid
