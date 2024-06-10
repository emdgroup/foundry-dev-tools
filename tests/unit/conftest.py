from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import mock

import pytest

from foundry_dev_tools.config.config import Config
from foundry_dev_tools.utils.config import CFG_FILE_NAME
from tests.unit.mocks import MOCK_ADAPTER, FoundryMockContext, MockTokenProvider

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path


@pytest.fixture()
def foundry_token(request):
    """Generates a fake token that is the name of the test function + '_token'.

    This is useful to trace back a token to a test function.
    """
    return request.node.name + "_token"


@pytest.fixture()
def foundry_client_id(request):
    """Generates a fake client_id that is the name of the test function + 'client_id'.

    This is useful to trace back a client_id to a test function.
    """
    return request.node.name + "_client_id"


@pytest.fixture()
def foundry_client_secret(request):
    """Generates a fake client_secret that is the name of the test function + 'client_secret'.

    This is useful to trace back a client_secret to a test function.
    """
    return request.node.name + "_client_secret"


@pytest.fixture()
def test_context_mock(request, tmp_path_factory) -> Generator[FoundryMockContext, None, None]:
    """This fixture provides a context with default config, a :py:class:`~tests.unit.mocks.MockTokenProvider` and a :py:class:`requests_mock.Adapter`.

    Useful for mocking API responses. see :py:class:`~tests.unit.mocks.FoundryMockContext`.
    """  # noqa: E501
    yield FoundryMockContext(
        Config(cache_dir=tmp_path_factory.mktemp(f"foundry_dev_tools_test__{request.node.name}").absolute()),
        MockTokenProvider(jwt=request.node.name + "_token"),
    )
    MOCK_ADAPTER.reset()


@pytest.fixture()
def mock_config_location(tmp_path_factory, request) -> Generator[dict[Path, None], None, None]:
    """Mocks the locations where the config files are read and returns them.

    Can be used in tests like this:
    .. code-block:: python

       from foundry_dev_tools.config import config


       def test_xyz(mock_config_location):
           assert mock_config_location == config.cfg_files()  # true

    """

    paths = dict.fromkeys(
        [
            tmp_path_factory.mktemp(f"{request.node.name}_site_cfg").joinpath(CFG_FILE_NAME),
            tmp_path_factory.mktemp(f"{request.node.name}_user_cfg").joinpath(CFG_FILE_NAME),
        ],
    )
    with mock.patch("foundry_dev_tools.config.config.cfg_files", return_value=paths):
        yield paths
