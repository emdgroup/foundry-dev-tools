from __future__ import annotations

import os
import warnings
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from foundry_dev_tools import FoundryContext
from foundry_dev_tools.errors.config import FoundryConfigError
from tests.unit.mocks import TEST_DOMAIN

if TYPE_CHECKING:
    from pathlib import Path


def test_app_service(mock_config_location: dict[Path, None]):
    with (  # noqa: PT012
        mock.patch.dict(os.environ, {"APP_SERVICE_TS": "1", "FDT_CREDENTIALS__DOMAIN": TEST_DOMAIN}, clear=True),
        pytest.raises(
            FoundryConfigError,
            match="Could not get Foundry token from flask/dash/streamlit headers.",
        ),
        warnings.catch_warnings(),
    ):
        warnings.simplefilter("error")  # raise errors on warnings
        _ = FoundryContext()
