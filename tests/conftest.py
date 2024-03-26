from pathlib import Path

import pytest

from foundry_dev_tools.utils.spark import get_spark_session

TEST_FOLDER = Path(__file__).parent.resolve()


@pytest.fixture(scope="session")
def spark_session():
    return get_spark_session()
