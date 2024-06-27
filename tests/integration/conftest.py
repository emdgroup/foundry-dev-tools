from __future__ import annotations

import pytest

from tests.integration.utils import TestSingleton

TEST_SINGLETON = TestSingleton()


@pytest.fixture(autouse=True)
def _cache_dir_per_test(tmp_path_factory):
    TEST_SINGLETON.ctx.config.cache_dir = tmp_path_factory.mktemp("transforms_cache")
    yield
    # if cached_foundry_client has been accessed and cached
    # create new cached_foundry_client, to update the cache path and remove the already cached items from the dict
    if "cached_foundry_client" in TEST_SINGLETON.ctx.__dict__:
        del TEST_SINGLETON.ctx.cached_foundry_client
