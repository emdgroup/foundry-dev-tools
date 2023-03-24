"""Foundry Clients developed at Merck KGaA, Darmstadt, Germany."""
from transforms import __version__

import foundry_dev_tools.config

(
    INITIAL_CONFIG,
    FOUNDRY_DEV_TOOLS_DIRECTORY,
) = foundry_dev_tools.config.initial_config()
Configuration = foundry_dev_tools.config.Config()

from foundry_dev_tools.cached_foundry_client import (  # pylint: disable=C0413
    CachedFoundryClient,
)
from foundry_dev_tools.foundry_api_client import (  # pylint: disable=C0413
    FoundryRestClient,
)

__all__ = [__version__, "FoundryRestClient", "CachedFoundryClient", "Configuration"]
