"""Foundry Clients developed at Merck KGaA, Darmstadt, Germany."""
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version(__name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
from .cached_foundry_client import CachedFoundryClient
from .config import FOUNDRY_DEV_TOOLS_DIRECTORY, INITIAL_CONFIG, Configuration
from .foundry_api_client import FoundryRestClient

__all__ = [
    "__version__",
    "FoundryRestClient",
    "CachedFoundryClient",
    "Configuration",
    "INITIAL_CONFIG",
    "FOUNDRY_DEV_TOOLS_DIRECTORY",
]
