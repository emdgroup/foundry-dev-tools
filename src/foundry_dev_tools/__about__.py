"""FoundryDevTools version."""
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version(__name__.split(".")[0])
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
