"""Helper code inspired by "import_optional_dependency" from pandas.

https://github.com/pandas-dev/pandas/blob/e068f4ca368d1a392d8690b0a3c354503d169914/pandas/compat/_optional.py#L87
to make pyspark an optional dependency
"""
import importlib
from types import ModuleType
from typing import Optional


def import_optional_dependency(name: str, extra: str = "") -> Optional[ModuleType]:
    """Import an optional dependency.

    By default, if a dependency is missing an ImportError with a nice
    message will be raised.

    Args:
        name (str): The module name.
        extra (str): Additional text to include in the ImportError message.

    Returns:
        `Optional[ModuleType]`:
            The imported module, when found.

    """
    msg = (
        f"Missing optional dependency '{name}'. {extra} "
        f"Use pip or conda to install {name}."
    )
    try:
        return importlib.import_module(name)
    except ImportError:
        fake_module = ModuleType(name)

        def _failed_import_message(*args):
            raise ValueError(msg)

        fake_module.__getattr__ = _failed_import_message
        return fake_module
