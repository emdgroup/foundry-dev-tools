from __future__ import annotations

try:
    import pandas as pd

    pd.__fake__ = False
except ImportError:
    from foundry_dev_tools._optional import FakeModule

    pd = FakeModule("pandas")

__all__ = ["pd"]
