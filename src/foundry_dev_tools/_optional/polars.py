from __future__ import annotations

try:
    import polars as pl

    pl.__fake__ = False
except ImportError:
    from foundry_dev_tools._optional import FakeModule

    pl = FakeModule("polars")

__all__ = ["pl"]
