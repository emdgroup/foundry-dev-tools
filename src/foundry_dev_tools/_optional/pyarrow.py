from __future__ import annotations

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    pa.__fake__ = False
    pq.__fake__ = False
except ImportError:
    from foundry_dev_tools._optional import FakeModule

    pa = FakeModule("pyarrow")
    pq = FakeModule("pyarrow")

__all__ = ["pa"]
