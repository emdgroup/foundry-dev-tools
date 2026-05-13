from __future__ import annotations

try:
    import adbc_driver_flightsql
    import adbc_driver_flightsql.dbapi
    from adbc_driver_flightsql import DatabaseOptions

    adbc_driver_flightsql.__fake__ = False
except ImportError:
    from foundry_dev_tools._optional import FakeModule

    adbc_driver_flightsql = FakeModule("adbc-driver-flightsql")
    DatabaseOptions = None

__all__ = ["adbc_driver_flightsql", "DatabaseOptions"]
