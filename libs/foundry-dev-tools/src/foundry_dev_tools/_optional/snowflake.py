from __future__ import annotations

try:
    import snowflake.connector.errors as snowflake_errors
    import snowflake.snowpark.exceptions as snowpark_exceptions

    snowflake_errors.__fake__ = False
    snowpark_exceptions.__fake__ = False
except ImportError:
    from foundry_dev_tools._optional import FakeModule

    snowflake_errors = FakeModule("snowflake-snowpark-python")
    snowpark_exceptions = FakeModule("snowflake-snowpark-python")

__all__ = ["snowflake_errors", "snowpark_exceptions"]
