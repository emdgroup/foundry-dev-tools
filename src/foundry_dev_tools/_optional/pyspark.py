from __future__ import annotations

try:
    import pyspark
    import pyspark.sql as pyspark_sql
    import pyspark.sql.types as pyspark_sql_types

    pyspark.__fake__ = False
    pyspark_sql.__fake__ = False
    pyspark_sql_types.__fake__ = False
except ImportError:
    from foundry_dev_tools._optional import FakeModule

    pyspark = FakeModule("pyspark")
    pyspark_sql = FakeModule("pyspark")
    pyspark_sql_types = FakeModule("pyspark")

__all__ = ["pyspark"]
