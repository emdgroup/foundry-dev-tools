"""Helper functions for unified access to Apache Spark."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyspark


def get_spark_session() -> pyspark.sql.SparkSession:
    """Get or create the pyspark session.

    Returns:
        :external+spark:py:class:`~pyspark.sql.SparkSession`:
            the pyspark session

    """
    from foundry_dev_tools._optional.pyspark import pyspark

    return (
        pyspark.sql.SparkSession.builder.master("local[*]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        .appName("foundry-dev-tools")
        .getOrCreate()
    )
