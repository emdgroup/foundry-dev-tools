"""Helper functions for unified access to Apache Spark."""
from foundry_dev_tools.utils.importer import import_optional_dependency

pyspark = import_optional_dependency("pyspark")


def get_spark_session() -> "pyspark.sql.SparkSession":
    """Get or create the pyspark session.

    Returns:
        :external+spark:py:class:`~pyspark.sql.SparkSession`:
            the pyspark session

    """
    spark_session = (
        pyspark.sql.SparkSession.builder.master("local[*]")
        .appName("foundry-dev-tools")
        .getOrCreate()
    )
    spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark_session.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    return spark_session
