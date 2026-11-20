"""Fast Snowflake to Foundry ingestion for lightweight transforms, requires the ``snowflake`` extra."""

from foundry_dev_tools.helpers.fast_snowflake_ingestion._impl import (
    FastSnowflakeIngestion,
    disable_s3_transfer_acceleration,
)
from foundry_dev_tools.helpers.fast_snowflake_ingestion.utils import (
    FoundryPath,
    FoundryPathWithBranchId,
    TransactionPath,
    ViewPath,
)

__all__ = [
    "FastSnowflakeIngestion",
    "FoundryPath",
    "FoundryPathWithBranchId",
    "TransactionPath",
    "ViewPath",
    "disable_s3_transfer_acceleration",
]
