"""Utility functions for spark-testgen."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def is_serverless_environment(df: DataFrame) -> bool:
    """Detect if we're running in a serverless/Spark Connect environment.

    Serverless Spark environments (like Databricks serverless) don't support:
    - cache() / persist() operations
    - Direct JVM access via _jdf
    - PERSIST TABLE operations

    This function checks for Spark Connect by examining the DataFrame class.

    Args:
        df: A DataFrame to check

    Returns:
        True if running in serverless/Connect mode, False otherwise
    """
    # Simple and direct: check if DataFrame is from pyspark.sql.connect module
    df_module = type(df).__module__

    # Spark Connect DataFrames are in pyspark.sql.connect.dataframe
    if "connect" in df_module.lower():
        logger.debug(f"Spark Connect detected from module: {df_module}")
        return True

    logger.debug(f"Classic PySpark detected from module: {df_module}")
    return False


def clear_environment_cache() -> None:
    """Clear the cached environment detection results.

    No longer used - kept for API compatibility.
    """
    pass
    _serverless_cache = {}
