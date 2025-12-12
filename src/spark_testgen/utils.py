"""Utility functions for spark-testgen."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

# Cache for environment detection (per session)
_serverless_cache: dict[int, bool] = {}


def is_serverless_environment(df: DataFrame) -> bool:
    """Detect if we're running in a serverless/Spark Connect environment.

    Serverless Spark environments (like Databricks serverless) don't support:
    - cache() / persist() operations
    - Direct JVM access via _jdf
    - PERSIST TABLE operations

    This function detects such environments by checking for Spark Connect
    client characteristics. The result is cached per SparkSession.

    Args:
        df: A DataFrame to check (used to access SparkSession)

    Returns:
        True if running in serverless/Connect mode, False otherwise
    """
    try:
        # Get session ID for caching
        session = df.sparkSession
        session_id = id(session)

        # Check cache first
        if session_id in _serverless_cache:
            return _serverless_cache[session_id]

        # Detection method 1: Check DataFrame class name
        # Spark Connect DataFrames have different class paths
        df_class = type(df).__module__
        is_connect = "connect" in df_class.lower()

        if is_connect:
            logger.debug(f"Detected Spark Connect from DataFrame module: {df_class}")
            _serverless_cache[session_id] = True
            return True

        # Detection method 2: Check SparkSession class
        session_class = type(session).__module__
        is_connect_session = "connect" in session_class.lower()

        if is_connect_session:
            logger.debug(f"Detected Spark Connect from Session module: {session_class}")
            _serverless_cache[session_id] = True
            return True

        # Detection method 3: Try to access _jdf (will fail on Connect)
        # This is a definitive test but we do it last as it's more expensive
        try:
            _ = df._jdf
            # If we get here without exception, JVM access works
            _serverless_cache[session_id] = False
            return False
        except BaseException:
            # _jdf access failed - this is a Connect/serverless environment
            logger.debug("Detected serverless: _jdf access failed")
            _serverless_cache[session_id] = True
            return True

    except BaseException as e:
        # If anything fails during detection, assume serverless for safety
        logger.debug(f"Environment detection failed, assuming serverless: {e}")
        return True


def clear_environment_cache() -> None:
    """Clear the cached environment detection results.

    Useful for testing or when session changes.
    """
    global _serverless_cache
    _serverless_cache = {}
