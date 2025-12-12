"""Utility functions for spark-testgen."""

from __future__ import annotations

import logging
import os
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

    This function uses multiple detection strategies and caches the result.

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

        # Detection method 1: Environment variable for Databricks serverless
        # This is the most reliable check for Databricks
        databricks_runtime = os.environ.get("DATABRICKS_RUNTIME_VERSION", "")
        if databricks_runtime:
            print(f"[spark-testgen] Databricks runtime: {databricks_runtime}")
            # Check if it's serverless (no persistent compute)
            compute_type = os.environ.get("SPARK_CONNECT_MODE", "")
            is_serverless_env = os.environ.get("IS_SERVERLESS", "").lower() == "true"
            print(f"[spark-testgen] SPARK_CONNECT_MODE={compute_type}, IS_SERVERLESS={is_serverless_env}")
            
            # Also check for Spark Connect client indicators
            if is_serverless_env or compute_type:
                print("[spark-testgen] => Detected serverless via env vars")
                _serverless_cache[session_id] = True
                return True

        # Detection method 2: Check DataFrame class hierarchy for Connect
        df_class_name = type(df).__name__
        df_module = type(df).__module__
        
        print(f"[spark-testgen] DataFrame class: {df_module}.{df_class_name}")
        
        # Spark Connect DataFrames are in pyspark.sql.connect module
        if "connect" in df_module.lower() or "connect" in df_class_name.lower():
            print("[spark-testgen] => Detected Spark Connect from DataFrame class")
            _serverless_cache[session_id] = True
            return True
        
        # Detection method 3: Check SparkSession class for Connect
        session_class_name = type(session).__name__
        session_module = type(session).__module__
        
        print(f"[spark-testgen] Session class: {session_module}.{session_class_name}")
        
        if "connect" in session_module.lower() or "connect" in session_class_name.lower():
            print("[spark-testgen] => Detected Spark Connect from session class")
            _serverless_cache[session_id] = True
            return True

        # Detection method 4: Check if _jdf attribute is actually accessible
        # This is the definitive test - try to actually use _jdf
        if not hasattr(df, "_jdf"):
            print("[spark-testgen] No _jdf attribute => assuming Spark Connect")
            _serverless_cache[session_id] = True
            return True

        try:
            # Actually try to access _jdf - this will raise on Connect
            print("[spark-testgen] Attempting _jdf access...")
            jdf = df._jdf
            if jdf is None:
                print("[spark-testgen] _jdf is None => assuming Spark Connect")
                _serverless_cache[session_id] = True
                return True
            
            # Try to call a method on it to be sure
            _ = jdf.queryExecution()
            
            # If we get here, JVM access works - not serverless
            print("[spark-testgen] JVM access OK => NOT serverless")
            _serverless_cache[session_id] = False
            return False
            
        except BaseException as e:
            # Any error accessing _jdf means Connect/serverless
            print(f"[spark-testgen] _jdf access failed: {type(e).__name__}: {e}")
            _serverless_cache[session_id] = True
            return True

    except BaseException as e:
        # If anything fails during detection, assume serverless for safety
        print(f"[spark-testgen] Detection error, assuming serverless: {e}")
        return True


def clear_environment_cache() -> None:
    """Clear the cached environment detection results.

    Useful for testing or when session changes.
    """
    global _serverless_cache
    _serverless_cache = {}
