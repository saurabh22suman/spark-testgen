"""Observer module for capturing DataFrame transformation behavior."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

from .inference.plan_extractor import PlanExtractor

logger = logging.getLogger(__name__)


@dataclass
class Observation:
    """Captured data from a single transformation run.

    Contains all information needed to generate tests for a transformation.

    Attributes:
        function_name: Name of the decorated function
        input_schema: Schema of the input DataFrame
        output_schema: Schema of the output DataFrame
        input_sample: Sampled rows from input DataFrame
        output_sample: Sampled rows from output DataFrame
        logical_plan: String representation of logical execution plan
        physical_plan: String representation of physical execution plan
        row_count_in: Total row count of input (if computed)
        row_count_out: Total row count of output (if computed)
        timestamp: When the observation was captured
    """

    function_name: str
    input_schema: StructType
    output_schema: StructType
    input_sample: DataFrame
    output_sample: DataFrame
    logical_plan: str
    physical_plan: str
    row_count_in: int | None = None
    row_count_out: int | None = None
    timestamp: datetime | None = None

    def __post_init__(self) -> None:
        if self.timestamp is None:
            self.timestamp = datetime.now()


class Observer:
    """Observes and captures DataFrame transformation behavior.

    The observer is responsible for:
    - Capturing input/output schemas
    - Sampling data for snapshot tests
    - Extracting execution plans
    - Minimizing Spark actions for performance
    """

    def __init__(self, sample_size: int = 20, seed: int = 42) -> None:
        """Initialize observer.

        Args:
            sample_size: Number of rows to sample for snapshots
            seed: Random seed for deterministic sampling
        """
        self.sample_size = sample_size
        self.seed = seed
        self._plan_extractor = PlanExtractor()

    def capture(
        self,
        input_df: DataFrame,
        output_df: DataFrame,
        func_name: str,
    ) -> Observation:
        """Capture observation from a transformation run.

        Args:
            input_df: The input DataFrame passed to the transformation
            output_df: The output DataFrame returned by the transformation
            func_name: Name of the transformation function

        Returns:
            Observation containing all captured data
        """
        logger.info(f"Capturing observation for function: {func_name}")

        # Capture schemas (no Spark action required)
        input_schema = input_df.schema
        output_schema = output_df.schema

        # Extract execution plans (no Spark action required)
        logical_plan = self._extract_logical_plan(output_df)
        physical_plan = self._extract_physical_plan(output_df)

        # Sample data (requires Spark action)
        input_sample = self._sample_dataframe(input_df)
        output_sample = self._sample_dataframe(output_df)

        logger.info(
            f"Captured: input_schema={len(input_schema.fields)} fields, "
            f"output_schema={len(output_schema.fields)} fields, "
            f"input_sample={input_sample.count()} rows, "
            f"output_sample={output_sample.count()} rows"
        )

        return Observation(
            function_name=func_name,
            input_schema=input_schema,
            output_schema=output_schema,
            input_sample=input_sample,
            output_sample=output_sample,
            logical_plan=logical_plan,
            physical_plan=physical_plan,
        )

    def _safe_cache(self, df: DataFrame) -> DataFrame:
        """Safely cache a DataFrame, handling serverless environments.

        On serverless Spark (e.g., Databricks), cache/persist operations
        are not supported. This method attempts to cache but gracefully
        handles the case where it's not available.

        Args:
            df: DataFrame to cache

        Returns:
            The DataFrame (cached if possible, original otherwise)
        """
        try:
            df.cache()
            return df
        except BaseException as e:
            error_msg = str(e).lower()
            if any(
                pattern in error_msg
                for pattern in ["persist", "not supported", "serverless", "cache"]
            ):
                logger.debug(f"Cache not available (serverless mode): {e}")
                return df
            raise

    def _sample_dataframe(self, df: DataFrame) -> DataFrame:
        """Sample rows from DataFrame for snapshot testing.

        Uses limit() for small datasets, sample() for large ones.
        The result is cached to avoid recomputation (if caching is available).

        Args:
            df: DataFrame to sample

        Returns:
            Sampled DataFrame with at most sample_size rows
        """
        # Use limit for determinism - sample() can vary
        # For large datasets, this gets first N rows which is acceptable
        # as we're testing schema and basic transformations
        sampled = df.limit(self.sample_size)

        # Cache to avoid recomputation when saving (skip on serverless)
        return self._safe_cache(sampled)

    def _extract_logical_plan(self, df: DataFrame) -> str:
        """Extract logical execution plan from DataFrame.

        Uses PlanExtractor for robust cross-environment support,
        including Spark Connect and serverless Spark.

        Args:
            df: DataFrame to extract plan from

        Returns:
            String representation of logical plan
        """
        result = self._plan_extractor.get_logical_plan(df)
        if result.success:
            return result.plan
        logger.warning(f"Failed to extract logical plan: {result.error}")
        return f"<unable to extract logical plan: {result.error}>"

    def _extract_physical_plan(self, df: DataFrame) -> str:
        """Extract physical execution plan from DataFrame.

        Uses PlanExtractor for robust cross-environment support,
        including Spark Connect and serverless Spark.

        Args:
            df: DataFrame to extract plan from

        Returns:
            String representation of physical plan
        """
        result = self._plan_extractor.get_physical_plan(df)
        if result.success:
            return result.plan
        logger.warning(f"Failed to extract physical plan: {result.error}")
        return f"<unable to extract physical plan: {result.error}>"
