"""Statistics analysis for DataFrame transformations."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


@dataclass
class ColumnStats:
    """Statistics for a single column.

    Attributes:
        name: Column name
        data_type: Column data type
        null_count: Number of null values
        distinct_count: Number of distinct values (approximate)
        min_value: Minimum value (for numeric/date types)
        max_value: Maximum value (for numeric/date types)
        mean_value: Mean value (for numeric types)
    """

    name: str
    data_type: str
    null_count: int | None = None
    distinct_count: int | None = None
    min_value: Any = None
    max_value: Any = None
    mean_value: float | None = None


@dataclass
class DataFrameStats:
    """Statistics for an entire DataFrame.

    Attributes:
        row_count: Total number of rows
        column_count: Total number of columns
        column_stats: Statistics for each column
        has_nulls: Whether any column contains nulls
        empty: Whether the DataFrame is empty
    """

    row_count: int = 0
    column_count: int = 0
    column_stats: list[ColumnStats] = field(default_factory=list)
    has_nulls: bool = False
    empty: bool = False


class StatsAnalyzer:
    """Analyzes DataFrame statistics for test generation.

    Collects summary statistics that inform edge case generation
    and data validation tests.
    """

    def __init__(self, compute_distinct: bool = False) -> None:
        """Initialize stats analyzer.

        Args:
            compute_distinct: Whether to compute distinct counts (expensive)
        """
        self.compute_distinct = compute_distinct

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

    def analyze(self, df: DataFrame) -> DataFrameStats:
        """Analyze a DataFrame and compute statistics.

        Args:
            df: DataFrame to analyze

        Returns:
            DataFrameStats with computed statistics
        """
        # Try to cache for multiple passes (skip on serverless)
        df = self._safe_cache(df)

        row_count = df.count()

        if row_count == 0:
            return DataFrameStats(
                row_count=0,
                column_count=len(df.schema.fields),
                empty=True,
            )

        column_stats = []
        has_any_nulls = False

        for struct_field in df.schema.fields:
            col_stats = self._analyze_column(df, struct_field.name, struct_field.dataType)
            column_stats.append(col_stats)

            if col_stats.null_count and col_stats.null_count > 0:
                has_any_nulls = True

        return DataFrameStats(
            row_count=row_count,
            column_count=len(df.schema.fields),
            column_stats=column_stats,
            has_nulls=has_any_nulls,
            empty=False,
        )

    def _analyze_column(
        self,
        df: DataFrame,
        col_name: str,
        data_type: Any,
    ) -> ColumnStats:
        """Analyze a single column.

        Args:
            df: DataFrame containing the column
            col_name: Name of the column
            data_type: PySpark DataType of the column

        Returns:
            ColumnStats for the column
        """
        from pyspark.sql import functions as F

        type_name = type(data_type).__name__
        stats = ColumnStats(name=col_name, data_type=type_name)

        # Null count - always compute
        null_count_row = df.select(
            F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias("null_count")
        ).collect()[0]
        stats.null_count = null_count_row["null_count"] or 0

        # Numeric types: compute min, max, mean
        numeric_types = {
            "IntegerType",
            "LongType",
            "DoubleType",
            "FloatType",
            "ShortType",
            "ByteType",
            "DecimalType",
        }

        if type_name in numeric_types:
            agg_row = df.select(
                F.min(F.col(col_name)).alias("min_val"),
                F.max(F.col(col_name)).alias("max_val"),
                F.avg(F.col(col_name)).alias("mean_val"),
            ).collect()[0]

            stats.min_value = agg_row["min_val"]
            stats.max_value = agg_row["max_val"]
            stats.mean_value = float(agg_row["mean_val"]) if agg_row["mean_val"] else None

        # Date/Timestamp: compute min, max
        date_types = {"DateType", "TimestampType"}
        if type_name in date_types:
            agg_row = df.select(
                F.min(F.col(col_name)).alias("min_val"),
                F.max(F.col(col_name)).alias("max_val"),
            ).collect()[0]

            stats.min_value = agg_row["min_val"]
            stats.max_value = agg_row["max_val"]

        # Distinct count (expensive, optional)
        if self.compute_distinct:
            distinct_row = df.select(
                F.approx_count_distinct(F.col(col_name)).alias("distinct_count")
            ).collect()[0]
            stats.distinct_count = distinct_row["distinct_count"]

        return stats

    def quick_summary(self, df: DataFrame) -> dict[str, Any]:
        """Get a quick summary without expensive operations.

        Args:
            df: DataFrame to summarize

        Returns:
            Dictionary with basic summary info
        """
        schema = df.schema
        return {
            "column_count": len(schema.fields),
            "columns": [f.name for f in schema.fields],
            "types": {f.name: str(f.dataType) for f in schema.fields},
            "nullable": {f.name: f.nullable for f in schema.fields},
        }
