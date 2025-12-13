"""pytest configuration and fixtures for spark-testgen tests."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create a SparkSession for testing.

    This is a session-scoped fixture that creates a single SparkSession
    for all tests, improving test performance.
    """
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("spark-testgen-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-testgen")
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby-testgen")
        .getOrCreate()
    )

    yield spark

    spark.stop()


@pytest.fixture(scope="session")
def _spark(spark: SparkSession) -> SparkSession:
    """Alias for spark fixture for tests that don't use the session directly.

    This allows tests to declare they need spark initialized (for sample_df)
    without triggering unused-argument linter warnings.
    """
    return spark


@pytest.fixture
def sample_df(spark: SparkSession):
    """Create a sample DataFrame for testing."""
    data = [
        (1, "Alice", 100, True),
        (2, "Bob", 200, False),
        (3, None, 300, True),
        (4, "Diana", None, None),
    ]
    return spark.createDataFrame(data, ["id", "name", "value", "active"])


@pytest.fixture
def sample_df_with_nulls(spark: SparkSession):
    """Create a sample DataFrame with null values for edge case testing."""
    data = [
        (None, None, None, None),
        (1, "", 0, True),
        (2, "test", -1, False),
    ]
    return spark.createDataFrame(data, ["id", "name", "value", "active"])


@pytest.fixture
def complex_schema_df(spark: SparkSession):
    """Create a DataFrame with complex types for testing."""
    from pyspark.sql.types import (
        ArrayType,
        IntegerType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("tags", ArrayType(StringType()), True),
            StructField("metadata", MapType(StringType(), IntegerType()), True),
            StructField(
                "nested",
                StructType(
                    [
                        StructField("inner_id", IntegerType(), True),
                        StructField("inner_name", StringType(), True),
                    ]
                ),
                True,
            ),
        ]
    )

    data = [
        (1, ["a", "b"], {"x": 1, "y": 2}, (10, "inner1")),
        (2, ["c"], {"z": 3}, (20, "inner2")),
        (3, None, None, None),
    ]

    return spark.createDataFrame(data, schema)
