"""
Basic usage example for spark-testgen.

This example demonstrates:
1. Decorating a transformation function with @autogen_tests
2. Running the transformation with test generation enabled
3. The generated tests can then be run independently

Usage:
    # Normal run (no test generation)
    python basic_usage.py

    # Run with test generation
    SPARK_TESTGEN=1 python basic_usage.py

    # Run with synthetic data mode
    SPARK_TESTGEN=1 SPARK_TESTGEN_MODE=synthetic python basic_usage.py
"""

from pyspark.sql import DataFrame, SparkSession

from spark_testgen import autogen_tests


@autogen_tests
def add_status_column(df: DataFrame) -> DataFrame:
    """Add a status column based on value thresholds.

    Args:
        df: Input DataFrame with 'value' column

    Returns:
        DataFrame with new 'status' column
    """
    from pyspark.sql import functions as F

    return df.withColumn(
        "status",
        F.when(F.col("value") > 100, "high")
        .when(F.col("value") > 50, "medium")
        .otherwise("low"),
    )


@autogen_tests
def filter_active_users(df: DataFrame) -> DataFrame:
    """Filter to only active users with valid names.

    Args:
        df: Input DataFrame with 'active' and 'name' columns

    Returns:
        Filtered DataFrame
    """
    return df.filter(
        (df.active == True) & (df.name.isNotNull())  # noqa: E712
    )


@autogen_tests
def calculate_metrics(df: DataFrame) -> DataFrame:
    """Calculate derived metrics from value column.

    Args:
        df: Input DataFrame with 'value' column

    Returns:
        DataFrame with additional metric columns
    """
    from pyspark.sql import functions as F

    return df.withColumn("value_squared", F.col("value") ** 2).withColumn(
        "value_normalized", F.col("value") / 100.0
    )


def main():
    """Main function demonstrating spark-testgen usage."""
    # Create SparkSession
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("spark-testgen-example")
        .getOrCreate()
    )

    # Create sample data
    data = [
        (1, "Alice", 150, True),
        (2, "Bob", 75, True),
        (3, "Charlie", 25, False),
        (4, None, 100, True),
        (5, "Eve", None, True),
        (6, "Frank", 50, None),
    ]
    df = spark.createDataFrame(data, ["id", "name", "value", "active"])

    print("=" * 60)
    print("spark-testgen Example")
    print("=" * 60)

    print("\n📊 Input DataFrame:")
    df.show()

    # Run transformations
    # When SPARK_TESTGEN=1, tests will be generated for each function

    print("\n🔄 Running add_status_column...")
    result1 = add_status_column(df)
    result1.show()

    print("\n🔄 Running filter_active_users...")
    result2 = filter_active_users(df)
    result2.show()

    print("\n🔄 Running calculate_metrics...")
    # Filter out null values for this transformation
    df_clean = df.filter(df.value.isNotNull())
    result3 = calculate_metrics(df_clean)
    result3.show()

    print("\n✅ All transformations complete!")
    print("\nIf SPARK_TESTGEN=1 was set, check the 'tests/' directory for generated tests.")

    spark.stop()


if __name__ == "__main__":
    main()
