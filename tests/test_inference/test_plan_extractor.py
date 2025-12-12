"""Tests for the plan extractor module."""

from __future__ import annotations

from unittest import mock

import pytest
from pyspark.sql import DataFrame, SparkSession

from spark_testgen.inference.plan_extractor import (
    ExtractionMethod,
    PlanExtractor,
    PlanResult,
    PlanType,
    get_logical_plan,
    get_physical_plan,
    get_extended_plan,
    has_jvm_access,
)


class TestPlanResult:
    """Tests for PlanResult dataclass."""

    def test_successful_result(self):
        """Test creating a successful plan result."""
        result = PlanResult(
            plan="Project [id#1]",
            method=ExtractionMethod.JVM_QUERY_EXECUTION,
            success=True,
        )
        assert result.success is True
        assert result.error is None
        assert "Project" in result.plan

    def test_unavailable_result(self):
        """Test creating an unavailable plan result."""
        result = PlanResult.unavailable("JVM not available")
        assert result.success is False
        assert result.method == ExtractionMethod.UNAVAILABLE
        assert "PLAN_UNAVAILABLE" in result.plan
        assert result.error == "JVM not available"


class TestPlanExtractor:
    """Tests for PlanExtractor class."""

    def test_extractor_initialization(self):
        """Test extractor initializes correctly."""
        extractor = PlanExtractor()
        assert extractor._jvm_available is None  # Not yet determined

    def test_get_logical_plan_classic_spark(self, spark: SparkSession, sample_df):
        """Test logical plan extraction with classic Spark."""
        extractor = PlanExtractor()
        result = extractor.get_logical_plan(sample_df)

        # Should succeed with JVM or fallback method
        assert result.success is True
        assert len(result.plan) > 0
        assert result.method in (
            ExtractionMethod.JVM_QUERY_EXECUTION,
            ExtractionMethod.EXPLAIN_STRING,
            ExtractionMethod.EXPLAIN_CAPTURE,
        )

    def test_get_physical_plan_classic_spark(self, spark: SparkSession, sample_df):
        """Test physical plan extraction with classic Spark."""
        extractor = PlanExtractor()
        result = extractor.get_physical_plan(sample_df)

        assert result.success is True
        assert len(result.plan) > 0

    def test_get_extended_plan(self, spark: SparkSession, sample_df):
        """Test extended plan extraction."""
        extractor = PlanExtractor()
        result = extractor.get_extended_plan(sample_df)

        assert result.success is True
        assert len(result.plan) > 0

    def test_has_jvm_access_classic_spark(self, spark: SparkSession, sample_df):
        """Test JVM access detection with classic Spark."""
        extractor = PlanExtractor()
        # Classic Spark should have JVM access
        has_access = extractor.has_jvm_access(sample_df)
        # This could be True or False depending on environment
        assert isinstance(has_access, bool)

    def test_plan_contains_expected_content(self, spark: SparkSession):
        """Test that extracted plan contains expected operations."""
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        filtered_df = df.filter(df.id > 1)

        extractor = PlanExtractor()
        result = extractor.get_logical_plan(filtered_df)

        assert result.success is True
        # Plan should mention filter operation
        plan_lower = result.plan.lower()
        assert "filter" in plan_lower or ">" in result.plan

    def test_fallback_when_jvm_unavailable(self, spark: SparkSession, sample_df):
        """Test fallback methods when JVM access is blocked."""
        extractor = PlanExtractor()

        # Mock _check_jvm_capability to return False
        with mock.patch.object(extractor, "_check_jvm_capability", return_value=False):
            result = extractor.get_logical_plan(sample_df)

            # Should still succeed via fallback
            assert result.success is True
            assert result.method in (
                ExtractionMethod.EXPLAIN_STRING,
                ExtractionMethod.EXPLAIN_CAPTURE,
            )


class TestPlanExtractorEdgeCases:
    """Tests for edge cases and error handling."""

    def test_handles_transformed_dataframe(self, spark: SparkSession, sample_df):
        """Test plan extraction on a transformed DataFrame."""
        transformed = (
            sample_df.filter(sample_df.value > 100)
            .withColumn("doubled", sample_df.value * 2)
            .select("id", "doubled")
        )

        extractor = PlanExtractor()
        logical = extractor.get_logical_plan(transformed)
        physical = extractor.get_physical_plan(transformed)

        assert logical.success is True
        assert physical.success is True

    def test_handles_aggregation(self, spark: SparkSession, sample_df):
        """Test plan extraction on aggregated DataFrame."""
        from pyspark.sql import functions as F

        aggregated = sample_df.groupBy("active").agg(F.sum("value").alias("total"))

        extractor = PlanExtractor()
        result = extractor.get_logical_plan(aggregated)

        assert result.success is True
        # Should contain aggregation info
        plan_lower = result.plan.lower()
        assert "aggregate" in plan_lower or "sum" in plan_lower or "group" in plan_lower

    def test_handles_join(self, spark: SparkSession):
        """Test plan extraction on joined DataFrames."""
        df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val1"])
        df2 = spark.createDataFrame([(1, "x"), (2, "y")], ["id", "val2"])
        joined = df1.join(df2, "id")

        extractor = PlanExtractor()
        result = extractor.get_logical_plan(joined)

        assert result.success is True
        plan_lower = result.plan.lower()
        assert "join" in plan_lower

    def test_empty_dataframe(self, spark: SparkSession):
        """Test plan extraction on empty DataFrame."""
        empty_df = spark.createDataFrame([], "id INT, name STRING")

        extractor = PlanExtractor()
        result = extractor.get_logical_plan(empty_df)

        assert result.success is True


class TestModuleFunctions:
    """Tests for module-level convenience functions."""

    def test_get_logical_plan_function(self, spark: SparkSession, sample_df):
        """Test module-level get_logical_plan function."""
        plan = get_logical_plan(sample_df)
        assert isinstance(plan, str)
        assert len(plan) > 0

    def test_get_physical_plan_function(self, spark: SparkSession, sample_df):
        """Test module-level get_physical_plan function."""
        plan = get_physical_plan(sample_df)
        assert isinstance(plan, str)
        assert len(plan) > 0

    def test_get_extended_plan_function(self, spark: SparkSession, sample_df):
        """Test module-level get_extended_plan function."""
        plan = get_extended_plan(sample_df)
        assert isinstance(plan, str)
        assert len(plan) > 0

    def test_has_jvm_access_function(self, spark: SparkSession, sample_df):
        """Test module-level has_jvm_access function."""
        result = has_jvm_access(sample_df)
        assert isinstance(result, bool)


class TestSparkConnectSimulation:
    """Tests simulating Spark Connect behavior where JVM is unavailable."""

    def test_graceful_degradation_no_jdf(self, spark: SparkSession, sample_df):
        """Test behavior when _jdf attribute is missing."""
        extractor = PlanExtractor()

        # Create a mock DataFrame without _jdf
        class MockDataFrame:
            def __init__(self, real_df):
                self._real_df = real_df
                # Note: no _jdf attribute

            def explain(self, *args, **kwargs):
                return self._real_df.explain(*args, **kwargs)

            @property
            def schema(self):
                return self._real_df.schema

        # This simulates what Spark Connect DataFrames look like
        mock_df = MockDataFrame(sample_df)

        # Should detect no JVM access
        assert extractor.has_jvm_access(mock_df) is False

    def test_graceful_degradation_jdf_none(self, spark: SparkSession, sample_df):
        """Test behavior when _jdf is None."""
        extractor = PlanExtractor()

        # Create a mock with _jdf = None
        class MockDataFrame:
            def __init__(self, real_df):
                self._real_df = real_df
                self._jdf = None  # Simulates Connect behavior

            def explain(self, *args, **kwargs):
                return self._real_df.explain(*args, **kwargs)

        mock_df = MockDataFrame(sample_df)

        # Should detect no JVM access
        assert extractor.has_jvm_access(mock_df) is False

    def test_graceful_degradation_jdf_raises(self, spark: SparkSession, sample_df):
        """Test behavior when accessing _jdf raises exception."""
        extractor = PlanExtractor()

        # Create a mock where _jdf access raises
        class MockDataFrame:
            def __init__(self, real_df):
                self._real_df = real_df

            @property
            def _jdf(self):
                raise AttributeError("No JVM in Spark Connect")

            def explain(self, *args, **kwargs):
                return self._real_df.explain(*args, **kwargs)

        mock_df = MockDataFrame(sample_df)

        # Should detect no JVM access
        assert extractor.has_jvm_access(mock_df) is False
