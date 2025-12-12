"""Tests for the observer module."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from spark_testgen.observer import Observation, Observer


class TestObservation:
    """Tests for Observation dataclass."""

    def test_observation_creation(self, spark: SparkSession, sample_df):
        """Test creating an observation."""
        output_df = sample_df.withColumn("doubled", sample_df.value * 2)

        observation = Observation(
            function_name="test_transform",
            input_schema=sample_df.schema,
            output_schema=output_df.schema,
            input_sample=sample_df,
            output_sample=output_df,
            logical_plan="test plan",
            physical_plan="test physical plan",
        )

        assert observation.function_name == "test_transform"
        assert observation.timestamp is not None
        assert len(observation.input_schema.fields) == 4
        assert len(observation.output_schema.fields) == 5  # Original + doubled


class TestObserver:
    """Tests for Observer class."""

    def test_observer_initialization(self):
        """Test observer with default values."""
        observer = Observer()
        assert observer.sample_size == 20
        assert observer.seed == 42

    def test_observer_custom_values(self):
        """Test observer with custom values."""
        observer = Observer(sample_size=10, seed=123)
        assert observer.sample_size == 10
        assert observer.seed == 123

    def test_capture_basic(self, spark: SparkSession, sample_df):
        """Test capturing a basic transformation."""
        output_df = sample_df.withColumn("doubled", sample_df.value * 2)

        observer = Observer(sample_size=10)
        observation = observer.capture(sample_df, output_df, "test_func")

        assert observation.function_name == "test_func"
        assert len(observation.input_schema.fields) == 4
        assert len(observation.output_schema.fields) == 5
        assert observation.input_sample.count() <= 10
        assert observation.output_sample.count() <= 10

    def test_capture_extracts_plans(self, spark: SparkSession, sample_df):
        """Test that plans are extracted."""
        output_df = sample_df.filter(sample_df.value > 100)

        observer = Observer()
        observation = observer.capture(sample_df, output_df, "filter_func")

        assert len(observation.logical_plan) > 0
        assert len(observation.physical_plan) > 0
        assert "Filter" in observation.logical_plan or "filter" in observation.logical_plan.lower()

    def test_sample_respects_limit(self, spark: SparkSession):
        """Test that sampling respects the limit."""
        # Create a larger DataFrame
        data = [(i, f"name_{i}") for i in range(100)]
        large_df = spark.createDataFrame(data, ["id", "name"])

        observer = Observer(sample_size=5)
        observation = observer.capture(large_df, large_df, "identity")

        assert observation.input_sample.count() == 5
        assert observation.output_sample.count() == 5
