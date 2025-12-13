"""Tests for the decorator module."""

from __future__ import annotations

import os
from unittest import mock

import pytest
from pyspark.sql import DataFrame, SparkSession

from spark_testgen.config import SPARK_TESTGEN_ENV, reset_config
from spark_testgen.decorator import autogen_tests


class TestAutogenTestsDecorator:
    """Tests for @autogen_tests decorator."""

    def setup_method(self):
        """Reset config before each test."""
        reset_config()

    def teardown_method(self):
        """Reset config after each test."""
        reset_config()

    def test_decorator_passthrough_when_disabled(self, _spark: SparkSession, sample_df):
        """Test decorator does nothing when SPARK_TESTGEN is not set."""

        @autogen_tests
        def transform(df: DataFrame) -> DataFrame:
            return df.withColumn("new_col", df.value * 2)

        # Should work normally without generating tests
        result = transform(sample_df)

        assert "new_col" in result.columns
        assert result.count() == sample_df.count()

    def test_decorator_preserves_function_metadata(self):
        """Test decorator preserves function name and docstring."""

        @autogen_tests
        def my_transform(df: DataFrame) -> DataFrame:
            """This is my transform function."""
            return df

        assert my_transform.__name__ == "my_transform"
        assert "This is my transform" in my_transform.__doc__

    def test_decorator_raises_on_no_arguments(self, _spark: SparkSession):
        """Test decorator raises error when no DataFrame is provided."""

        @autogen_tests
        def transform(df: DataFrame) -> DataFrame:
            return df

        with pytest.raises(ValueError, match="must receive a DataFrame"):
            transform()

    @mock.patch.dict(os.environ, {SPARK_TESTGEN_ENV: "1"})
    def test_decorator_enabled_executes_function(self, _spark: SparkSession, sample_df, tmp_path):
        """Test decorator still executes the function when enabled."""
        # Patch output directory to use temp path
        with mock.patch("spark_testgen.config.Config.from_environment") as mock_config:
            from spark_testgen.config import Config, Mode

            mock_config.return_value = Config(
                enabled=True,
                mode=Mode.MASKED,
                output_dir=tmp_path / "tests",
                seed=42,
            )
            reset_config()

            @autogen_tests
            def transform(df: DataFrame) -> DataFrame:
                return df.withColumn("doubled", df.value * 2)

            # This should execute the transformation
            result = transform(sample_df)

            # Verify the transformation was applied
            assert "doubled" in result.columns
