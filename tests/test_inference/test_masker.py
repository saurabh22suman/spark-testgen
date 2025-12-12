"""Tests for the masker module."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from spark_testgen.config import Mode
from spark_testgen.inference.masker import Masker


class TestMasker:
    """Tests for Masker class."""

    def test_masker_initialization(self):
        """Test masker with default values."""
        masker = Masker()
        assert masker.mode == Mode.MASKED
        assert masker.seed == 42

    def test_masker_unsafe_raw_passthrough(self, _spark: SparkSession, sample_df):
        """Test unsafe_raw mode passes data through unchanged."""
        masker = Masker(mode=Mode.UNSAFE_RAW)
        result = masker.process(sample_df)

        # Should be the same DataFrame
        assert result.collect() == sample_df.collect()

    def test_masker_masked_changes_strings(self, _spark: SparkSession, sample_df):
        """Test masked mode changes string values."""
        masker = Masker(mode=Mode.MASKED, seed=42)
        result = masker.process(sample_df)

        original_names = [row.name for row in sample_df.collect() if row.name]
        masked_names = [row.name for row in result.collect() if row.name]

        # Names should be different
        assert original_names != masked_names
        # Masked names should have prefix
        for name in masked_names:
            assert name.startswith("masked_")

    def test_masker_preserves_nulls(self, _spark: SparkSession, sample_df):
        """Test masker preserves null values."""
        masker = Masker(mode=Mode.MASKED)
        result = masker.process(sample_df)

        # Count nulls in original
        original_null_count = sample_df.filter(sample_df.name.isNull()).count()
        result_null_count = result.filter(result.name.isNull()).count()

        assert original_null_count == result_null_count

    def test_masker_synthetic_generates_data(self, _spark: SparkSession, sample_df):
        """Test synthetic mode generates new data."""
        masker = Masker(mode=Mode.SYNTHETIC, seed=42)
        result = masker.process(sample_df)

        # Should have same schema
        assert result.schema == sample_df.schema
        # Should have rows
        assert result.count() > 0
        # Values should be synthetic
        names = [row.name for row in result.collect() if row.name]
        for name in names:
            assert "synthetic_" in name

    def test_masker_deterministic(self, _spark: SparkSession, sample_df):
        """Test masker produces same results with same seed."""
        masker1 = Masker(mode=Mode.MASKED, seed=123)
        masker2 = Masker(mode=Mode.MASKED, seed=123)

        result1 = masker1.process(sample_df)
        result2 = masker2.process(sample_df)

        # Results should be identical
        rows1 = result1.collect()
        rows2 = result2.collect()

        assert len(rows1) == len(rows2)
        for r1, r2 in zip(rows1, rows2):
            assert r1 == r2
