"""Tests for the schema analyzer."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from spark_testgen.inference.schema import SchemaAnalyzer, SchemaAnalysis, SchemaComparison


class TestSchemaAnalyzer:
    """Tests for SchemaAnalyzer class."""

    def test_analyze_simple_schema(self, spark: SparkSession, sample_df):
        """Test analyzing a simple schema."""
        analyzer = SchemaAnalyzer()
        analysis = analyzer.analyze(sample_df.schema)

        assert len(analysis.fields) == 4
        assert "id" in [f.name for f in analysis.fields]
        assert "name" in [f.name for f in analysis.fields]
        assert len(analysis.nullable_fields) > 0

    def test_analyze_identifies_complex_types(self, spark: SparkSession, complex_schema_df):
        """Test analyzer identifies complex types."""
        analyzer = SchemaAnalyzer()
        analysis = analyzer.analyze(complex_schema_df.schema)

        assert "tags" in analysis.complex_types  # ArrayType
        assert "metadata" in analysis.complex_types  # MapType
        assert "nested" in analysis.complex_types  # StructType
        assert "id" in analysis.simple_types

    def test_compare_schemas_added_fields(self, spark: SparkSession, sample_df):
        """Test comparing schemas with added fields."""
        analyzer = SchemaAnalyzer()

        output_df = sample_df.withColumn("new_col", sample_df.value * 2)
        comparison = analyzer.compare(sample_df.schema, output_df.schema)

        assert "new_col" in comparison.added_fields
        assert len(comparison.removed_fields) == 0
        assert "id" in comparison.common_fields

    def test_compare_schemas_removed_fields(self, spark: SparkSession, sample_df):
        """Test comparing schemas with removed fields."""
        analyzer = SchemaAnalyzer()

        output_df = sample_df.drop("name")
        comparison = analyzer.compare(sample_df.schema, output_df.schema)

        assert "name" in comparison.removed_fields
        assert len(comparison.added_fields) == 0

    def test_schema_to_code(self, spark: SparkSession):
        """Test generating Python code for schema."""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
        ])

        analyzer = SchemaAnalyzer()
        code = analyzer.schema_to_code(schema)

        assert "StructType" in code
        assert "StructField" in code
        assert '"id"' in code
        assert "IntegerType()" in code
        assert "False" in code  # nullable
