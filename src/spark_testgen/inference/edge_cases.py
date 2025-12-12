"""Edge case synthesis for DataFrame test generation."""

from __future__ import annotations

import random
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


class EdgeCaseSynthesizer:
    """Synthesizes edge case data for testing DataFrame transformations.

    Generates rows with:
    - Null values in all nullable columns
    - Empty strings
    - Boundary numeric values (0, -1, max, min)
    - Edge case dates (epoch, far future)
    - Empty arrays/maps
    """

    def __init__(self, seed: int = 42) -> None:
        """Initialize edge case synthesizer.

        Args:
            seed: Random seed for reproducibility
        """
        self.seed = seed
        self._random = random.Random(seed)

    def generate(
        self,
        schema: StructType,
        spark: SparkSession,
        max_rows: int = 10,
    ) -> DataFrame | None:
        """Generate edge case DataFrame for testing.

        Args:
            schema: Schema to generate edge cases for
            spark: SparkSession for creating DataFrame
            max_rows: Maximum number of edge case rows

        Returns:
            DataFrame with edge case rows, or None if no edge cases applicable
        """
        edge_cases = []

        # 1. All nulls row (if any nullable columns)
        all_nulls = self._generate_all_nulls_row(schema)
        if all_nulls:
            edge_cases.append(all_nulls)

        # 2. Empty strings row
        empty_strings = self._generate_empty_strings_row(schema)
        if empty_strings:
            edge_cases.append(empty_strings)

        # 3. Boundary values row (zeros, mins, maxs)
        boundary_row = self._generate_boundary_row(schema)
        if boundary_row:
            edge_cases.append(boundary_row)

        # 4. Negative values row
        negative_row = self._generate_negative_row(schema)
        if negative_row:
            edge_cases.append(negative_row)

        # 5. Edge case dates
        date_edge_row = self._generate_date_edge_row(schema)
        if date_edge_row:
            edge_cases.append(date_edge_row)

        # 6. Very long strings
        long_strings_row = self._generate_long_strings_row(schema)
        if long_strings_row:
            edge_cases.append(long_strings_row)

        # 7. Mixed edge cases
        for i in range(min(4, max_rows - len(edge_cases))):
            mixed_row = self._generate_mixed_edge_row(schema, i)
            if mixed_row:
                edge_cases.append(mixed_row)

        if not edge_cases:
            return None

        # Limit to max_rows
        edge_cases = edge_cases[:max_rows]

        return spark.createDataFrame(edge_cases, schema)

    def _generate_all_nulls_row(self, schema: StructType) -> tuple[Any, ...] | None:
        """Generate row with all nullable columns as null."""
        from pyspark.sql.types import (
            ArrayType,
            BooleanType,
            DateType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            MapType,
            StringType,
            StructType,
            TimestampType,
        )

        values = []
        has_nullable = False

        for field in schema.fields:
            if field.nullable:
                values.append(None)
                has_nullable = True
            else:
                # Non-nullable: provide default value
                values.append(self._default_value(field.dataType))

        return tuple(values) if has_nullable else None

    def _generate_empty_strings_row(self, schema: StructType) -> tuple[Any, ...] | None:
        """Generate row with empty strings where applicable."""
        from pyspark.sql.types import StringType

        values = []
        has_string = False

        for field in schema.fields:
            if isinstance(field.dataType, StringType):
                values.append("")
                has_string = True
            else:
                values.append(self._default_value(field.dataType))

        return tuple(values) if has_string else None

    def _generate_boundary_row(self, schema: StructType) -> tuple[Any, ...] | None:
        """Generate row with boundary numeric values."""
        from pyspark.sql.types import (
            ByteType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            ShortType,
        )

        values = []
        has_numeric = False

        numeric_bounds = {
            IntegerType: 2147483647,  # Max int32
            LongType: 9223372036854775807,  # Max int64
            ShortType: 32767,
            ByteType: 127,
            FloatType: 3.4028235e38,
            DoubleType: 1.7976931348623157e308,
        }

        for field in schema.fields:
            data_type = type(field.dataType)
            if data_type in numeric_bounds:
                values.append(numeric_bounds[data_type])
                has_numeric = True
            else:
                values.append(self._default_value(field.dataType))

        return tuple(values) if has_numeric else None

    def _generate_negative_row(self, schema: StructType) -> tuple[Any, ...] | None:
        """Generate row with negative numeric values."""
        from pyspark.sql.types import (
            ByteType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            ShortType,
        )

        values = []
        has_numeric = False

        for field in schema.fields:
            data_type = type(field.dataType)
            if data_type in (IntegerType, LongType, ShortType, ByteType):
                values.append(-1)
                has_numeric = True
            elif data_type in (FloatType, DoubleType):
                values.append(-1.0)
                has_numeric = True
            else:
                values.append(self._default_value(field.dataType))

        return tuple(values) if has_numeric else None

    def _generate_date_edge_row(self, schema: StructType) -> tuple[Any, ...] | None:
        """Generate row with edge case dates."""
        from pyspark.sql.types import DateType, TimestampType

        values = []
        has_date = False

        for field in schema.fields:
            if isinstance(field.dataType, DateType):
                # Epoch date
                values.append(date(1970, 1, 1))
                has_date = True
            elif isinstance(field.dataType, TimestampType):
                # Epoch timestamp
                values.append(datetime(1970, 1, 1, 0, 0, 0))
                has_date = True
            else:
                values.append(self._default_value(field.dataType))

        return tuple(values) if has_date else None

    def _generate_long_strings_row(self, schema: StructType) -> tuple[Any, ...] | None:
        """Generate row with very long strings."""
        from pyspark.sql.types import StringType

        values = []
        has_string = False

        for field in schema.fields:
            if isinstance(field.dataType, StringType):
                # Generate a long string (1000 chars)
                values.append("x" * 1000)
                has_string = True
            else:
                values.append(self._default_value(field.dataType))

        return tuple(values) if has_string else None

    def _generate_mixed_edge_row(
        self, schema: StructType, idx: int
    ) -> tuple[Any, ...] | None:
        """Generate row with mixed edge cases."""
        values = []

        edge_cases_per_type = {
            "StringType": ["", " ", "\n", "\t", "NULL", "null", "None"],
            "IntegerType": [0, -1, 1, 2147483647, -2147483648],
            "LongType": [0, -1, 1],
            "DoubleType": [0.0, -0.0, float("inf"), float("-inf")],
            "FloatType": [0.0, -0.0],
            "BooleanType": [True, False],
        }

        for field in schema.fields:
            type_name = type(field.dataType).__name__

            if type_name in edge_cases_per_type:
                options = edge_cases_per_type[type_name]
                value = options[idx % len(options)]
                values.append(value)
            else:
                values.append(self._default_value(field.dataType))

        return tuple(values) if values else None

    def _default_value(self, data_type: Any) -> Any:
        """Get a default value for a data type.

        Args:
            data_type: PySpark DataType

        Returns:
            Default value for the type
        """
        from pyspark.sql.types import (
            ArrayType,
            BinaryType,
            BooleanType,
            ByteType,
            DateType,
            DecimalType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            MapType,
            ShortType,
            StringType,
            StructType,
            TimestampType,
        )

        if isinstance(data_type, StringType):
            return "default"

        if isinstance(data_type, (IntegerType, LongType, ShortType, ByteType)):
            return 0

        if isinstance(data_type, (DoubleType, FloatType)):
            return 0.0

        if isinstance(data_type, DecimalType):
            return Decimal("0")

        if isinstance(data_type, BooleanType):
            return False

        if isinstance(data_type, DateType):
            return date(2024, 1, 1)

        if isinstance(data_type, TimestampType):
            return datetime(2024, 1, 1, 0, 0, 0)

        if isinstance(data_type, BinaryType):
            return b""

        if isinstance(data_type, ArrayType):
            return []

        if isinstance(data_type, MapType):
            return {}

        if isinstance(data_type, StructType):
            # Recursively generate default struct
            inner_values = [
                self._default_value(f.dataType) for f in data_type.fields
            ]
            return tuple(inner_values)

        return None
