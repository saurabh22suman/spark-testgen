"""Data masking and synthetic data generation for test resources."""

from __future__ import annotations

import hashlib
import random
import string
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from ..config import Mode


class Masker:
    """Masks or synthesizes data for security-safe test resources.

    Supports three modes:
    - MASKED: Replace real values with hashed/transformed values
    - SYNTHETIC: Generate fully synthetic data matching schema
    - UNSAFE_RAW: Pass through raw data (opt-in only)
    """

    def __init__(self, mode: Mode = Mode.MASKED, seed: int = 42) -> None:
        """Initialize masker.

        Args:
            mode: Data handling mode
            seed: Random seed for deterministic generation
        """
        self.mode = mode
        self.seed = seed
        self._random = random.Random(seed)

    def process(self, df: DataFrame) -> DataFrame:
        """Process a DataFrame according to the configured mode.

        Args:
            df: DataFrame to process

        Returns:
            Processed DataFrame (masked, synthetic, or raw)
        """
        if self.mode == Mode.UNSAFE_RAW:
            return df

        if self.mode == Mode.SYNTHETIC:
            return self._generate_synthetic(df)

        # Default: MASKED
        return self._mask_dataframe(df)

    def _mask_dataframe(self, df: DataFrame) -> DataFrame:
        """Mask sensitive data in DataFrame.

        Applies type-appropriate masking:
        - Strings: Hash-based replacement
        - Numbers: Preserve range but change values
        - Dates: Shift by random offset
        - Complex types: Recurse into structure

        Args:
            df: DataFrame to mask

        Returns:
            Masked DataFrame
        """
        from pyspark.sql import functions as F
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

        result_df = df
        schema = df.schema

        for field in schema.fields:
            col_name = field.name
            data_type = field.dataType
            type_name = type(data_type).__name__

            if isinstance(data_type, StringType):
                # Hash strings to preserve uniqueness
                result_df = result_df.withColumn(
                    col_name,
                    F.when(
                        F.col(col_name).isNull(),
                        F.lit(None),
                    ).otherwise(
                        F.concat(
                            F.lit("masked_"),
                            F.substring(F.sha2(F.col(col_name), 256), 1, 8),
                        )
                    ),
                )

            elif isinstance(data_type, (IntegerType, LongType)):
                # Add random offset to integers
                offset = self._random.randint(-1000, 1000)
                result_df = result_df.withColumn(
                    col_name,
                    F.when(
                        F.col(col_name).isNull(),
                        F.lit(None),
                    ).otherwise(
                        F.abs(F.col(col_name) + offset) % 10000
                    ),
                )

            elif isinstance(data_type, (DoubleType, FloatType)):
                # Scale doubles/floats
                scale = self._random.uniform(0.5, 2.0)
                result_df = result_df.withColumn(
                    col_name,
                    F.when(
                        F.col(col_name).isNull(),
                        F.lit(None),
                    ).otherwise(
                        F.round(F.col(col_name) * scale, 2)
                    ),
                )

            elif isinstance(data_type, DateType):
                # Shift dates by random days
                days_offset = self._random.randint(-365, 365)
                result_df = result_df.withColumn(
                    col_name,
                    F.when(
                        F.col(col_name).isNull(),
                        F.lit(None),
                    ).otherwise(
                        F.date_add(F.col(col_name), days_offset)
                    ),
                )

            elif isinstance(data_type, TimestampType):
                # Shift timestamps by random seconds
                seconds_offset = self._random.randint(-86400 * 30, 86400 * 30)
                result_df = result_df.withColumn(
                    col_name,
                    F.when(
                        F.col(col_name).isNull(),
                        F.lit(None),
                    ).otherwise(
                        F.col(col_name) + F.expr(f"INTERVAL {seconds_offset} SECONDS")
                    ),
                )

            elif isinstance(data_type, BooleanType):
                # Keep booleans as-is (not sensitive)
                pass

            elif isinstance(data_type, (ArrayType, MapType, StructType)):
                # Complex types: convert to JSON, mask, convert back
                # For now, we'll just keep them as-is
                # TODO: Deep masking for complex types
                pass

        return result_df

    def _generate_synthetic(self, df: DataFrame) -> DataFrame:
        """Generate fully synthetic data matching schema.

        Args:
            df: DataFrame whose schema to match

        Returns:
            DataFrame with synthetic data
        """
        spark = df.sparkSession
        schema = df.schema
        row_count = min(df.count(), 20)  # Limit synthetic rows

        synthetic_data = []
        for i in range(row_count):
            row = self._generate_synthetic_row(schema, i)
            synthetic_data.append(row)

        return spark.createDataFrame(synthetic_data, schema)

    def _generate_synthetic_row(self, schema: Any, row_idx: int) -> tuple[Any, ...]:
        """Generate a single synthetic row.

        Args:
            schema: Schema to match
            row_idx: Row index for deterministic variation

        Returns:
            Tuple of synthetic values
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

        values = []
        for field in schema.fields:
            data_type = field.dataType

            # Occasionally generate nulls for nullable fields
            if field.nullable and self._random.random() < 0.1:
                values.append(None)
                continue

            value = self._generate_value_for_type(data_type, row_idx)
            values.append(value)

        return tuple(values)

    def _generate_value_for_type(self, data_type: Any, idx: int) -> Any:
        """Generate a synthetic value for a data type.

        Args:
            data_type: PySpark DataType
            idx: Index for variation

        Returns:
            Synthetic value
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
        from decimal import Decimal

        type_name = type(data_type).__name__

        if isinstance(data_type, StringType):
            return f"synthetic_{idx}_{self._random_string(6)}"

        if isinstance(data_type, IntegerType):
            return self._random.randint(0, 10000) + idx

        if isinstance(data_type, LongType):
            return self._random.randint(0, 1000000) + idx

        if isinstance(data_type, ShortType):
            return self._random.randint(0, 100) + idx

        if isinstance(data_type, ByteType):
            return self._random.randint(0, 127)

        if isinstance(data_type, DoubleType):
            return round(self._random.uniform(0, 1000) + idx * 0.1, 4)

        if isinstance(data_type, FloatType):
            return round(self._random.uniform(0, 100) + idx * 0.1, 2)

        if isinstance(data_type, DecimalType):
            return Decimal(str(round(self._random.uniform(0, 1000), data_type.scale)))

        if isinstance(data_type, BooleanType):
            return self._random.choice([True, False])

        if isinstance(data_type, DateType):
            base = date(2024, 1, 1)
            return base + timedelta(days=idx + self._random.randint(0, 365))

        if isinstance(data_type, TimestampType):
            base = datetime(2024, 1, 1, 12, 0, 0)
            return base + timedelta(
                days=idx, seconds=self._random.randint(0, 86400)
            )

        if isinstance(data_type, BinaryType):
            return bytes([self._random.randint(0, 255) for _ in range(8)])

        if isinstance(data_type, ArrayType):
            element_type = data_type.elementType
            length = self._random.randint(1, 3)
            return [
                self._generate_value_for_type(element_type, idx + i)
                for i in range(length)
            ]

        if isinstance(data_type, MapType):
            key_type = data_type.keyType
            value_type = data_type.valueType
            return {
                self._generate_value_for_type(key_type, idx): self._generate_value_for_type(
                    value_type, idx
                )
            }

        if isinstance(data_type, StructType):
            return self._generate_synthetic_row(data_type, idx)

        # Unknown type - return None
        return None

    def _random_string(self, length: int) -> str:
        """Generate a random alphanumeric string.

        Args:
            length: String length

        Returns:
            Random string
        """
        chars = string.ascii_lowercase + string.digits
        return "".join(self._random.choice(chars) for _ in range(length))
