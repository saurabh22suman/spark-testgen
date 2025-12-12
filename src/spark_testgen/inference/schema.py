"""Schema analysis utilities for DataFrame transformations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.types import DataType, StructType


@dataclass
class FieldInfo:
    """Information about a single schema field."""

    name: str
    data_type: str
    nullable: bool
    is_complex: bool = False  # True for ArrayType, MapType, StructType


@dataclass
class SchemaAnalysis:
    """Analysis results for a DataFrame schema.

    Attributes:
        fields: List of field information
        nullable_fields: Names of nullable fields
        non_nullable_fields: Names of non-nullable fields
        complex_types: Names of fields with complex types (array, map, struct)
        simple_types: Names of fields with simple types
    """

    fields: list[FieldInfo] = field(default_factory=list)
    nullable_fields: list[str] = field(default_factory=list)
    non_nullable_fields: list[str] = field(default_factory=list)
    complex_types: list[str] = field(default_factory=list)
    simple_types: list[str] = field(default_factory=list)


@dataclass
class SchemaComparison:
    """Comparison between input and output schemas.

    Attributes:
        added_fields: Fields in output but not in input
        removed_fields: Fields in input but not in output
        common_fields: Fields in both input and output
        type_changes: Fields with different types (field_name -> (old_type, new_type))
        nullable_changes: Fields with different nullability
    """

    added_fields: list[str] = field(default_factory=list)
    removed_fields: list[str] = field(default_factory=list)
    common_fields: list[str] = field(default_factory=list)
    type_changes: dict[str, tuple[str, str]] = field(default_factory=dict)
    nullable_changes: dict[str, tuple[bool, bool]] = field(default_factory=dict)


class SchemaAnalyzer:
    """Analyzes PySpark DataFrame schemas for test generation."""

    def analyze(self, schema: StructType) -> SchemaAnalysis:
        """Analyze a DataFrame schema.

        Args:
            schema: PySpark StructType schema

        Returns:
            SchemaAnalysis containing field details
        """
        analysis = SchemaAnalysis()

        for struct_field in schema.fields:
            is_complex = self._is_complex_type(struct_field.dataType)
            field_info = FieldInfo(
                name=struct_field.name,
                data_type=str(struct_field.dataType),
                nullable=struct_field.nullable,
                is_complex=is_complex,
            )
            analysis.fields.append(field_info)

            if struct_field.nullable:
                analysis.nullable_fields.append(struct_field.name)
            else:
                analysis.non_nullable_fields.append(struct_field.name)

            if is_complex:
                analysis.complex_types.append(struct_field.name)
            else:
                analysis.simple_types.append(struct_field.name)

        return analysis

    def compare(
        self, input_schema: StructType, output_schema: StructType
    ) -> SchemaComparison:
        """Compare input and output schemas.

        Args:
            input_schema: Schema of input DataFrame
            output_schema: Schema of output DataFrame

        Returns:
            SchemaComparison with differences
        """
        input_fields = {f.name: f for f in input_schema.fields}
        output_fields = {f.name: f for f in output_schema.fields}

        input_names = set(input_fields.keys())
        output_names = set(output_fields.keys())

        comparison = SchemaComparison(
            added_fields=sorted(output_names - input_names),
            removed_fields=sorted(input_names - output_names),
            common_fields=sorted(input_names & output_names),
        )

        # Check for type and nullability changes in common fields
        for name in comparison.common_fields:
            input_field = input_fields[name]
            output_field = output_fields[name]

            if str(input_field.dataType) != str(output_field.dataType):
                comparison.type_changes[name] = (
                    str(input_field.dataType),
                    str(output_field.dataType),
                )

            if input_field.nullable != output_field.nullable:
                comparison.nullable_changes[name] = (
                    input_field.nullable,
                    output_field.nullable,
                )

        return comparison

    def _is_complex_type(self, data_type: DataType) -> bool:
        """Check if a data type is complex (array, map, struct).

        Args:
            data_type: PySpark DataType

        Returns:
            True if complex type, False otherwise
        """
        type_name = type(data_type).__name__
        return type_name in ("ArrayType", "MapType", "StructType")

    def schema_to_code(self, schema: StructType) -> str:
        """Generate Python code that recreates the schema.

        Args:
            schema: PySpark StructType schema

        Returns:
            Python code string that creates the schema
        """
        lines = ["StructType(["]
        for struct_field in schema.fields:
            type_code = self._type_to_code(struct_field.dataType)
            nullable = str(struct_field.nullable)
            lines.append(
                f'    StructField("{struct_field.name}", {type_code}, {nullable}),'
            )
        lines.append("])")
        return "\n".join(lines)

    def _type_to_code(self, data_type: DataType) -> str:
        """Convert a DataType to Python code.

        Args:
            data_type: PySpark DataType

        Returns:
            Python code string for the type
        """
        type_name = type(data_type).__name__

        # Simple types
        simple_types = {
            "StringType",
            "IntegerType",
            "LongType",
            "DoubleType",
            "FloatType",
            "BooleanType",
            "DateType",
            "TimestampType",
            "BinaryType",
            "ByteType",
            "ShortType",
            "NullType",
        }

        if type_name in simple_types:
            return f"{type_name}()"

        # DecimalType with precision and scale
        if type_name == "DecimalType":
            return f"DecimalType({data_type.precision}, {data_type.scale})"

        # ArrayType
        if type_name == "ArrayType":
            element_code = self._type_to_code(data_type.elementType)
            return f"ArrayType({element_code}, {data_type.containsNull})"

        # MapType
        if type_name == "MapType":
            key_code = self._type_to_code(data_type.keyType)
            value_code = self._type_to_code(data_type.valueType)
            return f"MapType({key_code}, {value_code}, {data_type.valueContainsNull})"

        # StructType (nested)
        if type_name == "StructType":
            inner_fields = []
            for f in data_type.fields:
                inner_type = self._type_to_code(f.dataType)
                inner_fields.append(
                    f'StructField("{f.name}", {inner_type}, {f.nullable})'
                )
            fields_str = ", ".join(inner_fields)
            return f"StructType([{fields_str}])"

        # Fallback for unknown types
        return f"{type_name}()"
