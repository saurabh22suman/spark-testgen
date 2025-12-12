"""Execution plan analysis for DataFrame transformations."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .plan_extractor import (
    ExtractionMethod,
    PlanExtractor,
    get_logical_plan,
    get_physical_plan,
    has_jvm_access,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass
class PlanAnalysis:
    """Analysis results for execution plans.

    Attributes:
        raw_logical_plan: Original logical plan string
        raw_physical_plan: Original physical plan string
        normalized_logical_plan: Plan with volatile parts removed
        normalized_physical_plan: Plan with volatile parts removed
        detected_operations: List of detected Spark operations
        extraction_method: Method used to extract the plans
        jvm_available: Whether JVM-based extraction was available
    """

    raw_logical_plan: str
    raw_physical_plan: str
    normalized_logical_plan: str
    normalized_physical_plan: str
    detected_operations: list[str]
    extraction_method: ExtractionMethod = ExtractionMethod.UNAVAILABLE
    jvm_available: bool = False


class PlanAnalyzer:
    """Analyzes PySpark execution plans for test generation.

    Provides utilities to extract and normalize plans for soft regression testing.
    Plans are normalized to remove volatile parts (IDs, timestamps, paths) that
    would cause false positives in comparison tests.

    This analyzer uses the universal PlanExtractor which works across all
    PySpark environments including Spark Connect and serverless Spark.
    """

    # Patterns to remove from plans for normalization
    VOLATILE_PATTERNS = [
        # Object IDs and hash codes
        (r"#\d+", "#ID"),
        (r"@[0-9a-f]+", "@HASH"),
        # File paths
        (r"file:/[^\s,\]]+", "file:/PATH"),
        (r"hdfs:/[^\s,\]]+", "hdfs:/PATH"),
        (r"s3[an]?:/[^\s,\]]+", "s3:/PATH"),
        # Timestamps and dates in various formats
        (r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[.\d]*Z?", "TIMESTAMP"),
        # Numeric IDs
        (r"id=\d+", "id=ID"),
        (r"operatorId=\d+", "operatorId=ID"),
        # Memory addresses
        (r"0x[0-9a-fA-F]+", "0xADDR"),
        # Partition counts (can vary)
        (r"numPartitions=\d+", "numPartitions=N"),
        # Batch IDs
        (r"batchId=\d+", "batchId=ID"),
    ]

    def __init__(self) -> None:
        """Initialize the plan analyzer with a universal extractor."""
        self._extractor = PlanExtractor()

    def extract_logical_plan(self, df: DataFrame) -> str:
        """Extract logical execution plan from DataFrame.

        Uses the universal PlanExtractor which works across all environments.

        Args:
            df: DataFrame to extract plan from

        Returns:
            String representation of logical plan
        """
        return get_logical_plan(df)

    def extract_physical_plan(self, df: DataFrame) -> str:
        """Extract physical execution plan from DataFrame.

        Uses the universal PlanExtractor which works across all environments.

        Args:
            df: DataFrame to extract plan from

        Returns:
            String representation of physical plan
        """
        return get_physical_plan(df)

    def normalize_plan(self, plan: str) -> str:
        """Normalize a plan by removing volatile parts.

        This makes plans comparable across different runs by removing:
        - Object IDs and hash codes
        - File paths
        - Timestamps
        - Memory addresses
        - Partition counts

        Args:
            plan: Raw plan string

        Returns:
            Normalized plan string suitable for comparison
        """
        normalized = plan

        for pattern, replacement in self.VOLATILE_PATTERNS:
            normalized = re.sub(pattern, replacement, normalized)

        # Normalize whitespace
        normalized = re.sub(r"\s+", " ", normalized)
        normalized = normalized.strip()

        return normalized

    def analyze(self, df: DataFrame) -> PlanAnalysis:
        """Perform full plan analysis on a DataFrame.

        Uses the universal PlanExtractor for environment-agnostic extraction.

        Args:
            df: DataFrame to analyze

        Returns:
            PlanAnalysis with raw and normalized plans, plus extraction metadata
        """
        # Get plan results with metadata
        logical_result = self._extractor.get_logical_plan(df)
        physical_result = self._extractor.get_physical_plan(df)

        logical = logical_result.plan
        physical = physical_result.plan

        # Use the method from logical plan (or physical if logical failed)
        extraction_method = (
            logical_result.method if logical_result.success else physical_result.method
        )

        return PlanAnalysis(
            raw_logical_plan=logical,
            raw_physical_plan=physical,
            normalized_logical_plan=self.normalize_plan(logical),
            normalized_physical_plan=self.normalize_plan(physical),
            detected_operations=self._detect_operations(logical),
            extraction_method=extraction_method,
            jvm_available=has_jvm_access(df),
        )

    def _detect_operations(self, logical_plan: str) -> list[str]:
        """Detect common Spark operations from logical plan.

        Args:
            logical_plan: Logical plan string

        Returns:
            List of detected operation names
        """
        operations = []

        # Common operations to detect
        operation_patterns = [
            ("Project", r"Project\s*\["),
            ("Filter", r"Filter\s*\("),
            ("Join", r"Join\s+\w+"),
            ("Aggregate", r"Aggregate\s*\["),
            ("Sort", r"Sort\s*\["),
            ("Window", r"Window\s*\["),
            ("Union", r"Union\b"),
            ("Distinct", r"Distinct\b"),
            ("Limit", r"(?:Global)?Limit\s+\d+"),
            ("Repartition", r"Repartition\b"),
            ("SubqueryAlias", r"SubqueryAlias\b"),
        ]

        for name, pattern in operation_patterns:
            if re.search(pattern, logical_plan):
                operations.append(name)

        return operations

    def plans_similar(
        self,
        plan1: str,
        plan2: str,
        similarity_threshold: float = 0.8,
    ) -> bool:
        """Check if two plans are similar enough for soft regression.

        Uses normalized plans and a similarity metric to allow minor differences.

        Args:
            plan1: First plan string (raw)
            plan2: Second plan string (raw)
            similarity_threshold: Minimum similarity ratio (0.0-1.0)

        Returns:
            True if plans are similar enough
        """
        norm1 = self.normalize_plan(plan1)
        norm2 = self.normalize_plan(plan2)

        # Quick check for exact match
        if norm1 == norm2:
            return True

        # Simple similarity based on common words
        words1 = set(norm1.split())
        words2 = set(norm2.split())

        if not words1 or not words2:
            return False

        intersection = len(words1 & words2)
        union = len(words1 | words2)

        similarity = intersection / union if union > 0 else 0.0

        return similarity >= similarity_threshold
