"""Universal plan extraction for PySpark DataFrames.

This module provides safe, environment-agnostic plan extraction that works across:
- Classic PySpark (JVM-backed)
- Spark Connect
- Remote Spark clients
- Serverless Spark
- Cloud vendor-managed Spark
- Python-only clients without JVM handles

The module uses capability detection rather than environment heuristics,
ensuring graceful degradation when JVM internals are unavailable.
"""

from __future__ import annotations

import io
import sys
from contextlib import redirect_stdout
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class PlanType(Enum):
    """Type of execution plan."""

    LOGICAL = "logical"
    PHYSICAL = "physical"
    EXTENDED = "extended"  # Combined output from explain()


class ExtractionMethod(Enum):
    """Method used to extract the plan."""

    JVM_QUERY_EXECUTION = "jvm_query_execution"  # Direct JVM access
    EXPLAIN_STRING = "explain_string"  # df._explain_string() method
    EXPLAIN_CAPTURE = "explain_capture"  # Captured df.explain() output
    UNAVAILABLE = "unavailable"  # No method succeeded


@dataclass
class PlanResult:
    """Result of a plan extraction attempt.

    Attributes:
        plan: The extracted plan string, or error message if unavailable
        method: The method used to extract the plan
        success: Whether extraction was successful
        error: Error message if extraction failed, None otherwise
    """

    plan: str
    method: ExtractionMethod
    success: bool
    error: str | None = None

    @classmethod
    def unavailable(cls, reason: str) -> PlanResult:
        """Create an unavailable plan result."""
        return cls(
            plan=f"PLAN_UNAVAILABLE: {reason}",
            method=ExtractionMethod.UNAVAILABLE,
            success=False,
            error=reason,
        )


class PlanExtractor:
    """Safe plan extractor that works across all PySpark environments.

    This class uses capability detection to determine the best available
    method for extracting execution plans from a DataFrame. It never
    assumes JVM availability and provides clean fallbacks.

    Example:
        >>> extractor = PlanExtractor()
        >>> logical = extractor.get_logical_plan(df)
        >>> physical = extractor.get_physical_plan(df)
        >>> print(logical.plan)
    """

    def __init__(self) -> None:
        """Initialize the plan extractor."""
        # Cache for capability detection results
        self._jvm_available: bool | None = None

    def get_logical_plan(self, df: DataFrame) -> PlanResult:
        """Extract the logical execution plan from a DataFrame.

        Tries multiple extraction methods in order of preference:
        1. JVM queryExecution().logical() - most detailed
        2. df._explain_string() with mode="simple" - Spark 3.0+
        3. Captured df.explain() output - universal fallback

        Args:
            df: PySpark DataFrame

        Returns:
            PlanResult with the logical plan or error information
        """
        # Try JVM-based extraction first
        result = self._try_jvm_logical_plan(df)
        if result.success:
            return result

        # Try explain string method
        result = self._try_explain_string(df, mode="simple")
        if result.success:
            return result

        # Fall back to captured explain output
        result = self._try_explain_capture(df, extended=False)
        if result.success:
            return result

        return PlanResult.unavailable(
            "No plan extraction method available for this DataFrame type"
        )

    def get_physical_plan(self, df: DataFrame) -> PlanResult:
        """Extract the physical execution plan from a DataFrame.

        Tries multiple extraction methods in order of preference:
        1. JVM queryExecution().executedPlan() - most detailed
        2. df._explain_string() with mode="extended" - Spark 3.0+
        3. Captured df.explain(extended=True) output - universal fallback

        Args:
            df: PySpark DataFrame

        Returns:
            PlanResult with the physical plan or error information
        """
        # Try JVM-based extraction first
        result = self._try_jvm_physical_plan(df)
        if result.success:
            return result

        # Try explain string method with extended mode
        result = self._try_explain_string(df, mode="extended")
        if result.success:
            # Extract physical plan section from extended output
            plan = self._extract_physical_section(result.plan)
            return PlanResult(
                plan=plan,
                method=ExtractionMethod.EXPLAIN_STRING,
                success=True,
            )

        # Fall back to captured explain output
        result = self._try_explain_capture(df, extended=True)
        if result.success:
            # Extract physical plan section from extended output
            plan = self._extract_physical_section(result.plan)
            return PlanResult(
                plan=plan,
                method=ExtractionMethod.EXPLAIN_CAPTURE,
                success=True,
            )

        return PlanResult.unavailable(
            "No plan extraction method available for this DataFrame type"
        )

    def get_extended_plan(self, df: DataFrame) -> PlanResult:
        """Extract the full extended plan (logical + physical) from a DataFrame.

        This returns the complete explain output including analyzed logical plan,
        optimized logical plan, and physical plan.

        Args:
            df: PySpark DataFrame

        Returns:
            PlanResult with the extended plan or error information
        """
        # Try explain string method first
        result = self._try_explain_string(df, mode="extended")
        if result.success:
            return result

        # Fall back to captured explain output
        result = self._try_explain_capture(df, extended=True)
        if result.success:
            return result

        # Try JVM-based extraction and combine
        logical = self._try_jvm_logical_plan(df)
        physical = self._try_jvm_physical_plan(df)

        if logical.success and physical.success:
            combined = f"== Logical Plan ==\n{logical.plan}\n\n== Physical Plan ==\n{physical.plan}"
            return PlanResult(
                plan=combined,
                method=ExtractionMethod.JVM_QUERY_EXECUTION,
                success=True,
            )

        return PlanResult.unavailable(
            "No plan extraction method available for this DataFrame type"
        )

    def has_jvm_access(self, df: DataFrame) -> bool:
        """Check if JVM-based plan extraction is available for this DataFrame.

        Uses safe capability detection without raising exceptions.

        Args:
            df: PySpark DataFrame

        Returns:
            True if JVM internals are accessible, False otherwise
        """
        return self._check_jvm_capability(df)

    # =========================================================================
    # Private extraction methods
    # =========================================================================

    def _try_jvm_logical_plan(self, df: DataFrame) -> PlanResult:
        """Try to extract logical plan via JVM queryExecution.

        Args:
            df: PySpark DataFrame

        Returns:
            PlanResult with plan or failure information
        """
        if not self._check_jvm_capability(df):
            return PlanResult.unavailable("JVM internals not accessible")

        try:
            # Access the internal Java DataFrame
            jdf = df._jdf
            query_execution = jdf.queryExecution()
            logical_plan = query_execution.logical()
            plan_string = logical_plan.toString()

            return PlanResult(
                plan=plan_string,
                method=ExtractionMethod.JVM_QUERY_EXECUTION,
                success=True,
            )
        except Exception as e:
            return PlanResult.unavailable(f"JVM logical plan extraction failed: {e}")

    def _try_jvm_physical_plan(self, df: DataFrame) -> PlanResult:
        """Try to extract physical plan via JVM queryExecution.

        Args:
            df: PySpark DataFrame

        Returns:
            PlanResult with plan or failure information
        """
        if not self._check_jvm_capability(df):
            return PlanResult.unavailable("JVM internals not accessible")

        try:
            # Access the internal Java DataFrame
            jdf = df._jdf
            query_execution = jdf.queryExecution()
            physical_plan = query_execution.executedPlan()
            plan_string = physical_plan.toString()

            return PlanResult(
                plan=plan_string,
                method=ExtractionMethod.JVM_QUERY_EXECUTION,
                success=True,
            )
        except Exception as e:
            return PlanResult.unavailable(f"JVM physical plan extraction failed: {e}")

    def _try_explain_string(self, df: DataFrame, mode: str = "simple") -> PlanResult:
        """Try to extract plan using df._explain_string() method.

        This method is available in Spark 3.0+ and returns the plan as a string
        without printing to stdout.

        Args:
            df: PySpark DataFrame
            mode: Explain mode - "simple", "extended", "codegen", "cost", "formatted"

        Returns:
            PlanResult with plan or failure information
        """
        # Check if _explain_string method exists
        if not hasattr(df, "_explain_string"):
            return PlanResult.unavailable("_explain_string method not available")

        try:
            explain_string_method = getattr(df, "_explain_string")

            # Try calling with mode parameter (Spark 3.0+)
            try:
                plan_string = explain_string_method(mode)
            except TypeError:
                # Fallback for older signatures
                if mode == "extended":
                    plan_string = explain_string_method(True)
                else:
                    plan_string = explain_string_method(False)

            if plan_string and isinstance(plan_string, str):
                return PlanResult(
                    plan=plan_string,
                    method=ExtractionMethod.EXPLAIN_STRING,
                    success=True,
                )
            else:
                return PlanResult.unavailable("_explain_string returned empty result")

        except Exception as e:
            return PlanResult.unavailable(f"_explain_string failed: {e}")

    def _try_explain_capture(self, df: DataFrame, extended: bool = False) -> PlanResult:
        """Try to extract plan by capturing df.explain() stdout output.

        This is the most universal method as explain() is available on all
        DataFrame implementations, but it requires capturing stdout.

        Args:
            df: PySpark DataFrame
            extended: Whether to use extended explain mode

        Returns:
            PlanResult with plan or failure information
        """
        if not hasattr(df, "explain"):
            return PlanResult.unavailable("explain method not available")

        try:
            # Capture stdout
            captured = io.StringIO()

            with redirect_stdout(captured):
                # Try different explain signatures
                try:
                    # Spark 3.0+ with mode parameter
                    if extended:
                        df.explain(mode="extended")
                    else:
                        df.explain(mode="simple")
                except TypeError:
                    # Fallback for older Spark versions
                    df.explain(extended=extended)

            plan_string = captured.getvalue()

            if plan_string and plan_string.strip():
                return PlanResult(
                    plan=plan_string.strip(),
                    method=ExtractionMethod.EXPLAIN_CAPTURE,
                    success=True,
                )
            else:
                return PlanResult.unavailable("explain() produced no output")

        except Exception as e:
            return PlanResult.unavailable(f"explain capture failed: {e}")

    # =========================================================================
    # Utility methods
    # =========================================================================

    def _check_jvm_capability(self, df: DataFrame) -> bool:
        """Safely check if JVM internals are accessible.

        Uses capability detection rather than environment heuristics.
        Results are cached per DataFrame type for efficiency.

        Args:
            df: PySpark DataFrame

        Returns:
            True if JVM access is available, False otherwise
        """
        # Check if _jdf attribute exists
        if not hasattr(df, "_jdf"):
            return False

        try:
            # Try to access _jdf
            jdf = df._jdf

            # Check if it's None (can happen with Spark Connect)
            if jdf is None:
                return False

            # Try to access queryExecution - this will fail for Connect clients
            query_execution = jdf.queryExecution()

            # Verify we can call a method on it
            _ = query_execution.logical()

            return True

        except (AttributeError, TypeError, Exception):
            # Any error means JVM access is not available
            return False

    def _extract_physical_section(self, extended_plan: str) -> str:
        """Extract the physical plan section from extended explain output.

        Args:
            extended_plan: Full extended explain output

        Returns:
            Physical plan section, or original string if section not found
        """
        # Look for physical plan markers
        markers = [
            "== Physical Plan ==",
            "== Optimized Physical Plan ==",
            "Physical Plan:",
        ]

        for marker in markers:
            if marker in extended_plan:
                # Find the section after this marker
                parts = extended_plan.split(marker)
                if len(parts) >= 2:
                    physical_section = parts[1]

                    # Find the end of the physical plan section
                    # (next section marker or end of string)
                    end_markers = ["==", "\n\n\n"]
                    for end_marker in end_markers:
                        if end_marker in physical_section:
                            physical_section = physical_section.split(end_marker)[0]
                            break

                    return physical_section.strip()

        # If no markers found, return the whole thing
        return extended_plan


# =============================================================================
# Module-level convenience functions
# =============================================================================

# Singleton extractor instance
_extractor: PlanExtractor | None = None


def _get_extractor() -> PlanExtractor:
    """Get the singleton PlanExtractor instance."""
    global _extractor
    if _extractor is None:
        _extractor = PlanExtractor()
    return _extractor


def get_logical_plan(df: DataFrame) -> str:
    """Extract the logical execution plan from a DataFrame.

    This is a convenience function that uses the singleton PlanExtractor.

    Args:
        df: PySpark DataFrame

    Returns:
        Logical plan as a string, or error message if unavailable
    """
    result = _get_extractor().get_logical_plan(df)
    return result.plan


def get_physical_plan(df: DataFrame) -> str:
    """Extract the physical execution plan from a DataFrame.

    This is a convenience function that uses the singleton PlanExtractor.

    Args:
        df: PySpark DataFrame

    Returns:
        Physical plan as a string, or error message if unavailable
    """
    result = _get_extractor().get_physical_plan(df)
    return result.plan


def get_extended_plan(df: DataFrame) -> str:
    """Extract the full extended plan from a DataFrame.

    This is a convenience function that uses the singleton PlanExtractor.

    Args:
        df: PySpark DataFrame

    Returns:
        Extended plan as a string, or error message if unavailable
    """
    result = _get_extractor().get_extended_plan(df)
    return result.plan


def get_plan_result(df: DataFrame, plan_type: PlanType = PlanType.LOGICAL) -> PlanResult:
    """Extract a plan with full result metadata.

    Args:
        df: PySpark DataFrame
        plan_type: Type of plan to extract

    Returns:
        PlanResult with plan and extraction metadata
    """
    extractor = _get_extractor()

    if plan_type == PlanType.LOGICAL:
        return extractor.get_logical_plan(df)
    elif plan_type == PlanType.PHYSICAL:
        return extractor.get_physical_plan(df)
    else:
        return extractor.get_extended_plan(df)


def has_jvm_access(df: DataFrame) -> bool:
    """Check if JVM-based plan extraction is available.

    Args:
        df: PySpark DataFrame

    Returns:
        True if JVM internals are accessible
    """
    return _get_extractor().has_jvm_access(df)
