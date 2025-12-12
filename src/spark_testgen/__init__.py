"""
spark-testgen: Auto-generate pytest test suites for PySpark DataFrame transformations.

Usage:
    from spark_testgen import autogen_tests

    @autogen_tests
    def transform(df):
        return df.withColumn("flag", df.value > 10)

Then run with SPARK_TESTGEN=1 to generate tests.
"""

from __future__ import annotations

try:
    from ._version import version as __version__
except ImportError:
    __version__ = "0.0.0.dev0"  # Fallback for editable installs without scm

from .decorator import autogen_tests

__all__ = ["autogen_tests", "__version__"]
