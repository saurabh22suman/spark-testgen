"""Resource writer for test data files."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

from ..utils import is_serverless_environment
from .paths import TestPaths

logger = logging.getLogger(__name__)


class ResourceWriter:
    """Writes test resource files (parquet, plans, schemas).

    Handles:
    - Parquet files for input/output snapshots
    - Edge case parquet files
    - Logical and physical plan text files
    - Schema JSON files
    """

    def __init__(self, overwrite: bool = True) -> None:
        """Initialize resource writer.

        Args:
            overwrite: Whether to overwrite existing files
        """
        self.overwrite = overwrite

    def write(
        self,
        input_df: DataFrame,
        output_df: DataFrame,
        logical_plan: str,
        physical_plan: str,
        paths: TestPaths,
    ) -> None:
        """Write all resource files.

        Args:
            input_df: Input DataFrame (masked/synthetic)
            output_df: Output DataFrame (masked/synthetic)
            logical_plan: Logical execution plan text
            physical_plan: Physical execution plan text
            paths: TestPaths instance with file locations
        """
        paths.ensure_directories()

        # Write parquet files
        self._write_dataframe(input_df, paths.input_parquet)
        self._write_dataframe(output_df, paths.output_parquet)

        # Write plan files
        self._write_text(logical_plan, paths.logical_plan)
        self._write_text(physical_plan, paths.physical_plan)

        # Write schema JSON
        self._write_schema(output_df.schema, paths.schema_json)

        logger.info(f"Wrote resources to {paths.resource_dir}")

    def write_edge_cases(self, edge_cases_df: DataFrame, paths: TestPaths) -> None:
        """Write edge cases parquet file.

        Args:
            edge_cases_df: Edge cases DataFrame
            paths: TestPaths instance with file locations
        """
        self._write_dataframe(edge_cases_df, paths.edge_cases_parquet)
        logger.info(f"Wrote edge cases to {paths.edge_cases_parquet}")

    def _write_dataframe(self, df: DataFrame, path: Path) -> None:
        """Write DataFrame to parquet.

        Supports multiple execution environments:
        - Classic PySpark with local filesystem access
        - Spark Connect (uses pandas fallback)
        - Serverless Spark (uses pandas fallback)

        Args:
            df: DataFrame to write
            path: Target path
        """
        path_str = str(path)

        # Remove existing if overwrite
        if self.overwrite and path.exists():
            import shutil

            shutil.rmtree(path_str)

        # Check for serverless/Connect environment BEFORE trying Spark write
        # This avoids deferred execution errors where the write appears to
        # succeed but fails later during query execution
        if is_serverless_environment(df):
            logger.debug("Serverless environment detected, using pandas for write")
            self._write_dataframe_via_pandas(df, path)
            return

        # Try native Spark write for classic PySpark
        try:
            # Coalesce to single file for simplicity
            df.coalesce(1).write.mode("overwrite" if self.overwrite else "error").parquet(path_str)
            logger.debug(f"Wrote DataFrame to {path_str} (native Spark)")
            return
        except BaseException as e:
            logger.debug(f"Native Spark write failed, falling back to pandas: {e}")

        # Fallback: Use pandas
        self._write_dataframe_via_pandas(df, path)

    def _write_dataframe_via_pandas(self, df: DataFrame, path: Path) -> None:
        """Write DataFrame to parquet using pandas as a fallback.

        This is used when native Spark write is not available (e.g., serverless).
        Note: This collects data to the driver, so it's only suitable for
        test data which should be small.

        Args:
            df: DataFrame to write
            path: Target path
        """
        try:
            # Collect to pandas
            pdf = df.toPandas()

            # Ensure directory exists
            path.mkdir(parents=True, exist_ok=True)

            # Write as parquet
            parquet_file = path / "part-00000.parquet"
            pdf.to_parquet(parquet_file, index=False, engine="pyarrow")

            logger.debug(f"Wrote DataFrame to {path} (via pandas)")
        except ImportError as e:
            raise RuntimeError(
                "pandas or pyarrow is required for writing DataFrames in "
                "serverless/Connect environments. Install with: "
                "pip install pandas pyarrow"
            ) from e
        except BaseException as e:
            raise RuntimeError(f"Failed to write DataFrame via pandas fallback: {e}") from e

    def _write_text(self, content: str, path: Path) -> None:
        """Write text content to file.

        Args:
            content: Text content
            path: Target path
        """
        if not self.overwrite and path.exists():
            logger.warning(f"Skipping existing file: {path}")
            return

        path.write_text(content, encoding="utf-8")
        logger.debug(f"Wrote text to {path}")

    def _write_schema(self, schema: StructType, path: Path) -> None:
        """Write schema as JSON.

        Args:
            schema: PySpark StructType
            path: Target path
        """
        if not self.overwrite and path.exists():
            logger.warning(f"Skipping existing file: {path}")
            return

        schema_dict = json.loads(schema.json())
        path.write_text(
            json.dumps(schema_dict, indent=2),
            encoding="utf-8",
        )
        logger.debug(f"Wrote schema to {path}")
