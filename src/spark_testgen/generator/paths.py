"""Path utilities for test generation."""

from __future__ import annotations

from pathlib import Path


class TestPaths:
    """Manages paths for generated test files and resources.

    Provides consistent path structure for:
    - Test file: tests/test_<function_name>.py
    - Resources: tests/resources/<function_name>/
        - input.parquet
        - output.parquet
        - edge_cases_input.parquet
        - plan_logical.txt
        - plan_physical.txt
    """

    def __init__(
        self,
        function_name: str,
        base_dir: Path | str = "tests",
    ) -> None:
        """Initialize test paths.

        Args:
            function_name: Name of the decorated function
            base_dir: Base directory for tests (default: "tests")
        """
        self.function_name = function_name
        self.base_dir = Path(base_dir)

        # Test file
        self.test_file = self.base_dir / f"test_{function_name}.py"

        # Resource directory
        self.resource_dir = self.base_dir / "resources" / function_name

        # Resource files
        self.input_parquet = self.resource_dir / "input.parquet"
        self.output_parquet = self.resource_dir / "output.parquet"
        self.edge_cases_parquet = self.resource_dir / "edge_cases_input.parquet"
        self.logical_plan = self.resource_dir / "plan_logical.txt"
        self.physical_plan = self.resource_dir / "plan_physical.txt"
        self.schema_json = self.resource_dir / "schema.json"

    def ensure_directories(self) -> None:
        """Create all necessary directories."""
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.resource_dir.mkdir(parents=True, exist_ok=True)

    def relative_resource_path(self) -> str:
        """Get relative path from test file to resource directory.

        Returns:
            Relative path string for use in generated test code
        """
        return f"resources/{self.function_name}"

    def all_paths(self) -> dict[str, Path]:
        """Get all paths as a dictionary.

        Returns:
            Dictionary mapping path names to Path objects
        """
        return {
            "test_file": self.test_file,
            "resource_dir": self.resource_dir,
            "input_parquet": self.input_parquet,
            "output_parquet": self.output_parquet,
            "edge_cases_parquet": self.edge_cases_parquet,
            "logical_plan": self.logical_plan,
            "physical_plan": self.physical_plan,
            "schema_json": self.schema_json,
        }

    def __repr__(self) -> str:
        return (
            f"TestPaths(function_name={self.function_name!r}, "
            f"base_dir={self.base_dir!r})"
        )
