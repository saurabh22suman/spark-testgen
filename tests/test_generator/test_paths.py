"""Tests for paths module."""

from __future__ import annotations

from pathlib import Path

import pytest

from spark_testgen.generator.paths import TestPaths


class TestTestPaths:
    """Tests for TestPaths class."""

    def test_paths_initialization(self):
        """Test paths are correctly constructed."""
        paths = TestPaths("my_transform")

        assert paths.function_name == "my_transform"
        assert paths.test_file == Path("tests/test_my_transform.py")
        assert paths.resource_dir == Path("tests/resources/my_transform")
        assert paths.input_parquet == Path("tests/resources/my_transform/input.parquet")
        assert paths.output_parquet == Path("tests/resources/my_transform/output.parquet")

    def test_paths_custom_base_dir(self):
        """Test paths with custom base directory."""
        paths = TestPaths("func", base_dir="custom_tests")

        assert paths.test_file == Path("custom_tests/test_func.py")
        assert paths.resource_dir == Path("custom_tests/resources/func")

    def test_ensure_directories(self, tmp_path):
        """Test directory creation."""
        paths = TestPaths("test_func", base_dir=tmp_path / "tests")
        paths.ensure_directories()

        assert paths.base_dir.exists()
        assert paths.resource_dir.exists()

    def test_relative_resource_path(self):
        """Test relative path generation."""
        paths = TestPaths("my_transform")
        assert paths.relative_resource_path() == "resources/my_transform"

    def test_all_paths(self):
        """Test all_paths returns complete dict."""
        paths = TestPaths("func")
        all_paths = paths.all_paths()

        assert "test_file" in all_paths
        assert "resource_dir" in all_paths
        assert "input_parquet" in all_paths
        assert "output_parquet" in all_paths
        assert "edge_cases_parquet" in all_paths
        assert "logical_plan" in all_paths
        assert "physical_plan" in all_paths
