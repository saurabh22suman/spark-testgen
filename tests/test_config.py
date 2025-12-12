"""Tests for the config module."""

from __future__ import annotations

import os
from unittest import mock

import pytest

from spark_testgen.config import (
    Config,
    Mode,
    SPARK_TESTGEN_ENV,
    SPARK_TESTGEN_MODE_ENV,
    get_config,
    reset_config,
)


class TestMode:
    """Tests for Mode enum."""

    def test_mode_values(self):
        """Verify mode enum values."""
        assert Mode.MASKED.value == "masked"
        assert Mode.SYNTHETIC.value == "synthetic"
        assert Mode.UNSAFE_RAW.value == "unsafe_raw"


class TestConfig:
    """Tests for Config class."""

    def setup_method(self):
        """Reset config before each test."""
        reset_config()

    def teardown_method(self):
        """Reset config after each test."""
        reset_config()

    def test_default_values(self):
        """Verify default config values."""
        config = Config()
        assert config.enabled is False
        assert config.mode == Mode.MASKED
        assert config.seed == 42
        assert config.sample_size == 20
        assert config.max_edge_cases == 10

    @mock.patch.dict(os.environ, {SPARK_TESTGEN_ENV: "1"})
    def test_enabled_from_environment(self):
        """Test enabling via environment variable."""
        config = Config.from_environment()
        assert config.enabled is True

    @mock.patch.dict(os.environ, {SPARK_TESTGEN_ENV: "true"})
    def test_enabled_from_environment_true_string(self):
        """Test enabling via 'true' string."""
        config = Config.from_environment()
        assert config.enabled is True

    @mock.patch.dict(os.environ, {SPARK_TESTGEN_ENV: "0"})
    def test_disabled_from_environment(self):
        """Test disabled when value is '0'."""
        config = Config.from_environment()
        assert config.enabled is False

    @mock.patch.dict(os.environ, {SPARK_TESTGEN_MODE_ENV: "synthetic"})
    def test_mode_from_environment(self):
        """Test mode from environment variable."""
        config = Config.from_environment()
        assert config.mode == Mode.SYNTHETIC

    @mock.patch.dict(os.environ, {SPARK_TESTGEN_MODE_ENV: "invalid"})
    def test_invalid_mode_defaults_to_masked(self):
        """Test invalid mode falls back to masked with warning."""
        with pytest.warns(UserWarning, match="Invalid"):
            config = Config.from_environment()
        assert config.mode == Mode.MASKED

    @mock.patch.dict(os.environ, {SPARK_TESTGEN_ENV: "1"})
    def test_is_enabled_class_method(self):
        """Test is_enabled class method."""
        assert Config.is_enabled() is True

    @mock.patch.dict(os.environ, {})
    def test_is_enabled_when_not_set(self):
        """Test is_enabled when env var not set."""
        assert Config.is_enabled() is False


class TestGetConfig:
    """Tests for get_config function."""

    def setup_method(self):
        """Reset config before each test."""
        reset_config()

    def teardown_method(self):
        """Reset config after each test."""
        reset_config()

    @mock.patch.dict(os.environ, {SPARK_TESTGEN_ENV: "1", SPARK_TESTGEN_MODE_ENV: "synthetic"})
    def test_get_config_returns_singleton(self):
        """Test get_config returns same instance."""
        config1 = get_config()
        config2 = get_config()
        assert config1 is config2
        assert config1.enabled is True
        assert config1.mode == Mode.SYNTHETIC
