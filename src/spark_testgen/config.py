"""Configuration management for spark-testgen."""

from __future__ import annotations

import logging
import os
import warnings
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# Environment variable names
SPARK_TESTGEN_ENV = "SPARK_TESTGEN"
SPARK_TESTGEN_MODE_ENV = "SPARK_TESTGEN_MODE"
SPARK_TESTGEN_OUTPUT_DIR_ENV = "SPARK_TESTGEN_OUTPUT_DIR"
SPARK_TESTGEN_SEED_ENV = "SPARK_TESTGEN_SEED"


class Mode(Enum):
    """Data handling mode for generated test resources."""

    MASKED = "masked"  # Default: mask sensitive data while preserving structure
    SYNTHETIC = "synthetic"  # Generate fully synthetic data matching schema
    UNSAFE_RAW = "unsafe_raw"  # Use raw data (explicit opt-in, security warning)


@dataclass
class Config:
    """Configuration for spark-testgen test generation.

    Attributes:
        enabled: Whether test generation is active
        mode: Data handling mode (masked, synthetic, unsafe_raw)
        output_dir: Base directory for generated tests
        seed: Random seed for deterministic generation
        sample_size: Number of rows to sample for snapshots
        max_edge_cases: Maximum number of edge case rows to generate
    """

    enabled: bool = False
    mode: Mode = Mode.MASKED
    output_dir: Path = field(default_factory=lambda: Path("tests"))
    seed: int = 42
    sample_size: int = 20
    max_edge_cases: int = 10

    @classmethod
    def from_environment(cls) -> Config:
        """Create configuration from environment variables.

        Environment Variables:
            SPARK_TESTGEN: Set to "1" or "true" to enable test generation
            SPARK_TESTGEN_MODE: One of "masked", "synthetic", "unsafe_raw"
            SPARK_TESTGEN_OUTPUT_DIR: Path for generated tests (default: "tests")
            SPARK_TESTGEN_SEED: Random seed for determinism (default: 42)

        Returns:
            Config instance populated from environment
        """
        enabled = _parse_bool_env(SPARK_TESTGEN_ENV)
        mode = _parse_mode_env(SPARK_TESTGEN_MODE_ENV)
        output_dir = Path(os.environ.get(SPARK_TESTGEN_OUTPUT_DIR_ENV, "tests"))
        seed = int(os.environ.get(SPARK_TESTGEN_SEED_ENV, "42"))

        config = cls(
            enabled=enabled,
            mode=mode,
            output_dir=output_dir,
            seed=seed,
        )

        # Safety check for unsafe mode
        if config.enabled and config.mode == Mode.UNSAFE_RAW:
            _warn_unsafe_mode()

        return config

    @classmethod
    def is_enabled(cls) -> bool:
        """Quick check if test generation is enabled."""
        return _parse_bool_env(SPARK_TESTGEN_ENV)


def _parse_bool_env(env_var: str) -> bool:
    """Parse boolean environment variable."""
    value = os.environ.get(env_var, "").lower()
    return value in ("1", "true", "yes", "on")


def _parse_mode_env(env_var: str) -> Mode:
    """Parse mode environment variable."""
    value = os.environ.get(env_var, "").lower()
    try:
        return Mode(value) if value else Mode.MASKED
    except ValueError:
        valid_modes = ", ".join(m.value for m in Mode)
        warnings.warn(
            f"Invalid {env_var}='{value}'. Using 'masked'. Valid modes: {valid_modes}",
            UserWarning,
            stacklevel=3,
        )
        return Mode.MASKED


def _check_git_repo() -> bool:
    """Check if current directory is inside a git repository."""
    current = Path.cwd()
    for parent in [current, *current.parents]:
        if (parent / ".git").exists():
            return True
    return False


def _warn_unsafe_mode() -> None:
    """Warn user about unsafe mode, especially in git repositories."""
    message = (
        "⚠️  SPARK_TESTGEN_MODE=unsafe_raw is enabled! "
        "Raw data will be written to test resources."
    )

    if _check_git_repo():
        message += (
            "\n⚠️  Git repository detected! Raw data may be accidentally committed. "
            "Consider using 'masked' or 'synthetic' mode instead."
        )

    warnings.warn(message, UserWarning, stacklevel=4)
    logger.warning(message)


# Global configuration instance (lazy-loaded)
_config: Optional[Config] = None


def get_config() -> Config:
    """Get the global configuration instance.

    Creates configuration from environment on first call.
    """
    global _config
    if _config is None:
        _config = Config.from_environment()
    return _config


def reset_config() -> None:
    """Reset global configuration (useful for testing)."""
    global _config
    _config = None
