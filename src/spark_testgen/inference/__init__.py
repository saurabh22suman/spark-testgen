"""Inference layer for analyzing DataFrames and generating test blueprints."""

from __future__ import annotations

from .edge_cases import EdgeCaseSynthesizer
from .masker import Masker
from .plan import PlanAnalyzer
from .plan_extractor import (
    ExtractionMethod,
    PlanExtractor,
    PlanResult,
    PlanType,
    get_extended_plan,
    get_logical_plan,
    get_physical_plan,
    has_jvm_access,
)
from .schema import SchemaAnalyzer
from .stats import StatsAnalyzer

__all__ = [
    "SchemaAnalyzer",
    "StatsAnalyzer",
    "PlanAnalyzer",
    "EdgeCaseSynthesizer",
    "Masker",
    # Plan extractor exports
    "PlanExtractor",
    "PlanResult",
    "PlanType",
    "ExtractionMethod",
    "get_logical_plan",
    "get_physical_plan",
    "get_extended_plan",
    "has_jvm_access",
]
