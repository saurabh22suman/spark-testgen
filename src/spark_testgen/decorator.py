"""Decorator for auto-generating tests from PySpark transformations."""

from __future__ import annotations

import functools
import logging
from typing import TYPE_CHECKING, Callable, TypeVar

from .config import Config, get_config
from .observer import Observer

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

# Type variable for preserving function signatures
F = TypeVar("F", bound=Callable[..., "DataFrame"])


def autogen_tests(func: F) -> F:
    """Decorator to auto-generate pytest tests for PySpark transformations.

    When SPARK_TESTGEN=1 is set, this decorator will:
    1. Capture the input/output DataFrames
    2. Analyze schemas, plans, and sample data
    3. Generate pytest test files and snapshot resources
    4. Return the original output DataFrame unchanged

    When SPARK_TESTGEN is not set, the decorator is a no-op.

    Usage:
        from spark_testgen import autogen_tests

        @autogen_tests
        def transform(df: DataFrame) -> DataFrame:
            return df.withColumn("flag", df.value > 10)

    Then run with:
        SPARK_TESTGEN=1 python your_script.py

    Generated files will be placed in:
        tests/
            test_<function_name>.py
            resources/<function_name>/
                input.parquet
                output.parquet
                edge_cases_input.parquet
                plan_logical.txt
                plan_physical.txt

    After generation, remove the decorator and run pytest normally.

    Args:
        func: A function that takes a DataFrame as its first argument
              and returns a DataFrame.

    Returns:
        Wrapped function that generates tests when SPARK_TESTGEN=1 is set.

    Example:
        >>> @autogen_tests
        ... def add_flag_column(df):
        ...     return df.withColumn("is_high", df.value > 100)
        ...
        >>> # Run normally - decorator does nothing
        >>> result = add_flag_column(input_df)
        ...
        >>> # Run with SPARK_TESTGEN=1 - generates tests
        >>> # SPARK_TESTGEN=1 python script.py
    """

    @functools.wraps(func)
    def wrapper(*args: object, **kwargs: object) -> DataFrame:
        # Get the input DataFrame (first positional argument)
        if not args:
            raise ValueError(
                f"Function {func.__name__} decorated with @autogen_tests "
                "must receive a DataFrame as its first argument."
            )

        input_df = args[0]

        # Check if test generation is enabled
        if not Config.is_enabled():
            # Normal execution - no test generation
            return func(*args, **kwargs)

        logger.info(f"spark-testgen: Generating tests for {func.__name__}")

        # Execute the transformation
        output_df = func(*args, **kwargs)

        # Capture observation
        config = get_config()
        observer = Observer(sample_size=config.sample_size, seed=config.seed)
        observation = observer.capture(input_df, output_df, func.__name__)

        # Run the generation pipeline
        _run_pipeline(observation, config)

        logger.info(f"spark-testgen: Tests generated for {func.__name__}")

        return output_df

    return wrapper  # type: ignore[return-value]


def _run_pipeline(observation: Observation, config: Config) -> None:
    """Run the full test generation pipeline.

    Args:
        observation: Captured transformation data
        config: Generation configuration
    """
    # Import here to avoid circular imports
    from .generator import ResourceWriter, TestPaths, TestWriter
    from .inference import Masker, PlanAnalyzer, SchemaAnalyzer

    # Set up paths
    paths = TestPaths(observation.function_name, config.output_dir)

    # Ensure directories exist
    paths.ensure_directories()

    # Analyze the observation
    schema_analyzer = SchemaAnalyzer()
    plan_analyzer = PlanAnalyzer()
    masker = Masker(mode=config.mode, seed=config.seed)

    input_analysis = schema_analyzer.analyze(observation.input_schema)
    output_analysis = schema_analyzer.analyze(observation.output_schema)
    schema_comparison = schema_analyzer.compare(observation.input_schema, observation.output_schema)

    # Normalize plans for comparison
    plan_analyzer.normalize_plan(observation.logical_plan)
    plan_analyzer.normalize_plan(observation.physical_plan)

    # Mask/synthesize sample data based on mode
    masked_input = masker.process(observation.input_sample)
    masked_output = masker.process(observation.output_sample)

    # Write resources
    resource_writer = ResourceWriter()
    resource_writer.write(
        input_df=masked_input,
        output_df=masked_output,
        logical_plan=observation.logical_plan,
        physical_plan=observation.physical_plan,
        paths=paths,
    )

    # Generate edge cases
    from .inference import EdgeCaseSynthesizer

    edge_synthesizer = EdgeCaseSynthesizer(seed=config.seed)
    spark = observation.input_sample.sparkSession
    edge_cases = edge_synthesizer.generate(
        observation.input_schema, spark, max_rows=config.max_edge_cases
    )
    if edge_cases is not None:
        masked_edge_cases = masker.process(edge_cases)
        resource_writer.write_edge_cases(masked_edge_cases, paths)

    # Generate test file
    test_writer = TestWriter()
    test_content = test_writer.generate(
        function_name=observation.function_name,
        _input_analysis=input_analysis,
        output_analysis=output_analysis,
        schema_comparison=schema_comparison,
        paths=paths,
    )
    test_writer.write(test_content, paths.test_file)

    logger.info(f"Generated test file: {paths.test_file}")
    logger.info(f"Generated resources in: {paths.resource_dir}")


# Import Observation here to avoid circular import issues at module level
from .observer import Observation  # noqa: E402
