# Examples

This directory contains example usage of `spark-testgen`.

## Basic Usage

See `basic_usage.py` for a simple example demonstrating:

1. Decorating a transformation function with `@autogen_tests`
2. Running with `SPARK_TESTGEN=1` to generate tests
3. Running the generated tests with `pytest`

## Running the Example

```bash
# 1. Install spark-testgen (from project root)
pip install -e .

# 2. Run the example with test generation enabled
cd examples
SPARK_TESTGEN=1 python basic_usage.py

# 3. Check generated tests
ls -la tests/

# 4. Run the generated tests
pytest tests/
```

## Data Security Modes

```bash
# Default: masked data (safe for git)
SPARK_TESTGEN=1 python basic_usage.py

# Synthetic data (fully generated)
SPARK_TESTGEN=1 SPARK_TESTGEN_MODE=synthetic python basic_usage.py

# Raw data (opt-in, not recommended for git)
SPARK_TESTGEN=1 SPARK_TESTGEN_MODE=unsafe_raw python basic_usage.py
```
