# Contributing to spark-testgen

Thank you for your interest in contributing to spark-testgen! 🎉

## Development Setup

### Prerequisites

- Python 3.9+
- Java 11+ (for PySpark)
- Git

### Setting Up Development Environment

```bash
# Clone the repository
git clone https://github.com/yourusername/spark-testgen.git
cd spark-testgen

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install in development mode with dev dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=spark_testgen --cov-report=html

# Run specific test file
pytest tests/test_config.py

# Run tests matching a pattern
pytest -k "test_masker"
```

### Code Quality

```bash
# Format code
ruff format src tests

# Lint code
ruff check src tests

# Type check
mypy src

# Run all checks (via tox)
tox -e lint,type
```

### Multi-Version Testing

```bash
# Test across Python versions (requires pyenv or multiple Python installations)
tox

# Test specific Python version
tox -e py310
```

## Contribution Guidelines

### Pull Request Process

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass: `pytest`
6. Ensure code quality: `ruff check src tests && mypy src`
7. Commit with clear messages
8. Push and create a Pull Request

### Commit Messages

Follow conventional commits format:

```
feat: add support for Window functions
fix: handle null values in masker
docs: update README with new examples
test: add tests for edge case synthesizer
refactor: simplify schema analyzer logic
```

### Code Style

- Follow PEP 8 (enforced by ruff)
- Use type hints for all function signatures
- Write docstrings for public functions and classes
- Keep functions focused and small
- Write tests for new functionality

### Testing Guidelines

- Tests should be deterministic
- Use fixtures from `conftest.py` for SparkSession
- Mock external dependencies when appropriate
- Test both success and error cases
- Use descriptive test names: `test_masker_preserves_nulls`

## Project Structure

```
spark_testgen/
├── src/spark_testgen/
│   ├── __init__.py       # Public API
│   ├── decorator.py      # @autogen_tests decorator
│   ├── observer.py       # DataFrame observation
│   ├── config.py         # Configuration management
│   ├── inference/        # Analysis modules
│   │   ├── schema.py     # Schema analysis
│   │   ├── plan.py       # Execution plan analysis
│   │   ├── stats.py      # Statistics analysis
│   │   ├── masker.py     # Data masking
│   │   └── edge_cases.py # Edge case generation
│   └── generator/        # Code generation
│       ├── paths.py      # Path utilities
│       ├── resource_writer.py  # Write parquet/plans
│       └── test_writer.py      # Generate pytest files
└── tests/                # Test suite
```

## Feature Requests & Bug Reports

- Use GitHub Issues
- Search existing issues before creating new ones
- Provide clear reproduction steps for bugs
- Include Python and PySpark versions

## Questions?

Feel free to open a GitHub Discussion or Issue!

---

Happy contributing! 🚀
