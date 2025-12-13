# spark-testgen

Auto-generate pytest test suites for PySpark DataFrame transformations by observing a single run of your function.

## 🚀 What Is spark-testgen?

`spark-testgen` is a lightweight yet sharp tool that watches your PySpark transformation **once**, learns its behavior, and generates a full test suite for you:

- Schema tests  
- Snapshot comparison tests  
- Null + edge-case tests  
- Soft plan-regression tests  
- Masked or synthetic sample DataFrames  
- CI-friendly and deterministic  

After generation, you **remove the decorator**, and your tests live on independently.

### ⚡ Regression Testing, Not Logic Verification

**Important distinction**: `spark-testgen` is a **regression testing** tool, not a correctness checker.

- ✅ **What it does**: Captures a "known-good" snapshot of your transformation's behavior (schema, output data, execution plan) and generates tests that verify future runs match this baseline.

- ❌ **What it doesn't do**: Verify that your transformation logic is correct in the first place. If your original code has a bug, the generated tests will encode that bug as the expected behavior.

**Use case**: You have a working transformation and want to ensure refactoring, Spark upgrades, or dependency changes don't accidentally break it. Run once to capture the baseline, then use the generated tests as a safety net.

**Not a use case**: Validating that your business logic is correct. You still need to manually verify your transformation works correctly before generating tests.

---

## ✨ Features

- One-line decorator to mark functions for test generation  
- No heavy AST parsing or Spark monkey-patching  
- Synthetic + masked sample DataFrames for data security  
- Snapshot-style regression tests  
- Optional fuzzy plan-regression validation  
- Deterministic test behavior for CI  
- Works with PySpark 3.3+  
- **Spark Connect / Databricks Serverless**: Experimental support with fallbacks (see Limitations)

---

## 📦 Installation

Install from GitHub:

```bash
pip install git+https://github.com/saurabh22suman/spark-testgen.git
```

Or clone and install locally:

```bash
git clone https://github.com/saurabh22suman/spark-testgen.git
cd spark-testgen
pip install -e .
```

---

## 🧪 Quick Start

### Step 1: Annotate your transformation

```python
from spark_testgen import autogen_tests

@autogen_tests
def transform(df):
    return df.withColumn("flag", df.value > 10)
```

### Step 2: Run with test generation enabled

```bash
SPARK_TESTGEN=1 python your_script.py
```

Or in a notebook/Databricks:

```python
import os
os.environ["SPARK_TESTGEN"] = "1"

# Then call your decorated function
result = transform(input_df)
```

### Step 3: Check generated files

```
tests/
  test_transform.py          # Generated pytest file
  resources/transform/
    input.parquet             # Input snapshot
    output.parquet            # Expected output snapshot
    edge_cases_input.parquet  # Edge case test data
    plan_logical.txt          # Logical execution plan
    plan_physical.txt         # Physical execution plan
    schema.json               # Output schema
```

### Step 4: Remove decorator and run tests

```python
# Remove @autogen_tests decorator
def transform(df):
    return df.withColumn("flag", df.value > 10)
```

```bash
pytest tests/test_transform.py -v
```

---

## ⚙️ Configuration

| Environment Variable | Values | Default | Description |
|---------------------|--------|---------|-------------|
| `SPARK_TESTGEN` | `1`, `true` | Not set | Enable test generation (safety switch) |
| `SPARK_TESTGEN_MODE` | `masked`, `synthetic`, `unsafe_raw` | `masked` | Data handling mode |
| `SPARK_TESTGEN_OUTPUT_DIR` | Path | `tests/` | Output directory |
| `SPARK_TESTGEN_SAMPLE_SIZE` | Integer | `20` | Rows to sample |

### Why `SPARK_TESTGEN` is Required

The `SPARK_TESTGEN` environment variable acts as a **safety switch**. By default, the decorator is a no-op, meaning your production code runs normally without any test generation overhead.

This design ensures:
- ✅ No accidental test file generation in production
- ✅ No performance impact from observation logic in prod
- ✅ Explicit opt-in when you want to generate tests
- ✅ Safe to leave decorators in code during development

### Example

```bash
SPARK_TESTGEN=1 \
SPARK_TESTGEN_MODE=synthetic \
SPARK_TESTGEN_OUTPUT_DIR=my_tests/ \
python main.py
```

---

## 🔐 Data Security

By default:

- Raw data is **never** written.  
- All data is **masked** before saving.  
- Synthetic-only mode available:  
  ```bash
  SPARK_TESTGEN_MODE=synthetic
  ```

Unsafe mode is opt-in:

```bash
SPARK_TESTGEN_MODE=unsafe_raw
```

Includes warnings if `.git` is detected.

---

## ⚠️ Limitations & Known Issues

### What Works

| Environment | Status | Notes |
|-------------|--------|-------|
| Local PySpark (with JVM) | ✅ Full support | All features work |
| Databricks Classic Compute | ✅ Full support | All features work |
| EMR, Dataproc | ✅ Full support | All features work |

### What Has Limitations

| Environment | Status | Limitations |
|-------------|--------|-------------|
| **Databricks Serverless** | 🧪 Experimental | See below |
| **Spark Connect** | 🧪 Experimental | See below |

#### Databricks Serverless / Spark Connect Limitations

> ⚠️ **Status: Experimental** - These environments have not been fully validated. Fallback mechanisms are implemented but may not work in all cases.

On serverless compute, the following features are **not available**:

1. **DataFrame caching** - `cache()` is not supported, may impact performance for large DataFrames
2. **JVM-based plan extraction** - Falls back to `explain()` output (less detailed)
3. **Native parquet writes** - Falls back to pandas-based writes (requires `pandas` and `pyarrow`)

**Known Issues**:
- Package caching on Databricks may require `pip install --force-reinstall` after updates
- Some JVM attribute errors may still occur in edge cases

**Workaround**: The tool attempts to detect serverless environments and use fallback methods automatically. If you encounter issues, please report them.

#### Not Supported

| Feature | Reason |
|---------|--------|
| Streaming DataFrames | Only batch DataFrames supported |
| Multi-output functions | Functions must return a single DataFrame |
| UDFs with external dependencies | UDF code is not captured |
| Delta Lake time travel | Snapshots capture current state only |

---

## 🧠 How It Works

1. Decorator intercepts the DataFrame input/output on first run.  
2. Collector gathers schema, summary stats, and plans.  
3. Inference layer generates:
   - edge cases  
   - masked/synthetic samples  
   - assertions  
4. Generator writes:
   - pytest file  
   - parquet samples  
   - plan text files  

Architecture is split into:
- `observer`
- `inference`
- `generator`

Clean, modular, and extensible.

---

## 📁 Project Structure

```
spark_testgen/
    decorator.py
    observer.py
    inference/
        schema.py
        stats.py
        plan.py
        edge_cases.py
        masker.py
    generator/
        test_writer.py
        resource_writer.py
        paths.py
```

---

## 🛣 Roadmap

**v1** ✅
- Decorator → snapshot → pytest generation
- Masking + synthetic modes
- Minimal edge-case synthesis
- CI-friendly and deterministic

**v1.1** 🚧
- Spark Connect / Serverless support (in progress)

**v2**
- CLI tool for regeneration
- Config-driven generation behavior
- Richer inference for joins, window functions

**v3**
- Multi-DF pipeline support
- Visual test reports
- Integration with Delta/Lakehouse-specific features

---

## 🐳 Local Development with Docker

If you don't have Java installed locally, use Docker for testing:

```bash
# Build the test image
docker build -f Dockerfile.test -t spark-testgen-test .

# Generate tests
docker run --rm -v "$(pwd)/tests:/app/tests" spark-testgen-test

# Run pytest
docker run --rm -v "$(pwd)/tests:/app/tests" spark-testgen-test \
  python -m pytest tests/test_transform.py -v
```

---

## 🤝 Contributing

Contributions are welcome!  
Open issues, PRs, or suggestions on GitHub once the repo is live.

---

## 📄 License

MIT License.

---

Happy testing — may your DataFrames always be well-behaved.
