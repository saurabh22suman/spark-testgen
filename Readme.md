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

---

## ✨ Features

- One-line decorator to mark functions for test generation  
- No heavy AST parsing or Spark monkey-patching  
- Synthetic + masked sample DataFrames for data security  
- Snapshot-style regression tests  
- Optional fuzzy plan-regression validation  
- Deterministic test behavior for CI  
- Works with PySpark 3.3+  

---

## 📦 Installation

Coming soon to PyPI. For now, clone the repo and install locally:

```bash
pip install -e .
```

---

## 🧪 Usage

Annotate a transformation function:

```python
from spark_testgen import autogen_tests

@autogen_tests
def transform(df):
    return df.withColumn("flag", df.value > 10)
```

Then run your pipeline or script **once**:

```bash
SPARK_TESTGEN=1 python main.py
```

This generates:

```
tests/
  test_transform.py
  resources/transform/
    input.parquet
    output.parquet
    edge_cases_input.parquet
    plan_logical.txt
    plan_physical.txt
```

Remove the decorator and run:

```bash
pytest
```

You now have a real test suite guarding your pipeline.

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

**v1**
- Decorator → snapshot → pytest generation
- Masking + synthetic modes
- Minimal edge-case synthesis
- CI-friendly and deterministic

**v2**
- CLI tool for regeneration
- Config-driven generation behavior
- Richer inference for joins, window functions

**v3**
- Multi-DF pipeline support
- Visual test reports
- Integration with Delta/Lakehouse-specific features

---

## 🤝 Contributing

Contributions are welcome!  
Open issues, PRs, or suggestions on GitHub once the repo is live.

---

## 📄 License

MIT License.

---

Happy testing — may your DataFrames always be well-behaved.
