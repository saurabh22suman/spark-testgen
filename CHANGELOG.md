# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial project structure with src-layout
- `@autogen_tests` decorator for marking transformation functions
- Observer module for capturing DataFrame input/output
- Inference layer: schema analysis, plan capture, data masking, edge case synthesis
- Generator layer: pytest file generation, parquet resource writing
- Data security modes: masked (default), synthetic, unsafe_raw
- CI/CD pipeline with GitHub Actions
- Support for Python 3.9-3.12
- Support for PySpark 3.3+

## [0.1.0] - TBD

### Added
- First public release
