# Changelog
All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/) and
the format of [Keep a Changelog](https://keepachangelog.com/).
## [Unreleased]
### Added
- `LambdaMethods.ArrayTransform` overloads for two and three input arrays (`ARRAY_TRANSFORM(lambda, a, b[, c])`), enabling element-wise transforms across parallel arrays (e.g. `TRANSFORM(x, y -> (y - x), starts, ends)`).
- `ArrayMethods.At` — 1-based Firebolt array subscript access (`arr[index]`).
- `ArrayMethods.AsSqlArray` — builds a single-element array literal (`[value]`) from a scalar column.
- `ConditionalMethods.NullIf` — `NULLIF(value, compare)`, primarily for guarding divisors against zero.

### Fixed
- `LambdaBuilder` now emits `IS NULL` / `IS NOT NULL` for `== null` / `!= null` comparisons instead of the semantically-incorrect `= NULL` / `!= NULL`.
- `LambdaBuilder` now supports arithmetic (`+`, `-`, `*`, `/`) and boolean (`AND`, `OR`) operators inside array-lambda bodies.

## [5.0.0] - 2025-11-24
- Initial release
- Initial implementation of base extension methods
