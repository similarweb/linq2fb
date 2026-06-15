# Changelog
All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/) and
the format of [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

## [6.1.0] - 2026-06-15
- Added `net10.0` target framework

## [6.0.0] - 2026-08-12
- Migrate provider to LinqToDB 6.x (compatible with 6.0–6.4; NuGet package built against 6.0.0)
- CI matrix: linq2db `{6.0.0–6.4.0}` × FireboltNetSDK `{1.9.1, 1.10.1}` × TFM `{net8.0, net9.0}`
- Materialized CTE: native `IsMaterialized` when built against linq2db ≥ 6.3; name-suffix fallback for 6.0–6.2
- Fix linq2db 6.4 SQL generation: inherit `AliasesContext` in nested builders; quote reserved table/CTE aliases
- Firebolt string search: emit `NOT (… LIKE …)`; translate `string.Contains` via `STRPOS`
- Translate `group.Select(…).ToArray()` to `ARRAY_AGG`
- Integration tests start Firebolt Core via Testcontainers and seed Northwind in-process
- Stop tracking `packages.lock.json`

## [5.0.0] - 2025-11-24
- Initial release
- Initial implementation of base extension methods
