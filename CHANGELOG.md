# Changelog
All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/) and
the format of [Keep a Changelog](https://keepachangelog.com/).
## [Unreleased]

### Changed
- Integration tests start Firebolt Core via Testcontainers (and seed Northwind in-process) instead of requiring a pre-run `docker compose` instance.
- Firebolt Core Testcontainers image and ports are configured in `testsettings.json` (`fireboltCore`).

### Removed
- `scripts/seed_db.py` — seeding is handled by the test host (`NorthwindSeeder`).

## [5.0.0] - 2025-11-24
- Initial release
- Initial implementation of base extension methods
