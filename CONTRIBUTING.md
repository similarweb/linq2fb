# Contributing

- Install the pinned .NET SDK (`dotnet --version` should match `global.json`).
- Docker must be available locally (Docker Desktop, Colima, etc.) — integration tests start Firebolt Core via [Testcontainers](https://dotnet.testcontainers.org/).
- Restore tools: `dotnet tool restore`
- Build: `dotnet build -c Release`
- Test: `dotnet test -c Release`
- Format: `dotnet format --verify-no-changes`

Core image/ports are configured in `tests/.../testsettings.json` (`fireboltCore`). To point tests at a pre-started, pre-seeded Core instead of Testcontainers, set `fireboltCore.externalUrl` in `testsettings.local.json` (or env `FireboltCore__ExternalUrl`).

PRs should include tests and update the CHANGELOG where relevant.
