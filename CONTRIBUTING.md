# Contributing

- Install the pinned .NET SDK (`dotnet --version` should match `global.json`).
- Restore tools: `dotnet tool restore`
- Build: `dotnet build -c Release`
- Test: `dotnet test -c Release`
- Format: `dotnet format --verify-no-changes`

PRs should include tests and update the CHANGELOG where relevant.
