# Firebolt LinqToDB driver
## Version
Documentation for the LinqToDB **6.x**-based package ([`v6.0.0-rc.1`](https://www.nuget.org/packages/Similarweb.LinqToDB.Firebolt/6.0.0-rc.1)). Older docs: [v5.4](https://similarweb.github.io/linq2fb/v5.4/), [v3.7](https://similarweb.github.io/linq2fb/v3.7/). Source: [similarweb/linq2fb](https://github.com/similarweb/linq2fb).

## Target platforms
- [NET8](https://dotnet.microsoft.com/en-us/download/dotnet/8.0) (8.0.414) — backporting
- [NET9](https://dotnet.microsoft.com/en-us/download/dotnet/9.0) (9.0.305) — mainstream

## Dogfooding
We in [Similarweb](https://similarweb.com) use this package to fetch data from [Firebolt V2](https://firebolt.io), so we keep improving it.

### Supported versions
* Firebolt: tested on [Firebolt](https://firebolt.io) v2 only (v1 is deprecated / untested).
  * Install [FireboltNetSDK](https://github.com/firebolt-db/firebolt-net-sdk) **1.9.1 or 1.10.x** yourself (peer dependency; not bundled).
* LinqToDB: **6.0 – 6.4** (one nupkg; dependency `linq2db >= 6.0.0`).
  * Materialized CTE (`AsMaterializedCte`): name-suffix + `BuildWithClause` fallback when built against &lt; 6.3; native `IsMaterialized` when built against ≥ 6.3. The published package is built against **6.0.0** (suffix fallback).
  * CI matrix: linq2db `{6.0.0, 6.1.0, 6.2.0, 6.3.0, 6.4.0}` × FireboltNetSDK `{1.9.1, 1.10.1}` × TFM `{net8.0, net9.0}`.

## How to use
1. Install this package (see [how to install](#installing-package))
2. Add [FireboltNetSDK](https://github.com/firebolt-db/firebolt-net-sdk) since this provider loads ADO.NET classes the same way LinqToDB does
3. In your code add:
   ```csharp
   Registration.AddDataProvider(); // registers the Firebolt provider
   var options = new DataOptions()
       .UseConnectionString(Registration.DataProviderName, connectionString);
   var db = new DataConnection(options);
   // use `db` as a usual LinqToDB connection
   ```

4. For LinqToDB table APIs, use `db.GetTable<T>()`:
   ```csharp
   var table = db.GetTable<YourEntity>();
   var result = table.Where(x => x.Id == 1).ToList();
   ```

## Installing package
### Using .NET CLI
```shell
dotnet add package Similarweb.LinqToDB.Firebolt
```

### Using Visual Studio UI
`Tools > NuGet Package Manager > Manage NuGet Packages for Solution` and search for `Similarweb.LinqToDB.Firebolt`

### Using Rider UI
`Tools > NuGet > Manage NuGet packages for <solution name>` and search for `Similarweb.LinqToDB.Firebolt`

### Using Package Manager Console:
```shell
Install-Package Similarweb.LinqToDB.Firebolt
```
