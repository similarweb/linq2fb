# Firebolt LinqToDB driver
## Motivation
Currently, there are no official [LinqToDB](https://github.com/linq2db/linq2db) driver for Firebolt. This leads to following issues:
* usage of direct SQL queries instead of Linq expressions;
* overcomplicated infrastructure around [ADO.NET](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview) classes;
* can't use variety of ORMs and features/extensions;
* devs have to write all SQL queries, even simple ones like `SELECT * FROM table`.

## Goals
Providing a LinqToDB driver for Firebolt will simplify usage of DB for newcomers.

## Limitations
Currently, it is an MVP to narrow down the scope of the project.
* No support for LinqToDB features like `InsertOrUpdate`, `InsertOrReplace`, etc.;
* Not all Firebolt-specific functions are supported (e.g. [geospatial](https://docs.firebolt.io/sql_reference/functions-reference/geospatial/), [DataSketches](https://docs.firebolt.io/sql_reference/functions-reference/datasketches/), etc.);
* No support for Schema retrieval (yet);
* Some other issues could be present, but they are not critical for current goal.

## Target platforms
- [NET8](https://dotnet.microsoft.com/en-us/download/dotnet/8.0) (8.0.414) — backporting
- [NET9](https://dotnet.microsoft.com/en-us/download/dotnet/9.0) (9.0.305) — mainstream

## Dogfooding
We in [Similarweb](https://similarweb.com) are using this package. This would encourage us to constantly improve it.

### Supported versions
* Firebolt: tested on [Firebolt](https://firebolt.io) v2 only (v1 is deprecated / untested).
  * Install [FireboltNetSDK](https://github.com/firebolt-db/firebolt-net-sdk) **1.9.1 or 1.10.x** yourself (peer dependency; not bundled).
* LinqToDB: **6.0 – 6.3** tested (one nupkg; dependency `linq2db >= 6.0.0`). **6.4.0** is not in CI yet: upstream alias/GroupBy regressions (nested `AS` vs outer `*_1`, GroupBy key columns collapsing to `"Value"`).
  * Materialized CTE (`AsMaterializedCte`): name-suffix + `BuildWithClause` fallback when built against &lt; 6.3; native `IsMaterialized` when built against ≥ 6.3. The published package is built against **6.0.0** (suffix fallback).
  * CI matrix: linq2db `{6.0.0, 6.1.0, 6.2.0, 6.3.0}` × FireboltNetSDK `{1.9.1, 1.10.1}` × TFM `{net8.0, net9.0}`.

## How to use
1. Install this package (see [how to install](#installing-package))
2. Add [FireboltNetSDK](https://github.com/firebolt-db/firebolt-net-sdk) package since this implementation uses same way of loading ADO.NET classes, as LinqToDB;
3. In your code add:
   ```csharp
   Registration.AddDataProvider(); // this will register Firebolt provider
   var options = new DataOptions()
       .UseConnectionString(Registration.DataProviderName, connectionString);
   var db = new DataConnection(options);
   // here you may use `db` as usual LinqToDB connection
   ```

4. In case you want real LinqToDB features, you should use `db.GetTable<T>()` method to get table as LinqToDB table. For example:
   ```csharp
   var table = db.GetTable<YourEntity>();
   var result = table.Where(x => x.Id == 1).ToList();
   ```

## Installing package
### Using .NET CLI
```shell
dotnet add package Similarweb.LinqToFirebolt
```

### Using Visual Studio UI
`Tools > NuGet Package Manager > Manage NuGet Packages for Solution` and search for `Similarweb.LinqToFirebolt`

### Using Rider UI
`Tools > NuGet > Manage NuGet packages for <solution name>` and search for `SimilarWeb.LinqToFirebolt`

### Using Package Manager Console:
```shell
Install-Package SimilarWeb.LinqToFirebolt
```
