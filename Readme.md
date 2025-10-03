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
* Firebolt: I tested [Firebolt](https://firebolt.io) only for v2 since v1 is deprecated. V1 is not tested and you my try to use it without any warranties.
  * You will need [FireboltNetSDK](https://github.com/firebolt-db/firebolt-net-sdk) v.1.9.1 and above for using this package.
* LinqToDB:
  * [v3.7.0](https://github.com/linq2db/linq2db/tree/v3.7.0) _(discontinued)_
  * [v5.4.1](https://github.com/linq2db/linq2db/tree/v5.4.1) (main development)

## How to use
1. Install this package (see [how to install](#installing-package))
2. Add [FireboltNetSDK](https://github.com/firebolt-db/firebolt-net-sdk) package since this implementation uses same way of loading ADO.NET classes, as LinqToDB;
3. In your code add (v3.7.0)
   ```csharp
   Registration.AddDataProvider(); // this will register Firebolt provider
   var options = new LinqToDbConnectionOptionsBuilder()
       .UseConnectionString(Registration.DataProviderName, connectionString)
       .Build();
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
