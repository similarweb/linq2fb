# Firebolt LinqToDB driver
## Version
This is documentation for outdated LinqToDb [v5.4.1](https://github.com/linq2db/linq2db/tree/v5.4.1)-based version. Please feel free exploring our [code](https://github.com/similarweb/linq2fb)!

## Target platforms
- [NET8](https://dotnet.microsoft.com/en-us/download/dotnet/8.0) (8.0.414) — backporting
- [NET9](https://dotnet.microsoft.com/en-us/download/dotnet/9.0) (9.0.305) — mainstream

## Dogfooding
We in [Similarweb](https://developers.similarweb.com/docs/similarweb-web-traffic-api) use this package to fetch data from [Firebolt V2](https://firebolt.io), so constantly improving it.

### Supported versions
* Firebolt: I tested [Firebolt](https://firebolt.io) only for v2 since v1 is deprecated. V1 is not tested and you my try to use it without any warranties.
  * You will need [FireboltNetSDK](https://github.com/firebolt-db/firebolt-net-sdk) v.1.9.1 and above for using this package.
* LinqToDB:
  * [v3.7.0](https://github.com/linq2db/linq2db/tree/v3.7.0) — _deprecated_, not being updated. You may find usages [on previous version page](https://similarweb.github.io/linq2fb/v3.7);
  * [v5.4.1](https://github.com/linq2db/linq2db/tree/v5.4.1) — check this documentation;

## How to use
1. Install this package (see [how to install](#installing-package))
2. Add [FireboltNetSDK](https://github.com/firebolt-db/firebolt-net-sdk) package since this implementation uses same way of loading ADO.NET classes, as LinqToDB;
3. In your code add following:
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
