using LinqToDB;
using LinqToDB.Data;
using Microsoft.Extensions.Options;
using Similarweb.LinqToDB.Firebolt.Tests.Common;
using Similarweb.LinqToDB.Firebolt.Tests.CoreConnection;
using Similarweb.LinqToDB.Firebolt.Tests.Options;
using Xunit;
using Xunit.DependencyInjection;

namespace Similarweb.LinqToDB.Firebolt.Tests.Fixtures;

public class ContextFixture<TContext> : IDisposable, IAsyncDisposable
    where TContext : DataConnection
{
    private readonly ITestContextAccessor _testContextAccessor;
    private readonly ITestOutputHelperAccessor _testOutputHelperAccessor;

    public TContext Context { get; }

    public ContextFixture(
        ConnectionStringsProvider connectionStringsProvider,
        IOptionsMonitor<LinqToDbTestSettings> testSettings,
        ITestContextAccessor testContextAccessor,
        ITestOutputHelperAccessor testOutputHelperAccessor
    )
    {
        ArgumentNullException.ThrowIfNull(testSettings);
        _testContextAccessor = testContextAccessor;
        _testOutputHelperAccessor = testOutputHelperAccessor;

        Registration.AddDataProvider();
        var connectionString = connectionStringsProvider?.Get(testSettings.CurrentValue.AccountName) ?? string.Empty;
        var dataProvider = DataConnection.GetDataProvider(Registration.DataProviderName, connectionString)
                           ?? throw new InvalidOperationException($"DataProvider not found for {connectionString}");
        var mappingSchema = new global::LinqToDB.Mapping.MappingSchema();
        var dataOptions = new DataOptions()
            .UseConnectionFactory(dataProvider, connectionFactory: _ => new FireboltCoreConnection(connectionString))
            .UseMappingSchema(mappingSchema);

        Context = Activator.CreateInstance(typeof(TContext), dataOptions) as TContext ??
                  throw new InvalidOperationException($"Cannot create instance of {typeof(TContext).Name}");
    }

    public void LogLastQuery()
    {
        _testOutputHelperAccessor.Output?.WriteLine(
            "-- Firebolt Test SQL for {0}:\n{1}\n\n",
            _testContextAccessor.Current.Test?.TestDisplayName ?? "<unknown test>",
            Context.LastQuery ?? "<unknown query>"
        );
    }

    public void Dispose()
    {
        Context.Dispose();
        GC.SuppressFinalize(this);
    }

    public async ValueTask DisposeAsync()
    {
        await Context.DisposeAsync();
        GC.SuppressFinalize(this);
    }
}
