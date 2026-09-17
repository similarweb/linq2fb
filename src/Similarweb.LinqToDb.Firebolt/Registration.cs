using LinqToDB.Data;

namespace Similarweb.LinqToDB.Firebolt;

/// <summary>
/// <see href="https://github.com/similarweb/linq2fb">LinqToFirebolt</see> entry point. Your journey begins here.
/// </summary>
public static class Registration
{
    /// <summary>
    /// Gets data provider Id.
    /// </summary>
    public static string DataProviderName => DataProvider.V2Id;

    /// <summary>
    /// Register Firebolt data provider in LinqToDb.
    /// </summary>
    /// <param name="mappingSchemaConfig">Method for configuring <see cref="MappingSchema"/>.</param>
    public static void AddDataProvider(Action<global::LinqToDB.Mapping.MappingSchema>? mappingSchemaConfig = null)
    {
        EnsureCompatibleLinqToDb();
        var dataProvider = new DataProvider();
        DataConnection.AddDataProvider(dataProvider);
        mappingSchemaConfig?.Invoke(dataProvider.MappingSchema);
    }

    private static void EnsureCompatibleLinqToDb()
    {
        var version = typeof(DataConnection).Assembly.GetName().Version
            ?? throw new InvalidOperationException("linq2db assembly version is missing.");
        var linqToDb64 = new Version(6, 4, 0, 0);
#if LINQ2DB_6_4_LINE
        if (version < linqToDb64)
        {
            throw new InvalidOperationException(
                $"This Similarweb.LinqToDB.Firebolt build requires linq2db 6.4.0 or later (got {version}). Use the 6.0.x package for linq2db 6.0–6.3.");
        }
#else
        if (version >= linqToDb64)
        {
            throw new InvalidOperationException(
                $"This Similarweb.LinqToDB.Firebolt build supports linq2db 6.0–6.3 (got {version}). Use the 6.4.x package for linq2db 6.4+.");
        }
#endif
    }
}
