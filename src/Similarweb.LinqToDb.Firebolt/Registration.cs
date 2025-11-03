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
        var dataProvider = new DataProvider();
        DataConnection.AddDataProvider(dataProvider);
        mappingSchemaConfig?.Invoke(dataProvider.MappingSchema);
    }
}
