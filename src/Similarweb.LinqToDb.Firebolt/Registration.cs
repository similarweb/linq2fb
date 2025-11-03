using LinqToDB.Data;

namespace Similarweb.LinqToDB.Firebolt;

public static class Registration
{
    public static string DataProviderName => DataProvider.V2Id;

    public static void AddDataProvider(Action<global::LinqToDB.Mapping.MappingSchema>? mappingSchemaConfig = null)
    {
        var dataProvider = new DataProvider();
        DataConnection.AddDataProvider(dataProvider);
        mappingSchemaConfig?.Invoke(dataProvider.MappingSchema);
    }
}
