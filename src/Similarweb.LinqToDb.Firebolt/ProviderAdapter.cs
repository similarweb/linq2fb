using System.Data;
using LinqToDB.DataProvider;
using LinqToDB.Expressions;

namespace Similarweb.LinqToDB.Firebolt;

#if NET8_0
internal partial class ProviderAdapter
{
    private static readonly object AdapterInstanceLock = new();
}
#elif NET9_0_OR_GREATER
internal partial class ProviderAdapter
{
    private static readonly Lock AdapterInstanceLock = new();
}
#endif

internal partial class ProviderAdapter : IDynamicProviderAdapter
{
    private static ProviderAdapter? _adapterInstance;

    private const string SdkAssemblyName = "FireboltDotNetSdk";
    private const string SdkNamespace = "FireboltDotNetSdk.Client";

    public Type ConnectionType { get; }
    public Type DataReaderType { get; }
    public Type ParameterType { get; }
    public Type CommandType { get; }
    public Type TransactionType { get; }
    public global::LinqToDB.Mapping.MappingSchema MappingSchema { get; }

    protected ProviderAdapter(
        Type connectionType,
        Type dataReaderType,
        Type parameterType,
        Type commandType,
        Type transactionType,
        global::LinqToDB.Mapping.MappingSchema mappingSchema,
        Func<IDbDataParameter, DbType> dbTypeGetter
    )
    {
        ConnectionType = connectionType;
        DataReaderType = dataReaderType;
        ParameterType = parameterType;
        CommandType = commandType;
        TransactionType = transactionType;
        MappingSchema = mappingSchema;
        GetDbType = dbTypeGetter;
    }

    public static ProviderAdapter GetInstance(string name)
    {
        if (_adapterInstance == null)
        {
            lock (AdapterInstanceLock)
            {
                _adapterInstance ??= CreateAdapter();
            }
        }

        return _adapterInstance;
    }

    private static ProviderAdapter CreateAdapter()
    {
        var assembly = global::LinqToDB.Common.Tools.TryLoadAssembly(SdkAssemblyName, null)
                       ?? throw new InvalidOperationException($"Cannot load assembly {SdkAssemblyName}");

        var connectionType = assembly.GetType($"{SdkNamespace}.FireboltConnection", true)!;
        var dataReaderType = assembly.GetType($"{SdkNamespace}.FireboltDataReader", true)!;
        var parameterType = assembly.GetType($"{SdkNamespace}.FireboltParameter", true)!;
        var commandType = assembly.GetType($"{SdkNamespace}.FireboltCommand", true)!;
        var transactionType = assembly.GetType($"{SdkNamespace}.FireboltTransaction", true)!;

        var mappingSchema = new MappingSchema();

        var typeMapper = new TypeMapper();
        typeMapper.RegisterTypeWrapper<FireboltParameter>(parameterType);

        var dbTypeGetter = typeMapper.Type<FireboltParameter>().Member(p => p.DbType).BuildGetter<IDbDataParameter>();

        return new ProviderAdapter(
            connectionType,
            dataReaderType,
            parameterType,
            commandType,
            transactionType,
            mappingSchema,
            dbTypeGetter
        );
    }

    [Wrapper]
    private sealed class FireboltParameter
    {
        public DbType DbType { get; set; }
    }

    public readonly Func<IDbDataParameter, DbType> GetDbType;
}
