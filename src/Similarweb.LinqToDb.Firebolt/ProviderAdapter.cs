using System.Data;
using LinqToDB.DataProvider;
using LinqToDB.Expressions;

namespace Similarweb.LinqToDB.Firebolt;

/// <summary>
/// Adapter for DataProvider. Holds all types required for work, plus wrappers (if needed). Provides required
/// communication between LinqToDb internals and ADO.NET types.
/// </summary>
internal partial class ProviderAdapter : IDynamicProviderAdapter
{
    private const string SdkAssemblyName = "FireboltDotNetSdk";
    private const string SdkNamespace = "FireboltDotNetSdk.Client";

    private static ProviderAdapter? _adapterInstance;

    /// <summary>
    /// Initializes a new instance of the <see cref="ProviderAdapter"/> class.
    /// </summary>
    /// <param name="connectionType">Database connection type (see <see cref="ConnectionType"/>).</param>
    /// <param name="dataReaderType">Database reader type (see <see cref="DataReaderType"/>).</param>
    /// <param name="parameterType">Parameter type (see <see cref="ParameterType"/>).</param>
    /// <param name="commandType">Database command type (see <see cref="CommandType"/>).</param>
    /// <param name="transactionType">Transaction type (see <see cref="TransactionType"/>).</param>
    /// <param name="mappingSchema">Mapping schema type (see <see cref="MappingSchema"/>).</param>
    /// <param name="dbTypeGetter">Method for getting <see cref="DbType"/> for <see cref="IDbDataParameter"/> (see <see cref="GetDbType"/>).</param>
    protected ProviderAdapter(
        Type connectionType,
        Type dataReaderType,
        Type parameterType,
        Type commandType,
        Type transactionType,
        MappingSchema mappingSchema,
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

    /// <inheritdoc/>
    public Type ConnectionType { get; }

    /// <inheritdoc/>
    public Type DataReaderType { get; }

    /// <inheritdoc/>
    public Type ParameterType { get; }

    /// <inheritdoc/>
    public Type CommandType { get; }

    /// <inheritdoc/>
    public Type TransactionType { get; }

    /// <summary>
    /// Gets adapter's <see cref="MappingSchema"/>.
    /// </summary>
    public MappingSchema MappingSchema { get; }

    /// <summary>
    /// Gets method for retrieving <see cref="DbType"/> for <see cref="IDbDataParameter"/>.
    /// </summary>
    public Func<IDbDataParameter, DbType> GetDbType { get; }

    /// <summary>
    /// Gets or creates new <see cref="ProviderAdapter"/> instance.
    /// </summary>
    /// <param name="name">Name.</param>
    /// <returns><see cref="ProviderAdapter"/>.</returns>
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
}
