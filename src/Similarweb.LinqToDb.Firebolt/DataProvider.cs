using System.Data;
using System.Data.Common;
using LinqToDB;
using LinqToDB.Common;
using LinqToDB.Data;
using LinqToDB.DataProvider;
using LinqToDB.SchemaProvider;
using LinqToDB.SqlProvider;
using MappingSchemaBase = LinqToDB.Mapping.MappingSchema;

namespace Similarweb.LinqToDB.Firebolt;

/// <summary>
/// Firebolt Data Provider.
/// </summary>
internal class DataProvider : DynamicDataProviderBase<ProviderAdapter>
{
    /// <summary>Provider ID.</summary>
    internal const string V2Id = "Firebolt.v2";
    private static readonly HashSet<Type> ArrayTypes = [typeof(double[]), typeof(int[]), typeof(float[]), typeof(long[]), typeof(string[])];

    private readonly ISqlOptimizer _sqlOptimizer;

    /// <summary>
    /// Initializes a new instance of the <see cref="DataProvider"/> class.
    /// </summary>
    public DataProvider()
        : this(V2Id)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DataProvider"/> class.
    /// </summary>
    /// <param name="name">Provider ID.</param>
    /// <param name="mappingSchema">Mapping schema for provider.</param>
    protected DataProvider(
        string name,
        MappingSchemaBase? mappingSchema = null
    ) : base(
        name,
        GetMappingSchema(name, mappingSchema),
        ProviderAdapter.GetInstance(name)
    )
    {
        SqlProviderFlags.IsCommonTableExpressionsSupported = true;
        SqlProviderFlags.IsSkipSupported = true;
        SqlProviderFlags.IsTakeSupported = true;

        _sqlOptimizer = new SqlOptimizer(SqlProviderFlags);
    }

    /// <inheritdoc />
    public override TableOptions SupportedTableOptions => TableOptions.DropIfExists | TableOptions.CreateIfNotExists;

    /// <inheritdoc />
    public override ISchemaProvider GetSchemaProvider() => new SchemaProvider();

    /// <inheritdoc />
    public override ISqlBuilder CreateSqlBuilder(MappingSchemaBase mappingSchema, DataOptions dataOptions) =>
        new SqlBuilder(this, mappingSchema, dataOptions, GetSqlOptimizer(dataOptions), SqlProviderFlags);

    /// <inheritdoc />
    public override ISqlOptimizer GetSqlOptimizer(DataOptions dataOptions) => _sqlOptimizer;

    /// <inheritdoc />
    public override void SetParameter(
        DataConnection dataConnection,
        DbParameter parameter,
        string name,
        DbDataType dataType,
        object? value
    )
    {
        var newName = name.StartsWith(SqlBuilder.ParameterSymbol)
            ? name
            : $"{SqlBuilder.ParameterSymbol}{name}";
        base.SetParameter(dataConnection, parameter, newName, dataType, value);
    }

    /// <inheritdoc />
    protected override void SetParameterType(
        DataConnection dataConnection,
        DbParameter parameter,
        DbDataType dataType
    )
    {
        if (ArrayTypes.Contains(dataType.SystemType))
        {
            parameter.DbType = DbType.Object;
            parameter.Size = int.MaxValue;
            return;
        }

        if (dataType.SystemType == typeof(DateTime) || dataType.SystemType == typeof(DateTime?))
        {
            parameter.DbType = DbType.DateTime;
            return;
        }

        base.SetParameterType(dataConnection, parameter, dataType);
    }

    private static MappingSchemaBase GetMappingSchema(string name, MappingSchemaBase? providerSchema)
    {
        var localSchema = ProviderAdapter.GetInstance(name).MappingSchema;

        return providerSchema switch
        {
            not null => new MappingSchemaBase(providerSchema, localSchema),
            _ => name switch
            {
                V2Id => new MappingSchemaBase(name, localSchema, Firebolt.MappingSchema.Instance),
                _ => throw new NotSupportedException($"Only {V2Id} is supported"),
            },
        };
    }
}
