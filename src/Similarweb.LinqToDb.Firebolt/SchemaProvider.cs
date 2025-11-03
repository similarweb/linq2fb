using LinqToDB;
using LinqToDB.Data;
using LinqToDB.SchemaProvider;

namespace Similarweb.LinqToDB.Firebolt;

/// <inheritdoc/>
internal class SchemaProvider : SchemaProviderBase
{
    /// <inheritdoc/>
    protected override List<DataTypeInfo> GetDataTypes(DataConnection dataConnection)
    {
        return base.GetDataTypes(dataConnection)
            .Select(dataType => dataType.CreateFormat?.EndsWith(" UNSIGNED", StringComparison.OrdinalIgnoreCase) == true
                ? new DataTypeInfo
                {
                    TypeName = dataType.CreateFormat,
                    DataType = dataType.DataType,
                    CreateFormat = dataType.CreateFormat,
                    CreateParameters = dataType.CreateParameters,
                    ProviderDbType = dataType.ProviderDbType,
                }
                : dataType)
            .ToList();
    }

    /// <inheritdoc/>
    protected override DataType GetDataType(string? dataType, string? columnType, long? length, int? prec, int? scale) => throw new NotImplementedException();

    /// <inheritdoc/>
    protected override List<TableInfo> GetTables(DataConnection dataConnection, GetSchemaOptions options) => throw new NotImplementedException();

    /// <inheritdoc/>
    protected override IReadOnlyCollection<PrimaryKeyInfo> GetPrimaryKeys(DataConnection dataConnection, IEnumerable<TableSchema> tables, GetSchemaOptions options) => throw new NotImplementedException();

    /// <inheritdoc/>
    protected override List<ColumnInfo> GetColumns(DataConnection dataConnection, GetSchemaOptions options) => throw new NotImplementedException();

    /// <inheritdoc/>
    protected override IReadOnlyCollection<ForeignKeyInfo> GetForeignKeys(DataConnection dataConnection, IEnumerable<TableSchema> tables, GetSchemaOptions options) => throw new NotImplementedException();

    /// <inheritdoc/>
    protected override string? GetProviderSpecificTypeNamespace() => throw new NotImplementedException();
}
