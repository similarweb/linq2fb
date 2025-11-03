using System.Globalization;
using System.Text;
using LinqToDB;
using LinqToDB.Linq;
using LinqToDB.SqlQuery;
using Similarweb.LinqToDB.Firebolt.Extensions;

namespace Similarweb.LinqToDB.Firebolt;

/// <inheritdoc/>
internal class MappingSchema : global::LinqToDB.Mapping.MappingSchema
{
    internal static readonly global::LinqToDB.Mapping.MappingSchema Instance = new MappingSchema();

    public MappingSchema() : this(DataProvider.V2Id)
    {
    }

    protected MappingSchema(string configuration) : base(configuration)
    {
        SetDataType(typeof(DateTime), DataType.DateTime);

        Expressions.MapMember(DataProvider.V2Id, Expressions.M(() => DateTime.Now.Year), (DateTime obj) => FBSql.DatePart(Sql.DateParts.Year, obj)!.Value);
        Expressions.MapMember(DataProvider.V2Id, Expressions.M(() => DateTime.Now.Month), (DateTime obj) => FBSql.DatePart(Sql.DateParts.Month, obj)!.Value);
        Expressions.MapMember(DataProvider.V2Id, Expressions.M(() => DateTime.Now.DayOfYear), (DateTime obj) => FBSql.DatePart(Sql.DateParts.DayOfYear, obj)!.Value);
        Expressions.MapMember(DataProvider.V2Id, Expressions.M(() => DateTime.Now.Day), (DateTime obj) => FBSql.DatePart(Sql.DateParts.Day, obj)!.Value);
        Expressions.MapMember(DataProvider.V2Id, Expressions.M(() => DateTime.Now.DayOfWeek), (DateTime obj) => FBSql.DatePart(Sql.DateParts.WeekDay, obj)!.Value - 1);
        Expressions.MapMember(DataProvider.V2Id, Expressions.M(() => DateTime.Now.Hour), (DateTime obj) => FBSql.DatePart(Sql.DateParts.Hour, obj)!.Value);
        Expressions.MapMember(DataProvider.V2Id, Expressions.M(() => DateTime.Now.Minute), (DateTime obj) => FBSql.DatePart(Sql.DateParts.Minute, obj)!.Value);
        Expressions.MapMember(DataProvider.V2Id, Expressions.M(() => DateTime.Now.Second), (DateTime obj) => FBSql.DatePart(Sql.DateParts.Second, obj)!.Value);

        Expressions.MapMember(DataProvider.V2Id, Expressions.M(() => DateTime.Now), () => FBSql.CurrentDateTime);

        // NB: These converters are applied only for logs. Queries are built in FireboltCommand class.
        //     Main idea is that we shouldn't be too concerned with possible issues here, see tests.
        SetValueToSqlConverter(typeof(double[]), ArrayConverter<double>);
        SetValueToSqlConverter(typeof(float[]), ArrayConverter<float>);
        SetValueToSqlConverter(typeof(long[]), ArrayConverter<long>);
        SetValueToSqlConverter(typeof(int[]), ArrayConverter<int>);
        SetValueToSqlConverter(typeof(string[]), ArrayConverter<string>);
        SetValueToSqlConverter(typeof(bool), ConvertToSql);
        SetValueToSqlConverter(typeof(double), ConvertToSql);
        SetValueToSqlConverter(typeof(float), ConvertToSql);

        var longDataType = new SqlDataType(DataType.Int64, typeof(long), "LONG");
        AddScalarType(typeof(long), longDataType);
        AddScalarType(typeof(long?), longDataType);
        var decimalDataType = new SqlDataType(DataType.Decimal, typeof(decimal), "DECIMAL");
        AddScalarType(typeof(decimal), decimalDataType);
        AddScalarType(typeof(decimal?), decimalDataType);
        return;

        static void ArrayConverter<T>(StringBuilder sb, SqlDataType dataType, object arr)
        {
            if (arr is not T[] data)
            {
                return;
            }

            sb.Append('[');
            for (var i = 0; i < data.Length - 1; i++)
            {
                sb.Append(ConvertToString(data[i]));
                sb.Append(',');
            }

            if (data.Length >= 1)
            {
                sb.Append(ConvertToString(data[^1]));
            }

            sb.Append(']');
        }

        static void ConvertToSql(StringBuilder sb, SqlDataType dataType, object obj)
        {
            sb.Append(ConvertToString(obj));
        }

        static string ConvertToString(object? item) =>
            item switch
            {
                double d => d.ToString("0.0################", CultureInfo.InvariantCulture),
                float f => f.ToString("0.0#########", CultureInfo.InvariantCulture),
                long l => l.ToString(CultureInfo.InvariantCulture),
                int i => i.ToString(CultureInfo.InvariantCulture),
                string s => $"'{EscapeQuotes(s)}'",
                null => "NULL",
                bool b => b ? "True" : "False",
                _ => item.ToString() ?? string.Empty,
            };

        static string EscapeQuotes(string s) => s.Replace("'", "''");
    }
}
