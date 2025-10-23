using System.Data;
using System.Text;
using LinqToDB;
using LinqToDB.SqlProvider;
using LinqToDB.SqlQuery;
using LinqExtensions = Similarweb.LinqToDB.Firebolt.Extensions.LinqExtensions;

namespace Similarweb.LinqToDB.Firebolt;

internal class SqlBuilder(
    DataProvider? dataProvider,
    global::LinqToDB.Mapping.MappingSchema mappingSchema,
    ISqlOptimizer sqlOptimizer,
    SqlProviderFlags sqlProviderFlags
) : BasicSqlBuilder(
    mappingSchema,
    sqlOptimizer,
    sqlProviderFlags
)
{
    private const char NativeParameterPrefix = '@';
    private const char FbNumericParameterPrefix = '$';

    public SqlBuilder(
        global::LinqToDB.Mapping.MappingSchema mappingSchema,
        ISqlOptimizer sqlOptimizer,
        SqlProviderFlags sqlProviderFlags
    ) : this(null, mappingSchema, sqlOptimizer, sqlProviderFlags)
    { }

    public static char ParameterSymbol => NativeParameterPrefix;

    protected override ISqlBuilder CreateSqlBuilder() => new SqlBuilder(MappingSchema, SqlOptimizer, SqlProviderFlags);

    protected override string LimitFormat(SelectQuery selectQuery) => "LIMIT {0}";

    protected override string OffsetFormat(SelectQuery selectQuery) => "OFFSET {0}";

    private bool IsValidIdentifier(string value)
    {
        return !string.IsNullOrEmpty(value) && // empty is not valid
               !IsReserved(value) && // for reserved words like `date`
               char.IsLetter(value[0]) && // no first underscores without quoting
               value.All(c => char.IsLower(c) || char.IsDigit(c) || c == '_'); // identifier should be lower_snake_case otherwise quoted
    }

    protected override void BuildExprExprPredicate(SqlPredicate.ExprExpr expr)
    {
        var isGuidWorkaround = expr.Expr2.SystemType == typeof(Guid);
        BuildExpression(GetPrecedence(expr), expr.Expr1);

        BuildExprExprPredicateOperator(expr);

        if (isGuidWorkaround)
            StringBuilder.Append('\'');
        BuildExpression(GetPrecedence(expr), expr.Expr2);
        if (isGuidWorkaround)
            StringBuilder.Append('\'');
    }

    protected override StringBuilder BuildExpression(
        ISqlExpression expr,
        bool buildTableName,
        bool checkParentheses,
        string? alias,
        ref bool addAlias,
        bool throwExceptionIfTableNotFound = true)
    {
        var isGuidWorkaround = expr.SystemType == typeof(Guid) && expr.ElementType != QueryElementType.SqlParameter;
        if (isGuidWorkaround)
            StringBuilder.Append("LOWER(");
        base.BuildExpression(expr, buildTableName, checkParentheses, alias, ref addAlias, throwExceptionIfTableNotFound);
        if (isGuidWorkaround)
            StringBuilder.Append(')');
        return StringBuilder;
    }

    public override StringBuilder Convert(StringBuilder sb, string value, ConvertType convertType)
    {
        return convertType switch
        {
            ConvertType.NameToQueryParameter => sb.Append(ParameterSymbol).Append(value),
            // needs proper list of reserved words and name validation
            ConvertType.NameToQueryFieldAlias
                or ConvertType.NameToQueryField
                or ConvertType.NameToQueryTable
                or ConvertType.NameToServer
                or ConvertType.SequenceName
                or ConvertType.NameToSchema
                or ConvertType.TriggerName => !IsValidIdentifier(value)
                    ? sb.Append('"').Append(value).Append('"')
                    : sb.Append(value),
            _ => sb.Append(value)
        };
    }

    /// <summary>
    /// copied as is from LinqToDB.SqlProvider.SqlBuilder.BuildWithClause because it does not support injection of MATERIALIZED keyword
    /// </summary>
    /// <param name="with"></param>
    protected override void BuildWithClause(SqlWithClause? with)
    {
        if (with == null || with.Clauses.Count == 0)
            return;

        var first = true;

        foreach (var cte in with.Clauses)
        {
            if (first)
            {
                AppendIndent();
                StringBuilder.Append("WITH ");

                if (IsRecursiveCteKeywordRequired && with.Clauses.Any(c => c.IsRecursive))
                    StringBuilder.Append("RECURSIVE ");

                first = false;
            }
            else
            {
                StringBuilder.AppendLine(Comma);
                AppendIndent();
            }

            var isMaterialized = cte.Name!.Contains(LinqExtensions.CteMaterializedEnding, StringComparison.Ordinal);

            ConvertTableName(StringBuilder, null, null, null, cte.Name!, TableOptions.None);

            if (cte.Fields!.Length > 3)
            {
                StringBuilder.AppendLine();
                AppendIndent();
                StringBuilder.AppendLine(OpenParens);
                ++Indent;

                var firstField = true;
                foreach (var field in cte.Fields)
                {
                    if (!firstField)
                        StringBuilder.AppendLine(Comma);
                    firstField = false;
                    AppendIndent();
                    Convert(StringBuilder, field.PhysicalName, ConvertType.NameToQueryField);
                }

                --Indent;
                StringBuilder.AppendLine();
                AppendIndent();
                StringBuilder.AppendLine(")");
            }
            else if (cte.Fields.Length > 0)
            {
                StringBuilder.Append(" (");

                var firstField = true;
                foreach (var field in cte.Fields)
                {
                    if (!firstField)
                        StringBuilder.Append(InlineComma);
                    firstField = false;
                    Convert(StringBuilder, field.PhysicalName, ConvertType.NameToQueryField);
                }
                StringBuilder.AppendLine(")");
            }
            else
            {
                StringBuilder.Append(' ');
            }

            AppendIndent();
            StringBuilder.AppendLine("AS");
            AppendIndent();
            if (isMaterialized)
            {
                StringBuilder.Append("MATERIALIZED ");
                AppendIndent();
            }
            StringBuilder.AppendLine(OpenParens);

            Indent++;

            BuildCteBody(cte.Body!);

            Indent--;

            AppendIndent();
            StringBuilder.Append(')');
        }

        StringBuilder.AppendLine();
    }

    protected override void PrintParameterName(StringBuilder sb, IDbDataParameter parameter)
    {
        if (!parameter.ParameterName.StartsWith(ParameterSymbol))
            sb.Append(ParameterSymbol);
        sb.Append(parameter.ParameterName);
    }

    protected override string? GetProviderTypeName(IDbDataParameter parameter)
    {
        var param = dataProvider?.TryGetProviderParameter(parameter, MappingSchema);
        return param != null
            ? dataProvider?.Adapter.GetDbType(param).ToString()
            : base.GetProviderTypeName(parameter);
    }
}
