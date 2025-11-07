using System.Data.Common;
using System.Text;
using LinqToDB;
using LinqToDB.SqlProvider;
using LinqToDB.SqlQuery;
using LinqExtensions = Similarweb.LinqToDB.Firebolt.Extensions.LinqExtensions;

namespace Similarweb.LinqToDB.Firebolt;

/// <inheritdoc/>
internal class SqlBuilder(
    DataProvider? dataProvider,
    global::LinqToDB.Mapping.MappingSchema mappingSchema,
    DataOptions dataOptions,
    ISqlOptimizer sqlOptimizer,
    SqlProviderFlags sqlProviderFlags
) : BasicSqlBuilder(
    dataProvider,
    mappingSchema,
    dataOptions,
    sqlOptimizer,
    sqlProviderFlags
)
{
    private const char NativeParameterPrefix = '@';
    private const char FbNumericParameterPrefix = '$';

    /// <summary>
    /// Gets symbol used as parameter prefix.
    /// </summary>
    public static char ParameterSymbol => NativeParameterPrefix;

    /// <inheritdoc/>
    public override StringBuilder Convert(StringBuilder sb, string value, ConvertType convertType)
    {
        return convertType switch
        {
            ConvertType.NameToQueryParameter => sb.Append(ParameterSymbol).Append(value),
            ConvertType.NameToQueryFieldAlias
                or ConvertType.NameToQueryField
                or ConvertType.NameToQueryTable
                or ConvertType.NameToServer
                or ConvertType.SequenceName
                or ConvertType.NameToSchema
                or ConvertType.TriggerName => !IsValidIdentifier(value)
                    ? sb.Append('"').Append(value).Append('"')
                    : sb.Append(value),
            _ => sb.Append(value),
        };
    }

    /// <inheritdoc/>
    protected override ISqlBuilder CreateSqlBuilder() => new SqlBuilder(dataProvider, MappingSchema, DataOptions, SqlOptimizer, SqlProviderFlags);

    /// <inheritdoc/>
    protected override string LimitFormat(SelectQuery selectQuery) => "LIMIT {0}";

    /// <inheritdoc/>
    protected override string OffsetFormat(SelectQuery selectQuery) => "OFFSET {0}";

    /// <inheritdoc/>
    protected override void BuildExprExprPredicate(SqlPredicate.ExprExpr expr)
    {
        var isGuidWorkaround = expr.Expr2.SystemType == typeof(Guid);
        BuildExpression(GetPrecedence(expr), expr.Expr1);

        BuildExprExprPredicateOperator(expr);

        if (isGuidWorkaround)
        {
            StringBuilder.Append('\'');
        }

        BuildExpression(GetPrecedence(expr), expr.Expr2);
        if (isGuidWorkaround)
        {
            StringBuilder.Append('\'');
        }
    }

    /// <inheritdoc/>
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
        {
            StringBuilder.Append("LOWER(");
        }

        base.BuildExpression(expr, buildTableName, checkParentheses, alias, ref addAlias, throwExceptionIfTableNotFound);
        if (isGuidWorkaround)
        {
            StringBuilder.Append(')');
        }

        return StringBuilder;
    }

    /// <summary>
    /// Copied as is from <c>LinqToDB.SqlProvider.SqlBuilder.BuildWithClause</c> because it does not support injection of <c>MATERIALIZED</c> keyword.
    /// </summary>
    /// <param name="with"><see cref="SqlWithClause"/> clause.</param>
    protected override void BuildWithClause(SqlWithClause? with)
    {
        if (with == null || with.Clauses.Count == 0)
        {
            return;
        }

        var first = true;

        foreach (var cte in with.Clauses)
        {
            if (first)
            {
                AppendIndent();
                StringBuilder.Append("WITH ");

                if (IsRecursiveCteKeywordRequired && with.Clauses.Any(c => c.IsRecursive))
                {
                    StringBuilder.Append("RECURSIVE ");
                }

                first = false;
            }
            else
            {
                StringBuilder.AppendLine(Comma);
                AppendIndent();
            }

            var isMaterialized = cte.Name!.Contains(LinqExtensions.CteMaterializedEnding, StringComparison.Ordinal);

            BuildObjectName(StringBuilder, new(cte.Name), ConvertType.NameToQueryTable, true, TableOptions.None);

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
                    {
                        StringBuilder.AppendLine(Comma);
                    }

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
                    {
                        StringBuilder.Append(InlineComma);
                    }

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

    /// <inheritdoc/>
    protected override void PrintParameterName(StringBuilder sb, DbParameter parameter)
    {
        if (!parameter.ParameterName.StartsWith(ParameterSymbol))
        {
            sb.Append(ParameterSymbol);
        }

        sb.Append(parameter.ParameterName);
    }

    /// <inheritdoc/>
    protected override string? GetProviderTypeName(IDataContext dataContext, DbParameter parameter)
    {
        var param = dataProvider?.TryGetProviderParameter(dataContext, parameter);
        return param != null
            ? dataProvider?.Adapter.GetDbType(param).ToString()
            : base.GetProviderTypeName(dataContext, parameter);
    }

    private bool IsValidIdentifier(string value)
    {
        return !string.IsNullOrEmpty(value) && // empty is not valid
               !IsReserved(value) && // for reserved words like `date`
               char.IsLetter(value[0]) && // no first underscores without quoting
               value.All(c => char.IsLower(c) || char.IsDigit(c) || c == '_'); // identifier should be lower_snake_case otherwise quoted
    }
}
