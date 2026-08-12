using System.Data.Common;
using System.Text;
using LinqToDB;
using LinqToDB.Internal.SqlProvider;
using LinqToDB.Internal.SqlQuery;
using LinqExtensions = Similarweb.LinqToDB.Firebolt.Extensions.LinqExtensions;

namespace Similarweb.LinqToDB.Firebolt;

/// <inheritdoc/>
internal class SqlBuilder : BasicSqlBuilder
{
    private const char NativeParameterPrefix = '@';

    /// <summary>
    /// Initializes a new instance of the <see cref="SqlBuilder"/> class.
    /// </summary>
    /// <param name="dataProvider">Firebolt data provider.</param>
    /// <param name="mappingSchema">Mapping schema.</param>
    /// <param name="dataOptions">Data options.</param>
    /// <param name="sqlOptimizer">SQL optimizer.</param>
    /// <param name="sqlProviderFlags">Provider flags.</param>
    internal SqlBuilder(
        DataProvider? dataProvider,
        global::LinqToDB.Mapping.MappingSchema mappingSchema,
        DataOptions dataOptions,
        ISqlOptimizer sqlOptimizer,
        SqlProviderFlags sqlProviderFlags
    ) : base(dataProvider, mappingSchema, dataOptions, sqlOptimizer, sqlProviderFlags)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="SqlBuilder"/> class.
    /// </summary>
    /// <remarks>
    /// Nested builder: copies parent <see cref="BasicSqlBuilder.AliasesContext"/> (required since linq2db 6.4).
    /// </remarks>
    /// <param name="parentBuilder">Parent SQL builder.</param>
    protected SqlBuilder(BasicSqlBuilder parentBuilder)
        : base(parentBuilder)
    {
    }

    /// <summary>
    /// Gets symbol used as parameter prefix.
    /// </summary>
    public static char ParameterSymbol => NativeParameterPrefix;

#if LINQ2DB_HAS_MATERIALIZED_CTE
    /// <inheritdoc/>
    protected override bool SupportsMaterializedCteHint => true;
#endif

    /// <inheritdoc/>
    public override StringBuilder Convert(StringBuilder sb, string value, ConvertType convertType)
    {
        return convertType switch
        {
            ConvertType.NameToQueryParameter => sb.Append(ParameterSymbol).Append(value),
            ConvertType.NameToQueryFieldAlias
                or ConvertType.NameToQueryField
                or ConvertType.NameToQueryTable
                or ConvertType.NameToQueryTableAlias
                or ConvertType.NameToCteName
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
    protected override ISqlBuilder CreateSqlBuilder() => new SqlBuilder(this);

    /// <inheritdoc/>
    protected override string LimitFormat(SelectQuery selectQuery) => "LIMIT {0}";

    /// <inheritdoc/>
    protected override string OffsetFormat(SelectQuery selectQuery) => "OFFSET {0}";

    /// <inheritdoc/>
    /// <remarks>
    /// Firebolt has no <c>NOT LIKE</c> operator (it is parsed as unknown <c>notLike</c>).
    /// Emit <c>NOT (expr LIKE pattern)</c> instead.
    /// </remarks>
    protected override void BuildLikePredicate(SqlPredicate.Like predicate)
    {
        if (!predicate.IsNot)
        {
            base.BuildLikePredicate(predicate);
            return;
        }

        var precedence = GetPrecedence(predicate);
        StringBuilder.Append("NOT (");
        BuildExpression(precedence, predicate.Expr1);
        StringBuilder
            .Append(' ')
            .Append(predicate.FunctionName ?? "LIKE")
            .Append(' ');
        BuildExpression(precedence, predicate.Expr2);

        if (predicate.Escape != null)
        {
            StringBuilder.Append(" ESCAPE ");
            BuildExpression(predicate.Escape);
        }

        StringBuilder.Append(')');
    }

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

#if !LINQ2DB_HAS_MATERIALIZED_CTE
    /// <summary>
    /// Copied from linq2db <c>BasicSqlBuilder.BuildWithClause</c> with Firebolt
    /// <c>MATERIALIZED</c> injected when the CTE name contains
    /// <see cref="LinqExtensions.CteMaterializedEnding"/> (linq2db &lt; 6.3 fallback).
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

                if (IsRecursiveCteKeywordRequired && with.Clauses.Any(static c => c.IsRecursive))
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

            var isMaterialized = cte.Name?.Contains(LinqExtensions.CteMaterializedEnding, StringComparison.Ordinal) == true;

            BuildObjectName(StringBuilder, new(cte.Name!), ConvertType.NameToCteName, true, TableOptions.NotSet);

            if (IsCteColumnListSupported)
            {
                if (cte.Fields.Count > 3)
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
                else if (cte.Fields.Count > 0)
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
#endif

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
        if (DataProvider is not DataProvider provider)
        {
            return base.GetProviderTypeName(dataContext, parameter);
        }

        var param = provider.TryGetProviderParameter(dataContext, parameter);
        return param != null
            ? provider.Adapter.GetDbType(param).ToString()
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
