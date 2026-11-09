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
    /// linq2db &lt; 6.3 fallback: emit Firebolt <c>MATERIALIZED</c> when the CTE name contains
    /// <see cref="LinqExtensions.CteMaterializedEnding"/>.
    /// </summary>
    /// <remarks>
    /// Must not touch <c>CteClause.Fields</c>: that getter is <c>List&lt;SqlField&gt;</c> in 6.0–6.3 and
    /// <c>List&lt;SqlCteField&gt;</c> in 6.4, so a 6.0-compiled nupkg would fail to JIT this override
    /// on 6.4 (even for queries with no CTE). Call <c>base</c> and splice the keyword into the SQL.
    /// </remarks>
    /// <param name="with"><see cref="SqlWithClause"/> clause.</param>
    protected override void BuildWithClause(SqlWithClause? with)
    {
        if (with == null || with.Clauses.Count == 0)
        {
            return;
        }

        var start = StringBuilder.Length;
        base.BuildWithClause(with);

        for (var i = with.Clauses.Count - 1; i >= 0; i--)
        {
            var cte = with.Clauses[i];
            if (cte.Name?.Contains(LinqExtensions.CteMaterializedEnding, StringComparison.Ordinal) != true)
            {
                continue;
            }

            InsertMaterializedKeyword(start, cte.Name);
        }

        void InsertMaterializedKeyword(int withClauseStart, string cteName)
        {
            var renderedName = new StringBuilder();
            Convert(renderedName, cteName, ConvertType.NameToCteName);

            var withSql = StringBuilder.ToString(withClauseStart, StringBuilder.Length - withClauseStart);
            var nameOffset = withSql.IndexOf(renderedName.ToString(), StringComparison.Ordinal);
            if (nameOffset < 0)
            {
                return;
            }

            var asOffset = IndexOfAsKeyword(withSql, nameOffset + renderedName.Length);
            if (asOffset < 0)
            {
                return;
            }

            var insertAt = withClauseStart + asOffset + 2;
            while (insertAt < StringBuilder.Length && char.IsWhiteSpace(StringBuilder[insertAt]))
            {
                insertAt++;
            }

            StringBuilder.Insert(insertAt, "MATERIALIZED ");
        }

        static int IndexOfAsKeyword(string sql, int from)
        {
            for (var i = from; i < sql.Length - 1; i++)
            {
                if (sql[i] != 'A' || sql[i + 1] != 'S')
                {
                    continue;
                }

                var precededByNonWord = i == 0 || !char.IsLetterOrDigit(sql[i - 1]);
                var followedByNonWord = i + 2 >= sql.Length || !char.IsLetterOrDigit(sql[i + 2]);
                if (precededByNonWord && followedByNonWord)
                {
                    return i;
                }
            }

            return -1;
        }
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
