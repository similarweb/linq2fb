using LinqToDB.Internal.SqlProvider;
using LinqToDB.Internal.SqlQuery;
using LinqToDB.SqlQuery;

namespace Similarweb.LinqToDB.Firebolt;

/// <inheritdoc/>
internal class SqlOptimizer(
    SqlProviderFlags sqlProviderFlags
) : BasicSqlOptimizer(sqlProviderFlags)
{
    /// <inheritdoc/>
    public override SqlExpressionConvertVisitor CreateConvertVisitor(bool allowModify) => new ConvertVisitor(allowModify);

    private class ConvertVisitor(bool allowModify) : SqlExpressionConvertVisitor(allowModify)
    {
        public override bool LikeIsEscapeSupported => false;

        /// <inheritdoc/>
        /// <remarks>
        /// Firebolt <c>LIKE</c> only accepts constant patterns. <c>string.Contains</c> over
        /// a local collection expands to <c>LIKE</c> with a column pattern, which fails. Use
        /// <c>STRPOS</c> instead (works for both constants and expressions).
        /// </remarks>
        public override ISqlPredicate ConvertSearchStringPredicate(SqlPredicate.SearchString predicate)
        {
            if (predicate.Kind != SqlPredicate.SearchString.SearchKind.Contains)
            {
                return base.ConvertSearchStringPredicate(predicate);
            }

            var dataExpr = predicate.Expr1;
            var searchExpr = predicate.Expr2;

            if (predicate.CaseSensitive.EvaluateBoolExpression(EvaluationContext) == false)
            {
                dataExpr = PseudoFunctions.MakeToLower(dataExpr, MappingSchema);
                searchExpr = PseudoFunctions.MakeToLower(searchExpr, MappingSchema);
            }

            var intType = MappingSchema.GetDbDataType(typeof(int));
            var strPos = new SqlFunction(intType, "STRPOS", dataExpr, searchExpr);
            ISqlPredicate match = new SqlPredicate.ExprExpr(
                strPos,
                SqlPredicate.Operator.Greater,
                new SqlValue(0),
                unknownAsValue: null);

            return match.MakeNot(predicate.IsNot);
        }
    }
}
