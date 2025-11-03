using LinqToDB.Extensions;
using LinqToDB.SqlProvider;
using LinqToDB.SqlQuery;

namespace Similarweb.LinqToDB.Firebolt;

internal class SqlOptimizer(
    SqlProviderFlags sqlProviderFlags
) : BasicSqlOptimizer(sqlProviderFlags)
{
    /// <inheritdoc/>
    public override bool LikeIsEscapeSupported => false;

    /// <inheritdoc/>
    public override ISqlExpression ConvertExpressionImpl(ISqlExpression expression, ConvertVisitor<RunOptimizationContext> visitor)
    {
        expression = base.ConvertExpressionImpl(expression, visitor);

        return expression switch
        {
            SqlBinaryExpression be => be.Operation switch
            {
                "%" => new SqlFunction(
                    be.SystemType,
                    "Mod",
                    !be.Expr1.SystemType!.IsIntegerType() ? new SqlExpression(typeof(long), "{0}::BIGINT", Precedence.Primary, be.Expr1) : be.Expr1,
                    be.Expr2
                ),
                "^" => new SqlBinaryExpression(be.SystemType, be.Expr1, "#", be.Expr2),
                "+" => be.SystemType == typeof(string)
                    ? new SqlBinaryExpression(be.SystemType, be.Expr1, "||", be.Expr2, be.Precedence)
                    : expression,
                _ => expression,
            },
            SqlFunction func => func.Name switch
            {
                "Convert" when func.SystemType.ToUnderlying() == typeof(bool) =>
                    AlternativeConvertToBoolean(func, 1) ??
                    new SqlExpression(func.SystemType, "Cast({0} as {1})", Precedence.Primary, FloorBeforeConvert(func), func.Parameters[0]),
                "Convert" => new SqlExpression(func.SystemType, "Cast({0} as {1})", Precedence.Primary, FloorBeforeConvert(func), func.Parameters[0]),
                "CharIndex" => func.Parameters.Length == 2
                    ? new SqlExpression(func.SystemType, "Position({0} in {1})", Precedence.Primary, func.Parameters[0], func.Parameters[1])
                    : Add<int>(
                        new SqlExpression(
                            func.SystemType,
                            "Position({0} in {1})",
                            Precedence.Primary,
                            func.Parameters[0],
                            ConvertExpressionImpl(
                                new SqlFunction(
                                    typeof(string),
                                    "Substring",
                                    func.Parameters[1],
                                    func.Parameters[2],
                                    Sub<int>(
                                        ConvertExpressionImpl(new SqlFunction(typeof(int), "Length", func.Parameters[1]), visitor),
                                        func.Parameters[2]
                                    )
                                ),
                                visitor
                            )
                        ),
                        Sub(func.Parameters[2], 1)
                    ),
                _ => expression,
            },
            _ => expression,
        };
    }
}
