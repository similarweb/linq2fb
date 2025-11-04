using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Text;
using System.Text.RegularExpressions;
using LinqToDB;

namespace Similarweb.LinqToDB.Firebolt.Extensions.Builders;

/// <summary>
/// <para>Lambda builder for Array lambda methods.</para>
/// <para>TODO: make it use ISqlExpression.</para>
/// </summary>
/// <param name="expectedResultType">expected Result type.</param>
internal class LambdaBuilder(
    Type? expectedResultType
) : Sql.IExtensionCallBuilder
{
    private const string LambdaSign = "->";
    private const string ExpectedLambdaName = "lambda";

    private static readonly ReadOnlyDictionary<string, string> MethodList = new(
        new Dictionary<string, string>
        {
            ["ToNotNull"] = string.Empty,
            [nameof(string.Length)] = "LENGTH",
            [nameof(Regex.IsMatch)] = "REGEXP_LIKE",
        }
    );

    /// <summary>
    /// Initializes a new instance of the <see cref="LambdaBuilder"/> class.
    /// </summary>
    public LambdaBuilder() : this(null)
    {
    }

    /// <inheritdoc/>
    public void Build(Sql.ISqExtensionBuilder builder)
    {
        var lambda = builder.GetValue<Expression>(ExpectedLambdaName);
        while (lambda.CanReduce)
        {
            lambda = lambda.Reduce();
        }

        var validLambda = lambda as LambdaExpression ??
                          throw new LinqToDBException($"Invalid lambda expression type: {lambda.Type}. Expected LambdaExpression.");
        if (expectedResultType != null &&
            validLambda.ReturnType != expectedResultType &&
            validLambda.Parameters.Count == 1)
        {
            throw new LinqToDBException($"Invalid lambda expression type: {validLambda.Type}. Expected Func<T, bool>.");
        }

        var sqlExpr = new StringBuilder(validLambda.Parameters[0].Name);
        for (var i = 1; i < validLambda.Parameters.Count; i++)
        {
            sqlExpr.Append(", ");
            sqlExpr.Append(validLambda.Parameters[i].Name);
        }

        RecursiveParse(validLambda.Body, sqlExpr.Append(' ').Append(LambdaSign).Append(' '));
        builder.AddExpression(ExpectedLambdaName, sqlExpr.ToString());
        return;

        string GetOperator(BinaryExpression expr) =>
            expr.NodeType switch
            {
                ExpressionType.GreaterThan => ">",
                ExpressionType.GreaterThanOrEqual => ">=",
                ExpressionType.LessThan => "<",
                ExpressionType.LessThanOrEqual => "<=",
                ExpressionType.Equal => "=",
                ExpressionType.NotEqual => "!=",
                ExpressionType.Modulo => "%",
                _ => throw new LinqToDBException($"Invalid operator: {expr.NodeType}"),
            };

        StringBuilder RecursiveParse(Expression expr, StringBuilder? sb = null)
        {
            var innerBuilder = sb ?? new StringBuilder();
            switch (expr)
            {
                case ConditionalExpression conditionalExpression:
                    innerBuilder.Append("IF(");
                    RecursiveParse(conditionalExpression.Test, innerBuilder);
                    innerBuilder.Append(',');
                    RecursiveParse(conditionalExpression.IfTrue, innerBuilder);
                    innerBuilder.Append(',');
                    RecursiveParse(conditionalExpression.IfFalse, innerBuilder);
                    innerBuilder.AppendLine(")");
                    break;
                case UnaryExpression unaryExpression:
                    switch (unaryExpression.NodeType)
                    {
                        case ExpressionType.Negate:
                            innerBuilder.Append('-').Append(unaryExpression.Operand);
                            break;
                        case ExpressionType.Convert:
                            RecursiveParse(unaryExpression.Operand, innerBuilder);

                            // usually we don't need to explicitly cast to nullables.
                            if (Nullable.GetUnderlyingType(unaryExpression.Type) == null)
                            {
                                innerBuilder.Append("::").Append(builder.Mapping.GetDataType(unaryExpression.Type).Type.DbType);
                            }

                            break;
                    }

                    break;
                case BinaryExpression binaryExpr:
                    var left = RecursiveParse(binaryExpr.Left);
                    var right = RecursiveParse(binaryExpr.Right);
                    if (binaryExpr.NodeType == ExpressionType.Coalesce)
                    {
                        innerBuilder
                            .Append("COALESCE(")
                            .Append(left)
                            .Append(", ")
                            .Append(right)
                            .Append(')');
                    }
                    else
                    {
                        innerBuilder
                            .Append('(')
                            .Append(left)
                            .Append(' ')
                            .Append(GetOperator(binaryExpr))
                            .Append(' ')
                            .Append(right)
                            .Append(')');
                    }

                    break;
                case MemberExpression memberExpr:
                    innerBuilder.Append(memberExpr.Member.Name);
                    break;
                case ParameterExpression parameterExpr:
                    innerBuilder.Append(parameterExpr.Name);
                    break;
                case ConstantExpression constantExpr:
                    if (constantExpr.Value == null)
                    {
                        innerBuilder.Append("NULL");
                        break;
                    }

                    if (constantExpr.Type == typeof(string) ||
                        constantExpr.Type == typeof(Guid) ||
                        constantExpr.Type == typeof(DateTime))
                    {
                        innerBuilder.Append('\'').Append(constantExpr.Value).Append('\'');
                        break;
                    }

                    innerBuilder.Append(constantExpr.Value);
                    break;
                case MethodCallExpression methodCallExpr:
                    if (!MethodList.TryGetValue(methodCallExpr.Method.Name, out var sqlMethodName))
                    {
                        throw new LinqToDBException("Unsupported method: " + methodCallExpr.Method.Name);
                    }

                    if (!string.IsNullOrEmpty(sqlMethodName))
                    {
                        innerBuilder.Append(sqlMethodName).Append('(');
                    }

                    for (var i = 0; i < methodCallExpr.Arguments.Count; i++)
                    {
                        if (i > 0)
                        {
                            innerBuilder.Append(", ");
                        }

                        RecursiveParse(methodCallExpr.Arguments[i], innerBuilder);
                    }

                    if (!string.IsNullOrEmpty(sqlMethodName))
                    {
                        innerBuilder.Append(')');
                    }

                    break;
                default:
                    throw new LinqToDBException(
                        $"Invalid expression type: {expr.GetType().Name}");
            }

            return innerBuilder;
        }
    }
}
