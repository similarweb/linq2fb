using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Text;
using System.Text.RegularExpressions;
using LinqToDB;
using LinqToDB.Expressions;
using LinqToDB.Linq;

namespace Similarweb.LinqToDB.Firebolt.Extensions.Builders;

/// <summary>
/// <para>Lambda builder for Array lambda methods.</para>
/// <para>
/// Firebolt lambdas use <c>param -&gt; expr</c> syntax, so we hand-roll SQL from the
/// expression tree. Normal LINQ uses <c>IMemberTranslator</c>;
/// Extension builders do not. Before rendering we run <see cref="Expressions.ConvertMember"/>
/// so MappingSchema MapMembers (e.g. <c>string.Length</c> → <c>Sql.Length</c>) apply the same
/// way they did under linq2db v5.
/// </para>
/// <para>TODO: make it use ISqlExpression / ConvertExpressionToSql where binders allow.</para>
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
    public void Build(Sql.ISqlExtensionBuilder builder)
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

        // Apply MappingSchema MapMembers (string.Length → Sql.Length, etc.) before hand-rolling.
        var body = ExpandMembers(validLambda.Body, builder.Mapping);

        var sqlExpr = new StringBuilder(validLambda.Parameters[0].Name);
        for (var i = 1; i < validLambda.Parameters.Count; i++)
        {
            sqlExpr.Append(", ");
            sqlExpr.Append(validLambda.Parameters[i].Name);
        }

        RecursiveParse(body, sqlExpr.Append(' ').Append(LambdaSign).Append(' '));
        builder.AddFragment(ExpectedLambdaName, sqlExpr.ToString());
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
                        case ExpressionType.ConvertChecked:
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
                    // Nullable<T>.Value is an artifact of Sql.* returning T?; omit it.
                    if (IsNullableValueAccess(memberExpr))
                    {
                        RecursiveParse(memberExpr.Expression!, innerBuilder);
                        break;
                    }

                    if (MethodList.TryGetValue(memberExpr.Member.Name, out var memberSql) &&
                        !string.IsNullOrEmpty(memberSql))
                    {
                        innerBuilder.Append(memberSql).Append('(');
                        if (memberExpr.Expression != null)
                        {
                            RecursiveParse(memberExpr.Expression, innerBuilder);
                        }

                        innerBuilder.Append(')');
                        break;
                    }

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

                    var wroteArg = false;
                    if (methodCallExpr.Object != null)
                    {
                        RecursiveParse(methodCallExpr.Object, innerBuilder);
                        wroteArg = true;
                    }

                    for (var i = 0; i < methodCallExpr.Arguments.Count; i++)
                    {
                        if (wroteArg || i > 0)
                        {
                            innerBuilder.Append(", ");
                        }

                        RecursiveParse(methodCallExpr.Arguments[i], innerBuilder);
                        wroteArg = true;
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

    /// <summary>
    /// Applies <see cref="Expressions.ConvertMember"/> mappings from <see cref="MappingSchema"/>
    /// so Extension-builder lambdas see the same rewrites as linq2db v5 (e.g. Length → Sql.Length).
    /// </summary>
    private static Expression ExpandMembers(Expression expression, global::LinqToDB.Mapping.MappingSchema mapping) =>
        expression.Transform(mapping, static (ms, e) =>
        {
            switch (e)
            {
                case MemberExpression memberExpr:
                {
                    if (IsNullableValueAccess(memberExpr))
                    {
                        return memberExpr.Expression!;
                    }

                    var converted = Expressions.ConvertMember(ms, memberExpr.Expression?.Type, memberExpr.Member);
                    if (converted != null)
                    {
                        return ApplyMemberMapping(converted, memberExpr.Expression, e.Type);
                    }

                    break;
                }

                case MethodCallExpression methodCall:
                {
                    var converted = Expressions.ConvertMember(ms, methodCall.Object?.Type, methodCall.Method);
                    if (converted != null)
                    {
                        return ApplyMethodMapping(converted, methodCall, e.Type);
                    }

                    break;
                }
            }

            return e;
        });

    private static Expression ApplyMemberMapping(LambdaExpression mapping, Expression? instance, Type targetType)
    {
        var body = UnwrapMappingBody(mapping);
        var replaced = body.Transform(
            (mapping.Parameters, instance),
            static (ctx, node) =>
            {
                if (node is not ParameterExpression param)
                {
                    return node;
                }

                var index = ctx.Parameters.IndexOf(param);
                if (index < 0)
                {
                    return node;
                }

                // First mapping parameter is the instance for member mappings.
                return index == 0 && ctx.instance != null ? ctx.instance : node;
            });

        return replaced.Type == targetType ? replaced : Expression.Convert(replaced, targetType);
    }

    private static Expression ApplyMethodMapping(LambdaExpression mapping, MethodCallExpression call, Type targetType)
    {
        var body = UnwrapMappingBody(mapping);
        var replaced = body.Transform(
            (mapping.Parameters, call),
            static (ctx, node) =>
            {
                if (node is not ParameterExpression param)
                {
                    return node;
                }

                var index = ctx.Parameters.IndexOf(param);
                if (index < 0)
                {
                    return node;
                }

                if (!ctx.call.Method.IsStatic)
                {
                    return index == 0 ? ctx.call.Object! : ctx.call.Arguments[index - 1];
                }

                return ctx.call.Arguments[index];
            });

        return replaced.Type == targetType ? replaced : Expression.Convert(replaced, targetType);
    }

    private static Expression UnwrapMappingBody(LambdaExpression mapping)
    {
        var body = mapping.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Quote } quote)
        {
            body = quote.Operand;
        }

        return body is LambdaExpression inner ? inner.Body : body;
    }

    private static bool IsNullableValueAccess(MemberExpression memberExpr) =>
        memberExpr.Member.Name == nameof(Nullable<int>.Value) &&
        memberExpr.Expression != null &&
        Nullable.GetUnderlyingType(memberExpr.Expression.Type) != null;
}
