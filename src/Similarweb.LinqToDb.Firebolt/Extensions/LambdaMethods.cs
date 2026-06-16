using System.Linq.Expressions;
using LinqToDB;
using Similarweb.LinqToDB.Firebolt.Extensions.Builders;

namespace Similarweb.LinqToDB.Firebolt.Extensions;

/// <summary>
/// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/Lambda/">lambda functions</see> in Firebolt.
/// </summary>
public static class LambdaMethods
{
    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/reference-sql/functions-reference/lambda/transform">ARRAY_TRANSFORM</see> Firebolt method.
    /// </summary>
    /// <typeparam name="TItem">Type of the array items.</typeparam>
    /// <typeparam name="TResult">Type of resulting item.</typeparam>
    /// <param name="array">The array to sort.</param>
    /// <param name="lambda">Lambda for sort method.</param>
    /// <returns>A new array with sorted elements.</returns>
    [Sql.Extension(DataProvider.V2Id, "ARRAY_TRANSFORM({lambda}, {array})", BuilderType = typeof(LambdaBuilder), TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 10, IsAggregate = true, PreferServerSide = true)]
    public static TResult[] ArrayTransform<TItem, TResult>(
        [ExprParameter] this TItem[] array,
        Expression<Func<TItem, TResult>> lambda
    ) => throw new LinqToDBException("Not supported on client");

    /// <summary>
    /// <para>Implementation for <see href="https://docs.firebolt.io/reference-sql/functions-reference/lambda/transform">ARRAY_TRANSFORM</see> Firebolt method over two arrays.</para>
    /// <para>Applies <paramref name="lambda"/> element-wise to a pair of arrays (zipped by index).</para>
    /// </summary>
    /// <typeparam name="TItem">Type of the first array items.</typeparam>
    /// <typeparam name="TSecond">Type of the second array items.</typeparam>
    /// <typeparam name="TResult">Type of resulting item.</typeparam>
    /// <param name="array">First array (provides the first lambda parameter).</param>
    /// <param name="second">Second array (provides the second lambda parameter).</param>
    /// <param name="lambda">Transformation applied to each <c>(first, second)</c> pair.</param>
    /// <returns>A new array with transformed elements.</returns>
    [Sql.Extension(DataProvider.V2Id, "ARRAY_TRANSFORM({lambda}, {array}, {second})", BuilderType = typeof(LambdaBuilder), TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 10, IsAggregate = true, PreferServerSide = true)]
    public static TResult[] ArrayTransform<TItem, TSecond, TResult>(
        [ExprParameter] this TItem[] array,
        [ExprParameter] TSecond[] second,
        Expression<Func<TItem, TSecond, TResult>> lambda
    ) => array
        .Zip(second)
        .Select(pair => lambda.Compile().Invoke(pair.First, pair.Second))
        .ToArray();

    /// <summary>
    /// <para>Implementation for <see href="https://docs.firebolt.io/reference-sql/functions-reference/lambda/transform">ARRAY_TRANSFORM</see> Firebolt method over three arrays.</para>
    /// <para>Applies <paramref name="lambda"/> element-wise to three arrays (zipped by index).</para>
    /// </summary>
    /// <typeparam name="TItem">Type of the first array items.</typeparam>
    /// <typeparam name="TSecond">Type of the second array items.</typeparam>
    /// <typeparam name="TThird">Type of the third array items.</typeparam>
    /// <typeparam name="TResult">Type of resulting item.</typeparam>
    /// <param name="array">First array (provides the first lambda parameter).</param>
    /// <param name="second">Second array (provides the second lambda parameter).</param>
    /// <param name="third">Third array (provides the third lambda parameter).</param>
    /// <param name="lambda">Transformation applied to each <c>(first, second, third)</c> triple.</param>
    /// <returns>A new array with transformed elements.</returns>
    [Sql.Extension(DataProvider.V2Id, "ARRAY_TRANSFORM({lambda}, {array}, {second}, {third})", BuilderType = typeof(LambdaBuilder), TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 10, IsAggregate = true, PreferServerSide = true)]
    public static TResult[] ArrayTransform<TItem, TSecond, TThird, TResult>(
        [ExprParameter] this TItem[] array,
        [ExprParameter] TSecond[] second,
        [ExprParameter] TThird[] third,
        Expression<Func<TItem, TSecond, TThird, TResult>> lambda
    ) => array
        .Zip(second, (first, sec) => (first, sec))
        .Zip(third, (pair, thi) => lambda.Compile().Invoke(pair.first, pair.sec, thi))
        .ToArray();

    /// <summary>
    /// <para>Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-count.html">ARRAY_COUNT</see> Firebolt method.</para>
    /// <para>Counts the number of elements in the array for which <c>lambda.</c> returns <c>TRUE</c>.</para>
    /// </summary>
    /// <param name="array">Array to check.</param>
    /// <param name="lambda">Condition would be implemented to each element.</param>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <returns>The number of elements in the array that match the condition specified by the lambda expression.</returns>
    [Sql.Extension(DataProvider.V2Id, "ARRAY_COUNT({lambda}, {array})", BuilderType = typeof(LambdaBuilder<bool>), TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 10, IsAggregate = true, PreferServerSide = true)]
    public static int ArrayCount<T>(
        [ExprParameter("array")] this T[] array,
        Expression<Func<T, bool>> lambda
    ) => throw new LinqToDBException("Not supported on client");

    /// <summary>
    /// <para>Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/Lambda/filter.html">ARRAY_FILTER</see> Firebolt method.</para>
    /// <para>Filters the number of elements in the array for which <c>lambda.</c> returns <c>TRUE</c>.</para>
    /// </summary>
    /// <param name="array">Array to check.</param>
    /// <param name="lambda">Condition would be checked for each element.</param>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <returns>Elements in the array that match the condition specified by the lambda expression.</returns>
    [Sql.Extension(DataProvider.V2Id, "ARRAY_FILTER({lambda}, {array})", BuilderType = typeof(LambdaBuilder<bool>), TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 10, ServerSideOnly = true)]
    public static T[] ArrayFilter<T>(
        [ExprParameter("array")] this T[] array,
        Expression<Func<T, bool>> lambda
    ) => throw new LinqToDBException("Not supported on client");

    /// <summary>
    /// <para>Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/Lambda/filter.html">ARRAY_FILTER</see> Firebolt method.</para>
    /// <para>Filters the number of elements in the array for which <c>lambda.</c> returns <c>TRUE</c>.</para>
    /// </summary>
    /// <param name="array">Array to check.</param>
    /// <param name="second">Array for testing.</param>
    /// <param name="lambda">Condition would be checked for each element.</param>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <typeparam name="TF">Type of the testing array items.</typeparam>
    /// <returns>Elements in the array that match the condition specified by the lambda expression.</returns>
    [Sql.Extension(DataProvider.V2Id, "ARRAY_FILTER({lambda}, {array}, {second})", BuilderType = typeof(LambdaBuilder<bool>), TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 10, ServerSideOnly = true)]
    public static T[] ArrayFilter<T, TF>(
        [ExprParameter] this T[] array,
        [ExprParameter] TF[] second,
        Expression<Func<T, TF, bool>> lambda
    ) => array
        .Zip(second)
        .Where(pair => lambda.Compile().Invoke(pair.First, pair.Second))
        .Select(pair => pair.First)
        .ToArray();

    /// <summary>
    /// <para>Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/Lambda/array-any-match.html">ARRAY_ANY_MATCH</see> Firebolt method.</para>
    /// <para>Returns <c>TRUE</c> if any element in array satisfies condition.</para>
    /// </summary>
    /// <param name="array">Array to check.</param>
    /// <param name="lambda">Condition would be checked until any would found.</param>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <returns><c>True.</c> if element found, <c>False.</c> otherwise.</returns>
    [Sql.Extension(DataProvider.V2Id, "ARRAY_ANY_MATCH({lambda}, {array})", BuilderType = typeof(LambdaBuilder<bool>), TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 5)]
    public static bool ArrayAnyMatch<T>(
        [ExprParameter("array")] this T[] array,
        Expression<Func<T, bool>> lambda
    ) => throw new LinqToDBException("Not supported on client");

    /// <summary>
    /// <para>Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/Lambda/array-any-match.html">ARRAY_ANY_MATCH</see> Firebolt method.</para>
    /// <para>Returns <c>TRUE</c> if any element in array satisfies condition.</para>
    /// </summary>
    /// <param name="array">Array to get first lambda parameter.</param>
    /// <param name="otherArray">Array to get second lambda parameter.</param>
    /// <param name="lambda">Condition would be checked until any would found.</param>
    /// <typeparam name="T1">Type of first arrays' items.</typeparam>
    /// <typeparam name="T2">Type of second arrays' items.</typeparam>
    /// <returns><c>True.</c> if element found, <c>False.</c> otherwise.</returns>
    [Sql.Extension(DataProvider.V2Id, "ARRAY_ANY_MATCH({lambda}, {array}, {otherArray})", BuilderType = typeof(LambdaBuilder<bool>), TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1)]
    public static bool ArrayAnyMatch<T1, T2>(
        [ExprParameter("array")] this T1[] array,
        [ExprParameter("otherArray")] T2[] otherArray,
        Expression<Func<T1, T2, bool>> lambda
    ) => array.Zip(otherArray).Any(pair => lambda.Compile().Invoke(pair.First, pair.Second));

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-sort.html">ARRAY_SORT</see> Firebolt method.
    /// </summary>
    /// <typeparam name="TItem">Type of the array items.</typeparam>
    /// <typeparam name="TSort">Type for sorting element.</typeparam>
    /// <param name="array">The array to sort.</param>
    /// <param name="lambda">Lambda for sort method.</param>
    /// <returns>A new array with sorted elements.</returns>
    [Sql.Extension(DataProvider.V2Id, "ARRAY_SORT({lambda}, {array})", BuilderType = typeof(LambdaBuilder), TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 3, ServerSideOnly = true)]
    public static TItem[] ArraySort<TItem, TSort>(
        [ExprParameter] this TItem[] array,
        Expression<Func<TItem, TSort>> lambda
    ) => throw new LinqToDBException("Not supported on client");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-reverse-sort.html">ARRAY_REVERSE_SORT</see> Firebolt method.
    /// </summary>
    /// <typeparam name="TItem">Type of the array items.</typeparam>
    /// <typeparam name="TSort">Type for sorting element.</typeparam>
    /// <param name="array">The array to reverse sort.</param>
    /// <param name="lambda">Lambda for sort method.</param>
    /// <returns>A new array with reverse sorted elements.</returns>
    [Sql.Extension(DataProvider.V2Id, "ARRAY_REVERSE_SORT({lambda}, {array})", BuilderType = typeof(LambdaBuilder), TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 3, PreferServerSide = true)]
    public static TItem[] ArrayReverseSort<TItem, TSort>(
        [ExprParameter] this TItem[] array,
        Expression<Func<TItem, TSort>> lambda
    ) => throw new LinqToDBException("Not supported on client");
}
