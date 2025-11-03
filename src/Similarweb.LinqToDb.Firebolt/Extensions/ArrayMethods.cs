using System.Linq.Expressions;
using LinqToDB;
using LinqToDB.Linq;
using Similarweb.LinqToDB.Firebolt.Extensions.Builders;

namespace Similarweb.LinqToDB.Firebolt.Extensions;

/// <summary>
/// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/">array functions</see> in Firebolt.
/// </summary>
public static partial class ArrayMethods
{
    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/array-agg.html">ARRAY_AGG</see> Firebolt method.
    /// </summary>
    /// <param name="collection">Collection on which should be performed.</param>
    /// <param name="expr">expression to extract value.</param>
    /// <typeparam name="TEntity">collection item type.</typeparam>
    /// <typeparam name="TV">array item type.</typeparam>
    /// <returns><see cref="Sql.IAggregateFunctionNotOrdered{T,TR}"/>.</returns>
    [Sql.Extension(DataProvider.V2Id, "ARRAY_AGG({expr})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsAggregate = true)]
    public static Sql.IAggregateFunctionNotOrdered<TEntity, TV[]> ArrayAggregate<TEntity, TV>(
        this IEnumerable<TEntity> collection,
        [ExprParameter] Expression<Func<TEntity, TV>> expr
    ) => throw new LinqException("Not implemented on client side.");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/array-agg.html">ARRAY_AGG</see> Firebolt method (nullable version).
    /// </summary>
    /// <param name="collection">Collection on which should be performed.</param>
    /// <param name="expr">expression to extract nullable value.</param>
    /// <typeparam name="TEntity">collection item type.</typeparam>
    /// <typeparam name="TV">array item type.</typeparam>
    /// <returns><see cref="Sql.IAggregateFunctionNotOrdered{T,TR}"/>.</returns>
    [Sql.Extension(DataProvider.V2Id, "ARRAY_AGG({expr})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsAggregate = true)]
    public static Sql.IAggregateFunctionNotOrdered<TEntity, TV?[]> ArrayAggregate<TEntity, TV>(
        this IEnumerable<TEntity> collection,
        [ExprParameter] Expression<Func<TEntity, TV?>> expr
    )
        where TV : struct
        => throw new LinqException("Not implemented on client side.");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/array-agg.html">ARRAY_AGG</see> Firebolt method.
    /// </summary>
    /// <param name="collection">Query on which should be performed.</param>
    /// <param name="expr">expression to extract value.</param>
    /// <typeparam name="TEntity">query item type.</typeparam>
    /// <typeparam name="TV">array item type.</typeparam>
    /// <returns><see cref="Sql.IAggregateFunctionNotOrdered{T,TR}"/>.</returns>
    [Sql.Extension(DataProvider.V2Id, "ARRAY_AGG({expr})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsAggregate = true)]
    public static Sql.IAggregateFunctionNotOrdered<TEntity, TV[]> ArrayAggregate<TEntity, TV>(
        this IQueryable<TEntity> collection,
        [ExprParameter] Expression<Func<TEntity, TV>> expr
    )
    {
        ArgumentNullException.ThrowIfNull(collection);
        ArgumentNullException.ThrowIfNull(expr);

        var currentSource = global::LinqToDB.LinqExtensions.ProcessSourceQueryable?.Invoke(collection) ?? collection;

        var query = currentSource.Provider.CreateQuery<TV[]>(
            Expression.Call(
                null,
                MethodHelper.GetMethodInfo(ArrayAggregate, collection, expr),
                currentSource.Expression,
                Expression.Quote(expr)
            )
        );

        return new Sql.AggregateFunctionNotOrderedImpl<TEntity, TV[]>(query);
    }

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/array-agg.html">ARRAY_AGG</see> Firebolt method.
    /// </summary>
    /// <param name="dummy">Sequence on which should be performed.</param>
    /// <param name="expr">value to aggregate.</param>
    /// <typeparam name="TV">array item type.</typeparam>
    /// <returns><see cref="AnalyticFunctions.IAggregateFunctionSelfContained{TR}"/>.</returns>
    [Sql.Extension(DataProvider.V2Id, "ARRAY_AGG({expr})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<TV[]> ArrayAggregate<TV>(
        this Sql.ISqlExtension? dummy,
        [ExprParameter] TV? expr
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-contains.html">ARRAY_CONTAINS</see> Firebolt method.
    /// </summary>
    /// <param name="array">Array to check.</param>
    /// <param name="value">Value to check for.</param>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <returns>True if the array contains the value, otherwise false.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_CONTAINS({0}, {1})", PreferServerSide = true)]
    public static bool ArrayContains<T>(this T[] array, T value) => array.Contains(value);

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-distinct.html">ARRAY_DISTINCT</see> Firebolt method.
    /// </summary>
    /// <param name="array">array for deduplication.</param>
    /// <typeparam name="T">type of the array items.</typeparam>
    /// <returns>deduplicated array.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_DISTINCT({0})", PreferServerSide = true)]
    public static T[] ArrayDistinct<T>(this T[] array) => array.Distinct().ToArray();

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-sort.html">ARRAY_SORT</see> Firebolt method.
    /// </summary>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <param name="array">The array to sort.</param>
    /// <returns>A new array with sorted elements.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_SORT({0})", PreferServerSide = true)]
    public static T[] ArraySort<T>(this T[] array) => array.OrderBy(x => x).ToArray();

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
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <param name="array">The array to reverse sort.</param>
    /// <returns>A new array with reverse sorted elements.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_REVERSE_SORT({0})", PreferServerSide = true)]
    public static T[] ArrayReverseSort<T>(this T[] array) => array.OrderByDescending(x => x).ToArray();

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
    /// <para>Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-count.html">ARRAY_COUNT</see> Firebolt method.</para>
    /// <para>Counts the number of <c>TRUE</c> or <c>NOT NULL</c> elements in the array.</para>
    /// </summary>
    /// <param name="array">The array to count.</param>
    /// <returns>The number of elements in the array.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_COUNT({0})", PreferServerSide = true)]
    public static int ArrayCount(this bool[] array) => array.Count(x => x);

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
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-flatten.html">ARRAY_FLATTEN</see> Firebolt method.
    /// </summary>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <param name="array">The multi-dimensional array to flatten.</param>
    /// <returns>A one-dimensional array containing all elements.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_FLATTEN({0})", PreferServerSide = true)]
    public static T[] ArrayFlatten<T>(this T[][] array) => array.SelectMany(x => x).ToArray();

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-reverse.html">ARRAY_REVERSE</see> Firebolt method.
    /// </summary>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <param name="array">The array to reverse.</param>
    /// <returns>A new array with elements in reverse order.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_REVERSE({0})", PreferServerSide = true)]
    public static T[] ArrayReverse<T>(this T[] array) => array.Reverse().ToArray();

    /// <summary>
    /// Implements the <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-min.html">ARRAY_MIN</see> Firebolt method.
    /// </summary>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <param name="array">The array to find the minimum value in.</param>
    /// <returns>The minimum value in the array.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_MIN({0})", PreferServerSide = true)]
    public static T ArrayMin<T>(this T[] array)
        where T : struct => array.Min();

    /// <summary>
    /// Implements the <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-sum.html">ARRAY_SUM</see> Firebolt method.
    /// </summary>
    /// <param name="array">The array to sum up.</param>
    /// <returns>The sum of all values in the array.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_SUM({0})", PreferServerSide = true)]
    public static double ArraySum(this double[] array) => array.Sum();

    /// <summary>
    /// Implements the <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-sum.html">ARRAY_SUM</see> Firebolt method.
    /// </summary>
    /// <param name="array">The array to sum up.</param>
    /// <returns>The sum of all values in the array.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_SUM({0})", PreferServerSide = true)]
    public static long ArraySum(this int[] array) => array.Sum();

    /// <summary>
    /// Implements the <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-sum.html">ARRAY_SUM</see> Firebolt method.
    /// </summary>
    /// <param name="array">The array to sum up.</param>
    /// <returns>The sum of all values in the array.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_SUM({0})", PreferServerSide = true)]
    public static long ArraySum(this long[] array) => array.Sum();

    /// <summary>
    /// Implements the <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-max.html">ARRAY_MAX</see> Firebolt method.
    /// </summary>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <param name="array">The array to find the maximum value in.</param>
    /// <returns>The maximum value in the array.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_MAX({0})", PreferServerSide = true)]
    public static T ArrayMax<T>(this T[] array)
        where T : struct => array.Max();

    /// <summary>
    /// Implements the <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-intersect.html">ARRAY_INTERSECT</see> Firebolt method.
    /// </summary>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <param name="array">The first array.</param>
    /// <param name="other">Other arrays.</param>
    /// <returns>An array containing elements present in all arrays.</returns>
    [Sql.Extension(DataProvider.V2Id, "ARRAY_INTERSECT({array}, {other, ', '})", PreferServerSide = true)]
    public static T[] ArrayIntersect<T>(
        [ExprParameter] this T[] array,
        [ExprParameter] params T[][] other
    ) => other.Aggregate((IEnumerable<T>)array, (current, otherArray) => current.Intersect(otherArray), result => result.ToArray());

    /// <summary>
    /// Implements the <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-length.html">ARRAY_LENGTH</see> Firebolt method.
    /// </summary>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <param name="array">The array.</param>
    /// <returns>Array length.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_LENGTH({0})", PreferServerSide = true)]
    public static int ArrayLength<T>(
        this T[] array
    ) => throw new LinqToDBException("Not supported on client");

    /// <summary>
    /// Implements the <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-slice.html">ARRAY_SLICE</see> Firebolt method.
    /// </summary>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <param name="array">The array.</param>
    /// <param name="startIndex">1-based starting index.</param>
    /// <returns>Array length.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_SLICE({0}, {1})", PreferServerSide = true)]
    public static T[] ArraySlice<T>(
        this T[] array,
        int startIndex
    ) => array.Skip(startIndex - 1).ToArray();

    /// <summary>
    /// Implements the <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-slice.html">ARRAY_SLICE</see> Firebolt method.
    /// </summary>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <param name="array">The array.</param>
    /// <param name="startIndex">1-based starting index.</param>
    /// <param name="length">slice size.</param>
    /// <returns>Array length.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_SLICE({0}, {1}, {2})", PreferServerSide = true)]
    public static T[] ArraySlice<T>(
        this T[] array,
        int startIndex,
        int length
    ) => array.Skip(startIndex - 1).Take(length).ToArray();

    /// <summary>
    /// Implements the <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-to-string.html">ARRAY_TO_STRING</see> Firebolt method.
    /// </summary>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <param name="array">The array.</param>
    /// <param name="separator">separator.</param>
    /// <returns>Array length.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_TO_STRING({0}, {1})", PreferServerSide = true)]
    public static string ArrayToString<T>(
        this T[] array,
        string separator
    ) => string.Join(separator, array.Where(item => item != null));

    /// <summary>
    /// Implements the <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-to-string.html">ARRAY_TO_STRING</see> Firebolt method.
    /// </summary>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <param name="array">The array.</param>
    /// <param name="separator">separator.</param>
    /// <returns>Array length.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_TO_STRING({0}, {1})", PreferServerSide = true)]
    public static string ArrayToString<T>(
        this T[][] array,
        string separator
    ) => string.Join(separator, array.SelectMany(x => x).Where(item => item != null));

    /// <summary>
    /// Implements the <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-to-string.html">ARRAY_TO_STRING</see> Firebolt method.
    /// </summary>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <param name="array">The array.</param>
    /// <returns>Array length.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_TO_STRING({0})", PreferServerSide = true)]
    public static string ArrayToString<T>(
        this T[] array
    ) => string.Join(string.Empty, array.Where(item => item != null));

    /// <summary>
    /// Implements the <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/array-to-string.html">ARRAY_TO_STRING</see> Firebolt method.
    /// </summary>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <param name="array">The array.</param>
    /// <returns>Array length.</returns>
    [Sql.Expression(DataProvider.V2Id, "ARRAY_TO_STRING({0})", PreferServerSide = true)]
    public static string ArrayToString<T>(
        this T[][] array
    ) => string.Join(string.Empty, array.SelectMany(x => x).Where(item => item != null));

    /// <summary>
    /// Implements the <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/arrays-overlap.html">ARRAYS_OVERLAP</see> Firebolt method.
    /// </summary>
    /// <typeparam name="T">Type of the array items.</typeparam>
    /// <param name="array">Base array.</param>
    /// <param name="other">Other arrays to check.</param>
    /// <returns>If there are any element which exists in all arrays.</returns>
    [Sql.Extension(DataProvider.V2Id, "ARRAYS_OVERLAP({array}, {other, ', '})", PreferServerSide = true)]
    public static bool ArraysOverlap<T>(
        [ExprParameter] this T[] array,
        [ExprParameter] params T[][] other
    )
    {
        var otherSets = other.Select(otherArr => otherArr.ToHashSet()).ToArray();
        return array.Any(item => otherSets.All(otherArr => otherArr.Contains(item)));
    }

    /// <summary>
    /// <para>Unnest the array into a table.</para>
    /// </summary>
    /// <param name="dummy">dummy.</param>
    /// <param name="array">for unnesting.</param>
    /// <typeparam name="T">item type.</typeparam>
    /// <returns>Table.</returns>
    [Sql.Expression(DataProvider.V2Id, "UNNEST({1})", ServerSideOnly = true)]
    public static T Unnest<T>(
        this Sql.ISqlExtension? dummy,
        T[] array
    ) => throw new LinqToDBException("Unnest method may be used in query only, not in C# code. For C# use SelectMany");

    /// <summary>
    /// Unnest array and use it as table.
    /// </summary>
    /// <param name="dc">context.</param>
    /// <param name="array">array to be unnested.</param>
    /// <typeparam name="T">array type.</typeparam>
    /// <returns>table to be queried.</returns>
    [ExpressionMethod(nameof(UnnestImpl))]
    public static IQueryable<T> Unnest<T>(this IDataContext dc, T[] array)
    {
        return UnnestImpl<T>().Compile().Invoke(dc, array);
    }

    /// <summary>
    /// Unnest array and use it as table.
    /// </summary>
    /// <param name="dc">context.</param>
    /// <param name="array1">1st array to be unnested.</param>
    /// <param name="array2">2nd array to be unnested.</param>
    /// <typeparam name="T1">1st array type.</typeparam>
    /// <typeparam name="T2">2nd array type.</typeparam>
    /// <returns>table to be queried.</returns>
    [ExpressionMethod(nameof(UnnestImpl))]
    public static IQueryable<Unnested<T1, T2>> Unnest<T1, T2>(this IDataContext dc, T1[] array1, T2[] array2)
    {
        return UnnestImpl<T1, T2>().Compile().Invoke(dc, array1, array2);
    }

    /// <summary>
    /// Unnest array and use it as table.
    /// </summary>
    /// <param name="dc">context.</param>
    /// <param name="array1">1st array to be unnested.</param>
    /// <param name="array2">2nd array to be unnested.</param>
    /// <param name="array3">3rd array to be unnested.</param>
    /// <typeparam name="T1">1st array type.</typeparam>
    /// <typeparam name="T2">2nd array type.</typeparam>
    /// <typeparam name="T3">3rd array type.</typeparam>
    /// <returns>table to be queried.</returns>
    [ExpressionMethod(nameof(UnnestImpl))]
    public static IQueryable<Unnested<T1, T2, T3>> Unnest<T1, T2, T3>(this IDataContext dc, T1[] array1, T2[] array2, T3[] array3)
    {
        return UnnestImpl<T1, T2, T3>().Compile().Invoke(dc, array1, array2, array3);
    }

    /// <summary>
    /// Unnest array and use it as table.
    /// </summary>
    /// <param name="dc">context.</param>
    /// <param name="array1">1st array to be unnested.</param>
    /// <param name="array2">2nd array to be unnested.</param>
    /// <param name="array3">3rd array to be unnested.</param>
    /// <param name="array4">4th array to be unnested.</param>
    /// <typeparam name="T1">1st array type.</typeparam>
    /// <typeparam name="T2">2nd array type.</typeparam>
    /// <typeparam name="T3">3rd array type.</typeparam>
    /// <typeparam name="T4">4th array type.</typeparam>
    /// <returns>table to be queried.</returns>
    [ExpressionMethod(nameof(UnnestImpl))]
    public static IQueryable<Unnested<T1, T2, T3, T4>> Unnest<T1, T2, T3, T4>(this IDataContext dc, T1[] array1, T2[] array2, T3[] array3, T4[] array4)
    {
        return UnnestImpl<T1, T2, T3, T4>().Compile().Invoke(dc, array1, array2, array3, array4);
    }

    private static Expression<Func<IDataContext, T[], IQueryable<T>>> UnnestImpl<T>()
    {
        return (dc, array) => dc.FromSql<Unnested<T>>($"SELECT \"First\" FROM UNNEST({array}) t(\"First\")").Select(x => x.First);
    }

    private static Expression<Func<IDataContext, T1[], T2[], IQueryable<Unnested<T1, T2>>>> UnnestImpl<T1, T2>()
    {
        return (dc, array1, array2) => dc.FromSql<Unnested<T1, T2>>($"SELECT \"First\", \"Second\" FROM UNNEST({array1}, {array2}) t(\"First\", \"Second\")");
    }

    private static Expression<Func<IDataContext, T1[], T2[], T3[], IQueryable<Unnested<T1, T2, T3>>>> UnnestImpl<T1, T2, T3>()
    {
        return (dc, array1, array2, array3) => dc.FromSql<Unnested<T1, T2, T3>>($"SELECT \"First\", \"Second\", \"Third\" FROM UNNEST({array1}, {array2}, {array3}) t(\"First\", \"Second\", \"Third\")");
    }

    private static Expression<Func<IDataContext, T1[], T2[], T3[], T4[], IQueryable<Unnested<T1, T2, T3, T4>>>> UnnestImpl<T1, T2, T3, T4>()
    {
        return (dc, array1, array2, array3, array4) => dc.FromSql<Unnested<T1, T2, T3, T4>>(
            $"""
             SELECT "First", "Second", "Third", "Fourth"
             FROM UNNEST({array1}, {array2}, {array3}, {array4})
                 t("First", "Second", "Third", "Fourth")
             """
            );
    }
}
