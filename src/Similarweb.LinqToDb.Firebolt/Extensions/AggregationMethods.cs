using System.Linq.Expressions;
using LinqToDB;

namespace Similarweb.LinqToDB.Firebolt.Extensions;

/// <summary>
/// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/">aggregation functions.</see> in Firebolt.
/// </summary>
public static class AggregationMethods
{
        /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/any-value.html">ANY_VALUE</see> Firebolt method.
    /// </summary>
    /// <param name="collection">collection to retrieve preferably non-<c>null.</c> value.</param>
    /// <param name="expr">selector.</param>
    /// <typeparam name="T">Type.</typeparam>
    /// <typeparam name="TR">Resulting type.</typeparam>
    /// <returns>first non-<c>null.</c> value (or <c>null.</c> if all are <c>null.</c>s).</returns>
    [Sql.Extension(DataProvider.V2Id, "ANY_VALUE({expr})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<TR?> AnyValue<T, TR>(
        this IEnumerable<T> collection,
        [ExprParameter] Expression<Func<T, TR>> expr
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/any-value.html">ANY_VALUE</see> Firebolt method.
    /// </summary>
    /// <param name="dummy"><see cref="Sql.ISqlExtension"/> dummy anchor.</param>
    /// <param name="expr">expression to get value.</param>
    /// <typeparam name="TR">Resulting type.</typeparam>
    /// <returns>first non-<c>null.</c> value (or <c>null.</c> if all are <c>null.</c>s).</returns>
    [Sql.Extension(DataProvider.V2Id, "ANY_VALUE({expr})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<TR?> AnyValue<TR>(
        this Sql.ISqlExtension? dummy,
        [ExprParameter] TR? expr
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/approx-count-distinct.html">APPROX_COUNT_DISTINCT</see> Firebolt method.
    /// </summary>
    /// <param name="collection">collection to retrieve preferably non-<c>null.</c> value.</param>
    /// <param name="selector">selector.</param>
    /// <typeparam name="T">Type.</typeparam>
    /// <typeparam name="TR">Resulting type.</typeparam>
    /// <returns>approximate count.</returns>
    [Sql.Expression(DataProvider.V2Id, "APPROX_COUNT_DISTINCT({1})", PreferServerSide = true, IsAggregate = true)]
    public static int ApproxCountDistinct<T, TR>(
        this IEnumerable<T> collection,
        Expression<Func<T, TR?>> selector
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/approx-count-distinct.html">APPROX_COUNT_DISTINCT</see> Firebolt method.
    /// </summary>
    /// <param name="dummy"><see cref="Sql.ISqlExtension"/> dummy anchor.</param>
    /// <param name="expr">expression to get value.</param>
    /// <typeparam name="T">Item type.</typeparam>
    /// <returns>approximate count.</returns>
    [Sql.Extension(DataProvider.V2Id, "APPROX_COUNT_DISTINCT({expr})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<int> ApproxCountDistinct<T>(
        this Sql.ISqlExtension? dummy,
        [ExprParameter] T? expr
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/bit-and.html">BIT_AND</see> Firebolt method.
    /// </summary>
    /// <param name="collection">Collection on which should be performed.</param>
    /// <param name="dataExpression">expression to extract value.</param>
    /// <typeparam name="TEntity">collection item type.</typeparam>
    /// <returns>Bitwise <c>AND</c> on collection.</returns>
    [Sql.Expression(DataProvider.V2Id, "BIT_AND({1})", PreferServerSide = true, IsAggregate = true)]
    public static int BitAnd<TEntity>(
        this IEnumerable<TEntity> collection,
        Expression<Func<TEntity, int>> dataExpression
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/bit-and.html">BIT_AND</see> Firebolt method.
    /// </summary>
    /// <param name="collection">Collection on which should be performed.</param>
    /// <param name="dataExpression">expression to extract value.</param>
    /// <typeparam name="TEntity">collection item type.</typeparam>
    /// <returns>Bitwise <c>AND</c> on collection.</returns>
    [Sql.Expression(DataProvider.V2Id, "BIT_AND({1})", PreferServerSide = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<int> BitAnd<TEntity>(
        this IQueryable<TEntity> collection,
        Expression<Func<TEntity, int>> dataExpression
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/bit-and.html">BIT_AND</see> Firebolt method.
    /// </summary>
    /// <param name="dummy"><see cref="Sql.ISqlExtension"/> dummy anchor.</param>
    /// <param name="expr">value to be aggregated.</param>
    /// <returns>Bitwise <c>AND</c> on collection.</returns>
    [Sql.Extension(DataProvider.V2Id, "BIT_AND({expr})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<int> BitAnd(
        this Sql.ISqlExtension? dummy,
        [ExprParameter] int expr
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/bit-and.html">BIT_AND</see> Firebolt method.
    /// </summary>
    /// <param name="collection">Collection on which should be performed.</param>
    /// <param name="dataExpression">expression to extract value.</param>
    /// <typeparam name="TEntity">collection item type.</typeparam>
    /// <returns>Bitwise <c>AND</c> on collection.</returns>
    [Sql.Expression(DataProvider.V2Id, "BIT_AND({1})", PreferServerSide = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<long> BitAnd<TEntity>(
        this IEnumerable<TEntity> collection,
        Expression<Func<TEntity, long>> dataExpression
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/bit-and.html">BIT_AND</see> Firebolt method.
    /// </summary>
    /// <param name="dummy"><see cref="Sql.ISqlExtension"/> dummy anchor.</param>
    /// <param name="expr">value to be aggregated.</param>
    /// <returns>Bitwise <c>AND</c> on collection.</returns>
    [Sql.Extension(DataProvider.V2Id, "BIT_AND({expr})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<long> BitAnd(
        this Sql.ISqlExtension? dummy,
        [ExprParameter] long expr
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/bit-or.html">BIT_OR</see> Firebolt method.
    /// </summary>
    /// <param name="collection">Collection on which should be performed.</param>
    /// <param name="dataExpression">expression to extract value.</param>
    /// <typeparam name="TEntity">collection item type.</typeparam>
    /// <returns>Bitwise <c>OR</c> on collection.</returns>
    [Sql.Expression(DataProvider.V2Id, "BIT_OR({1})", PreferServerSide = true, IsAggregate = true)]
    public static int BitOr<TEntity>(
        this IEnumerable<TEntity> collection,
        Expression<Func<TEntity, int>> dataExpression
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/bit-or.html">BIT_OR</see> Firebolt method.
    /// </summary>
    /// <param name="dummy"><see cref="Sql.ISqlExtension"/> dummy anchor.</param>
    /// <param name="expr">value to be aggregated.</param>
    /// <returns>Bitwise <c>OR</c> on collection.</returns>
    [Sql.Extension(DataProvider.V2Id, "BIT_OR({expr})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<int> BitOr(
        this Sql.ISqlExtension? dummy,
        [ExprParameter] int expr
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/bit-or.html">BIT_OR</see> Firebolt method.
    /// </summary>
    /// <param name="collection">Collection on which should be performed.</param>
    /// <param name="dataExpression">expression to extract value.</param>
    /// <typeparam name="TEntity">collection item type.</typeparam>
    /// <returns>Bitwise <c>AND</c> on collection.</returns>
    [Sql.Expression(DataProvider.V2Id, "BIT_OR({1})", PreferServerSide = true, IsAggregate = true)]
    public static long BitOr<TEntity>(
        this IEnumerable<TEntity> collection,
        Expression<Func<TEntity, long>> dataExpression
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/bit-or.html">BIT_OR</see> Firebolt method.
    /// </summary>
    /// <param name="dummy"><see cref="Sql.ISqlExtension"/> dummy anchor.</param>
    /// <param name="expr">value to be aggregated.</param>
    /// <returns>Bitwise <c>OR</c> on collection.</returns>
    [Sql.Extension(DataProvider.V2Id, "BIT_OR({expr})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<long> BitOr(
        this Sql.ISqlExtension? dummy,
        [ExprParameter] long expr
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/bit-xor.html">BIT_XOR</see> Firebolt method.
    /// </summary>
    /// <param name="collection">Collection on which should be performed.</param>
    /// <param name="dataExpression">expression to extract value.</param>
    /// <typeparam name="TEntity">collection item type.</typeparam>
    /// <returns>Bitwise <c>XOR</c> on collection.</returns>
    [Sql.Expression(DataProvider.V2Id, "BIT_XOR({1})", PreferServerSide = true, IsAggregate = true)]
    public static int BitXor<TEntity>(
        this IEnumerable<TEntity> collection,
        Expression<Func<TEntity, int>> dataExpression
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/bit-xor.html">BIT_XOR</see> Firebolt method.
    /// </summary>
    /// <param name="dummy"><see cref="Sql.ISqlExtension"/> dummy anchor.</param>
    /// <param name="expr">value to be aggregated.</param>
    /// <returns>Bitwise <c>XOR</c> on collection.</returns>
    [Sql.Extension(DataProvider.V2Id, "BIT_XOR({expr})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<int> BitXor(
        this Sql.ISqlExtension? dummy,
        [ExprParameter] int expr
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/bit-xor.html">BIT_XOR</see> Firebolt method.
    /// </summary>
    /// <param name="collection">Collection on which should be performed.</param>
    /// <param name="dataExpression">expression to extract value.</param>
    /// <typeparam name="TEntity">collection item type.</typeparam>
    /// <returns>Bitwise <c>XOR</c> on collection.</returns>
    [Sql.Expression(DataProvider.V2Id, "BIT_XOR({1})", PreferServerSide = true, IsAggregate = true)]
    public static long BitXor<TEntity>(
        this IEnumerable<TEntity> collection,
        Expression<Func<TEntity, long>> dataExpression
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/bit-xor.html">BIT_XOR</see> Firebolt method.
    /// </summary>
    /// <param name="dummy"><see cref="Sql.ISqlExtension"/> dummy anchor.</param>
    /// <param name="expr">value to be aggregated.</param>
    /// <returns>Bitwise <c>XOR</c> on collection.</returns>
    [Sql.Extension(DataProvider.V2Id, "BIT_XOR({expr})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<long> BitXor(
        this Sql.ISqlExtension? dummy,
        [ExprParameter] long expr
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/bool-and.html">BOOL_AND</see> Firebolt method.
    /// </summary>
    /// <param name="collection">Collection on which should be performed.</param>
    /// <param name="dataExpression">expression to extract value.</param>
    /// <typeparam name="TEntity">collection item type.</typeparam>
    /// <returns>Bool <c>AND</c> on collection.</returns>
    [Sql.Expression(DataProvider.V2Id, "BOOL_AND({1})", PreferServerSide = true, IsAggregate = true)]
    public static bool BoolAnd<TEntity>(
        this IEnumerable<TEntity> collection,
        Expression<Func<TEntity, bool>> dataExpression
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/bool-and.html">BOOL_AND</see> Firebolt method.
    /// </summary>
    /// <param name="dummy"><see cref="Sql.ISqlExtension"/> dummy anchor.</param>
    /// <param name="expr">value to be aggregated.</param>
    /// <returns>Bool <c>AND</c> on collection.</returns>
    [Sql.Extension(DataProvider.V2Id, "BOOL_AND({expr})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<bool> BoolAnd(
        this Sql.ISqlExtension? dummy,
        [ExprParameter] bool expr
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/bool-or.html">BOOL_OR</see> Firebolt method.
    /// </summary>
    /// <param name="collection">Collection on which should be performed.</param>
    /// <param name="dataExpression">expression to extract value.</param>
    /// <typeparam name="TEntity">collection item type.</typeparam>
    /// <returns>Bool <c>OR</c> on collection.</returns>
    [Sql.Expression(DataProvider.V2Id, "BOOL_OR({1})", PreferServerSide = true, IsAggregate = true)]
    public static bool BoolOr<TEntity>(
        this IEnumerable<TEntity> collection,
        Expression<Func<TEntity, bool>> dataExpression
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/bool-or.html">BOOL_OR</see> Firebolt method.
    /// </summary>
    /// <param name="dummy"><see cref="Sql.ISqlExtension"/> dummy anchor.</param>
    /// <param name="expr">value to be aggregated.</param>
    /// <returns>Bool <c>OR</c> on collection.</returns>
    [Sql.Extension(DataProvider.V2Id, "BOOL_OR({expr})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<bool> BoolOr(
        this Sql.ISqlExtension? dummy,
        [ExprParameter] bool expr
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/corr.html">CORR</see> Firebolt method.
    /// </summary>
    /// <param name="dummy"><see cref="Sql.ISqlExtension"/> dummy anchor.</param>
    /// <param name="first">first value.</param>
    /// <param name="second">second value.</param>
    /// <returns>Pearson correlation.</returns>
    [Sql.Extension(DataProvider.V2Id, "CORR({first}, {second})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<double> Corr(
        this Sql.ISqlExtension? dummy,
        [ExprParameter] double first,
        [ExprParameter] double second
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/corr.html">CORR</see> Firebolt method.
    /// </summary>
    /// <param name="dummy"><see cref="Sql.ISqlExtension"/> dummy anchor.</param>
    /// <param name="first">first value.</param>
    /// <param name="second">second value.</param>
    /// <returns>Pearson correlation.</returns>
    [Sql.Extension(DataProvider.V2Id, "CORR({first}, {second})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<double> Corr(
        this Sql.ISqlExtension? dummy,
        [ExprParameter] long first,
        [ExprParameter] long second
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/hash-agg.html">HASH_AGG</see> Firebolt method.
    /// </summary>
    /// <param name="collection">Collection on which should be performed.</param>
    /// <param name="exprSelectors">expression 1.</param>
    /// <typeparam name="TEntity">collection item type.</typeparam>
    /// <returns>Aggregating hashes on selected cols.</returns>
    [Sql.Extension(DataProvider.V2Id, "HASH_AGG({exprSelectors, ', '})", PreferServerSide = true, IsAggregate = true)]
    public static long HashAgg<TEntity>(
        this IEnumerable<TEntity> collection,
        [ExprParameter] params Expression<Func<TEntity, object?>>[] exprSelectors
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/hash-agg.html">HASH_AGG</see> Firebolt method.
    /// </summary>
    /// <param name="collection">Collection on which should be performed.</param>
    /// <typeparam name="TEntity">collection item type.</typeparam>
    /// <returns>Aggregating hashes on all cols.</returns>
    [Sql.Expression(DataProvider.V2Id, "HASH_AGG(*)", PreferServerSide = true, IsAggregate = true)]
    public static long HashAgg<TEntity>(
        this IEnumerable<TEntity> collection
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/hash-agg.html">HASH_AGG</see> Firebolt method.
    /// </summary>
    /// <param name="dummy"><see cref="Sql.ISqlExtension"/> dummy anchor.</param>
    /// <returns>Aggregating hashes on all cols.</returns>
    [Sql.Extension(DataProvider.V2Id, "HASH_AGG(*)", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<long> HashAgg(
        this Sql.ISqlExtension? dummy
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/max-by.html">MAX_BY</see> Firebolt method.
    /// </summary>
    /// <param name="group">Group on which should be performed.</param>
    /// <param name="resultExpression">expression to extract target (resulting) value.</param>
    /// <param name="compareExpression">expression to extract value for comparison.</param>
    /// <typeparam name="TEntity">group item type.</typeparam>
    /// <typeparam name="TK">group key type.</typeparam>
    /// <typeparam name="TR">target value's type.</typeparam>
    /// <typeparam name="TV">compare value's type.</typeparam>
    /// <returns>Value.</returns>
    [Sql.Expression(DataProvider.V2Id, "MAX_BY({1}, {2})", PreferServerSide = true, IsAggregate = true)]
    public static TR MaxBy<TEntity, TK, TR, TV>(
        this IGrouping<TK, TEntity> group,
        Expression<Func<TEntity, TR>> resultExpression,
        Expression<Func<TEntity, TV>> compareExpression
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/max-by.html">MAX_BY</see> Firebolt method.
    /// </summary>
    /// <param name="dummy"><see cref="Sql.ISqlExtension"/> dummy anchor.</param>
    /// <param name="result">expression to extract target (resulting) value.</param>
    /// <param name="compare">expression to extract value for comparison.</param>
    /// <typeparam name="TR">target value's type.</typeparam>
    /// <typeparam name="TV">compare value's type.</typeparam>
    /// <returns><see cref="AnalyticFunctions.IAggregateFunctionSelfContained{TR}"/>.</returns>
    [Sql.Extension(DataProvider.V2Id, "MAX_BY({result}, {compare})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<TR> MaxBy<TR, TV>(
        this Sql.ISqlExtension? dummy,
        [ExprParameter] TR result,
        [ExprParameter] TV compare
    ) => throw new LinqToDBException("Not supported");
}
