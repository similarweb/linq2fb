using System.Linq.Expressions;
using LinqToDB;

namespace Similarweb.LinqToDB.Firebolt.Extensions;

/// <summary>
/// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/DataSketches/">DataSketches functions</see> in Firebolt.
/// </summary>
public static class DataSketchesMethods
{
    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/hll-count-build.html">HLL_COUNT_BUILD</see> Firebolt method.
    /// </summary>
    /// <param name="data">Collection.</param>
    /// <param name="expr">Column accessor.</param>
    /// <typeparam name="T">Entity type.</typeparam>
    /// <typeparam name="TR">Field type.</typeparam>
    /// <returns>Pearson correlation.</returns>
    [Sql.Extension(DataProvider.V2Id, "HLL_COUNT_BUILD({expr})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static byte[] HllCountBuild<T, TR>(
        this IEnumerable<T> data,
        [ExprParameter] Expression<Func<T, TR>> expr
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/hll-count-build.html">HLL_COUNT_BUILD</see> Firebolt method.
    /// </summary>
    /// <param name="dummy">dummy.</param>
    /// <param name="expr">Column accessor.</param>
    /// <typeparam name="TR">Field type.</typeparam>
    /// <returns>Pearson correlation.</returns>
    [Sql.Extension(DataProvider.V2Id, "HLL_COUNT_BUILD({expr})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<byte[]> HllCountBuild<TR>(
        this Sql.ISqlExtension? dummy,
        [ExprParameter] TR expr
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/reference-sql/functions-reference/numeric/hll-count-estimate">HLL_COUNT_ESTIMATE</see> Firebolt method.
    /// </summary>
    /// <param name="data">Collection.</param>
    /// <returns>Estimated count.</returns>
    [Sql.Extension(DataProvider.V2Id, "HLL_COUNT_ESTIMATE({data})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static long HllCountEstimate(
        [ExprParameter] this byte[] data
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/reference-sql/functions-reference/numeric/hll-count-estimate">HLL_COUNT_ESTIMATE</see> Firebolt method.
    /// </summary>
    /// <param name="dummy">dummy.</param>
    /// <param name="builtItem">Column.</param>
    /// <returns>Estimated count.</returns>
    [Sql.Extension(DataProvider.V2Id, "HLL_COUNT_ESTIMATE({builtItem})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<long> HllCountEstimate(
        this Sql.ISqlExtension? dummy,
        [ExprParameter] byte[] builtItem
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/hll-count-merge.html">HLL_COUNT_MERGE</see> Firebolt method.
    /// </summary>
    /// <param name="data">Collection.</param>
    /// <param name="colSelector">Column.</param>
    /// <typeparam name="T">Entity type.</typeparam>
    /// <returns>Merged builds.</returns>
    [Sql.Extension(DataProvider.V2Id, "HLL_COUNT_MERGE({colSelector})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static byte[] HllCountMerge<T>(
        this IEnumerable<T> data,
        [ExprParameter] Expression<Func<T, byte[]>> colSelector
    ) => throw new LinqToDBException("Not supported");

    /// <summary>
    /// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/hll-count-merge.html">HLL_COUNT_MERGE</see> Firebolt method.
    /// </summary>
    /// <param name="dummy">dummy.</param>
    /// <param name="column">Column.</param>
    /// <returns>Merged builds.</returns>
    [Sql.Extension(DataProvider.V2Id, "HLL_COUNT_MERGE({column})", TokenName = AnalyticFunctions.FunctionToken, ChainPrecedence = 1, IsWindowFunction = true, IsAggregate = true)]
    public static AnalyticFunctions.IAggregateFunctionSelfContained<byte[]> HllCountMerge(
        this Sql.ISqlExtension? dummy,
        [ExprParameter] byte[] column
    ) => throw new LinqToDBException("Not supported");
}


