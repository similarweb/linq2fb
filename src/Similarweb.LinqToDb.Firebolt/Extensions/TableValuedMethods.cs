using System.Linq.Expressions;
using LinqToDB;
using LinqToDB.Mapping;

namespace Similarweb.LinqToDB.Firebolt.Extensions;

/// <summary>
/// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/table-valued/">table-valued functions</see> in Firebolt.
/// </summary>
public static class TableValuedMethods
{
    #region With wrapper
    private static Func<IDataContext, int, int, IQueryable<int>>? _generateSeriesIntFunc;
    private static Func<IDataContext, int, int, int, IQueryable<int>>? _generateSeriesIntStepFunc;

    /// <summary>
    /// <para>Implementation of <see href="https://docs.firebolt.io/reference-sql/functions-reference/table-valued/generate-series">GENERATE_SERIES</see> Firebolt method.</para>
    /// </summary>
    /// <param name="dc">Data context</param>
    /// <param name="start">First value in interval</param>
    /// <param name="stop">Last value in interval</param>
    /// <returns>Table</returns>
    [ExpressionMethod(nameof(GenerateSeriesIntImpl))]
    public static IQueryable<int> GenerateSeriesWrap(
        this IDataContext dc,
        [ExprParameter] int start,
        [ExprParameter] int stop
    )
    {
        return (_generateSeriesIntFunc ??= GenerateSeriesIntImpl().Compile())(dc, start, stop);
    }

    private static Expression<Func<IDataContext, int, int, IQueryable<int>>> GenerateSeriesIntImpl()
    {
        return (dc, start, stop) => dc.FromSqlScalar<Serie<int>>($"SELECT serie.\"Col\" FROM GENERATE_SERIES({start}, {stop}) serie(\"Col\")").Select(item => item.Col);
    }

    /// <summary>
    /// <para>Implementation of <see href="https://docs.firebolt.io/reference-sql/functions-reference/table-valued/generate-series">GENERATE_SERIES</see> Firebolt method.</para>
    /// </summary>
    /// <param name="dc">Data context</param>
    /// <param name="start">First value in interval</param>
    /// <param name="stop">Last value in interval</param>
    /// <param name="step">Step</param>
    /// <returns>Table</returns>
    [ExpressionMethod(nameof(GenerateSeriesIntStepImpl))]
    public static IQueryable<int> GenerateSeriesWrap(
        this IDataContext dc,
        [ExprParameter] int start,
        [ExprParameter] int stop,
        [ExprParameter] int step
    )
    {
        return (_generateSeriesIntStepFunc ??= GenerateSeriesIntStepImpl().Compile())(dc, start, stop, step);
    }

    private static Expression<Func<IDataContext, int, int, int, IQueryable<int>>> GenerateSeriesIntStepImpl()
    {
        return (dc, start, stop, step) => dc.FromSqlScalar<Serie<int>>($"SELECT serie.\"Col\" FROM GENERATE_SERIES({start}, {stop}, {step}) serie(\"Col\")").Select(item => item.Col);
    }

    private class Serie<T>
    {
        public required T Col { get; init; }
    }
    #endregion // with wrapper

    #region Scalar version
    private static Func<IDataContext, int, int, IQueryable<int>>? _generateSeriesInt2Func;
    private static Func<IDataContext, int, int, int, IQueryable<int>>? _generateSeriesIntStep2Func;

    /// <summary>
    /// <para>Implementation of <see href="https://docs.firebolt.io/reference-sql/functions-reference/table-valued/generate-series">GENERATE_SERIES</see> Firebolt method.</para>
    /// </summary>
    /// <param name="dc">Data context</param>
    /// <param name="start">First value in interval</param>
    /// <param name="stop">Last value in interval</param>
    /// <returns>Table</returns>
    [ExpressionMethod(DataProvider.V2Id, nameof(GenerateSeriesInt2Impl))]
    public static IQueryable<int> GenerateSeries(
        this IDataContext dc,
        [ExprParameter] int start,
        [ExprParameter] int stop
    )
    {
        return (_generateSeriesInt2Func ??= GenerateSeriesInt2Impl().Compile())(dc, start, stop);
    }

    private static Expression<Func<IDataContext, int, int, IQueryable<int>>> GenerateSeriesInt2Impl()
    {
        return (dc, start, stop) => dc.FromSqlScalar<int>($"SELECT value.col FROM GENERATE_SERIES({start}, {stop}) as value(col)");
    }

    /// <summary>
    /// <para>Implementation of <see href="https://docs.firebolt.io/reference-sql/functions-reference/table-valued/generate-series">GENERATE_SERIES</see> Firebolt method.</para>
    /// </summary>
    /// <param name="dc">Data context</param>
    /// <param name="start">First value in interval</param>
    /// <param name="stop">Last value in interval</param>
    /// <param name="step">Step</param>
    /// <returns>Table</returns>
    [ExpressionMethod(DataProvider.V2Id, nameof(GenerateSeriesIntStep2Impl))]
    public static IQueryable<int> GenerateSeries(
        this IDataContext dc,
        [ExprParameter] int start,
        [ExprParameter] int stop,
        [ExprParameter] int step
    )
    {
        return (_generateSeriesIntStep2Func ??= GenerateSeriesIntStep2Impl().Compile())(dc, start, stop, step);
    }

    private static Expression<Func<IDataContext, int, int, int, IQueryable<int>>> GenerateSeriesIntStep2Impl()
    {
        return (dc, start, stop, step) => dc.FromSqlScalar<int>($"SELECT value.col FROM GENERATE_SERIES({start}, {stop}, {step}) as value(col)");
    }
    #endregion // Scalar version

    /// <summary>
    /// <para>Implementation of <see href="https://docs.firebolt.io/reference-sql/functions-reference/table-valued/generate-series">GENERATE_SERIES</see> Firebolt method.</para>
    /// </summary>
    /// <param name="_">dummy</param>
    /// <param name="start">First value in interval</param>
    /// <param name="stop">Last value in interval</param>
    /// <param name="step">Increment step</param>
    /// <returns>Table</returns>
    [Sql.Expression(DataProvider.V2Id, "GENERATE_SERIES({start}, {stop}, {step}) t({name})", ServerSideOnly = true)]
    public static IQueryable<int> GenerateSeries(
        this IDataContext? _,
        [ExprParameter] int start,
        [ExprParameter] int stop,
        [ExprParameter] int name,
        [ExprParameter] int step
    ) => throw new LinqToDBException("Not implemented on client side.");
}
