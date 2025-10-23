using System.Linq.Expressions;
using LinqToDB;
using LinqToDB.Mapping;

namespace Similarweb.LinqToDB.Firebolt.Extensions;

/// <summary>
/// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/table-valued/">table-valued functions</see> in Firebolt.
/// </summary>
public static class TableValuedMethods
{
    private static Func<IDataContext, int, int, IQueryable<int>>? _generateSeriesIntFunc;
    private static Func<IDataContext, int, int, int, IQueryable<int>>? _generateSeriesIntStepFunc;

    /// <summary>
    /// <para>Implementation of <see href="https://docs.firebolt.io/reference-sql/functions-reference/table-valued/generate-series">GENERATE_SERIES</see> Firebolt method.</para>
    /// </summary>
    /// <param name="dc">Data context.</param>
    /// <param name="start">First value in interval.</param>
    /// <param name="stop">Last value in interval.</param>
    /// <returns>Table.</returns>
    [ExpressionMethod(nameof(GenerateSeriesIntImpl))]
    public static IQueryable<int> GenerateSeries(
        this IDataContext dc,
        [ExprParameter] int start,
        [ExprParameter] int stop
    )
    {
        return (_generateSeriesIntFunc ??= GenerateSeriesIntImpl().Compile())(dc, start, stop);
    }

    /// <summary>
    /// <para>Implementation of <see href="https://docs.firebolt.io/reference-sql/functions-reference/table-valued/generate-series">GENERATE_SERIES</see> Firebolt method.</para>
    /// </summary>
    /// <param name="dc">Data context.</param>
    /// <param name="start">First value in interval.</param>
    /// <param name="stop">Last value in interval.</param>
    /// <param name="step">Step.</param>
    /// <returns>Table.</returns>
    [ExpressionMethod(nameof(GenerateSeriesIntStepImpl))]
    public static IQueryable<int> GenerateSeries(
        this IDataContext dc,
        [ExprParameter] int start,
        [ExprParameter] int stop,
        [ExprParameter] int step
    )
    {
        return (_generateSeriesIntStepFunc ??= GenerateSeriesIntStepImpl().Compile())(dc, start, stop, step);
    }

    private static Expression<Func<IDataContext, int, int, IQueryable<int>>> GenerateSeriesIntImpl()
    {
        return (dc, start, stop) => dc.FromSqlScalar<Serie<int>>($"SELECT serie.\"Col\" FROM GENERATE_SERIES({start}, {stop}) serie(\"Col\")").Select(item => item.Col);
    }

    private static Expression<Func<IDataContext, int, int, int, IQueryable<int>>> GenerateSeriesIntStepImpl()
    {
        return (dc, start, stop, step) => dc
            .FromSqlScalar<Serie<int>>($"SELECT serie.\"Col\" FROM GENERATE_SERIES({start}, {stop}, {step}) serie(\"Col\")")
            .Select(item => item.Col);
    }

    private class Serie<T>
    {
        public required T Col { get; init; }
    }
}
