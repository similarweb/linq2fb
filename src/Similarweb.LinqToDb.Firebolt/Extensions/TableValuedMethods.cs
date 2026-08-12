using System.Linq.Expressions;
using LinqToDB;

namespace Similarweb.LinqToDB.Firebolt.Extensions;

/// <summary>
/// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/table-valued/">table-valued functions</see> in Firebolt.
/// </summary>
public static class TableValuedMethods
{
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
    ) => dc.QueryFromExpression(() => dc.GenerateSeries(start, stop));

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
    ) => dc.QueryFromExpression(() => dc.GenerateSeries(start, stop, step));

    private static Expression<Func<IDataContext, int, int, IQueryable<int>>> GenerateSeriesIntImpl() =>
        (dc, start, stop) => dc.FromSqlScalar<int>($"GENERATE_SERIES({start}, {stop})");

    private static Expression<Func<IDataContext, int, int, int, IQueryable<int>>> GenerateSeriesIntStepImpl() =>
        (dc, start, stop, step) => dc.FromSqlScalar<int>($"GENERATE_SERIES({start}, {stop}, {step})");
}
