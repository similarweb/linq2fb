using System.Diagnostics.Contracts;
using System.Linq.Expressions;
using LinqToDB.Linq;

namespace Similarweb.LinqToDB.Firebolt.Extensions;

/// <summary>
/// Firebolt-specific LinqToDb extensions.
/// </summary>
public static class LinqExtensions
{
    /// <summary>
    /// Suffix for recognizing if CTE should be rendered as <see href="https://docs.firebolt.io/reference-sql/commands/queries/select#materialized-common-table-expressions">MATERIALIZED</see>.
    /// </summary>
    internal const string CteMaterializedEnding = "__mat__cte__";

    /// <summary>
    /// Specifies a temporary named result set, known as a common table expression (CTE), with <see href="https://docs.firebolt.io/reference-sql/commands/queries/select#materialized-common-table-expressions">MATERIALIZED</see> keyword (Firebolt-specific).
    /// </summary>
    /// <typeparam name="TSource">Source query record type.</typeparam>
    /// <param name="source">Source query.</param>
    /// <returns>Common table expression.</returns>
    [Pure]
    public static IQueryable<TSource> AsMaterializedCte<TSource>(this IQueryable<TSource> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        var currentSource = global::LinqToDB.LinqExtensions.ProcessSourceQueryable?.Invoke(source) ?? source;

        return currentSource.Provider.CreateQuery<TSource>(
            Expression.Call(
                null,
                MethodHelper.GetMethodInfo(global::LinqToDB.LinqExtensions.AsCte, source, CteMaterializedEnding),
                currentSource.Expression,
                Expression.Constant(CteMaterializedEnding)));
    }

    /// <summary>
    /// Specifies a temporary named result set, known as a common table expression (CTE), with <see href="https://docs.firebolt.io/reference-sql/commands/queries/select#materialized-common-table-expressions">MATERIALIZED</see> keyword (Firebolt-specific).
    /// </summary>
    /// <typeparam name="TSource">Source query record type.</typeparam>
    /// <param name="source">Source query.</param>
    /// <param name="name">Common table expression name.</param>
    /// <returns>Common table expression.</returns>
    [Pure]
    public static IQueryable<TSource> AsMaterializedCte<TSource>(
        this IQueryable<TSource> source,
        string? name)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        var currentSource = global::LinqToDB.LinqExtensions.ProcessSourceQueryable?.Invoke(source) ?? source;

        return currentSource.Provider.CreateQuery<TSource>(
            Expression.Call(
                null,
                MethodHelper.GetMethodInfo(global::LinqToDB.LinqExtensions.AsCte, source, name),
                currentSource.Expression,
                Expression.Constant((name ?? string.Empty) + CteMaterializedEnding)
            )
        );
    }
}
