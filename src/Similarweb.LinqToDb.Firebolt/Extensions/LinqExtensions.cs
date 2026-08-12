using System.Diagnostics.Contracts;
using LinqToDB;
#if !LINQ2DB_HAS_MATERIALIZED_CTE
using System.Linq.Expressions;
using LinqToDB.Internal.Linq;
#endif

namespace Similarweb.LinqToDB.Firebolt.Extensions;

/// <summary>
/// Firebolt-specific LinqToDb extensions.
/// </summary>
public static class LinqExtensions
{
#if LINQ2DB_HAS_MATERIALIZED_CTE
    /// <summary>
    /// Shortcut for <c>MATERIALIZED</c> CTEs (linq2db ≥ 6.3 native API).
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

        return source.AsCte(builder => builder.IsMaterialized());
    }

    /// <summary>
    /// Shortcut for <c>MATERIALIZED</c> CTEs with a name (linq2db ≥ 6.3 native API).
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

        return source.AsCte(builder => builder.IsMaterialized().HasName(name));
    }
#else
    /// <summary>
    /// Suffix for recognizing if CTE should be rendered as
    /// <see href="https://docs.firebolt.io/reference-sql/commands/queries/select#materialized-common-table-expressions">MATERIALIZED</see>
    /// when linq2db &lt; 6.3.0 (no native <c>IsMaterialized</c> API).
    /// </summary>
    internal const string CteMaterializedEnding = "__mat__cte__";

    /// <summary>
    /// Specifies a temporary named result set (CTE) with the Firebolt <c>MATERIALIZED</c> keyword.
    /// Uses a name-suffix convention understood by <see cref="SqlBuilder"/> (linq2db &lt; 6.3).
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
    /// Specifies a temporary named result set (CTE) with the Firebolt <c>MATERIALIZED</c> keyword.
    /// Appends <see cref="CteMaterializedEnding"/> so <see cref="SqlBuilder"/> can emit <c>MATERIALIZED</c>
    /// (linq2db &lt; 6.3).
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
                Expression.Constant((name ?? string.Empty) + CteMaterializedEnding)));
    }
#endif
}
