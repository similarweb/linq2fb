using System.Diagnostics.Contracts;
using LinqToDB;

namespace Similarweb.LinqToDB.Firebolt.Extensions;

/// <summary>
/// Firebolt-specific LinqToDb extensions.
/// </summary>
public static class LinqExtensions
{
    /// <summary>
    /// Shortcut for <c>MATERIALIZED</c> CTEs.
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
    /// Shortcut for <c>MATERIALIZED</c> CTEs with a name.
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
}
