using System.Collections.Generic;
using LinqToDB;

namespace Similarweb.LinqToDB.Firebolt.Extensions;

/// <summary>
/// Implementation for Firebolt <see href="https://docs.firebolt.io/sql_reference/functions-reference/conditional-and-miscellaneous/">conditional functions</see>.
/// </summary>
public static class ConditionalMethods
{
    /// <summary>
    /// <para>Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/conditional-and-miscellaneous/nullif.html">NULLIF</see> Firebolt method.</para>
    /// <para>Returns <c>NULL</c> when <paramref name="value"/> equals <paramref name="compare"/>, otherwise returns <paramref name="value"/>.</para>
    /// <para>Typical usage is guarding a divisor against zero: <c>x / value.NullIf(0)</c>.</para>
    /// </summary>
    /// <typeparam name="T">Value type.</typeparam>
    /// <param name="value">Value to return when it differs from <paramref name="compare"/>.</param>
    /// <param name="compare">Value to compare against.</param>
    /// <returns><c>null</c> when the values are equal, otherwise <paramref name="value"/>.</returns>
    [Sql.Expression(DataProvider.V2Id, "NULLIF({0}, {1})", PreferServerSide = true)]
    public static T? NullIf<T>(
        this T value,
        T compare
    )
        where T : struct
        => EqualityComparer<T>.Default.Equals(value, compare) ? null : value;
}
