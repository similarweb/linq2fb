using LinqToDB;

namespace Similarweb.LinqToDB.Firebolt.Extensions;

/// <summary>
/// Implementation for <see href="https://docs.firebolt.io/sql_reference/functions-reference/string/">string functions</see> in Firebolt.
/// </summary>
public static class StringMethods
{
    /// <summary>
    /// Implementation for <see href="https://firebolt-vector-search-index.mintlify.app/reference-sql/functions-reference/string/regexp-like-any">REGEXP_LIKE_ANY</see> Firebolt method.
    /// </summary>
    /// <param name="value">Value to check for.</param>
    /// <param name="patterns">Array of regex to check.</param>
    /// <returns>True if the array contains the value, otherwise false.</returns>
    [Sql.Extension(DataProvider.V2Id, "REGEXP_LIKE_ANY({value}, {patterns})", CanBeNull = false, IsPredicate = true, PreferServerSide = true)]
    public static bool RegexpLikeAny(
        [ExprParameter] this string value,
        [ExprParameter] string[] patterns
    ) => throw new NotImplementedException($"{nameof(RegexpLikeAny)} is not implemented");
}
