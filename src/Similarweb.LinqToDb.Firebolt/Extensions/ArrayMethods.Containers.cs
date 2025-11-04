using LinqToDB;

namespace Similarweb.LinqToDB.Firebolt.Extensions;

public static partial class ArrayMethods
{
    #region Unnest containers

    /// <summary>
    /// Unnested table. Produced with <see cref="ArrayMethods.Unnest{T1,T2}(IDataContext,T1[],T2[])"/> SQL extension method.
    /// </summary>
    /// <typeparam name="T1">First column type.</typeparam>
    /// <typeparam name="T2">Second column type.</typeparam>
    public class Unnested<T1, T2>
    {
        /// <summary>
        /// Gets first column value.
        /// </summary>
        public required T1 First { get; init; }

        /// <summary>
        /// Gets second column value.
        /// </summary>
        public required T2 Second { get; init; }
    }

    /// <summary>
    /// Unnested table. Produced with <see cref="ArrayMethods.Unnest{T1,T2,T3}(IDataContext,T1[],T2[],T3[])"/> SQL extension method.
    /// </summary>
    /// <typeparam name="T1">First column type.</typeparam>
    /// <typeparam name="T2">Second column type.</typeparam>
    /// <typeparam name="T3">Third column type.</typeparam>
    public class Unnested<T1, T2, T3>
    {
        /// <summary>
        /// Gets first column value.
        /// </summary>
        public required T1 First { get; init; }

        /// <summary>
        /// Gets second column value.
        /// </summary>
        public required T2 Second { get; init; }

        /// <summary>
        /// Gets third column value.
        /// </summary>
        public required T3 Third { get; init; }
    }

    /// <summary>
    /// Unnested table. Produced with <see cref="ArrayMethods.Unnest{T1,T2,T3,T4}(IDataContext,T1[],T2[],T3[],T4[])"/> SQL extension method.
    /// </summary>
    /// <typeparam name="T1">First column type.</typeparam>
    /// <typeparam name="T2">Second column type.</typeparam>
    /// <typeparam name="T3">Third column type.</typeparam>
    /// <typeparam name="T4">Fourth column type.</typeparam>
    public class Unnested<T1, T2, T3, T4>
    {
        /// <summary>
        /// Gets first column value.
        /// </summary>
        public required T1 First { get; init; }

        /// <summary>
        /// Gets second column value.
        /// </summary>
        public required T2 Second { get; init; }

        /// <summary>
        /// Gets third column value.
        /// </summary>
        public required T3 Third { get; init; }

        /// <summary>
        /// Gets fourth column value.
        /// </summary>
        public required T4 Fourth { get; init; }
    }

    private class Unnested<T>
    {
        public required T First { get; init; }
    }

    #endregion
}
