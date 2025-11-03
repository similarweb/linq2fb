namespace Similarweb.LinqToDB.Firebolt.Extensions;

public static partial class ArrayMethods
{
    #region Unnest containers

    public class Unnested<T1, T2>
    {
        public required T1 First { get; set; }

        public required T2 Second { get; set; }
    }

    public class Unnested<T1, T2, T3>
    {
        public required T1 First { get; init; }

        public required T2 Second { get; init; }

        public required T3 Third { get; init; }
    }

    public class Unnested<T1, T2, T3, T4>
    {
        public required T1 First { get; init; }

        public required T2 Second { get; init; }

        public required T3 Third { get; init; }

        public required T4 Fourth { get; init; }
    }

    private class Unnested<T>
    {
        public required T First { get; set; }
    }

    #endregion
}
