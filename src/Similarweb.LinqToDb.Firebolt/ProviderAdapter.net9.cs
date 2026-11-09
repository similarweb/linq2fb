namespace Similarweb.LinqToDB.Firebolt;

// net9.0 and net10.0. net8.0 has its own lock object.
#if NET9_0_OR_GREATER
internal partial class ProviderAdapter
{
    private static readonly Lock AdapterInstanceLock = new();
}
#endif
