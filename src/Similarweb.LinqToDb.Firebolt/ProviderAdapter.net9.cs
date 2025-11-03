namespace Similarweb.LinqToDB.Firebolt;

#if NET9_0_OR_GREATER
internal partial class ProviderAdapter
{
    private static readonly Lock AdapterInstanceLock = new();
}
#endif
