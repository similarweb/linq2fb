namespace Similarweb.LinqToDB.Firebolt.Tests.MockedConnection;

internal class EmptyDataProvider : IClientDataProvider
{
    public string Meta() => string.Empty;
    public string Data() => string.Empty;
    public int Rows() => 0;
}
