namespace Similarweb.LinqToDB.Firebolt.Tests.MockedConnection;

internal interface IClientDataProvider
{
    string Meta();
    string Data();
    int Rows();
}
