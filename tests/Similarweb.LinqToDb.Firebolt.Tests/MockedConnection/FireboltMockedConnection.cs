using System.Reflection;
using FireboltDotNetSdk;
using FireboltDotNetSdk.Client;

namespace Similarweb.LinqToDB.Firebolt.Tests.MockedConnection;

internal class FireboltMockedConnection(
    string connectionString,
    IClientDataProvider dataProvider,
    Action<string> queryCapturer
) : FireboltConnection(connectionString)
{
    private const string FireboltClientFieldName = "_fireboltClient";

    private readonly FieldInfo _fireboltClientAccessor =
        typeof(FireboltConnection).GetField(FireboltClientFieldName, BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new MissingMemberException(FireboltClientFieldName);

    public override async Task<bool> OpenAsync(CancellationToken cancellationToken)
    {
        if (_fireboltClientAccessor.GetValue(this) == null)
        {
            _fireboltClientAccessor.SetValue(this, CreateClient());
        }

        return await base.OpenAsync(cancellationToken);
    }

    private TestClient CreateClient() =>
        new(this, Principal, Secret, Endpoint, Env, Account, HttpClientSingleton.GetInstance(), dataProvider, queryCapturer);
}
