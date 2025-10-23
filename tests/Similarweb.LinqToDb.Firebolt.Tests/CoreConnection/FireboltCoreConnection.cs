using System.Reflection;
using FireboltDotNetSdk;
using FireboltDotNetSdk.Client;

namespace Similarweb.LinqToDB.Firebolt.Tests.CoreConnection;

internal class FireboltCoreConnection(
    string connectionString
) : FireboltConnection(connectionString)
{
    private const string FireboltClientFieldName = "_fireboltClient";
    private const string AllPropertiesFieldName = "AllProperties";

    private static readonly FieldInfo FireboltClientAccessor =
        typeof(FireboltConnection).GetField(FireboltClientFieldName, BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new MissingMemberException(FireboltClientFieldName);
    private static readonly FieldInfo AllPropertiesAccessor =
        typeof(FireboltConnectionStringBuilder).GetField(AllPropertiesFieldName, BindingFlags.Static | BindingFlags.NonPublic)
        ?? throw new MissingMemberException(AllPropertiesFieldName);

    static FireboltCoreConnection()
    {
        var allProperties = AllPropertiesAccessor.GetValue(null) as HashSet<string> ?? throw new MissingFieldException();
        allProperties.Add("Url");
    }

    public override async Task<bool> OpenAsync(CancellationToken cancellationToken)
    {
        if (FireboltClientAccessor.GetValue(this) == null)
        {
            FireboltClientAccessor.SetValue(this, CreateClient());
        }

        return await base.OpenAsync(cancellationToken);
    }

    private FireboltClient CreateClient() =>
        new FireboltCoreClient(this, Principal, Secret, Endpoint, Env, Account, HttpClientSingleton.GetInstance());
}
