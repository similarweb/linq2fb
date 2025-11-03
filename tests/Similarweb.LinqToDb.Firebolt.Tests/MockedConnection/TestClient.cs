using FireboltDotNetSdk;
using FireboltDotNetSdk.Client;

namespace Similarweb.LinqToDB.Firebolt.Tests.MockedConnection;

internal class TestClient(
    FireboltConnection connection,
    string id,
    string secret,
    string endpoint,
    string? env,
    string account,
    HttpClient httpClient,
    IClientDataProvider provider,
    Action<string>? capturer
) : FireboltClient2(connection, id, secret, endpoint, env, account, httpClient)
{
    public override async Task<FireResponse.ConnectionResponse> ConnectAsync(string? engineName,
        string database,
        CancellationToken cancellationToken)
    {
        return await Task.FromResult(new FireResponse.ConnectionResponse(engineName, database, false));
    }

    public override async Task<T> ExecuteQueryAsync<T>(
        string? engineEndpoint,
        string? databaseName,
        string? accountId,
        string query,
        HashSet<string> setParamList,
        CancellationToken cancellationToken)
    {
        capturer?.Invoke(query);
        var value =
            $$"""
              {
                  "meta": [
                      {{provider.Meta()}}
                  ],
                  "data": [
                      {{provider.Data()}}
                  ],
                  "rows": {{provider.Rows()}}
              }
              """;
        if (typeof(T) != typeof(string))
        {
            throw new InvalidOperationException("Non-string types are not supported");
        }

        return await Task.FromResult((T)(object)value);
    }
}
