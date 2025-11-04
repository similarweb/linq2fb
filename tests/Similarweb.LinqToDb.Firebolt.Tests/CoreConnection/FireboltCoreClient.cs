using System.Globalization;
using System.Reflection;
using System.Text;
using FireboltDotNetSdk;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Exception;

namespace Similarweb.LinqToDB.Firebolt.Tests.CoreConnection;

internal class FireboltCoreClient(
    FireboltConnection connection,
    string id,
    string secret,
    string endpoint,
    string? env,
    string account,
    HttpClient httpClient
) : FireboltClient2(connection, id, secret, endpoint, env, account, httpClient)
{
    private const string TextContentType = "text/plain";
    private static readonly PropertyInfo EngineNameProperty = typeof(FireboltConnection).GetProperty("EngineName", BindingFlags.Instance | BindingFlags.NonPublic)
                                                              ?? throw new InvalidOperationException();
    private static readonly FieldInfo QueryParametersField = typeof(FireboltClient).GetField("_queryParameters", BindingFlags.Instance | BindingFlags.NonPublic)
                                                             ?? throw new InvalidOperationException();
    private static readonly PropertyInfo InfraVersionInfo = typeof(FireboltConnection)
                                                                .GetProperty("InfraVersion", BindingFlags.Instance | BindingFlags.NonPublic)
                                                            ?? throw new InvalidOperationException();
    private static readonly PropertyInfo EngineUrlProperty = typeof(FireboltConnection)
                                                                 .GetProperty("EngineUrl", BindingFlags.Instance | BindingFlags.Public)
                                                            ?? throw new InvalidOperationException();
    private static readonly FieldInfo ConnectionStringInfo = typeof(FireboltConnection)
                                                                 .GetField("_connectionString", BindingFlags.Instance | BindingFlags.NonPublic)
                                                             ?? throw new InvalidOperationException();


    public override async Task<T> ExecuteQueryAsync<T>(
        string? engineEndpoint,
        string? databaseName,
        string? accountId,
        string query,
        HashSet<string> setParamList,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(engineEndpoint) || string.IsNullOrEmpty(query))
            throw new FireboltException($"Some parameters are null or empty: engineEndpoint: {engineEndpoint} or query: {query}");
        var isStreamingRequest = typeof (T) == typeof (StreamReader);
        var url = GetUrl(engineEndpoint, databaseName, accountId, setParamList, isStreamingRequest);
        var obj = await SendAsync<T>(HttpMethod.Post, url, query, TextContentType, true, true, cancellationToken);
        return obj;
    }

    private string GetUrl(
        string engineEndpoint,
        string? databaseName,
        string? accountId,
        HashSet<string> setParamList,
        bool isStreamingRequest)
    {
        var str = setParamList.Aggregate(new StringBuilder(), (current, item) => current.Append('&').Append(item), stringBuilder => stringBuilder.ToString());
        var uriBuilder = new UriBuilder(engineEndpoint);
        var queryString = GetQueryString(databaseName, accountId, isStreamingRequest);
        if (str.Length > 0)
            queryString.Append('&').Append(str);
        uriBuilder.Query = queryString.ToString();
        return uriBuilder.Uri.ToString();
    }

    private StringBuilder GetQueryString(string? databaseName, string? accountId, bool isStreamingRequest)
    {
        var outputFormat = isStreamingRequest ? "JSONLines_Compact" : "JSON_Compact";
        var parameters = new Dictionary<string, string> { { "output_format", outputFormat } };
        if (databaseName != null)
        {
            parameters["database"] = databaseName;
        }
        if (accountId != null)
        {
            parameters["account_id"] = accountId;
        }
        var engineName = EngineNameProperty.GetValue(_connection)?.ToString();
        if (engineName != null)
        {
            parameters["engine"] = engineName;
        }
        parameters["query_label"] = Guid.NewGuid().ToString();
        var queryParameters = QueryParametersField.GetValue(this) as IDictionary<string, string> ?? new Dictionary<string, string>();
        foreach (var item in queryParameters.Where(pair => pair.Key != "Url"))
        {
            parameters[item.Key] = item.Value;
        }
        var queryStr = string.Join("&", parameters.Select(parameter => $"{parameter.Key}={parameter.Value}"));
        return new StringBuilder(queryStr);
    }

    protected override Task<FireResponse.LoginResponse> Login(string id, string secret, string env)
    {
        return Task.FromResult(
            new FireResponse.LoginResponse(
                "fake_token",
                TimeSpan.FromDays(1).TotalSeconds.ToString(CultureInfo.InvariantCulture),
                "good"
            )
        );
    }

    public override async Task<FireResponse.ConnectionResponse> ConnectAsync(string? engineName, string? database, CancellationToken cancellationToken)
    {
        await EstablishConnection();

        InfraVersionInfo.SetValue(_connection, 2);
        var connectionString = ConnectionStringInfo.GetValue(_connection) as string;
        var parameters = (connectionString ?? string.Empty).Split(";")
            .Select(s => s.Split('=', 2))
            .ToDictionary(pair => pair[0], pair => pair[1], StringComparer.OrdinalIgnoreCase);
        if (!parameters.TryGetValue("url", out var url))
            throw new InvalidOperationException();
        EngineUrlProperty.SetValue(_connection, url);

        if (!string.IsNullOrEmpty(database))
        {
            var command = new FireboltCommand(_connection, $"USE DATABASE \"{database}\"");
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        return new FireResponse.ConnectionResponse(url, database: database ?? string.Empty, isSystem: false);
    }
}
