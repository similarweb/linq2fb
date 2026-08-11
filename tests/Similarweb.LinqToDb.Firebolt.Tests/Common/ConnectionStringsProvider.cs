using System.Collections.Frozen;
using Microsoft.Extensions.Options;
using Similarweb.LinqToDB.Firebolt.Tests.Fixtures;

namespace Similarweb.LinqToDB.Firebolt.Tests.Common;

public class ConnectionStringsProvider
{
    private static readonly HashSet<string> CoreAccounts = new(StringComparer.OrdinalIgnoreCase)
    {
        "core",
        "core_prepared",
    };

    private readonly IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> _accounts;
    private readonly FireboltCoreHost _fireboltCoreHost;

    public ConnectionStringsProvider(
        IOptionsMonitor<Dictionary<string, Dictionary<string, string>>> accountsMonitor,
        FireboltCoreHost fireboltCoreHost
    )
    {
        ArgumentNullException.ThrowIfNull(accountsMonitor);
        ArgumentNullException.ThrowIfNull(fireboltCoreHost);

        _accounts = accountsMonitor.CurrentValue
            .ToFrozenDictionary(
                static account => account.Key,
                static account => (IReadOnlyDictionary<string, string>)account.Value,
                StringComparer.OrdinalIgnoreCase);
        _fireboltCoreHost = fireboltCoreHost;
    }

    public string Get(string name)
    {
        if (!_accounts.TryGetValue(name, out var values))
        {
            return string.Empty;
        }

        if (CoreAccounts.Contains(name))
        {
            var withUrl = new Dictionary<string, string>(values, StringComparer.OrdinalIgnoreCase)
            {
                ["url"] = _fireboltCoreHost.BaseUrl,
            };
            return Convert(name, withUrl);
        }

        return Convert(name, values);
    }

    private static string Convert(string name, IReadOnlyDictionary<string, string> values) =>
        string.Join(";", values.Concat([new("account", name)]).Select(v => v.Key + "=" + v.Value));
}
