using System.Collections.Frozen;
using Microsoft.Extensions.Options;

namespace Similarweb.LinqToDB.Firebolt.Tests.Common;

public class ConnectionStringsProvider
{
    private readonly IReadOnlyDictionary<string, string> _connectionStrings;

    public string Get(string name) => _connectionStrings.GetValueOrDefault(name, string.Empty);

    public ConnectionStringsProvider(
        IOptionsMonitor<Dictionary<string, Dictionary<string, string>>> accountsMonitor
    )
    {
        ArgumentNullException.ThrowIfNull(accountsMonitor);
        _connectionStrings = accountsMonitor.CurrentValue
            .Select(account => new KeyValuePair<string, string>(account.Key, Convert(account.Key, account.Value)))
            .ToFrozenDictionary();
    }

    private static string Convert(string name, IReadOnlyDictionary<string, string> values) =>
        string.Join(";", values.Concat([new("account", name)]).Select(v => v.Key + "=" + v.Value));
}
