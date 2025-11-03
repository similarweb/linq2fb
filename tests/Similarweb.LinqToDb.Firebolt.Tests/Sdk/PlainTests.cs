using System.Globalization;
using FireboltDotNetSdk.Client;
using FireboltNETSDK.Exception;
using Similarweb.LinqToDB.Firebolt.Tests.Common;
using Similarweb.LinqToDB.Firebolt.Tests.CoreConnection;
using Similarweb.LinqToDB.Firebolt.Tests.MockedConnection;
using Xunit;

namespace Similarweb.LinqToDB.Firebolt.Tests.Sdk;

public class PlainTests(
    ConnectionStringsProvider stringsProvider
)
{
    [Fact]
    public async Task Test_EscapeSingleQuote()
    {
        string? captured = null;
        await using var conn = new FireboltMockedConnection(
            stringsProvider.Get("mock"),
            new TestDataProvider(),
            query => captured = query.Trim()
        );
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        var cmdText =
            """
            SELECT u.Name
            FROM unnest(['abc', 'def', 'ghq']) u(Name)
            WHERE u.Name = @name
            """;
        await using var cmd = new FireboltCommand(conn, cmdText);
        cmd.Parameters.Add(new FireboltParameter("@name", "'"));
        await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        await reader.ReadAsync(TestContext.Current.CancellationToken);

        const string expected =
            """
            SELECT u.Name
            FROM unnest(['abc', 'def', 'ghq']) u(Name)
            WHERE u.Name = ''''
            """;

        Assert.Equal(expected, captured);
    }

    [Fact]
    public async Task Test_Read_Decimal_Core()
    {
        var oldCulture = CultureInfo.CurrentCulture;
        var oldThreadCulture = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("CS-cz");
            CultureInfo.DefaultThreadCurrentCulture = new CultureInfo("CS-cz");
            await using var conn = new FireboltCoreConnection(stringsProvider.Get("core"));
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            await using var cmd = new FireboltCommand(conn, "SELECT 18.0::DOUBLE dbl, 18.0::DECIMAL(18,2) dec");

            await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
            var dbl = reader.GetDouble(0);
            var dec = reader.GetDecimal(1);
            Assert.Equal(18.0, dbl);
            Assert.Equal(18.0m, dec);
        }
        finally
        {
            CultureInfo.CurrentCulture = oldCulture;
            CultureInfo.DefaultThreadCurrentCulture = oldThreadCulture;
        }
    }

    [Fact(Skip = "For local runs only: requires secret to cloud Firebolt")]
    public async Task Test_Read_Decimal_Cloud()
    {
        var oldCulture = CultureInfo.CurrentCulture;
        var oldThreadCulture = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("CS-cz");
            CultureInfo.DefaultThreadCurrentCulture = new CultureInfo("CS-cz");
            await using var conn = new FireboltConnection(stringsProvider.Get("web-production"));
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            await using var cmd = new FireboltCommand(conn, "SELECT 18.0::DOUBLE dbl, 18.0::DECIMAL(18,2) dec");
            await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
            var dbl = reader.GetDouble(0);
            var dec = reader.GetDecimal(1);
            Assert.Equal(18.0, dbl);
            Assert.Equal(18.0m, dec);
        }
        finally
        {
            CultureInfo.CurrentCulture = oldCulture;
            CultureInfo.DefaultThreadCurrentCulture = oldThreadCulture;
        }
    }

    [Fact]
    public async Task Test_Read_Guid_Core()
    {
        var guid = Guid.NewGuid();

        await using var conn = new FireboltCoreConnection(stringsProvider.Get("core"));
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = new FireboltCommand(conn, "SELECT @param g");
        cmd.Parameters.Add(new FireboltParameter("@param", guid));
        var ex = await Assert.ThrowsAsync<AggregateException>(async () =>
        {
            await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
            var got = reader.GetGuid(0);
            Assert.Equal(guid, got);
        });
        _ = Assert.IsType<FireboltStructuredException>(ex.InnerException);
    }

    [Fact(Skip = "For local runs only: requires secret to cloud Firebolt")]
    public async Task Test_Read_Guid_Cloud()
    {
        var guid = Guid.NewGuid();

        await using var conn = new FireboltConnection(stringsProvider.Get("web-production"));
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = new FireboltCommand(conn, "SELECT @param g");
        cmd.Parameters.Add(new FireboltParameter("@param", guid));
        var ex = await Assert.ThrowsAsync<AggregateException>(async () =>
        {
            await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
            var got = reader.GetGuid(0);
            Assert.Equal(guid, got);
        });
        _ = Assert.IsType<FireboltStructuredException>(ex.InnerException);
    }

    [Fact]
    public async Task Test_Read_Date()
    {
        CultureInfo.DefaultThreadCurrentCulture = new CultureInfo("EN-us");
        await using var conn = new FireboltCoreConnection(stringsProvider.Get("core"));
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = new FireboltCommand(conn, "SELECT '1979-09-22'::DATE dt");
        await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
        var date = reader.GetDateTime(0);
        Assert.Equal(new DateTime(1979, 9, 22), date);
    }

    [Fact]
    public async Task Test_EscapeRegex_Server()
    {
        await using var conn = new FireboltCoreConnection(stringsProvider.Get("core_prepared"));
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        var cmdText =
            """
            SELECT
            	product.id,
            	product.product_name
            FROM
            	products product
            WHERE
            	REGEXP_LIKE_ANY(product.product_name, $1)
            """;
        await using var cmd = new FireboltCommand(conn, cmdText);
        cmd.Parameters.Add(new FireboltParameter("$1", new[] { "^Pa@", "ta$", "al.*?o" }.ToArray()));
        await cmd.PrepareAsync(TestContext.Current.CancellationToken);

        await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
        var id = reader.GetInt32(0);
        var name = reader.GetString(1);
        Assert.Equal(77, id);
        Assert.Equal("Original Frankfurter grüne Soße", name);
    }

    [Fact]
    public async Task Test_EscapeRegex_Client()
    {
        await using var conn = new FireboltCoreConnection(stringsProvider.Get("core"));
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        var cmdText =
            """
            SELECT
            	product.id,
            	product.product_name
            FROM
            	products product
            WHERE
            	REGEXP_LIKE_ANY(product.product_name, @patterns)
            ORDER BY
                product.id;
            """;
        await using var cmd = new FireboltCommand(conn, cmdText);
        cmd.Parameters.Add(new FireboltParameter("@patterns", new[] { "^Pa@", "ta$", "al.*?o" }.ToArray()));

        await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
        var id = reader.GetInt32(0);
        var name = reader.GetString(1);
        Assert.Equal(50, id);
        Assert.Equal("Valkoinen suklaa", name);
    }

    [Fact]
    public async Task Test_Avg_Rounding()
    {
        await using var conn = new FireboltCoreConnection(stringsProvider.Get("core"));
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        var cmdText =
            """
            select avg(x), avg(y)
            from (
                select n.x, n.y
                from unnest([69.75::decimal(12,2), 70, 340], [69.75, 70, 340]) as n(x, y)
            );
            """;
        await using var cmd = new FireboltCommand(conn, cmdText);

        await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
        var first = reader.GetDecimal(0);
        var second = reader.GetDecimal(1);
        Assert.NotEqual(first, second);
    }

    [Fact]
    public async Task Test_Dollar_Client()
    {
        await using var conn = new FireboltCoreConnection(stringsProvider.Get("core"));
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        var cmdText =
            """
            SELECT
            	product.id,
            	product.product_name
            FROM
            	products product
            WHERE
            	product_name = @name
            """;
        await using var cmd = new FireboltCommand(conn, cmdText);
        cmd.Parameters.Add(new FireboltParameter("@name", "some$value"));
        await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            var id = reader.GetInt32(0);
            var name = reader.GetString(1);
        }
    }

    internal class TestDataProvider : IClientDataProvider
    {
        public string Meta() => """
                                {"name":"Name","type":"text"}
                                """;

        public string Data() => """
                                ["abc"]
                                """;

        public int Rows() => 1;
    }
}
