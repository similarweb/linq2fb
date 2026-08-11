using DotNet.Testcontainers.Containers;

namespace Similarweb.LinqToDB.Firebolt.Tests.Seeding;

internal static class NorthwindSeeder
{
    private static readonly string[] SmokeQueries =
    [
        """SELECT COUNT(*) FROM northwind.public."Customers";""",
        "SELECT COUNT(*) FROM northwind.public.products;",
    ];

    public static async Task SeedAsync(IContainer container, string sqlFilePath, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(container);
        ArgumentException.ThrowIfNullOrWhiteSpace(sqlFilePath);

        if (!File.Exists(sqlFilePath))
        {
            throw new FileNotFoundException($"Northwind SQL script not found at '{sqlFilePath}'.", sqlFilePath);
        }

        var sqlText = await File.ReadAllTextAsync(sqlFilePath, cancellationToken).ConfigureAwait(false);
        var statements = SqlStatementSplitter.Split(sqlText);
        Console.WriteLine($"Seeding Firebolt Core: {statements.Count} statements from {sqlFilePath}");

        for (var i = 0; i < statements.Count; i++)
        {
            var statement = statements[i];
            var preview = statement.Replace('\n', ' ');
            if (preview.Length > 100)
            {
                preview = preview[..100] + "…";
            }

            Console.WriteLine($"[{i + 1}/{statements.Count}] {preview}");
            await ExecFbcliAsync(container, statement, cancellationToken).ConfigureAwait(false);
        }

        for (var i = 0; i < SmokeQueries.Length; i++)
        {
            Console.WriteLine($"[smoke {i + 1}] {SmokeQueries[i]}");
            await ExecFbcliAsync(container, SmokeQueries[i], cancellationToken).ConfigureAwait(false);
        }

        Console.WriteLine("Northwind seed completed.");
    }

    private static async Task ExecFbcliAsync(IContainer container, string sql, CancellationToken cancellationToken)
    {
        var result = await container.ExecAsync(["fbcli", "--command", sql], cancellationToken).ConfigureAwait(false);
        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"fbcli failed (exit {result.ExitCode}).{Environment.NewLine}" +
                $"SQL: {sql}{Environment.NewLine}" +
                $"stdout: {result.Stdout}{Environment.NewLine}" +
                $"stderr: {result.Stderr}");
        }
    }
}
