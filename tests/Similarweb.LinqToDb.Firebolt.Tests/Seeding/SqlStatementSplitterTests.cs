using Xunit;

namespace Similarweb.LinqToDB.Firebolt.Tests.Seeding;

public class SqlStatementSplitterTests
{
    [Fact]
    public void Split_RespectsQuotesAndComments()
    {
        const string sql =
            """
            -- heading
            CREATE DATABASE northwind;
            INSERT INTO t VALUES ('a;b', "c;d"); /* block ; comment */
            SELECT 1
            """;

        var statements = SqlStatementSplitter.Split(sql);

        Assert.Equal(3, statements.Count);
        Assert.Equal("CREATE DATABASE northwind", statements[0]);
        Assert.Equal("INSERT INTO t VALUES ('a;b', \"c;d\")", statements[1]);
        Assert.Equal("SELECT 1", statements[2]);
    }
}
