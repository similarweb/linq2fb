using LinqToDB;
using Similarweb.LinqToDB.Firebolt.Extensions;
using Similarweb.LinqToDB.Firebolt.Tests.Fixtures;
using Similarweb.LinqToDB.Firebolt.Tests.Northwind;
using Xunit;

namespace Similarweb.LinqToDB.Firebolt.Tests.Linq;

/// <summary>
/// Tests for Firebolt <see href="https://docs.firebolt.io/sql_reference/functions-reference/conditional-and-miscellaneous/">conditional functions</see>.
/// </summary>
public class ConditionalTests(
    ContextFixture<NorthwindContext> northwind
) : IClassFixture<ContextFixture<NorthwindContext>>, IDisposable, IAsyncDisposable
{
    #region NullIf

    [Fact]
    public async Task Test_NullIf_GuardsDivisorAgainstZero()
    {
        var result = await northwind.Context.OrderItems
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                Count = group.Count(),
                // NULLIF turns a zero divisor into NULL, so the ratio becomes NULL instead of throwing.
                Ratio = group.Sum(item => item.Quantity) / (double?)group.Count().NullIf(0),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.All(result, item => Assert.True(item.Count == 0 ? item.Ratio == null : item.Ratio != null));
    }

    #endregion // NullIf

    #region Disposing

    public void Dispose()
    {
        northwind.LogLastQuery();
        GC.SuppressFinalize(this);
    }

    public ValueTask DisposeAsync()
    {
        northwind.LogLastQuery();
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    #endregion // Disposing
}
