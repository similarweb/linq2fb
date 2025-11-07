using LinqToDB;
using LinqToDB.Data;
using Similarweb.LinqToDB.Firebolt.Extensions;
using Similarweb.LinqToDB.Firebolt.Tests.Fixtures;
using Similarweb.LinqToDB.Firebolt.Tests.Northwind;
using Xunit;

namespace Similarweb.LinqToDB.Firebolt.Tests.Linq;

/// <summary>
/// Tests for Firebolt <see href="https://docs.firebolt.io/sql_reference/functions-reference/date-and-time/">date and time functions</see>.
/// </summary>
public class DateTimeTests(
    ContextFixture<NorthwindContext> northwind
) : IClassFixture<ContextFixture<NorthwindContext>>, IDisposable, IAsyncDisposable
{
    #region DatePart

    [Fact]
    public async Task TestDate_Part_Year()
    {
        var result = await northwind.Context.Orders
            .Where(order => order.OrderDate.Year == 2012)
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(152, result.Count);
        Assert.Equivalent(new[] { 2012 }, result.Select(order => order.OrderDate.Year).Distinct());
    }

    [Fact]
    public async Task TestDate_Part_Month()
    {
        var result = await northwind.Context.Orders
            .Select(order => order.OrderDate.Month)
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(830, result.Count);
        Assert.Equal(12, result.Distinct().Count());
    }

    [Fact]
    public async Task TestDate_Part_Year_Explicit()
    {
        var result = await northwind.Context.Orders
            .Select(order => DateTimeMethods.DatePart(Sql.DateParts.Year, order.OrderDate))
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(830, result.Count);
        Assert.Equivalent(new[] { 2012, 2013, 2014 }, result.Distinct());
    }

    #endregion // DatePart

    #region DateDiff

    [Fact]
    public async Task TestDate_Diff()
    {
        var result = await northwind.Context.Orders
            .Join(
                northwind.Context.Orders
                    .GroupBy(order => order.CustomerId)
                    .Select(group => new
                    {
                        CustomerId = group.Key,
                        FirstOrderDate = group.Min(x => x.OrderDate),
                    }),
                SqlJoinType.Inner,
                (order, firstOrder) => order.CustomerId == firstOrder.CustomerId,
                (order, firstOrder) => new { order.OrderNumber, order.OrderDate, firstOrder.FirstOrderDate, })
            .Select(join => new { join.OrderNumber, join.OrderDate, join.FirstOrderDate, Diff = DateTimeMethods.DateDiff(join.OrderDate, join.FirstOrderDate) })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(830, result.Count);
        Assert.All(
            result.Select(item => new
            {
                item.OrderNumber,
                item.OrderDate,
                item.FirstOrderDate,
                item.Diff,
                ClientDiff = (item.OrderDate - item.FirstOrderDate).Days,
            }),
            item => Assert.Equal(item.ClientDiff, item.Diff)
        );
    }

    #endregion // DateDiff

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

