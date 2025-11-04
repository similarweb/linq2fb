using LinqToDB;
using Similarweb.LinqToDB.Firebolt.Extensions;
using Similarweb.LinqToDB.Firebolt.Tests.Fixtures;
using Similarweb.LinqToDB.Firebolt.Tests.Northwind;
using Xunit;

namespace Similarweb.LinqToDB.Firebolt.Tests.Linq;

public class LinqNativeTests(
    ContextFixture<NorthwindContext> northwind
) : IClassFixture<ContextFixture<NorthwindContext>>, IDisposable, IAsyncDisposable
{
    #region LinqStyle

    [Fact(Skip = "Linq style is not implemented yet")]
    public async Task TestSelect_ArrayAgg_ThenMany()
    {
        var cte = northwind.Context.OrderItems
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                Products = group.ArrayAggregate(item => item.ProductId).ToValue(),
            })
            .AsCte();
        var result = await cte
            .SelectMany(row => row.Products
                .Select(product => new
                {
                    Id = row.OrderId,
                    Name = product,
                })
            )
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotNull(result);
    }

    [Fact(Skip = "Linq style is not implemented yet")]
    public async Task Test_ToArray()
    {
        var result = await northwind.Context.OrderItems
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                Products = group.Select(item => item.ProductId).ToArray(),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotNull(result);
    }

    #endregion

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
