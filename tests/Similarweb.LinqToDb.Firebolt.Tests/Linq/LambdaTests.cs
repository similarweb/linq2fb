using System.Text.RegularExpressions;
using LinqToDB;
using Similarweb.LinqToDB.Firebolt.Extensions;
using Similarweb.LinqToDB.Firebolt.Tests.Fixtures;
using Similarweb.LinqToDB.Firebolt.Tests.Northwind;
using Xunit;

namespace Similarweb.LinqToDB.Firebolt.Tests.Linq;

/// <summary>
/// Tests for Firebolt <see href="https://docs.firebolt.io/sql_reference/functions-reference/Lambda/">lambda functions</see>.
/// </summary>
public class LambdaTests(
    ContextFixture<NorthwindContext> northwind
) : IClassFixture<ContextFixture<NorthwindContext>>, IDisposable, IAsyncDisposable
{
    #region ArrayCount

    [Fact]
    public async Task Test_ArrayCount_WithSimpleIntLambda()
    {
        var result = await northwind.Context.OrderItems
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                CheapStuff = group.ArrayAggregate(item => item.UnitPrice).ToValue()
                    .ArrayCount(price => price < 10),
            })
            .Where(pair => pair.CheapStuff > 0)
            .OrderByDescending(pair => pair.CheapStuff)
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(348, result.Count);
        Assert.Equal(5, result.First().CheapStuff);
    }

    [Fact]
    public async Task Test_ArrayCount_WithSimpleStringLambda()
    {
        var result = await northwind.Context.Products
            .GroupBy(product => product.SupplierId)
            .Select(group => new
            {
                SupplierId = group.Key,
                WithShortNamesCount = group.ArrayAggregate(product => product.ProductName).ToValue()
                    .ArrayDistinct()
                    .ArrayCount(productName => (productName ?? "").Length < 10),
            })
            .Where(pair => pair.WithShortNamesCount > 0)
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(11, result.Count);
        Assert.Contains(23, result.Select(pair => pair.SupplierId));
    }

    #endregion // ArrayCount

    #region ArrayFilter

    [Fact]
    public async Task Test_ArrayFilter_WithSimpleStringLambda()
    {
        var result = await northwind.Context.Products
            .GroupBy(product => product.SupplierId)
            .Select(group => new
            {
                SupplierId = group.Key,
                WithShortNames = group.ArrayAggregate(product => product.ProductName).ToValue()
                    .ArrayFilter(productName => (productName ?? "").Length < 10),
            })
            .Where(pair => pair.WithShortNames.ArrayLength() > 0)
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(11, result.Count);
        Assert.Contains(23, result.Select(pair => pair.SupplierId));
    }

    [Fact]
    public async Task Test_ArrayFilter_ByOtherArr()
    {
        var result = await northwind.Context.OrderItems
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                ProductIds = group.ArrayAggregate(it => it.ProductId).ToValue(),
                Prices = group.ArrayAggregate(it => it.UnitPrice).ToValue(),
                Qnt = group.ArrayAggregate(it => it.Quantity).ToValue(),
            })
            .Select(item => new
            {
                item.OrderId,
                item.ProductIds,
                item.Prices,
                item.Qnt,
                FilteredProductIds = item.ProductIds.ArrayFilter(item.Prices, (id, price) => price < 10),
                FilteredPrices = item.Prices.ArrayFilter(price => price < 10),
                FilteredQnt = item.Qnt.ArrayFilter(item.Prices, (qnt, price) => price < 10),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.All(
            result,
            item =>
            {
                Assert.Equivalent(item.FilteredProductIds, item.ProductIds.Zip(item.Prices, (id, price) => (id, price)).Where(pair => pair.price < 10).Select(pair => pair.id));
                Assert.Equivalent(item.FilteredPrices, item.Prices.Where(price => price < 10));
                Assert.Equivalent(item.FilteredQnt, item.Qnt.Zip(item.Prices, (qnt, price) => (qnt, price)).Where(pair => pair.price < 10).Select(pair => pair.qnt));
            });
    }

    [Fact]
    public async Task Test_ArrayFilter_ByOtherStrArr()
    {
        var result = await northwind.Context.OrderItems
            .LoadWith(item => item.Product)
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                ProductNames = group.ArrayAggregate(it => it.Product.ProductName).ToValue(),
                Prices = group.ArrayAggregate(it => it.UnitPrice).ToValue(),
                Qnt = group.ArrayAggregate(it => it.Quantity).ToValue(),
            })
            .Select(item => new
            {
                item.OrderId,
                item.ProductNames,
                item.Prices,
                item.Qnt,
                FilteredProductNames = item.ProductNames.ArrayFilter(name => Regex.IsMatch(name, @"(^Pav|che|c.*?r)")),
                FilteredPrices = item.Prices.ArrayFilter(item.ProductNames, (price, name) => Regex.IsMatch(name, @"(^Pav|che|c.*?r)")),
                FilteredQnt = item.Qnt.ArrayFilter(item.ProductNames, (qnt, name) => Regex.IsMatch(name, @"(^Pav|che|c.*?r)")),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.All(
            result,
            item =>
            {
                Assert.Equivalent(item.FilteredProductNames, item.ProductNames.Where(name => Regex.IsMatch(name, @"(^Pav|che|c.*?r)")));
                Assert.Equivalent(item.FilteredPrices, item.Prices.Zip(item.ProductNames, (id, name) => (id, name)).Where(pair => Regex.IsMatch(pair.name, @"(^Pav|che|c.*?r)")).Select(pair => pair.id));
                Assert.Equivalent(item.FilteredQnt, item.Qnt.Zip(item.ProductNames, (qnt, name) => (qnt, name)).Where(pair => Regex.IsMatch(pair.name, @"(^Pav|che|c.*?r)")).Select(pair => pair.qnt));
            });
        Assert.Contains(result, item => item.FilteredProductNames.Length > 0 || item.FilteredPrices.Length > 0 || item.FilteredQnt.Length > 0);
    }

    #endregion // ArrayFilter

    #region ArrayAnyMatch

    [Fact]
    public async Task Test_ArrayAnyMatch()
    {
        var result = await northwind.Context.OrderItems
            .LoadWith(item => item.Product)
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                HasPavlova = group.ArrayAggregate(item => item.Product.ProductName).ToValue()
                    .ArrayAnyMatch(name => (name ?? "unknown") == "Pavlova"),
            })
            .Where(pair => pair.HasPavlova)
            .CountAsync(token: TestContext.Current.CancellationToken);

        Assert.Equal(43, result);
    }

    [Fact]
    public async Task Test_ArrayAnyMatch_TwoArrays()
    {
        var result = await northwind.Context.OrderItems
            .LoadWith(item => item.Product)
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                Names = group.ArrayAggregate(item => item.Product.ProductName).ToValue(),
                Quantities = group.ArrayAggregate(item => item.Quantity).ToValue(),
            })
            .Select(tuple => new
            {
                tuple.OrderId,
                AreThereAnyNameWithLengthMoreThanQnt =
                    tuple.Names.ArrayAnyMatch(tuple.Quantities, (name, qnt) => (name ?? "").Length > qnt),
            })
            .Where(item => item.AreThereAnyNameWithLengthMoreThanQnt)
            .Select(item => item.OrderId)
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.Equal(546, result.Count);
    }

    #endregion // ArrayAnyMatch

    #region ArraySort

    [Fact]
    public async Task Test_ArraySort_WithLambda()
    {
        var result = await northwind.Context.OrderItems
            .GroupBy(item => item.ProductId)
            .Select(group => new
            {
                OrderId = group.Key,
                Sorted = group
                    .ArrayAggregate(item => item.Quantity).ToValue()
                    .ArraySort(x => -x),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        result.ForEach(pair => Assert.Equal(pair.Sorted.Max(), pair.Sorted.First()));
    }

    #endregion // ArraySort

    #region ArrayReverseSort

    [Fact]
    public async Task Test_ArrayReverseSort_WithLambda()
    {
        var result = await northwind.Context.OrderItems
            .GroupBy(item => item.ProductId)
            .Select(group => new
            {
                OrderId = group.Key,
                ReverseSorted = group
                    .ArrayAggregate(item => item.Quantity).ToValue()
                    .ArrayReverseSort(x => -x),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        result.ForEach(pair => Assert.Equal(pair.ReverseSorted.Min(), pair.ReverseSorted.First()));
    }

    #endregion // ArrayReverseSort

    #region ArrayTransform

    [Fact]
    public async Task Test_ArrayTransform()
    {
        var result = await northwind.Context.OrderItems
            .GroupBy(item => item.ProductId)
            .Select(group => new
            {
                OrderId = group.Key,
                Transformed = group
                    .ArrayAggregate(item => item.Quantity).ToValue()
                    .ArrayTransform(x => -x),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.True(result.All(item => item.Transformed.All(x => x < 0)));
    }

    [Fact]
    public async Task Test_ArrayTransform_FromAggregation()
    {
        var result = await northwind.Context.Customers
            .LeftJoin(northwind.Context.Orders, (customer, order) => customer.Id == order.CustomerId,
                (customer, order) => new { customer.FirstName, customer.LastName, order.TotalAmount })
            .GroupBy(customer => new { customer.FirstName, customer.LastName })
            .Select(group => new
            {
                group.Key.FirstName,
                group.Key.LastName,
                Orders = group.ArrayAggregate(item => item.TotalAmount).ToValue(),
                Filtered = group.ArrayAggregate(item => item.TotalAmount).ToValue()
                    .ArrayTransform(amount => amount > 500m ? amount : null),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.All(result, item => Assert.Equal(item.Orders.Select(x => x > 500m ? x : null), item.Filtered));
    }

    #endregion // ArrayTransform

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

