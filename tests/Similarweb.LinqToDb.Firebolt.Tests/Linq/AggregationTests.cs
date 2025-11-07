using LinqToDB;
using Similarweb.LinqToDB.Firebolt.Extensions;
using Similarweb.LinqToDB.Firebolt.Tests.Fixtures;
using Similarweb.LinqToDB.Firebolt.Tests.Northwind;
using Xunit;
using Sql = LinqToDB.Sql;

namespace Similarweb.LinqToDB.Firebolt.Tests.Linq;


/// <summary>
/// Tests for Firebolt <see href="https://docs.firebolt.io/sql_reference/functions-reference/aggregation/">aggregation functions</see>.
/// </summary>
public class AggregationTests(
    ContextFixture<NorthwindContext> northwind
) : IClassFixture<ContextFixture<NorthwindContext>>, IDisposable, IAsyncDisposable
{
    private const double Tolerance = 0.000000001;

    #region AnyValue

    [Fact]
    public async Task Test_AnyValue()
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
                Any = group.AnyValue(item => item.TotalAmount).ToValue(),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.All(result, item => Assert.Contains(item.Any, item.Orders));
    }

    [Fact]
    public async Task Test_AnyValue_UsingExt()
    {
        var result = await northwind.Context.Customers
            .LeftJoin(northwind.Context.Orders, (customer, order) => customer.Id == order.CustomerId,
                (customer, order) => new { customer.FirstName, customer.LastName, order.TotalAmount })
            .Select(item => new
            {
                item.FirstName,
                item.LastName,
                Orders = Sql.Ext
                    .ArrayAggregate(item.TotalAmount)
                    .Over()
                    .PartitionBy(item.FirstName, item.LastName)
                    .ToValue(),
                Any = Sql.Ext
                    .AnyValue(item.TotalAmount)
                    .Over()
                    .PartitionBy(item.FirstName, item.LastName)
                    .ToValue(),
            })
            .Where(item => item.FirstName == "Horst")
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.All(result, item => Assert.Contains(item.Any, item.Orders));
    }

    #endregion // AnyValue

    #region ApproxCountDistinct

    [Fact]
    public async Task Test_ApproxCountDistinct_Extension()
    {
        var result = await northwind.Context.OrderItems
            .Select(orderItem => new
            {
                Approx = Sql.Ext.ApproxCountDistinct(orderItem.Id).ToValue(),
                Count = Sql.Ext.Count(orderItem.Id).ToValue(),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        var item = Assert.Single(result);
        Assert.NotEqual(0, item.Count);
        Assert.NotEqual(0, item.Approx);
        // note that Count and Approx could be not same on large values
    }

    #endregion // ApproxCountDistinct

    #region MaxBy

    [Fact]
    public async Task TestSelect_MaxBy()
    {
        var data = await northwind.Context.Products
            .LoadWith(product => product.Supplier)
            .GroupBy(product => product.Supplier)
            .Select(group => new
            {
                SupplierName = group.Key!.CompanyName,
                MostExpensive = group.MaxBy(product => product.ProductName, product => product.UnitPrice),
            })
            .ToDictionaryAsync(pair => pair.SupplierName, pair => pair.MostExpensive, TestContext.Current.CancellationToken);

        Assert.NotEmpty(data);
        Assert.Equal(29, data.Count);
        Assert.Equal("Guaraná Fantástica", data["Refrescos Americanas LTDA"]);
    }

    [Fact]
    public async Task TestSelect_MaxBy_Join()
    {
        var data = await northwind.Context.Products
            .GroupBy(product => product.SupplierId)
            .Select(group => new
            {
                SupplierId = group.Key,
                MostExpensive = group.MaxBy(product => product.ProductName, product => product.UnitPrice),
            })
            .InnerJoin(
                northwind.Context.Suppliers,
                (pair, supplier) => pair.SupplierId == supplier.Id,
                (pair, supplier) => new { supplier.CompanyName, pair.MostExpensive, }
            )
            .ToDictionaryAsync(pair => pair.CompanyName, pair => pair.MostExpensive, TestContext.Current.CancellationToken);

        Assert.NotEmpty(data);
        Assert.Equal(29, data.Count);
        Assert.Equal("Guaraná Fantástica", data["Refrescos Americanas LTDA"]);
    }

    #endregion // MaxBy

    #region ArrayAggregate

    [Fact]
    public Task Test_ArrayAggregate()
    {
        var allPrices = northwind.Context.Products.ArrayAggregate(product => product.UnitPrice).ToValue();

        Assert.Equal(78, allPrices.Length);
        return Task.CompletedTask;
    }

    [Fact]
    public async Task Test_ArrayAggregate_UsingExt()
    {
        var first = await northwind.Context.Products
            .Select(product => new
            {
                product.SupplierId,
                Ids = Sql.Ext
                    .ArrayAggregate(product.Id)
                    .Over()
                    .PartitionBy(product.SupplierId)
                    .ToValue(),
            })
            .FirstAsync(pair => pair.SupplierId == 1, TestContext.Current.CancellationToken);

        Assert.Equivalent(new[] { 1, 2, 3, }, first.Ids);
    }

    [Fact]
    public async Task Test_ArrayAggregate_Window()
    {
        var allPricesBySupplier = await northwind.Context.Products
            .GroupBy(product => product.SupplierId)
            .Select(group => new
            {
                Id = group.Key,
                Arr = group
                    .ArrayAggregate(product => product.UnitPrice)
                    .ToValue(),
            })
            .ToDictionaryAsync(pair => pair.Id, pair => pair.Arr, TestContext.Current.CancellationToken);

        Assert.Equal(29, allPricesBySupplier.Count);
        Assert.Equivalent(new[] { 18.0m, 19.0m, 10.0m, }, allPricesBySupplier[1]);
    }

    [Fact]
    public async Task TestCte_WithJoin_ArrayAgg()
    {
        var cte = northwind.Context.Products
            .GroupBy(product => product.SupplierId)
            .Select(group => new
            {
                SupplierId = group.Key,
                Products = group
                    .ArrayAggregate(product => product.ProductName)
                    .ToValue(),
            })
            .AsCte();
        var query =
            from supplier in northwind.Context.Suppliers
            join pair in cte on supplier.Id equals pair.SupplierId
            orderby supplier.Id
            select new { supplier.Id, supplier.CompanyName, pair.Products };
        var result = await query.ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(29, result.Count);
        Assert.Equal(5, result.First(item => item.CompanyName == "Pavlova, Ltd.").Products.Length);
    }

    #endregion // ArrayAgg

    #region Avg

    [Fact]
    public async Task Test_Avg()
    {
        var result = await northwind.Context.OrderItems
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                Prices = group.ArrayAggregate(item => item.UnitPrice * item.Quantity).ToValue(),
                Avg = group.Average(item => item.UnitPrice * item.Quantity),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.All(result, x => Assert.Equal(x.Avg, Math.Round(x.Prices.Average(), 2, MidpointRounding.AwayFromZero)));
    }

    [Fact]
    public async Task Test_Avg_Extension()
    {
        var result = await northwind.Context.OrderItems
            .Where(item => item.OrderId == 100)
            .Select(item => Sql.Ext
                .Average<decimal>(item.UnitPrice * item.Quantity)
                .ToValue()
            )
            .FirstAsync(token: TestContext.Current.CancellationToken);

        Assert.Equal(232, result);
    }

    #endregion // Avg

    #region Bit

    #region And

    [Fact]
    public async Task Test_Bit_And()
    {
        int[] arr1 = [7, 4, 6, 20];
        int[] arr2 = [1, 2, 3, 6];
        var result = await northwind.Context.Unnest(arr1, arr2)
            .Select(it => new
            {
                First = Sql.Ext.BitAnd(it.First).ToValue(),
                Second = Sql.Ext.BitAnd(it.Second).ToValue(),
            })
            .FirstAsync(token: TestContext.Current.CancellationToken);

        Assert.Equal(4, result.First);
        Assert.Equal(0, result.Second);
    }

    [Fact]
    public async Task Test_Bit_And_Extension()
    {
        int[] arr1 = [7, 4, 6, 20];
        int[] arr2 = [1, 2, 3, 6];
        int[] arr3 = [1, 1, 2, 2];
        var result = await northwind.Context.Unnest(arr1, arr2, arr3)
            .GroupBy(it => it.Third)
            .Select(group => new
            {
                Group = group.Key,
                First = group.BitAnd(it => it.First),
                Second = group.BitAnd(it => it.Second),
            })
            .OrderBy(it => it.Group)
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Count);
        Assert.Equal([4, 4], result.Select(it => it.First));
        Assert.Equal([0, 2], result.Select(it => it.Second));
    }

    #endregion // And

    #region Or

    [Fact]
    public async Task Test_Bit_Or()
    {
        int[] arr1 = [1, 2, 4, 8];
        int[] arr2 = [7, 2, 3, 6];
        var result = await northwind.Context.Unnest(arr1, arr2)
            .Select(it => new
            {
                First = Sql.Ext.BitOr(it.First).ToValue(),
                Second = Sql.Ext.BitOr(it.Second).ToValue(),
            })
            .FirstAsync(token: TestContext.Current.CancellationToken);

        Assert.Equal(15, result.First);
        Assert.Equal(7, result.Second);
    }

    [Fact]
    public async Task Test_Bit_Or_Extension()
    {
        int[] arr1 = [1, 2, 4, 8];
        int[] arr2 = [7, 2, 3, 6];
        int[] arr3 = [1, 1, 2, 2];
        var result = await northwind.Context.Unnest(arr1, arr2, arr3)
            .GroupBy(it => it.Third)
            .Select(group => new
            {
                Group = group.Key,
                First = group.BitOr(it => it.First),
                Second = group.BitOr(it => it.Second),
            })
            .OrderBy(it => it.Group)
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Count);
        Assert.Equal([3, 12], result.Select(it => it.First));
        Assert.Equal([7, 7], result.Select(it => it.Second));
    }

    #endregion // Or

    #region Xor

    [Fact]
    public async Task Test_Bit_Xor()
    {
        int[] arr1 = [1, 2, 4, 8];
        int[] arr2 = [7, 2, 3, 6];
        var result = await northwind.Context.Unnest(arr1, arr2)
            .Select(it => new
            {
                First = Sql.Ext.BitXor(it.First).ToValue(),
                Second = Sql.Ext.BitXor(it.Second).ToValue(),
            })
            .FirstAsync(token: TestContext.Current.CancellationToken);

        Assert.Equal(15, result.First);
        Assert.Equal(0, result.Second);
    }

    [Fact]
    public async Task Test_Bit_Xor_Extension()
    {
        int[] arr1 = [7, 4, 6, 20];
        int[] arr2 = [1, 2, 3, 6];
        int[] arr3 = [1, 1, 2, 2];
        var result = await northwind.Context.Unnest(arr1, arr2, arr3)
            .GroupBy(it => it.Third)
            .Select(group => new
            {
                Group = group.Key,
                First = group.BitXor(it => it.First),
                Second = group.BitXor(it => it.Second),
            })
            .OrderBy(it => it.Group)
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Count);
        Assert.Equal([3, 18], result.Select(it => it.First));
        Assert.Equal([3, 5], result.Select(it => it.Second));
    }

    #endregion // Xor

    #endregion // Bit

    #region Bool

    #region And

    [Fact]
    public async Task Test_Bool_And()
    {
        bool[] arr1 = [true, true];
        bool[] arr2 = [true, false];
        bool[] arr3 = [false, true];
        bool[] arr4 = [false, false];
        var result = await northwind.Context.Unnest(arr1, arr2, arr3, arr4)
            .Select(it => new
            {
                First = Sql.Ext.BoolAnd(it.First).ToValue(),
                Second = Sql.Ext.BoolAnd(it.Second).ToValue(),
                Third = Sql.Ext.BoolAnd(it.Third).ToValue(),
                Forth = Sql.Ext.BoolAnd(it.Fourth).ToValue(),
            })
            .FirstAsync(token: TestContext.Current.CancellationToken);

        Assert.True(result.First);
        Assert.False(result.Second);
        Assert.False(result.Third);
        Assert.False(result.Forth);
    }

    [Fact]
    public async Task Test_Bool_And_Extension()
    {
        bool[] arr1 = [true, false, false, false];
        bool[] arr2 = [false, true, true, true];
        int[] arr3 = [1, 1, 2, 2];
        var result = await northwind.Context.Unnest(arr1, arr2, arr3)
            .GroupBy(it => it.Third)
            .Select(group => new
            {
                Group = group.Key,
                First = group.BoolAnd(it => it.First),
                Second = group.BoolAnd(it => it.Second),
            })
            .OrderBy(it => it.Group)
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Count);
        Assert.Equal([false, false], result.Select(it => it.First));
        Assert.Equal([false, true], result.Select(it => it.Second));
    }

    #endregion // And

    #region Or

    [Fact]
    public async Task Test_Bool_Or()
    {
        bool[] arr1 = [true, true];
        bool[] arr2 = [true, false];
        bool[] arr3 = [false, true];
        bool[] arr4 = [false, false];
        var result = await northwind.Context.Unnest(arr1, arr2, arr3, arr4)
            .Select(it => new
            {
                First = Sql.Ext.BoolOr(it.First).ToValue(),
                Second = Sql.Ext.BoolOr(it.Second).ToValue(),
                Third = Sql.Ext.BoolOr(it.Third).ToValue(),
                Forth = Sql.Ext.BoolOr(it.Fourth).ToValue(),
            })
            .FirstAsync(token: TestContext.Current.CancellationToken);

        Assert.True(result.First);
        Assert.True(result.Second);
        Assert.True(result.Third);
        Assert.False(result.Forth);
    }

    [Fact]
    public async Task Test_Bool_Or_Extension()
    {
        bool[] arr1 = [true, false, false, false];
        bool[] arr2 = [false, true, true, true];
        int[] arr3 = [1, 1, 2, 2];
        var result = await northwind.Context.Unnest(arr1, arr2, arr3)
            .GroupBy(it => it.Third)
            .Select(group => new
            {
                Group = group.Key,
                First = group.BoolOr(it => it.First),
                Second = group.BoolOr(it => it.Second),
            })
            .OrderBy(it => it.Group)
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Count);
        Assert.Equal([true, false], result.Select(it => it.First));
        Assert.Equal([true, true], result.Select(it => it.Second));
    }

    #endregion // Or

    #endregion // Bool

    #region MaxBy

    [Fact]
    public async Task Test_MaxBy()
    {
        var result = await northwind.Context.OrderItems
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                MostExpensiveItem = group.MaxBy(item => item.ProductId, item => item.UnitPrice),
                Items = group.ArrayAggregate(item => item.ProductId).ToValue(),
                Prices = group.ArrayAggregate(item => item.UnitPrice).ToValue(),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.All(result,
            item => Assert.Equal(item.MostExpensiveItem,
                item.Items.Zip(item.Prices).OrderByDescending(pair => pair.Second).FirstOrDefault().First));
    }

    [Fact]
    public async Task Test_MaxBy_Extension()
    {
        var result = await northwind.Context.OrderItems
            .Select(item => new
            {
                MostExpensiveItem = Sql.Ext
                    .MaxBy(item.ProductId, item.UnitPrice)
                    .ToValue(),
                Items = Sql.Ext.ArrayAggregate(item.ProductId).ToValue(),
                Prices = Sql.Ext.ArrayAggregate(item.UnitPrice).ToValue(),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        var item = Assert.Single(result);
        Assert.Equal(item.MostExpensiveItem,
            item.Items.Zip(item.Prices).OrderByDescending(pair => pair.Second).FirstOrDefault().First);
    }

    #endregion // MaxBy

    #region Corr

    private static double Correlation(double?[] x, double[] y)
    {
        if (x.Length != y.Length) throw new ArgumentException();
        if (x.Length == 0) throw new ArgumentException();
        if (x.Length == 1) return 0;
        var meanX = x.Average();
        var meanY = y.Average();
        var deviationX = Math.Sqrt(x.Sum(it => (it - meanX) * (it - meanX)) / (x.Length - 1) ?? 0);
        var deviationY = Math.Sqrt(y.Sum(it => (it - meanY) * (it - meanY)) / (y.Length - 1));
        return x.Zip(y, (a, b) => (a - meanX) * (b - meanY) / deviationX / deviationY).Sum() / (x.Length - 1) ?? 0;
    }

    [Fact]
    public async Task Test_Corr()
    {
        var result = await northwind.Context.OrderItems
            .LoadWith(item => item.Product)
            .Select(item => new
            {
                item.Product.SupplierId,
                item.ProductId,
                item.Product.UnitPrice,
                item.Quantity,
            })
            .GroupBy(item => new { item.ProductId, item.SupplierId, item.UnitPrice, }, item => item.Quantity)
            .Select(group => new
            {
                group.Key.ProductId,
                group.Key.SupplierId,
                UnitPrice = (double?)group.Key.UnitPrice!,
                Quantity = (double)group.Sum(),
            })
            .GroupBy(item => item.SupplierId)
            .Select(group => new
            {
                SupplierId = group.Key,
                Prices = group.ArrayAggregate(it => it.UnitPrice).ToValue(),
                Quantities = group.ArrayAggregate(it => it.Quantity).ToValue(),
                Corr = group.Corr(it => it.UnitPrice, it => it.Quantity),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.All(result,
            item => Assert.Equal((double)item.Corr, Correlation(item.Prices, item.Quantities), Tolerance));
    }

    [Fact]
    public async Task Test_Corr_Extension()
    {
        var result = await northwind.Context.OrderItems
            .LoadWith(item => item.Product)
            .Select(item => new
            {
                item.ProductId,
                UnitPrice = (double?)item.Product.UnitPrice,
                Quantity = (double)item.Quantity,
            })
            .GroupBy(item => new { item.ProductId, item.UnitPrice, }, item => item.Quantity)
            .Select(group => new
            {
                group.Key.ProductId,
                group.Key.UnitPrice,
                Qnt = group.Sum(),
            })
            .AsCte()
            .Select(item => new
            {
                Prices = Sql.Ext.ArrayAggregate(item.UnitPrice).ToValue(),
                Quantities = Sql.Ext.ArrayAggregate(item.Qnt).ToValue(),
                Corr = Sql.Ext.Corr<double>(item.UnitPrice, item.Qnt).ToValue(),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        var item = Assert.Single(result);
        Assert.Equal(item.Corr, Correlation(item.Prices, item.Quantities), Tolerance);
    }

    #endregion // Corr

    #region Count

    [Fact]
    public async Task Test_Count()
    {
        var result = await northwind.Context.OrderItems
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                UnitPrices = group.ArrayAggregate(item => item.UnitPrice).ToValue(),
                Count = group.Count(item => item.UnitPrice > 50),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.All(result, item => Assert.Equal(item.Count, item.UnitPrices.Count(x => x > 50)));
    }

    [Fact]
    public async Task Test_Count_Extension()
    {
        var result = await northwind.Context.OrderItems
            .Select(item => new
            {
                UnitPrices = Sql.Ext
                    .ArrayAggregate(item.UnitPrice)
                    .ToValue(),
                Count = Sql.Ext
                    .Count(item.UnitPrice, Sql.AggregateModifier.Distinct)
                    .ToValue(),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        var item = Assert.Single(result);
        Assert.Equal(item.Count, item.UnitPrices.Distinct().Count());
    }

    #endregion // Count

    #region CovarPop

    private static double CovariancePop(double?[] x, double[] y)
    {
        if (x.Length != y.Length) throw new ArgumentException();
        if (x.Length == 0) throw new ArgumentException();
        if (x.Length == 1) return 0;
        var meanX = x.Average();
        var meanY = y.Average();
        return x.Zip(y, (a, b) => (a - meanX) * (b - meanY)).Sum() / x.Length ?? 0;
    }

    [Fact]
    public async Task Test_CovarPop()
    {
        var result = await northwind.Context.OrderItems
            .LoadWith(item => item.Product)
            .Select(item => new
            {
                item.Product.SupplierId,
                item.ProductId,
                item.Product.UnitPrice,
                item.Quantity,
            })
            .GroupBy(item => new { item.ProductId, item.SupplierId, item.UnitPrice, }, item => item.Quantity)
            .Select(group => new
            {
                group.Key.ProductId,
                group.Key.SupplierId,
                UnitPrice = (double?)group.Key.UnitPrice!,
                Quantity = (double)group.Sum(),
            })
            .GroupBy(item => item.SupplierId)
            .Select(group => new
            {
                SupplierId = group.Key,
                Prices = group.ArrayAggregate(it => it.UnitPrice).ToValue(),
                Quantities = group.ArrayAggregate(it => it.Quantity).ToValue(),
                CovarPop = (double)group.CovarPop(it => it.UnitPrice, it => it.Quantity),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.All(result, item => Assert.Equal(item.CovarPop, CovariancePop(item.Prices, item.Quantities), Tolerance));
    }

    [Fact]
    public async Task Test_CovarPop_Extension()
    {
        var result = await northwind.Context.OrderItems
            .LoadWith(item => item.Product)
            .Select(item => new
            {
                item.ProductId,
                UnitPrice = (double?)item.Product.UnitPrice,
                Quantity = (double)item.Quantity,
            })
            .GroupBy(item => new { item.ProductId, item.UnitPrice, }, item => item.Quantity)
            .Select(group => new
            {
                group.Key.ProductId,
                group.Key.UnitPrice,
                Qnt = group.Sum(),
            })
            .AsCte()
            .Select(item => new
            {
                Prices = Sql.Ext.ArrayAggregate(item.UnitPrice).ToValue(),
                Quantities = Sql.Ext.ArrayAggregate(item.Qnt).ToValue(),
                CovarPop = Sql.Ext.CovarPop(item.UnitPrice, item.Qnt).ToValue(),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        var item = Assert.Single(result);
        Assert.Equal(item.CovarPop ?? 0, CovariancePop(item.Prices, item.Quantities), Tolerance);
    }

    #endregion // CovarPop

    #region CovarSamp

    private static double CovarianceSample(double?[] x, double[] y)
    {
        if (x.Length != y.Length) throw new ArgumentException();
        if (x.Length == 0) throw new ArgumentException();
        if (x.Length == 1) return 0;
        var meanX = x.Average();
        var meanY = y.Average();
        return x.Zip(y, (a, b) => (a - meanX) * (b - meanY)).Sum() / (x.Length - 1) ?? 0;
    }

    [Fact]
    public async Task Test_CovarSamp()
    {
        var result = await northwind.Context.OrderItems
            .LoadWith(item => item.Product)
            .Select(item => new
            {
                item.Product.SupplierId,
                item.ProductId,
                item.Product.UnitPrice,
                item.Quantity,
            })
            .GroupBy(item => new { item.ProductId, item.SupplierId, item.UnitPrice, }, item => item.Quantity)
            .Select(group => new
            {
                group.Key.ProductId,
                group.Key.SupplierId,
                UnitPrice = (double?)group.Key.UnitPrice!,
                Quantity = (double)group.Sum(),
            })
            .GroupBy(item => item.SupplierId)
            .Select(group => new
            {
                SupplierId = group.Key,
                Prices = group.ArrayAggregate(it => it.UnitPrice).ToValue(),
                Quantities = group.ArrayAggregate(it => it.Quantity).ToValue(),
                CovarSamp = (double)group.CovarSamp(it => it.UnitPrice, it => it.Quantity),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.All(result,
            item => Assert.Equal(item.CovarSamp, CovarianceSample(item.Prices, item.Quantities), Tolerance));
    }

    [Fact]
    public async Task Test_CovarSamp_Extension()
    {
        var result = await northwind.Context.OrderItems
            .LoadWith(item => item.Product)
            .Select(item => new
            {
                item.ProductId,
                UnitPrice = (double?)item.Product.UnitPrice,
                Quantity = (double)item.Quantity,
            })
            .GroupBy(item => new { item.ProductId, item.UnitPrice, }, item => item.Quantity)
            .Select(group => new
            {
                group.Key.ProductId,
                group.Key.UnitPrice,
                Qnt = group.Sum(),
            })
            .AsCte()
            .Select(item => new
            {
                Prices = Sql.Ext.ArrayAggregate(item.UnitPrice).ToValue(),
                Quantities = Sql.Ext.ArrayAggregate(item.Qnt).ToValue(),
                CovarSamp = Sql.Ext.CovarSamp(item.UnitPrice, item.Qnt).ToValue(),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        var item = Assert.Single(result);
        Assert.Equal(item.CovarSamp ?? 0, CovarianceSample(item.Prices, item.Quantities), Tolerance);
    }

    #endregion // CovarSamp

    #region Grouping

    #region GroupingSets

    [Fact]
    public async Task Test_Grouping_GroupingSets()
    {
        var result = await northwind.Context.OrderItems
            .LoadWith(item => item.Product)
            .GroupBy(item => Sql.GroupBy.GroupingSets(
                () => new
                {
                    Set1 = new { item.Product.SupplierId, item.ProductId },
                    Set2 = new { item.Product.SupplierId },
                })
            )
            .Select(group => new
            {
                Grouping = Sql.Grouping(group.Key.Set1.SupplierId, group.Key.Set1.ProductId),
                group.Key.Set1.SupplierId,
                group.Key.Set1.ProductId,
                ItemsCount = group.Count(),
            })
            .OrderByDescending(item => item.Grouping)
            .ThenBy(item => item.SupplierId)
            .ThenBy(item => item.ProductId)
            .ToListAsync(token: TestContext.Current.CancellationToken);

        var sums = result
            .Where(item => item.Grouping == 0)
            .GroupBy(item => item.SupplierId, item => item.ItemsCount)
            .ToDictionary(item => item.Key, item => item.Sum());

        Assert.NotNull(result);
        Assert.All(result.Where(item => item.Grouping == 1),
            item => Assert.Equal(item.ItemsCount, sums.GetValueOrDefault(item.SupplierId)));
    }

    #endregion // GroupingSets

    #region Rollup

    [Fact]
    public async Task Test_Grouping_Rollup()
    {
        var result = await northwind.Context.OrderItems
            .LoadWith(item => item.Product)
            .GroupBy(item => Sql.GroupBy.Rollup(() => new { item.Product.SupplierId, item.ProductId, }))
            .Select(group => new
            {
                Grouping = Sql.Grouping(group.Key.SupplierId, group.Key.ProductId),
                group.Key.SupplierId,
                group.Key.ProductId,
                ItemsCount = group.Count(),
            })
            .OrderByDescending(item => item.Grouping)
            .ThenBy(item => item.SupplierId)
            .ThenBy(item => item.ProductId)
            .ToListAsync(token: TestContext.Current.CancellationToken);

        var sums = result
            .Where(item => item.Grouping == 0)
            .GroupBy(item => item.SupplierId, item => item.ItemsCount)
            .ToDictionary(item => item.Key, item => item.Sum());
        var total = result
            .Where(item => item.Grouping == 1)
            .Sum(item => item.ItemsCount);

        Assert.NotNull(result);
        Assert.All(result.Where(item => item.Grouping == 1),
            item => Assert.Equal(item.ItemsCount, sums.GetValueOrDefault(item.SupplierId)));
        var totalItem = Assert.Single(result, item => item.Grouping == 3);
        Assert.Equal(total, totalItem.ItemsCount);
    }

    #endregion // Rollup

    #region Cube

    [Fact]
    public async Task Test_Grouping_Cube()
    {
        var result = await northwind.Context.OrderItems
            .LoadWith(item => item.Product)
            .GroupBy(item => Sql.GroupBy.Cube(() => new { item.Product.SupplierId, item.ProductId, }))
            .Select(group => new
            {
                Grouping = Sql.Grouping(group.Key.SupplierId, group.Key.ProductId),
                group.Key.SupplierId,
                group.Key.ProductId,
                ItemsCount = group.Count(),
            })
            .OrderByDescending(item => item.Grouping)
            .ThenBy(item => item.SupplierId)
            .ThenBy(item => item.ProductId)
            .ToListAsync(token: TestContext.Current.CancellationToken);

        var sumsBySupplier = result
            .Where(item => item.Grouping == 0)
            .GroupBy(item => item.SupplierId, item => item.ItemsCount)
            .ToDictionary(item => item.Key, item => item.Sum());
        var sumsByProduct = result
            .Where(item => item.Grouping == 0)
            .GroupBy(item => item.ProductId, item => item.ItemsCount)
            .ToDictionary(item => item.Key, item => item.Sum());
        var totalBySupplier = result
            .Where(item => item.Grouping == 1)
            .Sum(item => item.ItemsCount);
        var totalByProduct = result
            .Where(item => item.Grouping == 2)
            .Sum(item => item.ItemsCount);

        Assert.NotNull(result);
        Assert.All(result.Where(item => item.Grouping == 1),
            item => Assert.Equal(item.ItemsCount, sumsBySupplier.GetValueOrDefault(item.SupplierId)));
        Assert.All(result.Where(item => item.Grouping == 2),
            item => Assert.Equal(item.ItemsCount, sumsByProduct.GetValueOrDefault(item.ProductId)));
        var totalItem = Assert.Single(result, item => item.Grouping == 3);
        Assert.Equal(totalBySupplier, totalItem.ItemsCount);
        Assert.Equal(totalByProduct, totalItem.ItemsCount);
    }

    #endregion // Rollup

    #endregion // Grouping

    #region HashAgg

    [Fact]
    public async Task Test_HashAgg_All()
    {
        var result = await northwind.Context.OrderItems
            .GroupBy(item => item.OrderId)
            .Select(group => group.HashAgg())
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.All(result, item => Assert.NotEqual(0, item));
    }

    [Fact]
    public async Task Test_HashAgg_All_Extension()
    {
        var result = await northwind.Context.OrderItems
            .Select(item => Sql.Ext.HashAgg().ToValue())
            .ToListAsync(token: TestContext.Current.CancellationToken);

        var hash = Assert.Single(result);
        Assert.NotEqual(0, hash);
    }

    [Fact]
    public async Task Test_HashAgg_Cols()
    {
        var result = await northwind.Context.OrderItems
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                HashPartial = group.HashAgg(x => x.ProductId, x => x.UnitPrice),
                HashFull = group.HashAgg(),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.All(result, item =>
        {
            Assert.NotEqual(0, item.HashPartial);
            Assert.NotEqual(0, item.HashFull);
            Assert.NotEqual(item.HashPartial, item.HashFull);
        });
    }

    #endregion // HashAgg

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
