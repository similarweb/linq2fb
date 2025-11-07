using FireboltNETSDK.Exception;
using LinqToDB;
using LinqToDB.Data;
using Similarweb.LinqToDB.Firebolt.Extensions;
using Similarweb.LinqToDB.Firebolt.Tests.Fixtures;
using Similarweb.LinqToDB.Firebolt.Tests.Northwind;
using Xunit;

namespace Similarweb.LinqToDB.Firebolt.Tests.Linq;

/// <summary>
/// Proofs of concept tests for Firebolt LINQ provider. Will grow with implementing Firebolt-specific methods
/// </summary>
public class FireboltPoCTests(
    ContextFixture<NorthwindContext> northwind
) : IClassFixture<ContextFixture<NorthwindContext>>, IDisposable, IAsyncDisposable
{
    #region Selects
    [Fact]
    public async Task TestSelect_WithDefaultSettings()
    {
        var query = from customer in northwind.Context.Customers select customer;
        var result = await query.ToListAsync(token: TestContext.Current.CancellationToken);
        Assert.NotEmpty(result);
        Assert.Equal(92, result.Count);
    }

    [Fact]
    public async Task TestSelect_WithSnakeCase()
    {
        var query = from product in northwind.Context.Products select product;
        var result = await query.ToListAsync(token: TestContext.Current.CancellationToken);
        Assert.NotEmpty(result);
        Assert.Equal(78, result.Count);
    }

    [Fact]
    public async Task TestSelect_WithUnion()
    {
        var result = await northwind.Context.Customers
            .Where(customer => new[] { 1, 2, 3 }.Contains(customer.Id))
            .Select(customer => new { customer.Id, customer.FirstName, customer.LastName, })
            .Union(northwind.Context.Customers
                .Where(customer => new[] { 3, 4, 5 }.Contains(customer.Id))
                .Select(customer => new { customer.Id, customer.FirstName, customer.LastName, }))
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(5, result.Count);
        Assert.Equivalent(new[] { 1, 2, 3, 4, 5 }, result.Select(x => x.Id));
    }

    [Fact]
    public async Task TestSelect_WithUnionAll()
    {
        var result = await northwind.Context.Customers
            .Where(customer => new[] { 1, 2, 3 }.Contains(customer.Id))
            .Select(customer => new { customer.Id, customer.FirstName, customer.LastName, })
            .Concat(northwind.Context.Customers
                .Where(customer => new[] { 3, 4, 5 }.Contains(customer.Id))
                .Select(customer => new { customer.Id, customer.FirstName, customer.LastName, }))
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(6, result.Count);
        Assert.Equivalent(new[] { 1, 2, 3, 3, 4, 5 }, result.Select(x => x.Id));
    }

    [Fact]
    public async Task TestSelect_Many()
    {
        var result = await northwind.Context.Customers
            .SelectMany(customer => northwind.Context.Suppliers
                .Select(supplier => new { Name = customer.FirstName, Company = supplier.CompanyName })
            )
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(92 * 29, result.Count);
    }
    #endregion // Selects

    #region Grouping
    [Fact]
    public async Task TestGrouping_ForUsers()
    {
        var result = await northwind.Context.Orders
            .GroupBy(order => order.CustomerId)
            .Select(grouping => new { CustomerId = grouping.Key, OrderCount = grouping.Count() })
            .OrderByDescending(grouping => grouping.OrderCount)
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(31, result.First().OrderCount);
    }

    [Fact]
    public async Task TestGrouping_ForUsersLeftJoiningCompanies()
    {
        var result = await northwind.Context.Customers
            .Join(
                northwind.Context.Orders
                    .GroupBy(order => order.CustomerId)
                    .Select(grouping => new { CustomerId = grouping.Key, OrderCount = grouping.Count() }),
                SqlJoinType.Left,
                (customer, orderStat) => customer.Id == orderStat.CustomerId,
                (customer, orderStat) => new
                {
                    customer.Id,
                    customer.FirstName,
                    customer.LastName,
                    OrderCount = (int?)orderStat.OrderCount,
                })
            .OrderByDescending(grouping => grouping.OrderCount)
            .ThenBy(grouping => grouping.LastName)
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Null(result.First().OrderCount);
        Assert.Equal("Muad", result.First().FirstName);
        var nonNull = result.SkipWhile(x => x.OrderCount == null).First();
        Assert.NotNull(nonNull);
        Assert.Equal(31, nonNull.OrderCount);
        Assert.Equal("Pavarotti", nonNull.LastName);
    }
    #endregion // Grouping

    #region Find
    [Fact]
    public async Task TestFind_ById()
    {
        var query = from customer in northwind.Context.Customers where customer.Id == 1 select customer;
        var result = await query.ToListAsync(token: TestContext.Current.CancellationToken);

        var item = Assert.Single(result);
        Assert.NotNull(item);
        Assert.Equal(1, item.Id);
    }

    [Fact]
    public async Task TestFind_ByString()
    {
        var query = from customer in northwind.Context.Customers where customer.LastName == "Al'dhib" select customer.Id;
        var result = await query.ToListAsync(token: TestContext.Current.CancellationToken);

        var id = Assert.Single(result);
        Assert.Equal(92, id);
    }

    [Fact]
    public async Task TestFind_ByTrimmedLowerCaseString()
    {
        var query =
            from supplier in northwind.Context.Suppliers
            where supplier.CompanyName.ToLower().Trim() == "mayumi's"
            select supplier;
        var result = await query.ToListAsync(token: TestContext.Current.CancellationToken);

        var found = Assert.Single(result);
        Assert.NotNull(found);
        Assert.Equal(6, found.Id);
    }

    [Fact]
    public async Task TestFind_ByGuid()
    {
        var query = from supplier in northwind.Context.Suppliers where supplier.PublicId == Guid.Parse("05A09602-F480-4914-8FA0-7B3D93F5DAD4") select supplier;
        var result = await query.ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        var item = Assert.Single(result);
        Assert.NotNull(item);
        Assert.Equal(3, item.Id);
    }

    [Fact]
    public async Task TestFind_UsingInClause_ForInts()
    {
        var ids = new[] { 1, 2, 3, 123, };
        var query = from supplier in northwind.Context.Suppliers where ids.Contains(supplier.Id) select supplier;
        var result = await query.ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(3, result.Count);
        Assert.Equivalent(new[] { 1, 2, 3 }, result.Select(x => x.Id));
    }

    [Fact]
    public async Task TestFind_UsingInClause_ForGuids()
    {
        var ids = new[] { Guid.Parse("9C2D54C3-4B50-4E88-987A-4644E3DB40EA"), Guid.Parse("411974c9-adc2-42ff-b5a2-fca346929a8b"), Guid.NewGuid(), };
        var query = from supplier in northwind.Context.Suppliers where ids.Contains(supplier.PublicId) select supplier;
        var result = await query.ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(2, result.Count);
        Assert.Equivalent(new[] { 2, 22 }, result.Select(x => x.Id));
    }

    [Fact]
    public async Task TestFind_UsingInClause_ForStrings()
    {
        var names = new[] { "Mayumi's", "G'day, Mate", "some_crap" };
        var query = from supplier in northwind.Context.Suppliers where names.Contains(supplier.CompanyName) select supplier;
        var result = await query.ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(2, result.Count);
        Assert.Equivalent(new[] { "Mayumi's", "G'day, Mate" }, result.Select(x => x.CompanyName));
    }

    [Fact]
    public async Task TestFind_UsingInClause_ForStrings_WithLowercasing()
    {
        var names = new[] { "tunnbröd", "original frankfurter grüne soße" };
        var query = from product in northwind.Context.Products where names.Contains(product.ProductName.ToLower()) select product;
        var result = await query.ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(2, result.Count);
        Assert.Equivalent(new[] { 23, 77 }, result.Select(x => x.Id));
    }

    [Fact]
    public async Task TestFind_UsingInClause_ForStrings_WithInjections()
    {
        var names = new[] { "' OR 1 = 1; --" };
        var query = from product in northwind.Context.Products where names.Contains(product.ProductName) select product;
        var result = await query.ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.Empty(result);
    }

    [Fact]
    public async Task TestFind_UsingStartingLike()
    {
        var query = from product in northwind.Context.Products where product.ProductName.StartsWith("Sir") select product;
        var result = await query.ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(3, result.Count);
    }

    [Theory(Skip = "we're getting 2 errors here, main one is 'operator 'notLike' for input types (text, text) not found, try adding explicit casts'")]
    [InlineData(new[] { "fres" }, new[] { "sec", "Zaan" })]
    public async Task TestFind_Using_Likes(string[] includes, string[] excludes)
    {
        var result = await northwind.Context.Customers
            .Where(customer => includes.All(include => customer.FirstName.Contains(include)) && excludes.All(exclude => customer.FirstName.Contains(exclude)))
            .ToListAsync(token: TestContext.Current.CancellationToken);

        _ = Assert.Single(result);
    }

    [Fact]
    public async Task TestFind_UsingContainingLike()
    {
        var query = from customer in northwind.Context.Customers where customer.FirstName.Contains("eg") select customer;
        var result = await query.ToListAsync(token: TestContext.Current.CancellationToken);

        _ = Assert.Single(result);
    }
    #endregion // Find

    #region SkipTake
    [Fact]
    public async Task TestFind_SkipTake()
    {
        var result = await northwind.Context.Customers
            .OrderBy(customer => customer.FirstName)
            .Skip(5)
            .Take(10)
            .ToListAsync(token: TestContext.Current.CancellationToken);
        var noSkip = await northwind.Context.Customers
            .OrderBy(customer => customer.FirstName)
            .Take(6)
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.NotEmpty(noSkip);
        Assert.Equal(10, result.Count);
        Assert.Equal(6, noSkip.Count);
        Assert.Equal(result.First().Id, noSkip.Last().Id);
    }

    [Fact]
    public async Task TestFind_SkipTake_Subquery()
    {
        var result = await northwind.Context.Customers
            .OrderBy(customer => customer.LastName)
            .Skip(5)
            .Take(10)
            .Where(customer => customer.LastName.StartsWith('C'))
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.True(result.Count < 10);
        Assert.All(result, customer => customer.LastName.StartsWith('C'));
    }
    #endregion // SkipTake

    #region CTEs
    [Fact]
    public async Task TestCte_SimpleSelect()
    {
        var result = await northwind.Context.Orders
            .GroupBy(order => order.CustomerId)
            .Select(group => new { CustomerId = group.Key, Count = group.Count() })
            .OrderBy(order => order.CustomerId)
            .AsCte()
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(89, result.Count);
        Assert.Equal(6, result.First().Count);
    }

    [Fact]
    public async Task TestCte_TwoCtesWithSameNameSelect()
    {
        var sameYearCte = northwind.Context.Orders
            .GroupBy(order => DateTimeMethods.DatePart(Sql.DateParts.Year, order.OrderDate))
            .Select(group => new { OrderYear = group.Key, Count = group.Count() })
            .AsCte("duplicated");
        var sameYearMonthCte = northwind.Context.Orders
            .GroupBy(order => new { order.OrderDate.Year, order.OrderDate.Month })
            .Select(group => new { OrderYear = group.Key.Year, OrderMonth = group.Key.Month, Count = group.Count() })
            .AsCte("duplicated");
        var result = await sameYearCte
            .Join(
                sameYearMonthCte,
                SqlJoinType.Left,
                (year, yearMonth) => year.OrderYear == yearMonth.OrderYear,
                (year, yearMonth) => new { year.OrderYear, yearMonth.OrderMonth, YearCount = year.Count, YearMonthCount = yearMonth.Count })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(23, result.Count);
        Assert.All(
            result.GroupBy(item => item.OrderYear),
            group => Assert.All(
                group,
                item => Assert.Equal(group.First().YearCount, item.YearCount)
            )
        );
        Assert.All(
            result
                .GroupBy(item => item.OrderYear)
                .Select(group => new
                {
                    OrderYear = group.Key,
                    ClientYearCount = group.Sum(item => item.YearMonthCount),
                    ServerYearCount = group.First().YearCount,
                }),
            item => Assert.Equal(item.ClientYearCount, item.ServerYearCount)
        );
    }

    [Fact]
    public async Task TestCte_WithJoin()
    {
        var cte = northwind.Context.Products
            .GroupBy(product => product.SupplierId)
            .Select(group => new { SupplierId = group.Key, Count = group.Count() })
            .AsCte();
        var query =
            from supplier in northwind.Context.Suppliers
            join productStat in cte on supplier.Id equals productStat.SupplierId
            orderby supplier.CompanyName descending
            select new { supplier.Id, supplier.CompanyName, productStat.Count };
        var result = await query.ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(29, result.Count);
        Assert.Equal("Zaanse Snoepfabriek", result.First().CompanyName);
    }
    #endregion // CTEs

    #region Materialized CTEs
    [Fact]
    public async Task TestMaterializedCte_Select()
    {
        var result = await northwind.Context.Products
            .LoadWith(product => product.Supplier)
            .GroupBy(product => product.Supplier)
            .Select(group => new { Supplier = group.Key, Count = group.Count() })
            .AsMaterializedCte()
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.Contains("MATERIALIZED", northwind.Context.LastQuery);
        Assert.NotEmpty(result);
        Assert.Equal(29, result.Count);
    }

    [Fact]
    public async Task TestMaterializedCte_TwoNamedSelect()
    {
        var sameYearCte = northwind.Context.Orders
            .GroupBy(order => DateTimeMethods.DatePart(Sql.DateParts.Year, order.OrderDate))
            .Select(group => new { OrderYear = group.Key, Count = group.Count() })
            .AsMaterializedCte("same_year");
        var sameYearMonthCte = northwind.Context.Orders
            .GroupBy(order => new { order.OrderDate.Year, order.OrderDate.Month })
            .Select(group => new { OrderYear = group.Key.Year, OrderMonth = group.Key.Month, Count = group.Count() })
            .AsMaterializedCte("same_year_month");
        var result = await sameYearCte
            .Join(
                sameYearMonthCte,
                SqlJoinType.Left,
                (year, yearMonth) => year.OrderYear == yearMonth.OrderYear,
                (year, yearMonth) => new { year.OrderYear, yearMonth.OrderMonth, YearCount = year.Count, YearMonthCount = yearMonth.Count })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.Contains("MATERIALIZED", northwind.Context.LastQuery);
        Assert.NotEmpty(result);
        Assert.Equal(23, result.Count);
        Assert.All(
            result.GroupBy(item => item.OrderYear),
            group => Assert.All(
                group,
                item => Assert.Equal(group.First().YearCount, item.YearCount)
            )
        );
        Assert.All(
            result
                .GroupBy(item => item.OrderYear)
                .Select(group => new
                {
                    OrderYear = group.Key,
                    ClientYearCount = group.Sum(item => item.YearMonthCount),
                    ServerYearCount = group.First().YearCount,
                }),
            item => Assert.Equal(item.ClientYearCount, item.ServerYearCount)
        );
    }

    [Fact]
    public async Task TestMaterializedCte_TwoNamedUsingConventionsSelect()
    {
        var sameYearCte = northwind.Context.Orders
            .GroupBy(order => DateTimeMethods.DatePart(Sql.DateParts.Year, order.OrderDate))
            .Select(group => new { OrderYear = group.Key, Count = group.Count() })
            .AsCte("__mat__cte__same_year");
        var sameYearMonthCte = northwind.Context.Orders
            .GroupBy(order => new { order.OrderDate.Year, order.OrderDate.Month })
            .Select(group => new { OrderYear = group.Key.Year, OrderMonth = group.Key.Month, Count = group.Count() })
            .AsCte("same_year_month__mat__cte__");
        var result = await sameYearCte
            .Join(
                sameYearMonthCte,
                SqlJoinType.Left,
                (year, yearMonth) => year.OrderYear == yearMonth.OrderYear,
                (year, yearMonth) => new { year.OrderYear, yearMonth.OrderMonth, YearCount = year.Count, YearMonthCount = yearMonth.Count })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.Contains("MATERIALIZED", northwind.Context.LastQuery);
        Assert.NotEmpty(result);
        Assert.Equal(23, result.Count);
        Assert.All(
            result.GroupBy(item => item.OrderYear),
            group => Assert.All(
                group,
                item => Assert.Equal(group.First().YearCount, item.YearCount)
            )
        );
        Assert.All(
            result
                .GroupBy(item => item.OrderYear)
                .Select(group => new
                {
                    OrderYear = group.Key,
                    ClientYearCount = group.Sum(item => item.YearMonthCount),
                    ServerYearCount = group.First().YearCount,
                }),
            item => Assert.Equal(item.ClientYearCount, item.ServerYearCount)
        );
    }
    #endregion // Materialized CTEs

    #region Standard window functions
    [Fact]
    public async Task TestWindow_Rank()
    {
        var mostPopularProducts = northwind.Context.Products
            .LoadWith(product => product.OrderItems)
            .Select(product =>
                new
                {
                    product.Id,
                    product.ProductName,
                    product.SupplierId,
                    product.UnitPrice,
                    Rank = Sql.Ext.Rank()
                        .Over()
                        .PartitionBy(product.SupplierId)
                        .OrderByDesc(product.OrderItems.Sum(item => item.Quantity))
                        .ToValue(),
                })
            .Where(productStat => productStat.Rank == 1)
            .AsCte();
        var query =
            from supplier in northwind.Context.Suppliers
            join product in mostPopularProducts on supplier.Id equals product.SupplierId
            select new { supplier.Id, supplier.CompanyName, ProductId = product.Id, product.ProductName, product.UnitPrice };
        var result = await query.ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(29, result.Count);
        Assert.Equal("Chang", result.First(it => it.Id == 1).ProductName);
        Assert.Equal("Lakkalikööri", result.First(it => it.Id == 23).ProductName);
    }
    #endregion

    #region Parameters
    [Theory]
    [InlineData(1, 100, 2)]
    [InlineData(1, 500, 2)]
    [InlineData(4, 500, 3)]
    public async Task Test_Parameters(int start, int end, int step)
    {
        var serie = await northwind.Context
            .QueryToListAsync<int>(
                "SELECT t FROM GENERATE_SERIES(@start, @end, @step) t",
                new DataParameter(nameof(start), start),
                new DataParameter(nameof(end), end),
                new DataParameter(nameof(step), step)
            );

        Assert.NotEmpty(serie);
        Assert.Equal(start, serie.First());
        Assert.Equal(end - ((end - start) % step), serie.Last());
        Assert.Equal(1 + (end - start) / step, serie.Count);
    }

    [Fact]
    public async Task Test_Parameters_Misspelled()
    {
        var ex = await Assert.ThrowsAsync<AggregateException>(
            async () => await northwind.Context
                .QueryToListAsync<int>(
                    "SELECT t FROM GENERATE_SERIES(@start, @end, @step) t",
                    new DataParameter("start", 1),
                    new DataParameter("end", 100),
                    new DataParameter("stop", 2)
                )
        );
        var inner = Assert.IsType<FireboltStructuredException>(ex.InnerException);
        Assert.Contains("syntax error, unexpected character, expecting ')'", inner.Message);
    }

    [Fact]
    public async Task Test_Parameters_Missing()
    {
        var ex = await Assert.ThrowsAsync<AggregateException>(
            async () => await northwind.Context
                .QueryToListAsync<int>(
                    "SELECT t FROM GENERATE_SERIES(@start, @end, @step) t",
                    new DataParameter("start", 1),
                    new DataParameter("end", 100)
                )
        );
        var inner = Assert.IsType<FireboltStructuredException>(ex.InnerException);
        Assert.Contains("syntax error, unexpected character, expecting ')'", inner.Message);
    }
    #endregion // Parameters

    #region Conversion
    [Fact]
    public async Task Test_DecimalConvertedToInt()
    {
        var result = await northwind.Context.OrderItems
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                Prices = group.ArrayAggregate(item => Convert.ToInt32(item.UnitPrice)).ToValue(),
                UnitPrices = group.ArrayAggregate(item => item.UnitPrice).ToValue(),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.All(result, x => Assert.Equal(x.UnitPrices.Select(price => (int)Math.Round(price)), x.Prices));
    }

    [Fact]
    public async Task Test_DecimalRoundedToInt()
    {
        var result = await northwind.Context.OrderItems
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                Prices = group.ArrayAggregate(item => Convert.ToInt32(Math.Round(item.UnitPrice))).ToValue(),
                UnitPrices = group.ArrayAggregate(item => item.UnitPrice).ToValue(),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.All(result, x => Assert.Equal(x.UnitPrices.Select(price => (int)Math.Round(price)), x.Prices));
    }
    #endregion // Conversion

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
