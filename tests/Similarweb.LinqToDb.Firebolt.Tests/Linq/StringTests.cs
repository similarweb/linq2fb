using LinqToDB;
using Similarweb.LinqToDB.Firebolt.Extensions;
using Similarweb.LinqToDB.Firebolt.Tests.Fixtures;
using Similarweb.LinqToDB.Firebolt.Tests.Northwind;
using Xunit;

namespace Similarweb.LinqToDB.Firebolt.Tests.Linq;

public class StringTests(
    ContextFixture<NorthwindContext> northwind
) : IClassFixture<ContextFixture<NorthwindContext>>, IDisposable, IAsyncDisposable
{
    #region RegexLikeAny
    [Fact]
    public async Task Test_RegexLikeAny()
    {
        var patterns = new[] { "^Pa", "ta", "al.*?o" }.ToArray();
        var result = await northwind.Context.Products
            .Where(product => product.ProductName.RegexpLikeAny(patterns))
            .Select(product => new { product.Id, product.ProductName, })
            .ToListAsync();

        Assert.NotEmpty(result);
        Assert.Equal(5, result.Count);
    }

    [Fact]
    public async Task Test_RegexLikeAny_MoreComplex()
    {
        var patterns = new[] { "va$", "^Lou", "al.*?o" }.ToArray();
        var result = await northwind.Context.Products
            .Where(product => product.ProductName.RegexpLikeAny(patterns))
            .Select(product => new { product.Id, product.ProductName, })
            .ToListAsync();

        Assert.NotEmpty(result);
        Assert.Equal(6, result.Count);
    }

    [Fact]
    public async Task Test_RegexLikeAny_Escaping()
    {
        var patterns = new[] { "Yo", "'" }.Select(x => $"{x}").ToArray();
        var result = await northwind.Context.Customers
            .Where(customer => customer.FirstName.RegexpLikeAny(patterns) || customer.LastName.RegexpLikeAny(patterns))
            .Select(customer => new { customer.Id, customer.FirstName, customer.LastName, })
            .ToListAsync();

        Assert.NotEmpty(result);
        Assert.Equal(4, result.Count);
    }

    #endregion // RegexLikeAny

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
