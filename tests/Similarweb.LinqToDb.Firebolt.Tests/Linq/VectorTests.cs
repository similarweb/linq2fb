using LinqToDB;
using LinqToDB.Mapping;
using Similarweb.LinqToDB.Firebolt.Extensions;
using Similarweb.LinqToDB.Firebolt.Tests.Fixtures;
using Similarweb.LinqToDB.Firebolt.Tests.Northwind;
using Xunit;

namespace Similarweb.LinqToDB.Firebolt.Tests.Linq;

public class VectorTests(
    ContextFixture<NorthwindContext> northwind
) : IClassFixture<ContextFixture<NorthwindContext>>, IDisposable, IAsyncDisposable
{
    #region Basics: Arrays
    [Fact]
    public async Task Test_GetArr_Plain_Dbl()
    {
        var arr = await northwind.Context
            .FromSql<ArrHolder<double>>("SELECT [1.0, 2] arr")
            .Select(dto => dto.Arr)
            .ToListAsync(token: TestContext.Current.CancellationToken);
        var item = Assert.Single(arr);
        Assert.Equal([1, 2], item);
    }

    [Fact]
    public async Task Test_GetArr_Plain_Float()
    {
        var arr = await northwind.Context
            .FromSql<ArrHolder<double>>("SELECT [1.0, 2] arr")
            .Select(dto => dto.Arr)
            .ToListAsync(token: TestContext.Current.CancellationToken);
        var item = Assert.Single(arr);
        Assert.Equal([1, 2], item);
    }
    #endregion // Basics: Arrays

    #region VectorCosineSimilarity
    [Fact]
    public async Task Test_VectorCosineSimilarity_Plain_Dbl()
    {
        var first = northwind.Context
            .FromSql<ArrHolder<double>>("SELECT [1, 2] arr");
        var second = northwind.Context
            .FromSql<ArrHolder<double>>("SELECT [3, 4] arr");
        var result = await first
            .SelectMany(firstDto => second.Select(secondDto => firstDto.Arr.VectorCosineSimilarity(secondDto.Arr)))
            .ToListAsync(token: TestContext.Current.CancellationToken);

        var res = Assert.Single(result);
        Assert.Equal(0.9838699100999074, res, precision: 14);
    }

    [Fact]
    public async Task Test_VectorCosineSimilarity_Plain_Float()
    {
        var first = northwind.Context
            .FromSql<ArrHolder<float>>("SELECT [1, 2] arr");
        var second = northwind.Context
            .FromSql<ArrHolder<float>>("SELECT [3, 4] arr");
        var result = await first
            .SelectMany(firstDto => second.Select(secondDto => firstDto.Arr.VectorCosineSimilarity(secondDto.Arr)))
            .ToListAsync(token: TestContext.Current.CancellationToken);

        var res = Assert.Single(result);
        Assert.Equal(0.98386991024017334, res, precision: 7);
    }

    private class ArrHolder<T>
    {
        [Column("arr")]
        public required T[] Arr { get; init; }
    }
    #endregion // VectorCosineSimilarity

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
