using LinqToDB;
using Similarweb.LinqToDB.Firebolt.Extensions;
using Similarweb.LinqToDB.Firebolt.Tests.Fixtures;
using Similarweb.LinqToDB.Firebolt.Tests.Northwind;
using Xunit;
using Sql = LinqToDB.Sql;

namespace Similarweb.LinqToDB.Firebolt.Tests.Linq;

/// <summary>
/// Tests for Firebolt <see href="https://docs.firebolt.io/sql_reference/functions-reference/DataSketches/">DataSketches functions</see>.
/// </summary>
public class DataSketchesTests(
    ContextFixture<NorthwindContext> northwind
) : IClassFixture<ContextFixture<NorthwindContext>>, IDisposable, IAsyncDisposable
{
    #region HllCountBuild

    [Fact]
    public async Task Test_HllCountBuild()
    {
        var dataToCount1 = northwind.Context.GenerateSeries(0, 1_000_000, 3).AsCte();
        var dataToCount2 = northwind.Context.GenerateSeries(0, 1_000_000, 2).AsCte();
        var sketch1 = dataToCount1
            .GroupBy(x => 1)
            .Select(group => new
            {
                Build = group.HllCountBuild(x => x),
            });
        var sketch2 = dataToCount2
            .GroupBy(x => 1)
            .Select(group => new
            {
                Build = group.HllCountBuild(x => x),
            });
        var aggregate = sketch1.Concat(sketch2)
            .Select(group => new
            {
                Estimate = Sql.Ext.HllCountEstimate(group.Build).ToValue(),
                group.Build,
            });
        var intermediate = aggregate
            .GroupBy(x => 1)
            .Select(group => new
            {
                Merged = group.HllCountMerge(x => x.Build),
                Estimate2 = group.Sum(x => x.Estimate),
            })
            .AsCte();
        var result = await intermediate
            .Select(item => new
            {
                Estimate1 = Sql.Ext.HllCountEstimate(item.Merged).ToValue(),
                item.Estimate2,
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        var item = Assert.Single(result);
        Assert.NotEqual(0, item.Estimate1);
        Assert.NotEqual(0, item.Estimate2);
    }

    [Fact]
    public async Task Test_HllCountBuild_Extension()
    {
        var dataToCount1 = northwind.Context.GenerateSeries(0, 1_000_000, 3).AsCte();
        var dataToCount2 = northwind.Context.GenerateSeries(0, 1_000_000, 2).AsCte();
        var sketch1 = dataToCount1.Select(element => Sql.Ext.HllCountBuild(element).ToValue());
        var sketch2 = dataToCount2.Select(element => Sql.Ext.HllCountBuild(element).ToValue());
        var aggregate = sketch1.Concat(sketch2)
            .Select(hash => new
            {
                Estimate = Sql.Ext.HllCountEstimate(hash).ToValue(),
                Hash = hash,
            });
        var intermediate = aggregate
            .Select(item => new
            {
                Merged = Sql.Ext.HllCountMerge(item.Hash).ToValue(),
                Estimate2 = Sql.Ext.Sum(item.Estimate).ToValue(),
            })
            .AsCte();
        var result = await intermediate
            .Select(item => new
            {
                Estimate1 = Sql.Ext.HllCountEstimate(item.Merged).ToValue(),
                item.Estimate2,
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        var item = Assert.Single(result);
        Assert.NotEqual(0, item.Estimate1);
        Assert.NotEqual(0, item.Estimate2);
    }

    #endregion // HllCountBuild

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

