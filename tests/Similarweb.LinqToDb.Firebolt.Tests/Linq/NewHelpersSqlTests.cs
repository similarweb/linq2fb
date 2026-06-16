using LinqToDB;
using LinqToDB.Data;
using Similarweb.LinqToDB.Firebolt;
using Similarweb.LinqToDB.Firebolt.Extensions;
using Similarweb.LinqToDB.Firebolt.Tests.Northwind;
using Xunit;

namespace Similarweb.LinqToDB.Firebolt.Tests.Linq;

/// <summary>
/// <para>SQL-generation tests for the helpers added to support the Segments traffic-and-engagement conversion.</para>
/// <para>These assert the produced SQL only (via <see cref="object.ToString"/>) and do not require a live Firebolt connection.</para>
/// </summary>
public class NewHelpersSqlTests
{
    private static NorthwindContext CreateContext()
    {
        Registration.AddDataProvider();
        var options = new DataOptions()
            .UseConnectionString(Registration.DataProviderName, "database=db;engine=e;account=a;client_id=x;client_secret=y;env=");
        return new NorthwindContext(options);
    }

    [Fact]
    public void ArrayTransform_TwoArrays_EmitsArrayTransformWithTwoArrayArguments()
    {
        using var context = CreateContext();

        var sql = context.OrderItems
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                group.Key,
                Diffs = group.ArrayAggregate(item => item.Quantity).ToValue()
                    .ArrayTransform(
                        group.ArrayAggregate(item => item.ProductId).ToValue(),
                        (quantity, productId) => productId - quantity),
            })
            .ToString();

        Assert.Contains("ARRAY_TRANSFORM(", sql);
        Assert.Contains("->", sql);
    }

    [Fact]
    public void At_EmitsOneBasedSubscript()
    {
        using var context = CreateContext();

        var sql = context.OrderItems
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                group.Key,
                First = group.ArrayAggregate(item => item.Quantity).ToValue().At(1),
            })
            .ToString();

        Assert.Contains("[1]", sql);
    }

    [Fact]
    public void AsSqlArray_EmitsArrayLiteral()
    {
        using var context = CreateContext();

        var sql = context.OrderItems
            .Select(item => new
            {
                item.Id,
                Wrapped = item.Quantity.AsSqlArray(),
            })
            .ToString();

        Assert.Contains("[", sql);
    }

    [Fact]
    public void NullIf_EmitsNullIf()
    {
        using var context = CreateContext();

        var sql = context.OrderItems
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                group.Key,
                Share = group.Count().NullIf(0),
            })
            .ToString();

        Assert.Contains("NULLIF(", sql);
    }

    [Fact]
    public void ArrayCount_WithIsNotNullLambda_EmitsIsNotNull()
    {
        using var context = CreateContext();

        var sql = context.OrderItems
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                group.Key,
                NonNulls = group.ArrayAggregate(int? (item) => item.ProductId).ToValue()
                    .ArrayCount(value => value != null),
            })
            .ToString();

        Assert.Contains("IS NOT NULL", sql);
        Assert.DoesNotContain("!= NULL", sql);
    }
}
