using System.Globalization;
using System.Text.RegularExpressions;
using LinqToDB;
using Similarweb.LinqToDB.Firebolt.Extensions;
using Similarweb.LinqToDB.Firebolt.Tests.Fixtures;
using Similarweb.LinqToDB.Firebolt.Tests.Northwind;
using Xunit;

namespace Similarweb.LinqToDB.Firebolt.Tests.Linq;

/// <summary>
/// Tests for Firebolt <see href="https://docs.firebolt.io/sql_reference/functions-reference/array/">array functions</see>.
/// </summary>
public class ArrayTests(
    ContextFixture<NorthwindContext> northwind
) : IClassFixture<ContextFixture<NorthwindContext>>, IDisposable, IAsyncDisposable
{
    #region Contains

    [Fact]
    public async Task Test_ArrayContains()
    {
        var result = await northwind.Context.OrderItems
            .LoadWith(item => item.Product)
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                OrderHasPavlova = group
                    .ArrayAggregate(item => item.Product.ProductName).ToValue()
                    .ArrayContains("Pavlova"),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(43, result.Count(item => item.OrderHasPavlova));
    }

    #endregion // Contains

    #region Distinct

    [Fact]
    public async Task Test_ArrayDistinct()
    {
        var result = await northwind.Context.OrderItems
            .LoadWith(item => item.Order)
            .GroupBy(item => item.ProductId)
            .Select(group => new
            {
                ProductId = group.Key,
                OrderIds = group.ArrayAggregate(int? (item) => item.Order.CustomerId).ToValue(),
                DistinctOrderIds = group.ArrayAggregate(int? (item) => item.Order.CustomerId).ToValue()
                    .ArrayDistinct(),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        foreach (var item in result)
        {
            Assert.Equivalent(item.DistinctOrderIds, item.OrderIds.ToHashSet());
        }
    }

    #endregion // Distinct

    #region Sort

    [Fact]
    public async Task Test_ArraySort()
    {
        var result = await northwind.Context.OrderItems
            .LoadWith(item => item.Order)
            .ThenLoad(order => order.Customer)
            .GroupBy(item => item.ProductId)
            .Select(group => new
            {
                ProductId = group.Key,
                CustomerNames = group.ArrayAggregate(item => item.Order.Customer.FirstName).ToValue()
                    .ArraySort(),
            })
            .InnerJoin(
                northwind.Context.Products.Where(product => product.ProductName == "Pavlova"),
                (pair, product) => pair.ProductId == product.Id,
                (pair, _) => pair.CustomerNames)
            .FirstAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(43, result.Length);
        Assert.Equal(
        [
            "Alexander", "Annette", "Carine", "Carlos", "Christina", "Dominique", "Felipe", "Felipe", "Georg", "Hari",
            "Henriette", "Horst", "Janete", "Janete", "Jean", "Jean", "Jonas", "Jose", "Jose", "Jose", "Jytte", "Karl",
            "Karl", "Laurence", "Laurence", "Laurence", "Lino", "Liz", "Matti", "Michael", "Patricia", "Paula", "Peter",
            "Philip", "Pirkko", "Renate", "Rene", "Rene", "Rene", "Roland", "Roland", "Roland", "Sergio"
        ], result);
    }

    #endregion // Sort

    #region Reverse Sort

    [Fact]
    public async Task Test_ArrayReverseSort()
    {
        var result = await northwind.Context.OrderItems
            .LoadWith(item => item.Order)
            .ThenLoad(order => order.Customer)
            .GroupBy(item => item.ProductId)
            .Select(group => new
            {
                ProductId = group.Key,
                CustomerNames = group.ArrayAggregate(item => item.Order.Customer.FirstName).ToValue()
                    .ArrayReverseSort(),
            })
            .InnerJoin(
                northwind.Context.Products.Where(product => product.ProductName == "Pavlova"),
                (pair, product) => pair.ProductId == product.Id,
                (pair, _) => pair.CustomerNames)
            .FirstAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Equal(43, result.Length);
        Assert.Equal(
        [
            "Sergio", "Roland", "Roland", "Roland", "Rene", "Rene", "Rene", "Renate", "Pirkko", "Philip", "Peter",
            "Paula", "Patricia", "Michael", "Matti", "Liz", "Lino", "Laurence", "Laurence", "Laurence", "Karl", "Karl",
            "Jytte", "Jose", "Jose", "Jose", "Jonas", "Jean", "Jean", "Janete", "Janete", "Horst", "Henriette", "Hari",
            "Georg", "Felipe", "Felipe", "Dominique", "Christina", "Carlos", "Carine", "Annette", "Alexander"
        ], result);
    }

    #endregion // Reverse Sort

    #region Count

    [Fact]
    public async Task Test_ArrayCount()
    {
        var result = await northwind.Context.OrderItems
            .LoadWith(item => item.Product)
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                Count = group.ArrayAggregate(item => item.Product.ProductName).ToValue()
                    .ArrayTransform(item => item.Length < 10)
                    .ArrayCount(),
            })
            .OrderBy(item => item.OrderId)
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
    }

    #endregion // Sort

    #region Flatten

    [Fact]
    public async Task Test_ArrayFlatten_FlattensNestedArrays()
    {
        // Create nested arrays: group by OrderId, inside each group make a group by ProductId.
        // That yields 2-D array of quantities per product. Then flatten to 1-D.
        var rows = await northwind.Context.OrderItems
            .GroupBy(item => new { item.OrderId, item.ProductId })
            .Select(orderGroup => new
            {
                OrderKey = orderGroup.Key,
                Arrays = orderGroup.ArrayAggregate(i => i.Quantity).ToValue()
            })
            .GroupBy(pair => pair.OrderKey.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                TwoDim = group.ArrayAggregate(pair => pair.Arrays).ToValue(),
            })
            .Select(pair => new
            {
                pair.OrderId,
                pair.TwoDim,
                Flattened = pair.TwoDim.ArrayFlatten(),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(rows);

        foreach (var row in rows)
        {
            // Manual check: concat all inner arrays from TwoDim and compare to Flattened
            var manual = row.TwoDim.SelectMany(inner => inner).ToArray();
            Assert.Equal(manual, row.Flattened);
        }

        // Ensure we actually had a case with >1 inner array to prove flattening did work
        Assert.Contains(rows, r => r.TwoDim.Length > 1);
    }

    [Fact]
    public async Task Test_ArrayFlatten_ThreeDimensional()
    {
        // Level 1: for each (OrderId, ProductId, UnitPrice) group -> aggregate quantities into a 1-D array
        var lvl1 = northwind.Context.OrderItems
            .GroupBy(i => new { i.OrderId, i.ProductId, i.UnitPrice })
            .Select(g => new
            {
                g.Key,
                Arr1D = g.ArrayAggregate(i => i.Quantity).ToValue(), // int[]
            });

        // Level 2: for each (OrderId, ProductId) group -> aggregate Level-1 arrays into a 2-D array
        var lvl2 = lvl1
            .GroupBy(x => new { x.Key.OrderId, x.Key.ProductId })
            .Select(g => new
            {
                g.Key,
                Arr2D = g.ArrayAggregate(x => x.Arr1D).ToValue(), // int[][]
            });

        // Level 3: for each OrderId -> aggregate Level-2 arrays into a 3-D array
        var rows = await lvl2
            .GroupBy(x => x.Key.OrderId)
            .Select(g => new
            {
                OrderId = g.Key,
                Arr3D = g.ArrayAggregate(x => x.Arr2D).ToValue(), // int[][][]
            })
            .Select(r => new
            {
                r.OrderId,
                r.Arr3D,
                Flatten1 = r.Arr3D.ArrayFlatten(), // int[][]
                Flatten2 = r.Arr3D.ArrayFlatten().ArrayFlatten(), // int[]
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(rows);

        foreach (var r in rows)
        {
            // Manual one-level flatten (3D -> 2D): concat all 2D blocks
            int[][] manual2D = r.Arr3D.SelectMany(twoD => twoD).ToArray();
            Assert.Equal(manual2D, r.Flatten1);

            // Manual two-level flatten (3D -> 1D): concat all inner 1D arrays
            int[] manual1D = manual2D.SelectMany(inner => inner).ToArray();
            Assert.Equal(manual1D, r.Flatten2);
        }
    }

    #endregion // Flatten

    #region Reverse

    [Fact]
    public async Task Test_ArrayReverse_ExecutesAndIsInvolutive()
    {
        // We aggregate product ids per order, then apply reverse twice and compare.
        // Property under test: ARRAY_REVERSE(ARRAY_REVERSE(arr)) == arr
        var rows = await northwind.Context.OrderItems
            .GroupBy(i => i.OrderId)
            .Select(g => new
            {
                OrderId = g.Key,
                Orig = g.ArrayAggregate(i => i.ProductId).ToValue(),
                RevRev = g.ArrayAggregate(i => i.ProductId).ToValue()
                    .ArrayReverse()
                    .ArrayReverse(),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        // Sanity: we actually queried something
        Assert.NotEmpty(rows);

        // Main assertion: reverse(reverse(arr)) yields the original sequence
        foreach (var r in rows)
        {
            Assert.Equal(r.Orig, r.RevRev);
        }

        // Optional sanity: at least one group has 2+ elements (so reversing is meaningful)
        Assert.Contains(rows, r => r.Orig.Length >= 2);
    }

    #endregion // Reverse

    #region Min

    [Fact]
    public async Task Test_ArrayMin()
    {
        var rows = await northwind.Context.OrderItems
            .GroupBy(i => i.OrderId)
            .Select(g => new
            {
                OrderId = g.Key,
                Arr = g.ArrayAggregate(i => i.ProductId).ToValue(), // int[]
            })
            .Select(x => new
            {
                x.OrderId,
                x.Arr,
                MinServer = x.Arr.ArrayMin(), // int
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(rows);
        foreach (var r in rows)
            Assert.Equal(r.Arr.Min(), r.MinServer);

        // Sanity: at least one non-trivial group
        Assert.Contains(rows, r => r.Arr.Length >= 2);
    }

    #endregion // Min

    #region Max

    [Fact]
    public async Task Test_ArrayMax()
    {
        var rows = await northwind.Context.OrderItems
            .GroupBy(i => i.OrderId)
            .Select(g => new
            {
                OrderId = g.Key,
                Arr = g.ArrayAggregate(i => i.ProductId).ToValue(), // int[]
            })
            .Select(x => new
            {
                x.OrderId,
                x.Arr,
                MaxServer = x.Arr.ArrayMax(), // int
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(rows);
        foreach (var r in rows)
            Assert.Equal(r.Arr.Max(), r.MaxServer);

        Assert.Contains(rows, r => r.Arr.Length >= 2);
    }

    #endregion // Max

    #region Sum

    [Fact]
    public async Task Test_ArraySum_Int()
    {
        var rows = await northwind.Context.OrderItems
            .GroupBy(i => i.OrderId)
            .Select(g => new
            {
                OrderId = g.Key,
                Arr = g.ArrayAggregate(i => i.Quantity).ToValue(), // int[]
            })
            .Select(x => new
            {
                x.OrderId,
                x.Arr,
                SumServer = x.Arr.ArraySum(), // int
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(rows);
        foreach (var r in rows)
            Assert.Equal(r.Arr.Sum(), r.SumServer);

        Assert.Contains(rows, r => r.Arr.Length >= 2);
    }

    [Fact]
    public async Task Test_ArraySum_Long()
    {
        var rows = await northwind.Context.OrderItems
            .GroupBy(i => i.OrderId)
            .Select(g => new
            {
                OrderId = g.Key,
                Arr = g.ArrayAggregate(i => i.ProductId).ToValue()
                    .ArrayTransform(x => (long)x), // long[]
            })
            .Select(x => new
            {
                x.OrderId,
                x.Arr,
                SumServer = x.Arr.ArraySum(), // long
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(rows);
        foreach (var r in rows)
            Assert.Equal(r.Arr.Aggregate(0L, (acc, v) => acc + v), r.SumServer);

        Assert.Contains(rows, r => r.Arr.Length >= 2);
    }

    [Fact]
    public async Task Test_ArraySum_Double()
    {
        var rows = await northwind.Context.OrderItems
            .GroupBy(i => i.OrderId)
            .Select(g => new
            {
                OrderId = g.Key,
                // Force double on server side; Quantity is integer, 1.0 * x ensures double
                Arr = g.ArrayAggregate(i => 1.0 * i.Quantity).ToValue(), // double[]
            })
            .Select(x => new
            {
                x.OrderId,
                x.Arr,
                SumServer = x.Arr.ArraySum(), // double
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(rows);
        foreach (var r in rows)
        {
            var expected = r.Arr.Sum();
            var actual = r.SumServer;
            // Use precision-based equality for safety with FP, though here it should be exact.
            Assert.Equal(expected, actual, precision: 8);
        }

        Assert.Contains(rows, r => r.Arr.Length >= 2);
    }

    #endregion // Sum

    #region Intersect

    [Fact]
    public async Task Test_ArrayIntersect_Two()
    {
        var orderItemsByOrder = northwind.Context.OrderItems
            .GroupBy(item => item.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                ProductIds = group.ArrayAggregate(item => item.ProductId).ToValue(),
            })
            .AsCte();
        var result = await orderItemsByOrder
            .Where(pair => pair.OrderId == 38)
            .SelectMany(first => orderItemsByOrder
                .Where(pair => pair.OrderId == 47)
                .Select(second => first.ProductIds.ArrayIntersect(second.ProductIds)))
            .FirstAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Contains(1, result);
    }

    [Fact]
    public async Task Test_ArrayIntersect_Three()
    {
        var orderItemsByProduct = northwind.Context.OrderItems
            .GroupBy(item => item.ProductId)
            .Select(group => new
            {
                ProductId = group.Key,
                OrderIds = group.ArrayAggregate(item => item.OrderId).ToValue(),
            })
            .AsCte();
        var result = await orderItemsByProduct
            .Where(item => item.ProductId == 26)
            .SelectMany(
                first => orderItemsByProduct
                    .Where(item => item.ProductId == 65)
                    .SelectMany(
                        second => orderItemsByProduct
                            .Where(item => item.ProductId == 33),
                        (second, third) => new { second, third }
                    ),
                (first, secondAndThird) =>
                    first.OrderIds.ArrayIntersect(secondAndThird.second.OrderIds, secondAndThird.third.OrderIds)
            )
            .FirstAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(result);
        Assert.Contains(777, result);
    }

    #endregion // Intersect

    #region Overlap

    [Fact]
    public async Task Test_ArraysOverlap_Three()
    {
        var orderItemsByProduct = northwind.Context.OrderItems
            .GroupBy(item => item.ProductId)
            .Select(group => new
            {
                ProductId = group.Key,
                OrderIds = group.ArrayAggregate(item => item.OrderId).ToValue(),
            });
        var result = await orderItemsByProduct
            .Where(item => item.ProductId == 26)
            .SelectMany(
                first => orderItemsByProduct
                    .Where(item => item.ProductId == 65)
                    .SelectMany(
                        second => orderItemsByProduct
                            .Where(item => item.ProductId == 33),
                        (second, third) => new { second, third }
                    ),
                (first, secondAndThird) =>
                    first.OrderIds.ArraysOverlap(secondAndThird.second.OrderIds, secondAndThird.third.OrderIds)
            )
            .FirstAsync(token: TestContext.Current.CancellationToken);

        Assert.True(result);
    }

    #endregion // Overlap

    #region Length

    [Fact]
    public async Task Test_ArrayLength()
    {
        var rows = await northwind.Context.OrderItems
            .GroupBy(i => i.OrderId)
            .Select(group => new
            {
                OrderId = group.Key,
                Arr = group.ArrayAggregate(item => item.ProductId).ToValue(),
            })
            .Select(x => new
            {
                x.OrderId,
                x.Arr,
                ArrLenServer = x.Arr.ArrayLength(),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(rows);
        foreach (var r in rows)
            Assert.Equal(r.Arr.Length, r.ArrLenServer);

        Assert.Contains(rows, r => r.Arr.Length >= 2);
    }

    #endregion

    #region Slice

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task ArraySlice_StartOnly_Safe(int startIndex)
    {
        var rows = await northwind.Context.OrderItems
            .GroupBy(i => i.OrderId)
            .Select(g => new
            {
                OrderId = g.Key,
                Count = g.Count(), // array length proxy
                Arr = g.ArrayAggregate(i => i.Quantity).ToValue()
            })
            .Where(x => x.Count >= startIndex) // ensure startIndex is valid
            .Select(x => new
            {
                x.OrderId,
                x.Count,
                x.Arr,
                SliceServer = x.Arr.ArraySlice(startIndex)
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(rows);
        Assert.All(rows, r => Assert.True(r.Count >= startIndex));

        foreach (var r in rows)
        {
            var expected = r.Arr.Skip(startIndex - 1).ToArray();
            Assert.Equal(expected, r.SliceServer);
        }
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 10)] // length trimmed at array end
    public async Task ArraySlice_StartLen_Safe(int startIndex, int length)
    {
        var rows = await northwind.Context.OrderItems
            .GroupBy(i => i.OrderId)
            .Select(g => new
            {
                OrderId = g.Key,
                Count = g.Count(),
                Arr = g.ArrayAggregate(i => i.Quantity).ToValue()
            })
            .Where(x => x.Count >= startIndex) // avoid engine crash
            .Select(x => new
            {
                x.OrderId,
                x.Count,
                x.Arr,
                SliceServer = x.Arr.ArraySlice(startIndex, length)
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(rows);

        foreach (var r in rows)
        {
            var expected = r.Arr.Skip(startIndex - 1).Take(length).ToArray();
            Assert.Equal(expected, r.SliceServer);
        }
    }

    #endregion //Slice

    #region ToString

    [Fact]
    public async Task Test_ArrayToString_NoSeparator()
    {
        var rows = await northwind.Context.OrderItems
            .LoadWith(item => item.Product)
            .GroupBy(i => i.OrderId)
            .Select(g => new
            {
                OrderId = g.Key,
                Arr = g.ArrayAggregate(i => i.Product.ProductName).ToValue(),
            })
            .Select(x => new
            {
                x.OrderId,
                x.Arr,
                ServerText = x.Arr.ArrayToString(), // ARRAY_TO_STRING(arr)
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(rows);

        foreach (var r in rows)
        {
            var expected = string.Concat(r.Arr);
            Assert.Equal(expected, r.ServerText);
        }

        // Sanity: at least one row where concatenation is non-trivial
        Assert.Contains(rows, r => r.Arr.Length >= 2);
    }

    [Theory]
    [InlineData(",")]
    [InlineData(" | ")]
    [InlineData("::")]
    public async Task Test_ArrayToString_WithSeparator(string sep)
    {
        var rows = await northwind.Context.OrderItems
            .LoadWith(item => item.Product)
            .GroupBy(i => i.OrderId)
            .Select(g => new
            {
                OrderId = g.Key,
                Arr = g.ArrayAggregate(i => i.Product.ProductName).ToValue(),
            })
            .Select(x => new
            {
                x.OrderId,
                x.Arr,
                ServerText = x.Arr.ArrayToString(sep), // ARRAY_TO_STRING(arr, sep)
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(rows);

        foreach (var r in rows)
        {
            var expected = string.Join(sep, r.Arr);
            Assert.Equal(expected, r.ServerText);
        }

        Assert.Contains(rows, r => r.Arr.Length >= 2);
    }

    [Fact]
    public async Task Test_ArrayToString_2D_Strings_NoSeparator()
    {
        var rows = await northwind.Context.OrderItems
            .LoadWith(item => item.Product)
            .GroupBy(i => new { i.OrderId, i.ProductId })
            .Select(g => new
            {
                OrderKey = g.Key,
                Arrays = g.ArrayAggregate(i => i.Product.ProductName).ToValue(), // string[]
            })
            .GroupBy(x => x.OrderKey.OrderId)
            .Select(g => new
            {
                OrderId = g.Key,
                TwoDim = g.ArrayAggregate(x => x.Arrays).ToValue(), // string[][]
            })
            .Select(x => new
            {
                x.OrderId,
                x.TwoDim,
                ServerText = x.TwoDim.ArrayFlatten().ArrayToString(), // join with no separator
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(rows);

        foreach (var r in rows)
        {
            var flattened = r.TwoDim.SelectMany(inner => inner).ToArray();
            var expected = string.Concat(flattened);
            Assert.Equal(expected, r.ServerText);
        }

        Assert.Contains(rows, r => r.TwoDim.Length > 1 && r.TwoDim.Any(inner => inner.Length > 0));
    }

    [Theory]
    [InlineData(",")]
    [InlineData(" | ")]
    [InlineData("::")]
    public async Task Test_ArrayToString_2D_Strings_WithSeparator(string sep)
    {
        var rows = await northwind.Context.OrderItems
            .LoadWith(item => item.Product)
            .GroupBy(i => new { i.OrderId, i.ProductId })
            .Select(g => new
            {
                OrderKey = g.Key,
                Arrays = g.ArrayAggregate(i => i.Product.ProductName).ToValue(), // string[]
            })
            .GroupBy(x => x.OrderKey.OrderId)
            .Select(g => new
            {
                OrderId = g.Key,
                TwoDim = g.ArrayAggregate(x => x.Arrays).ToValue(), // string[][]
            })
            .Select(x => new
            {
                x.OrderId,
                x.TwoDim,
                ServerText = x.TwoDim.ArrayFlatten().ArrayToString(sep), // join with separator
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(rows);

        foreach (var r in rows)
        {
            var flattened = r.TwoDim.SelectMany(inner => inner).ToArray();
            var expected = string.Join(sep, flattened);
            Assert.Equal(expected, r.ServerText);
        }
    }

    [Fact]
    public async Task Test_ArrayToString_NoSeparator_Ints()
    {
        var rows = await northwind.Context.OrderItems
            .GroupBy(i => i.OrderId)
            .Select(g => new
            {
                OrderId = g.Key,
                Arr = g.ArrayAggregate(i => i.Quantity).ToValue(), // int[]
            })
            .Select(x => new
            {
                x.OrderId,
                x.Arr,
                ServerText = x.Arr.ArrayToString(), // no separator
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(rows);

        foreach (var r in rows)
        {
            var expected = string.Join("", r.Arr.Select(v => v.ToString(CultureInfo.InvariantCulture)));
            Assert.Equal(expected, r.ServerText);
        }

        Assert.Contains(rows, r => r.Arr.Length >= 2);
    }

    [Theory]
    [InlineData(",")]
    [InlineData(" / ")]
    [InlineData("|")]
    public async Task Test_ArrayToString_WithSeparator_Ints(string sep)
    {
        var rows = await northwind.Context.OrderItems
            .GroupBy(i => i.OrderId)
            .Select(g => new
            {
                OrderId = g.Key,
                Arr = g.ArrayAggregate(i => i.Quantity).ToValue(), // int[]
            })
            .Select(x => new
            {
                x.OrderId,
                x.Arr,
                ServerText = x.Arr.ArrayToString(sep),
            })
            .ToListAsync(token: TestContext.Current.CancellationToken);

        Assert.NotEmpty(rows);

        foreach (var r in rows)
        {
            var expected = string.Join(sep, r.Arr.Select(v => v.ToString(CultureInfo.InvariantCulture)));
            Assert.Equal(expected, r.ServerText);
        }

        Assert.Contains(rows, r => r.Arr.Length >= 2);
    }

    #endregion // ToString

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
