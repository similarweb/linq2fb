using LinqToDB.Mapping;

namespace Similarweb.LinqToDB.Firebolt.Tests.Northwind;

[Table("products")]
public class Product
{
    [Column("id")]
    public required int Id { get; init; }
    [Column("product_name")]
    public required string ProductName { get; init; }
    [Column("supplier_id")]
    public required int SupplierId { get; init; }
    [Column("unit_price")]
    public required decimal? UnitPrice { get; init; }
    [Column("package")]
    public required string? Package { get; init; }
    [Column("is_discontinued")]
    public required bool IsDiscontinued { get; init; }

    [Association(ThisKey = nameof(SupplierId), OtherKey = nameof(Northwind.Supplier.Id))]
    public Supplier? Supplier { get; init; }
    [Association(ThisKey = nameof(Id), OtherKey = nameof(OrderItem.ProductId))]
    public ICollection<OrderItem> OrderItems { get; init; } = [];
}
