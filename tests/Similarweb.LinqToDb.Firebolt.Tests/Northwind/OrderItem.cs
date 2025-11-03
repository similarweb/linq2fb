using LinqToDB.Mapping;

namespace Similarweb.LinqToDB.Firebolt.Tests.Northwind;

[Table("order_items")]
public class OrderItem
{
    [Column("id")]
    public required int Id { get; init; }
    [Column("order_id")]
    public required int OrderId { get; init; }
    [Column("product_id")]
    public required int ProductId { get; init; }
    [Column("unit_price")]
    public required decimal UnitPrice { get; init; }
    [Column("quantity")]
    public required int Quantity { get; init; }

    [Association(ThisKey = nameof(OrderId), OtherKey = nameof(Northwind.Order.Id))]
    public required Order Order { get; init; }
    [Association(ThisKey = nameof(ProductId), OtherKey = nameof(Northwind.Product.Id))]
    public required Product Product { get; init; }
}
