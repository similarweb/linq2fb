using LinqToDB.Mapping;

namespace Similarweb.LinqToDB.Firebolt.Tests.Northwind;

[Table("Orders")]
public class Order
{
    [Column("Id")]
    public required int Id { get; init; }
    [Column("OrderDate")]
    public required DateTime OrderDate { get; init; }
    [Column("OrderNumber")]
    public required string? OrderNumber { get; init; }
    [Column("CustomerId")]
    public required int CustomerId { get; init; }
    [Column("TotalAmount")]
    public required decimal? TotalAmount { get; init; }

    [Association(ThisKey = nameof(CustomerId), OtherKey = nameof(Northwind.Customer.Id))]
    public required Customer Customer { get; init; }
    [Association(ThisKey = nameof(Id), OtherKey = nameof(OrderItem.OrderId))]
    public required ICollection<OrderItem> OrderItems { get; init; }
}
