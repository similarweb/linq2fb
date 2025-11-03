using LinqToDB.Mapping;

namespace Similarweb.LinqToDB.Firebolt.Tests.Northwind;

[Table("Customers")]
public class Customer
{
    [Column("Id")]
    public required int Id { get; init; }
    [Column("FirstName")]
    public required string FirstName { get; init; }
    [Column("LastName")]
    public required string LastName { get; init; }
    [Column("City")]
    public required string? City { get; init; }
    [Column("Country")]
    public required string? Country { get; init; }
    [Column("Phone")]
    public required string? Phone { get; init; }

    [Association(ThisKey = nameof(Id), OtherKey = nameof(Order.CustomerId))]
    public required ICollection<Order> Orders { get; init; }
}
