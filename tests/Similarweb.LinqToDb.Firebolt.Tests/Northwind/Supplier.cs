using LinqToDB.Mapping;

namespace Similarweb.LinqToDB.Firebolt.Tests.Northwind;

[Table("suppliers")]
public class Supplier
{
    [Column("id")]
    public required int Id { get; init; }
    [Column("public_id")]
    public required Guid PublicId { get; init; }
    [Column("company_name")]
    public required string CompanyName { get; init; }
    [Column("contact_name")]
    public required string? ContactName { get; init; }
    [Column("contact_title")]
    public required string? ContactTitle { get; init; }
    [Column("city")]
    public required string? City { get; init; }
    [Column("country")]
    public required string? Country { get; init; }
    [Column("phone")]
    public required string? Phone { get; init; }
    [Column("fax")]
    public required string? Fax { get; init; }

    [Association(ThisKey = nameof(Id), OtherKey = nameof(Product.SupplierId))]
    public ICollection<Product>? Products { get; init; }
}
