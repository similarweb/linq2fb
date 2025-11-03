using LinqToDB;
using LinqToDB.Configuration;
using LinqToDB.Data;

namespace Similarweb.LinqToDB.Firebolt.Tests.Northwind;

public class NorthwindContext(LinqToDbConnectionOptions connectionOptions) : DataConnection(connectionOptions)
{
    public ITable<Customer> Customers => GetTable<Customer>();
    public ITable<Order> Orders => GetTable<Order>();
    public ITable<OrderItem> OrderItems => GetTable<OrderItem>();
    public ITable<Product> Products => GetTable<Product>();
    public ITable<Supplier> Suppliers => GetTable<Supplier>();
}
