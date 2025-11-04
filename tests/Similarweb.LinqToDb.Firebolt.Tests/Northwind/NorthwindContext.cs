using LinqToDB;
using LinqToDB.Configuration;
using LinqToDB.Data;

namespace Similarweb.LinqToDB.Firebolt.Tests.Northwind;

public class NorthwindContext(DataOptions dataOptions) : DataConnection(dataOptions)
{
    public ITable<Customer> Customers => this.GetTable<Customer>();
    public ITable<Order> Orders => this.GetTable<Order>();
    public ITable<OrderItem> OrderItems => this.GetTable<OrderItem>();
    public ITable<Product> Products => this.GetTable<Product>();
    public ITable<Supplier> Suppliers => this.GetTable<Supplier>();
}
