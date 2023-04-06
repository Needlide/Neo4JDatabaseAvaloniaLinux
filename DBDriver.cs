using Neo4j.Driver;
using System;
using System.Data;
using System.Threading.Tasks;
using System.Windows;

namespace Neo4JDatabaseAvaloniaLinux
{
	internal class DBDriver : IDisposable
	{
		private bool _disposed = false;
		private readonly IDriver _driver;
		public DBDriver(string uri, string user, string password)
		{
			_driver = GraphDatabase.Driver(uri, AuthTokens.Basic(user, password));
		}

		#region CRUD
		public async Task CreateCustomer(int customer_id,string first_name, string last_name, string email)
		{
			string query = "CREATE (:customer {" +
				"customer_id: $customer_id" +
				"first_name: $first_name," +
				"last_name: $last_name," +
				"email: $email" +
				"})";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { customer_id, first_name, last_name, email });
					return await cursor.SingleAsync();
				});
			}
			catch(Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task CreateOrder(int customer_id, int order_id, int country_id, string status, ZonedDateTime created_at)
		{
			string query = "CREATE (:orders {" +
				"customer_id: $customer_id," +
				"order_id: $order_id," +
				"country_id: $country_id" +
				"status: $status" +
				"created_at: $created_at" +
				"})";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { customer_id, order_id, country_id, status, created_at });
					return await cursor.SingleAsync();
				});
			}
			catch(Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task CreateOrderItem(int order_item_id, int product_id, int order_id, int quantity, float price)
		{
			string query = "CREATE (:order_item {" +
				"order_item_id: $order_item_id," +
				"product_id: $product_id," +
				"order_id: $order_id" +
				"quantity: $quantity" +
				"price: $price" +
				"})";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { order_item_id, product_id, order_id, quantity, price });
					return await cursor.SingleAsync();
				});
			}
			catch(Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task CreateCart(int cart_id, int customer_id, int quantity)
		{
			string query = "CREATE (:cart {" +
				"cart_id: $cart_id," +
				"customer_id: $customer_id," +
				"quantity: $quantity" +
				"})";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { cart_id, customer_id, quantity });
					return await cursor.SingleAsync();
				});
			}
			catch(Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task CreateCartItem(int cart_item_id, int cart_id, int product_id, int quantity)
		{
			string query = "CREATE (:cart_item {" +
				"cart_item_id: $cart_item_id," +
				"cart_id: $cart_id," +
				"product_id: $product_id" +
				"quantity: $quantity" +
				"})";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { cart_item_id, cart_id, product_id, quantity });
					return await cursor.SingleAsync();
				});
			}
			catch(Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task CreateInvoice(int invoice_id, int order_id, int total_amount, ZonedDateTime created_at)
		{
			string query = "CREATE (:invoice {" +
				"invoice_id: $invoice_id," +
				"order_id: $order_id," +
				"total_amount: $total_amount" +
				"created_at: $created_at" +
				"})";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { invoice_id, order_id, total_amount, created_at});
					return await cursor.SingleAsync();
				});
			}
			catch(Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task CreatePayment(int payment_id, int invoice_id, int amount, ZonedDateTime payment_date)
		{
			string query = "CREATE (:payment {" +
				"payment_id: $payment_id," +
				"invoice_id: $invoice_id," +
				"amount: $amount" +
				"payment_date: $payment_date" +
				"})";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { payment_id, invoice_id, amount, payment_date });
					return await cursor.SingleAsync();
				});
			}
			catch(Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task CreateProduct(int product_id, string name, string description, float price, int group_id)
		{
			string query = "CREATE (:product {" +
				"product_id: $product_id," +
				"name: $name," +
				"description: $description" +
				"price: $price" +
				"group_id: $group_id" +
				"})";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { product_id, name, description, price, group_id });
					return await cursor.SingleAsync();
				});
			}
			catch(Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task CreateProductGroup(int group_id, string name)
		{
			string query = "CREATE (:product_group {" +
				"group_id: $group_id," +
				"name: $name," +
				"})";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { group_id, name });
					return await cursor.SingleAsync();
				});
			}
			catch(Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task CreateShipment(int shipment_id, int order_id, ZonedDateTime created_at)
		{
			string query = "CREATE (:shipment {" +
				"shipment_id: $shipment_id," +
				"order_id: $order_id," +
				"created_at: $created_at" +
				"})";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { shipment_id, order_id, created_at });
					return await cursor.SingleAsync();
				});
			}
			catch(Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task CreateShipmentItem(int shipment_item_id, int shipment_id, int order_id)
		{
			string query = "CREATE (:shipment_item {" +
				"order_item_id: $shipment_item_id," +
				"shipment_id: $shipment_id," +
				"order_id: $order_id" +
				"})";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { shipment_item_id, shipment_id, order_id });
					return await cursor.SingleAsync();
				});
			}
			catch(Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task<DataTable> ReadAll()
		{
			string query = "MATCH (n) UNWIND keys(n) as property RETURN labels(n) as node, property, n[property] as value;";
			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var result = await session.ExecuteReadAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query);
					return await cursor.ToListAsync();
				});

				var table = new DataTable();
				table.Columns.Add("Node", typeof(string));
				table.Columns.Add("Property", typeof(string));
				table.Columns.Add("Value", typeof(string));

				foreach (var record in result)
				{
					var node = record["node"].As<System.Collections.Generic.List<object>>()[0].As<string>();
					var property = record["property"].As<string>();
					object value;
					ZonedDateTime dateTime;
					try
					{
						dateTime = record["value"].As<ZonedDateTime>();
						value = dateTime.Day.ToString() + '/' + dateTime.Month.ToString() + '/' + dateTime.Year.ToString();
					}
					catch (InvalidCastException)
					{
						value = record["value"].As<string>();
					}

					table.Rows.Add(node, property, value);
				}

				return table;
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
				return new DataTable();
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task<DataTable> ReadCustomers()
		{
			string query = "MATCH (c:customer) RETURN c.first_name, c.last_name, c.email";
			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var result = await session.ExecuteReadAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query);
					return await cursor.ToListAsync();
				});

				var table = new DataTable();
				table.Columns.Add("First Name", typeof(string));
				table.Columns.Add("Last Name", typeof(string));
				table.Columns.Add("Email", typeof(string));

				foreach (var record in result)
				{
					var firstName = record["c.first_name"].As<string>();
					var lastName = record["c.last_name"].As<string>();
					var email = record["c.email"].As<string>();
					table.Rows.Add(firstName, lastName, email);
				}

				return table;
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
				return new DataTable();
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task<DataTable> ReadOrders()
		{
			string query = "MATCH (o:orders) RETURN o.created_at, o.customer_id, o.order_id, o.country_id, o.status";
			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var result = await session.ExecuteReadAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query);
					return await cursor.ToListAsync();
				});

				var table = new DataTable();
				table.Columns.Add("Customer ID", typeof(string));
				table.Columns.Add("Order ID", typeof(string));
				table.Columns.Add("Country ID", typeof(string));
				table.Columns.Add("Created", typeof(string));
				table.Columns.Add("Status", typeof(string));

				foreach (var record in result)
				{
					var customerId = record["o.customer_id"].As<string>();
					var orderId = record["o.order_id"].As<string>();
					var countryId = record["o.country_id"].As<string>();
					var createdAtZoned = record["o.created_at"].As<ZonedDateTime>();
					var status = record["o.status"].As<string>();

					var createdAt = createdAtZoned.Day.ToString() + '/' + createdAtZoned.Month.ToString() + '/' + createdAtZoned.Year.ToString();
					table.Rows.Add(customerId, orderId, countryId, createdAt, status);
				}

				return table;
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
				return new DataTable();
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task<DataTable> ReadOrdersItems()
		{
			string query = "MATCH (oi:order_item) RETURN oi.order_item_id, oi.order_id, oi.product_id, oi.price, oi.quantity";
			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var result = await session.ExecuteReadAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query);
					return await cursor.ToListAsync();
				});

				var table = new DataTable();
				table.Columns.Add("Order item ID", typeof(string));
				table.Columns.Add("Order ID", typeof(string));
				table.Columns.Add("Product ID", typeof(string));
				table.Columns.Add("Price", typeof(string));
				table.Columns.Add("Quantity", typeof(string));

				foreach (var record in result)
				{
					var orderItemId = record["oi.order_item_id"].As<string>();
					var orderId = record["oi.order_id"].As<string>();
					var productId = record["oi.product_id"].As<string>();
					var price = record["oi.price"].As<string>();
					var quantity = record["oi.quantity"].As<string>();

					table.Rows.Add(orderItemId, orderId, productId, price, quantity);
				}

				return table;
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
				return new DataTable();
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task<DataTable> ReadCarts()
		{
			string query = "MATCH (c:cart) RETURN c.cart_id, c.customer_id, c.quantity";
			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var result = await session.ExecuteReadAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query);
					return await cursor.ToListAsync();
				});

				var table = new DataTable();
				table.Columns.Add("Cart ID", typeof(string));
				table.Columns.Add("Customer ID", typeof(string));
				table.Columns.Add("Quantity", typeof(string));

				foreach (var record in result)
				{
					var cartId = record["c.cart_id"].As<string>();
					var customerId = record["c.customer_id"].As<string>();
					var quantity = record["c.quantity"].As<string>();

					table.Rows.Add(cartId, customerId, quantity);
				}

				return table;
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
				return new DataTable();
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task<DataTable> ReadCartsItems()
		{
			string query = "MATCH (ci:cart_item) RETURN ci.cart_id, ci.quantity, ci.product_id, ci.cart_item_id";
			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var result = await session.ExecuteReadAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query);
					return await cursor.ToListAsync();
				});

				var table = new DataTable();
				table.Columns.Add("Cart item ID", typeof(string));
				table.Columns.Add("Cart ID", typeof(string));
				table.Columns.Add("Product ID", typeof(string));
				table.Columns.Add("Quantity", typeof(string));

				foreach (var record in result)
				{
					var cartItemId = record["ci.cart_item_id"].As<string>();
					var cartId = record["ci.cart_id"].As<string>();
					var productId = record["ci.product_id"].As<string>();
					var quantity = record["ci.quantity"].As<string>();

					table.Rows.Add(cartItemId, cartId, productId, quantity);
				}

				return table;
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
				return new DataTable();
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task<DataTable> ReadInvoices()
		{
			string query = "MATCH (i:invoice) RETURN i.invoice_id, i.order_id, i.total_amount, i.created_at";
			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var result = await session.ExecuteReadAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query);
					return await cursor.ToListAsync();
				});

				var table = new DataTable();
				table.Columns.Add("Invoice ID", typeof(string));
				table.Columns.Add("Order ID", typeof(string));
				table.Columns.Add("Total amount", typeof(string));
				table.Columns.Add("Created", typeof(string));

				foreach (var record in result)
				{
					var invoiceId = record["i.invoice_id"].As<string>();
					var orderId = record["i.order_id"].As<string>();
					var totalAmount = record["i.total_amount"].As<string>();
					var createdAtZoned = record["i.created_at"].As<ZonedDateTime>();

					var createdAt = createdAtZoned.Day.ToString() + '/' + createdAtZoned.Month.ToString() + '/' + createdAtZoned.Year.ToString();
					table.Rows.Add(invoiceId, orderId, totalAmount, createdAt);
				}

				return table;
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
				return new DataTable();
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task<DataTable> ReadPayments()
		{
			string query = "MATCH (p:payment) RETURN p.payment_id, p.invoice_id, p.amount, p.payment_date";
			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var result = await session.ExecuteReadAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query);
					return await cursor.ToListAsync();
				});

				var table = new DataTable();
				table.Columns.Add("Payment ID", typeof(string));
				table.Columns.Add("Invoice ID", typeof(string));
				table.Columns.Add("Amount", typeof(string));
				table.Columns.Add("Payment date", typeof(string));

				foreach (var record in result)
				{
					var paymentId = record["p.payment_id"].As<string>();
					var invoiceId = record["p.invoice_id"].As<string>();
					var amount = record["p.amount"].As<string>();
					var paymentDate = record["p.payment_date"].As<ZonedDateTime>();

					var createdAt = paymentDate.Day.ToString() + '/' + paymentDate.Month.ToString() + '/' + paymentDate.Year.ToString();
					table.Rows.Add(paymentId, invoiceId, amount, createdAt);
				}

				return table;
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
				return new DataTable();
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task<DataTable> ReadProducts()
		{
			string query = "MATCH (p:product) RETURN p.product_id, p.name, p.description, p.price, p.group_id";
			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var result = await session.ExecuteReadAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query);
					return await cursor.ToListAsync();
				});

				var table = new DataTable();
				table.Columns.Add("Product ID", typeof(string));
				table.Columns.Add("Name", typeof(string));
				table.Columns.Add("Description", typeof(string));
				table.Columns.Add("Price", typeof(string));
				table.Columns.Add("Group ID", typeof(string));

				foreach (var record in result)
				{
					var productId = record["p.product_id"].As<string>();
					var name = record["p.name"].As<string>();
					var description = record["p.description"].As<string>();
					var price = record["p.price"].As<string>();
					var groupId = record["p.group_id"].As<string>();

					table.Rows.Add(productId, name, description, price, groupId);
				}

				return table;
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
				return new DataTable();
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task<DataTable> ReadProductsGroups()
		{
			string query = "MATCH (pg:product_group) RETURN pg.group_id, pg.name";
			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var result = await session.ExecuteReadAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query);
					return await cursor.ToListAsync();
				});

				var table = new DataTable();
				table.Columns.Add("Group ID", typeof(string));
				table.Columns.Add("Name", typeof(string));

				foreach (var record in result)
				{
					var groupId = record["pg.group_id"].As<string>();
					var name = record["pg.name"].As<string>();

					table.Rows.Add(groupId, name);
				}

				return table;
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
				return new DataTable();
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task<DataTable> ReadShipments()
		{
			string query = "MATCH (s:shipment) RETURN s.shipment_id, s.order_id, s.created_at";
			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var result = await session.ExecuteReadAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query);
					return await cursor.ToListAsync();
				});

				var table = new DataTable();
				table.Columns.Add("Shipment ID", typeof(string));
				table.Columns.Add("Order ID", typeof(string));
				table.Columns.Add("Created", typeof(string));

				foreach (var record in result)
				{
					var shipmentId = record["s.shipment_id"].As<string>();
					var orderId = record["s.order_id"].As<string>();
					var createdAtZoned = record["s.created_at"].As<ZonedDateTime>();

					var createdAt = createdAtZoned.Day.ToString() + '/' + createdAtZoned.Month.ToString() + '/' + createdAtZoned.Year.ToString();
					table.Rows.Add(shipmentId, orderId, createdAt);
				}

				return table;
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
				return new DataTable();
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task<DataTable> ReadShipmentsItems()
		{
			string query = "MATCH (si:shipment_item) RETURN si.order_item_id, si.shipment_id, si.order_id";
			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var result = await session.ExecuteReadAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query);
					return await cursor.ToListAsync();
				});

				var table = new DataTable();
				table.Columns.Add("Shipment item ID", typeof(string));
				table.Columns.Add("Shipment ID", typeof(string));
				table.Columns.Add("Order ID", typeof(string));

				foreach (var record in result)
				{
					var shipmentItemId = record["si.order_item_id"].As<string>();
					var shipmentId = record["si.shipment_id"].As<string>();
					var orderId = record["si.order_id"].As<string>();

					table.Rows.Add(shipmentItemId, shipmentId, orderId);
				}

				return table;
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
				return new DataTable();
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task UpdateCustomer(int customer_id, string first_name, string last_name, string email)
		{
			string query = "MATCH (c:customer {customer_id: $customer_id})" +
				"SET c.first_name = $first_name," +
				"c.last_name = $last_name," +
				"c.email = $email";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { customer_id, first_name, last_name, email });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task UpdateOrder(int order_id, int customer_id, int country_id, string status, ZonedDateTime created_at)
		{
			string query = "MATCH(o:orders { order_id: $order_id})" +
				"SET o.customer_id = $customer_id," +
				"o.country_id = $country_id," +
				"o.status = $status," +
				"o.created_at = $created_at";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { order_id, customer_id, country_id, status, created_at });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task UpdateOrderItem(int order_item_id, int product_id, int order_id, int quantity, float price)
		{
			string query = "MATCH(o:order_item { order_item_id: $order_item_id})" +
				"SET o.product_id = $product_id," +
				"o.order_id = $order_id," +
				"o.quantity = $quantity," +
				"o.price = $price";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { order_item_id, product_id, order_id, quantity, price });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task UpdateCart(int cart_id, int customer_id, int quantity)
		{
			string query = "MATCH(c:cart { cart_id: $cart_id})" +
				"SET c.customer_id = $customer_id," +
				"c.quantity = $quantity";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { cart_id, customer_id, quantity });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task UpdateCartItem(int cart_item_id, int cart_id, int product_id, int quantity)
		{
			string query = "MATCH(c:cart_item { cart_item_id: $cart_item_id})" +
				"SET c.cart_id = $cart_id," +
				"c.product_id = $product_id," +
				"c.quantity = $quantity";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { cart_item_id, cart_id, product_id, quantity });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task UpdateInvoice(int invoice_id, int order_id, int total_amount, ZonedDateTime created_at)
		{
			string query = "MATCH(i:invoice { invoice_id: $invoice_id})" +
				"SET i.order_id = $order_id," +
				"i.total_amount = $total_amount," +
				"i.created_at = $created_at";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { invoice_id, order_id, total_amount, created_at });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task UpdatePayment(int payment_id, int invoice_id, int amount, ZonedDateTime payment_date)
		{
			string query = "MATCH(p:payment { payment_id: $payment_id})" +
				"SET p.invoice_id = $invoice_id," +
				"p.amount_id = $amount_id," +
				"p.payment_date = $payment_date";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { payment_id, invoice_id, amount, payment_date });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task UpdateProduct(int product_id, string name, string description, float price, int group_id)
		{
			string query = "MATCH(p:product { product_id: $product_id})" +
				"SET p.name = $name," +
				"p.description = $description," +
				"p.price = $price," +
				"p.group_id = $group_id";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { product_id, name, description, price, group_id });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task UpdateProductGroup(int group_id, string name)
		{
			string query = "MATCH(p:product_group { group_id: $group_id})" +
				"SET p.name = $name";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { group_id, name });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task UpdateShipment(int shipment_id, int order_id, ZonedDateTime created_at)
		{
			string query = "MATCH(s:shipment { shipment_id: $shipment_id})" +
				"SET s.order_id_id = $order_id," +
				"o.created_at = $created_at";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { shipment_id, order_id, created_at });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task UpdateShipmentItem(int shipment_item_id, int shipment_id, int order_id)
		{
			string query = "MATCH(s:shipment_item { shipment_item_id: $shipment_item_id})" +
				"SET s.shipment_id = $shipment_id," +
				"s.order_id = $order_id";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { shipment_item_id, shipment_id, order_id });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task DeleteCustomer(int customer_id)
		{
			string query = "MATCH (c:customer {customer_id: $customer_id}) DETACH DELETE c";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { customer_id });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task DeleteOrder(int order_id)
		{
			string query = "MATCH(o:orders { order_id: $order_id}) DETACH DELETE o";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { order_id });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task DeleteOrderItem(int order_item_id)
		{
			string query = "MATCH(o:order_item { order_item_id: $order_item_id}) DETACH DELETE o";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { order_item_id });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task DeleteCart(int cart_id)
		{
			string query = "MATCH(c:cart { cart_id: $cart_id}) DETACH DELETE c";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { cart_id });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task DeleteCartItem(int cart_item_id)
		{
			string query = "MATCH(c:cart_item { cart_item_id: $cart_item_id}) DETACH DELETE c";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { cart_item_id });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task DeleteInvoice(int invoice_id)
		{
			string query = "MATCH(i:invoice { invoice_id: $invoice_id}) DETACH DELETE i";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { invoice_id });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task DeletePayment(int payment_id)
		{
			string query = "MATCH(p:payment { payment_id: $payment_id}) DETACH DELETE p";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { payment_id });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task DeleteProduct(int product_id)
		{
			string query = "MATCH(p:product { product_id: $product_id}) DETACH DELETE p";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { product_id });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task DeleteProductGroup(int group_id)
		{
			string query = "MATCH(p:product_group { group_id: $group_id}) DETACH DELETE p";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { group_id });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task DeleteShipment(int shipment_id)
		{
			string query = "MATCH(s:shipment { shipment_id: $shipment_id}) DETACH DELETE s";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { shipment_id});
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		public async Task DeleteShipmentItem(int shipment_item_id)
		{
			string query = "MATCH(s:shipment_item { shipment_item_id: $shipment_item_id}) DETACH DELETE s";

			await using var session = _driver.AsyncSession(configBuilder => configBuilder.WithDatabase("neo4j"));
			try
			{
				var writeResults = await session.ExecuteWriteAsync(async tx =>
				{
					var cursor = await tx.RunAsync(query, new { shipment_item_id });
					return await cursor.SingleAsync();
				});
			}
			catch (Neo4jException ex)
			{
				MessageBox.Show(ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
			}
			finally
			{
				await session.CloseAsync();
			}
		}
		#endregion

		#region Disposing
		~DBDriver() => Dispose(false);

		protected virtual void Dispose(bool disposing)
		{
			if (_disposed)
				return;

			if (disposing)
			{
				_driver?.Dispose();
			}

			_disposed = true;
		}

		public void Dispose()
		{
			// Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
			Dispose(disposing: true);
			GC.SuppressFinalize(this);
		}
		#endregion
	}
}
