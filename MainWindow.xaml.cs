using System.Windows;

namespace Neo4JDatabaseAvaloniaLinux
{
	/// <summary>
	/// Interaction logic for MainWindow.xaml
	/// </summary>
	public partial class MainWindow : Window
	{
		public MainWindow()
		{
			InitializeComponent();
			FillDataGrid();
		}

		async void FillDataGrid()
		{
			string uri = "neo4j+s://f2c9fb11.databases.neo4j.io";
			string user = "neo4j";
			string password = "ZdYOocpzGYam6I5nhVWqCmR8dxwX-wOZqDidB61BSrc";
			DBDriver driver = new(uri, user, password);
			var table = await driver.ReadShipmentsItems();
			mainDataGrid.ItemsSource = table.DefaultView;
		}
	}
}
