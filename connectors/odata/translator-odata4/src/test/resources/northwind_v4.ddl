SET NAMESPACE 'http://www.jboss.org/teiiddesigner/ext/odata/2012' AS teiid_odata;

CREATE FOREIGN TABLE Categories (
	CategoryID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	CategoryName string(15) NOT NULL,
	Description string(2147483647),
	Picture varbinary(2147483647) OPTIONS (NATIVE_TYPE 'Edm.Binary'),
	PRIMARY KEY(CategoryID)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Category', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE CustomerDemographics (
	CustomerTypeID string(10) NOT NULL,
	CustomerDesc string(2147483647),
	PRIMARY KEY(CustomerTypeID)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.CustomerDemographic', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Customers (
	CustomerID string(5) NOT NULL,
	CompanyName string(40) NOT NULL,
	ContactName string(30),
	ContactTitle string(30),
	Address string(60),
	City string(15),
	Region string(15),
	PostalCode string(10),
	Country string(15),
	Phone string(24),
	Fax string(24),
	PRIMARY KEY(CustomerID)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Customer', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Employees (
	EmployeeID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	LastName string(20) NOT NULL,
	FirstName string(10) NOT NULL,
	Title string(30),
	TitleOfCourtesy string(25),
	BirthDate timestamp OPTIONS (NATIVE_TYPE 'Edm.DateTimeOffset'),
	HireDate timestamp OPTIONS (NATIVE_TYPE 'Edm.DateTimeOffset'),
	Address string(60),
	City string(15),
	Region string(15),
	PostalCode string(10),
	Country string(15),
	HomePhone string(24),
	Extension string(4),
	Photo varbinary(2147483647) OPTIONS (NATIVE_TYPE 'Edm.Binary'),
	Notes string(2147483647),
	ReportsTo integer OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	PhotoPath string(255),
	PRIMARY KEY(EmployeeID),
	CONSTRAINT Employees_Employee1 FOREIGN KEY(ReportsTo) REFERENCES northwind.Employees_Employee1 (EmployeeID)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Employee', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Order_Details (
	OrderID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	ProductID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	UnitPrice bigdecimal(19,4) NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Decimal'),
	Quantity short NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int16'),
	Discount float NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Single'),
	PRIMARY KEY(OrderID, ProductID),
	CONSTRAINT Order_Details_Order FOREIGN KEY(OrderID) REFERENCES northwind.Orders (OrderID),
	CONSTRAINT Order_Details_Product FOREIGN KEY(ProductID) REFERENCES northwind.Products (ProductID)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Order_Detail', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Orders (
	OrderID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	CustomerID string(5),
	EmployeeID integer OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	OrderDate timestamp OPTIONS (NATIVE_TYPE 'Edm.DateTimeOffset'),
	RequiredDate timestamp OPTIONS (NATIVE_TYPE 'Edm.DateTimeOffset'),
	ShippedDate timestamp OPTIONS (NATIVE_TYPE 'Edm.DateTimeOffset'),
	ShipVia integer OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	Freight bigdecimal(19,4) OPTIONS (NATIVE_TYPE 'Edm.Decimal'),
	ShipName string(40),
	ShipAddress string(60),
	ShipCity string(15),
	ShipRegion string(15),
	ShipPostalCode string(10),
	ShipCountry string(15),
	PRIMARY KEY(OrderID),
	CONSTRAINT Orders_Customer FOREIGN KEY(CustomerID) REFERENCES northwind.Customers (CustomerID),
	CONSTRAINT Orders_Employee FOREIGN KEY(EmployeeID) REFERENCES northwind.Employees (EmployeeID),
	CONSTRAINT Orders_Shipper FOREIGN KEY(ShipVia) REFERENCES northwind.Shippers (ShipperID)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Order', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Products (
	ProductID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	ProductName string(40) NOT NULL,
	SupplierID integer OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	CategoryID integer OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	QuantityPerUnit string(20),
	UnitPrice bigdecimal(19,4) OPTIONS (NATIVE_TYPE 'Edm.Decimal'),
	UnitsInStock short OPTIONS (NATIVE_TYPE 'Edm.Int16'),
	UnitsOnOrder short OPTIONS (NATIVE_TYPE 'Edm.Int16'),
	ReorderLevel short OPTIONS (NATIVE_TYPE 'Edm.Int16'),
	Discontinued boolean NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Boolean'),
	PRIMARY KEY(ProductID),
	CONSTRAINT Products_Category FOREIGN KEY(CategoryID) REFERENCES northwind.Categories (CategoryID),
	CONSTRAINT Products_Supplier FOREIGN KEY(SupplierID) REFERENCES northwind.Suppliers (SupplierID)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Product', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Regions (
	RegionID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	RegionDescription string(50) NOT NULL,
	PRIMARY KEY(RegionID)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Region', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Shippers (
	ShipperID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	CompanyName string(40) NOT NULL,
	Phone string(24),
	PRIMARY KEY(ShipperID)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Shipper', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Suppliers (
	SupplierID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	CompanyName string(40) NOT NULL,
	ContactName string(30),
	ContactTitle string(30),
	Address string(60),
	City string(15),
	Region string(15),
	PostalCode string(10),
	Country string(15),
	Phone string(24),
	Fax string(24),
	HomePage string(2147483647),
	PRIMARY KEY(SupplierID)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Supplier', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Territories (
	TerritoryID string(20) NOT NULL,
	TerritoryDescription string(50) NOT NULL,
	RegionID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	PRIMARY KEY(TerritoryID),
	CONSTRAINT Territories_Region FOREIGN KEY(RegionID) REFERENCES northwind.Regions (RegionID)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Territory', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Alphabetical_list_of_products (
	CategoryName string(15) NOT NULL,
	Discontinued boolean NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Boolean'),
	ProductID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	ProductName string(40) NOT NULL,
	SupplierID integer OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	CategoryID integer OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	QuantityPerUnit string(20),
	UnitPrice bigdecimal(19,4) OPTIONS (NATIVE_TYPE 'Edm.Decimal'),
	UnitsInStock short OPTIONS (NATIVE_TYPE 'Edm.Int16'),
	UnitsOnOrder short OPTIONS (NATIVE_TYPE 'Edm.Int16'),
	ReorderLevel short OPTIONS (NATIVE_TYPE 'Edm.Int16'),
	PRIMARY KEY(CategoryName, Discontinued, ProductID, ProductName)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Alphabetical_list_of_product', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Category_Sales_for_1997 (
	CategoryName string(15) NOT NULL,
	CategorySales bigdecimal(19,4) OPTIONS (NATIVE_TYPE 'Edm.Decimal'),
	PRIMARY KEY(CategoryName)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Category_Sales_for_1997', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Current_Product_Lists (
	ProductID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	ProductName string(40) NOT NULL,
	PRIMARY KEY(ProductID, ProductName)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Current_Product_List', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Customer_and_Suppliers_by_Cities (
	CompanyName string(40) NOT NULL,
	Relationship string(9) NOT NULL,
	City string(15),
	ContactName string(30),
	PRIMARY KEY(CompanyName, Relationship)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Customer_and_Suppliers_by_City', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Invoices (
	CustomerName string(40) NOT NULL,
	Discount float NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Single'),
	OrderID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	ProductID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	ProductName string(40) NOT NULL,
	Quantity short NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int16'),
	Salesperson string(31) NOT NULL,
	ShipperName string(40) NOT NULL,
	UnitPrice bigdecimal(19,4) NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Decimal'),
	ShipName string(40),
	ShipAddress string(60),
	ShipCity string(15),
	ShipRegion string(15),
	ShipPostalCode string(10),
	ShipCountry string(15),
	CustomerID string(5),
	Address string(60),
	City string(15),
	Region string(15),
	PostalCode string(10),
	Country string(15),
	OrderDate timestamp OPTIONS (NATIVE_TYPE 'Edm.DateTimeOffset'),
	RequiredDate timestamp OPTIONS (NATIVE_TYPE 'Edm.DateTimeOffset'),
	ShippedDate timestamp OPTIONS (NATIVE_TYPE 'Edm.DateTimeOffset'),
	ExtendedPrice bigdecimal(19,4) OPTIONS (NATIVE_TYPE 'Edm.Decimal'),
	Freight bigdecimal(19,4) OPTIONS (NATIVE_TYPE 'Edm.Decimal'),
	PRIMARY KEY(CustomerName, Discount, OrderID, ProductID, ProductName, Quantity, Salesperson, ShipperName, UnitPrice)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Invoice', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Order_Details_Extendeds (
	Discount float NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Single'),
	OrderID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	ProductID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	ProductName string(40) NOT NULL,
	Quantity short NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int16'),
	UnitPrice bigdecimal(19,4) NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Decimal'),
	ExtendedPrice bigdecimal(19,4) OPTIONS (NATIVE_TYPE 'Edm.Decimal'),
	PRIMARY KEY(Discount, OrderID, ProductID, ProductName, Quantity, UnitPrice)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Order_Details_Extended', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Order_Subtotals (
	OrderID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	Subtotal bigdecimal(19,4) OPTIONS (NATIVE_TYPE 'Edm.Decimal'),
	PRIMARY KEY(OrderID)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Order_Subtotal', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Orders_Qries (
	CompanyName string(40) NOT NULL,
	OrderID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	CustomerID string(5),
	EmployeeID integer OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	OrderDate timestamp OPTIONS (NATIVE_TYPE 'Edm.DateTimeOffset'),
	RequiredDate timestamp OPTIONS (NATIVE_TYPE 'Edm.DateTimeOffset'),
	ShippedDate timestamp OPTIONS (NATIVE_TYPE 'Edm.DateTimeOffset'),
	ShipVia integer OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	Freight bigdecimal(19,4) OPTIONS (NATIVE_TYPE 'Edm.Decimal'),
	ShipName string(40),
	ShipAddress string(60),
	ShipCity string(15),
	ShipRegion string(15),
	ShipPostalCode string(10),
	ShipCountry string(15),
	Address string(60),
	City string(15),
	Region string(15),
	PostalCode string(10),
	Country string(15),
	PRIMARY KEY(CompanyName, OrderID)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Orders_Qry', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Product_Sales_for_1997 (
	CategoryName string(15) NOT NULL,
	ProductName string(40) NOT NULL,
	ProductSales bigdecimal(19,4) OPTIONS (NATIVE_TYPE 'Edm.Decimal'),
	PRIMARY KEY(CategoryName, ProductName)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Product_Sales_for_1997', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Products_Above_Average_Prices (
	ProductName string(40) NOT NULL,
	UnitPrice bigdecimal(19,4) OPTIONS (NATIVE_TYPE 'Edm.Decimal'),
	PRIMARY KEY(ProductName)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Products_Above_Average_Price', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Products_by_Categories (
	CategoryName string(15) NOT NULL,
	Discontinued boolean NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Boolean'),
	ProductName string(40) NOT NULL,
	QuantityPerUnit string(20),
	UnitsInStock short OPTIONS (NATIVE_TYPE 'Edm.Int16'),
	PRIMARY KEY(CategoryName, Discontinued, ProductName)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Products_by_Category', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Sales_by_Categories (
	CategoryID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	CategoryName string(15) NOT NULL,
	ProductName string(40) NOT NULL,
	ProductSales bigdecimal(19,4) OPTIONS (NATIVE_TYPE 'Edm.Decimal'),
	PRIMARY KEY(CategoryID, CategoryName, ProductName)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Sales_by_Category', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Sales_Totals_by_Amounts (
	CompanyName string(40) NOT NULL,
	OrderID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	SaleAmount bigdecimal(19,4) OPTIONS (NATIVE_TYPE 'Edm.Decimal'),
	ShippedDate timestamp OPTIONS (NATIVE_TYPE 'Edm.DateTimeOffset'),
	PRIMARY KEY(CompanyName, OrderID)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Sales_Totals_by_Amount', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Summary_of_Sales_by_Quarters (
	OrderID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	ShippedDate timestamp OPTIONS (NATIVE_TYPE 'Edm.DateTimeOffset'),
	Subtotal bigdecimal(19,4) OPTIONS (NATIVE_TYPE 'Edm.Decimal'),
	PRIMARY KEY(OrderID)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Summary_of_Sales_by_Quarter', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Summary_of_Sales_by_Years (
	OrderID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	ShippedDate timestamp OPTIONS (NATIVE_TYPE 'Edm.DateTimeOffset'),
	Subtotal bigdecimal(19,4) OPTIONS (NATIVE_TYPE 'Edm.Decimal'),
	PRIMARY KEY(OrderID)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Summary_of_Sales_by_Year', "teiid_odata:Type" 'ENTITY_COLLECTION');

CREATE FOREIGN TABLE Employees_Employees1 (
	EmployeeID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	LastName string(20) NOT NULL,
	FirstName string(10) NOT NULL,
	Title string(30),
	TitleOfCourtesy string(25),
	BirthDate timestamp OPTIONS (NATIVE_TYPE 'Edm.DateTimeOffset'),
	HireDate timestamp OPTIONS (NATIVE_TYPE 'Edm.DateTimeOffset'),
	Address string(60),
	City string(15),
	Region string(15),
	PostalCode string(10),
	Country string(15),
	HomePhone string(24),
	Extension string(4),
	Photo varbinary(2147483647) OPTIONS (NATIVE_TYPE 'Edm.Binary'),
	Notes string(2147483647),
	ReportsTo integer OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	PhotoPath string(255),
	Employees_EmployeeID integer OPTIONS (UPDATABLE FALSE, "teiid_odata:PSEUDO" 'true'),
	PRIMARY KEY(EmployeeID),
	FOREIGN KEY(Employees_EmployeeID) REFERENCES northwind.Employees (EmployeeID)
) OPTIONS (NAMEINSOURCE 'Employees1', UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Employee', "teiid_odata:Type" 'NAVIGATION_COLLECTION');

CREATE FOREIGN TABLE Employees_Employee1 (
	EmployeeID integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	LastName string(20) NOT NULL,
	FirstName string(10) NOT NULL,
	Title string(30),
	TitleOfCourtesy string(25),
	BirthDate timestamp OPTIONS (NATIVE_TYPE 'Edm.DateTimeOffset'),
	HireDate timestamp OPTIONS (NATIVE_TYPE 'Edm.DateTimeOffset'),
	Address string(60),
	City string(15),
	Region string(15),
	PostalCode string(10),
	Country string(15),
	HomePhone string(24),
	Extension string(4),
	Photo varbinary(2147483647) OPTIONS (NATIVE_TYPE 'Edm.Binary'),
	Notes string(2147483647),
	ReportsTo integer OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	PhotoPath string(255),
	Employees_EmployeeID integer OPTIONS (UPDATABLE FALSE, "teiid_odata:PSEUDO" 'true'),
	PRIMARY KEY(EmployeeID),
	FOREIGN KEY(Employees_EmployeeID) REFERENCES northwind.Employees (EmployeeID)
) OPTIONS (NAMEINSOURCE 'Employee1', UPDATABLE TRUE, "teiid_odata:NameInSchema" 'NorthwindModel.Employee', "teiid_odata:Type" 'NAVIGATION');