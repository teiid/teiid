CREATE FOREIGN TABLE  Categories (
  CategoryID integer NOT NULL auto_increment,
  CategoryName varchar(15),
  Description varchar(4000),
  Picture varchar(40),
  PRIMARY KEY  (CategoryID),
  UNIQUE (CategoryName)
);


CREATE FOREIGN TABLE Suppliers (
  SupplierID integer NOT NULL auto_increment,
  CompanyName varchar(40),
  ContactName varchar(30),
  ContactTitle varchar(30),
  Address varchar(60),
  City varchar(15),
  Region varchar(15),
  PostalCode varchar(10),
  Country varchar(15),
  Phone varchar(24),
  Fax varchar(24),
  HomePage varchar(4000),
  PRIMARY KEY  (SupplierID)
);


CREATE FOREIGN TABLE Shippers (
  ShipperID integer NOT NULL auto_increment,
  CompanyName varchar(40),
  Phone varchar(24),
  PRIMARY KEY (ShipperID)
);

CREATE FOREIGN TABLE Customers (
  CustomerID varchar(5) NOT NULL default '',
  CompanyName varchar(40),
  ContactName varchar(30),
  ContactTitle varchar(30),
  Address varchar(60),
  City varchar(15),
  Region varchar(15),
  PostalCode varchar(10),
  Country varchar(15),
  Phone varchar(24),
  Fax varchar(24),
  PRIMARY KEY  (CustomerID)
);

CREATE FOREIGN TABLE Employees (
  EmployeeID integer NOT NULL auto_increment,
  LastName varchar(20),
  FirstName varchar(10),
  Title varchar(30),
  TitleOfCourtesy varchar(25),
  BirthDate date,
  HireDate date,
  Address varchar(60),
  City varchar(15),
  Region varchar(15),
  PostalCode varchar(10),
  Country varchar(15),
  HomePhone varchar(24),
  Extension varchar(4),
  Photo varchar(40),
  Notes varchar(4000),
  ReportsTo integer,
  PRIMARY KEY  (EmployeeID)
);

CREATE FOREIGN TABLE Products (
  ProductID integer NOT NULL auto_increment,
  ProductName varchar(40),
  SupplierID integer NOT NULL,
  CategoryID integer NOT NULL,
  QuantityPerUnit varchar(20),
  UnitPrice float default '0',
  UnitsInStock integer default '0',
  UnitsOnOrder integer default '0',
  ReorderLevel integer default '0',
  Discontinued integer default '0',
  PRIMARY KEY  (ProductID),
  FOREIGN KEY (CategoryID) REFERENCES Categories (CategoryID),
  FOREIGN KEY (SupplierID) REFERENCES Suppliers (SupplierID)
);

CREATE FOREIGN TABLE Orders (
  OrderID integer NOT NULL auto_increment,
  CustomerID varchar(5),
  EmployeeID integer,
  OrderDate date,
  RequiredDate date,
  ShippedDate date,
  ShipVia integer,
  Freight float default '0',
  ShipName varchar(40),
  ShipAddress varchar(60),
  ShipCity varchar(15),
  ShipRegion varchar(15),
  ShipPostalCode varchar(10),
  ShipCountry varchar(15),
  PRIMARY KEY  (OrderID),
  FOREIGN KEY (CustomerID) REFERENCES Customers (CustomerID),
  FOREIGN KEY (EmployeeID) REFERENCES Employees (EmployeeID),
  FOREIGN KEY (ShipVia) REFERENCES Shippers (ShipperID)
);


CREATE FOREIGN TABLE OrderDetails (
  odID integer,
  OrderID integer NOT NULL,
  ProductID integer NOT NULL,
  UnitPrice float default '0',
  Quantity integer default '1',
  Discount float default '0',
  FOREIGN KEY (OrderID) REFERENCES Orders (OrderID),
  FOREIGN KEY (ProductID) REFERENCES Products (ProductID),
  PRIMARY KEY (OrderID,ProductID)
);

CREATE FOREIGN PROCEDURE getUnit() RETURNS varchar(10);
CREATE FOREIGN PROCEDURE getCustomers(OUT p1 boolean, p2 varchar, INOUT p3 decimal) RETURNS (r1 varchar(20), r2 decimal)
