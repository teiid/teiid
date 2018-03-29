CREATE FOREIGN TABLE  Categories (
  CategoryID integer NOT NULL auto_increment,
  CategoryName varchar(15),
  Description varchar(4000),
  Picture varchar(40),
  PRIMARY KEY  (CategoryID),
  UNIQUE (CategoryName)
) OPTIONS(UPDATABLE 'TRUE', "teiid_mongo:EMBEDDABLE" 'TRUE');

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
)OPTIONS(UPDATABLE 'TRUE', "teiid_mongo:EMBEDDABLE" 'TRUE');

CREATE FOREIGN TABLE Shippers (
  ShipperID integer NOT NULL auto_increment,
  CompanyName varchar(40),
  Phone varchar(24),
  PRIMARY KEY (ShipperID)
)OPTIONS(UPDATABLE 'TRUE', "teiid_mongo:EMBEDDABLE" 'TRUE');

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
)OPTIONS(UPDATABLE 'TRUE');

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
) OPTIONS(UPDATABLE 'TRUE');

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
) OPTIONS(UPDATABLE 'TRUE');

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
) OPTIONS(UPDATABLE 'TRUE');

CREATE FOREIGN TABLE OrderDetails (
  odID integer NOT NULL,
  ProductID integer NOT NULL,
  UnitPrice double default '0',
  Quantity integer default '1',
  Discount float default '0',
  FOREIGN KEY (odID) REFERENCES Orders (OrderID),
  FOREIGN KEY (ProductID) REFERENCES Products (ProductID),
  PRIMARY KEY (odID,ProductID)
) OPTIONS ("teiid_mongo:MERGE" 'Orders', UPDATABLE 'TRUE');

CREATE FOREIGN TABLE users (
    id integer NOT NULL PRIMARY KEY,
    user_id varchar(30),
    age integer,
    status varchar(1),
    FOREIGN KEY (user_id) REFERENCES Customers (CustomerID)
);

CREATE FOREIGN TABLE G1 (
    e1 integer NOT NULL,
    e2 integer NOT NULL,
    e3 integer,
    PRIMARY KEY (e1, e2)
) OPTIONS(UPDATABLE 'TRUE');

CREATE FOREIGN TABLE G2 (
    e1 integer NOT NULL,
    e2 integer NOT NULL,
    e3 integer,
    FOREIGN KEY (e1, e2) REFERENCES G1 (e1, e2)
) OPTIONS(UPDATABLE 'TRUE');

CREATE FOREIGN TABLE T1 (
    e1 integer NOT NULL,
    e2 integer NOT NULL PRIMARY KEY,
    e3 integer,
    FOREIGN KEY (e1) REFERENCES T2 (t2e1)
) OPTIONS(UPDATABLE 'TRUE');

CREATE FOREIGN TABLE T2 (
    t2e1 integer NOT NULL PRIMARY KEY,
    t2e2 integer NOT NULL,
    t2e3 integer,
    FOREIGN KEY (t2e1) REFERENCES T3 (t3e1)
) OPTIONS(UPDATABLE 'TRUE', "teiid_mongo:EMBEDDABLE" 'TRUE');

CREATE FOREIGN TABLE T3 (
    t3e1 integer NOT NULL PRIMARY KEY,
    t3e2 integer NOT NULL,
    t3e3 integer
) OPTIONS(UPDATABLE 'TRUE', "teiid_mongo:EMBEDDABLE" 'TRUE');

CREATE FOREIGN TABLE payment (
    payment_id integer NOT NULL PRIMARY KEY,
    rental_id integer,
    amount decimal(9,2),
    FOREIGN KEY (rental_id) REFERENCES rental (rental_id)
) OPTIONS(UPDATABLE 'TRUE', "teiid_mongo:MERGE" 'rental');

CREATE FOREIGN TABLE rental (
    rental_id integer PRIMARY KEY,
    amount decimal(9,2) NOT NULL,
    customer_id integer,
    FOREIGN KEY (customer_id) REFERENCES customer (customer_id)
) OPTIONS(UPDATABLE 'TRUE', "teiid_mongo:MERGE" 'customer');

CREATE FOREIGN TABLE customer (
    customer_id integer PRIMARY KEY,
    name varchar(25)
) OPTIONS(UPDATABLE 'TRUE');

CREATE FOREIGN TABLE address (
    cust_id integer PRIMARY KEY,
    street varchar(25),
    zip varchar(25),
    FOREIGN KEY (cust_id) REFERENCES customer (customer_id)
) OPTIONS ( UPDATABLE 'TRUE', "teiid_mongo:MERGE" 'customer');

CREATE FOREIGN TABLE Notes (
    CustomerId integer,
    PostDate timestamp,
    Comment varchar(50),
    FOREIGN KEY (CustomerId) REFERENCES Customer (customer_id)
 ) OPTIONS(UPDATABLE 'TRUE', "teiid_mongo:MERGE" 'Customer');
 
 CREATE FOREIGN TABLE ArrayTest (
    id integer,
    column1 object[] OPTIONS (SEARCHABLE 'Unsearchable')
 ) OPTIONS(UPDATABLE 'TRUE');
 
CREATE FOREIGN TABLE N1 (
    e1 integer NOT NULL,
    e2 integer NOT NULL,
    e3 integer,
    PRIMARY KEY (e1)
) OPTIONS(UPDATABLE 'TRUE');

CREATE FOREIGN TABLE N2 (
    e1 integer NOT NULL PRIMARY KEY,
    e2 integer NOT NULL,
    e3 integer,
    FOREIGN KEY (e1) REFERENCES N1 (e1)
) OPTIONS(UPDATABLE 'TRUE', "teiid_mongo:MERGE" 'N1');

CREATE FOREIGN TABLE N3 (
    e1 integer NOT NULL PRIMARY KEY,
    e2 integer NOT NULL,
    e3 integer,
    FOREIGN KEY (e1) REFERENCES N2 (e1)
) OPTIONS(UPDATABLE 'TRUE', "teiid_mongo:MERGE" 'N2');

CREATE FOREIGN TABLE N4 (
    e1 integer NOT NULL PRIMARY KEY,
    e2 integer NOT NULL,
    e3 integer,
    FOREIGN KEY (e2) REFERENCES N2 (e1)
) OPTIONS(UPDATABLE 'TRUE', "teiid_mongo:MERGE" 'N2');

CREATE FOREIGN TABLE N5 (
    e1 integer NOT NULL PRIMARY KEY,
    e2 integer NOT NULL,
    e3 integer,
    FOREIGN KEY (e2) REFERENCES N1 (e1)
) OPTIONS(UPDATABLE 'TRUE', "teiid_mongo:MERGE" 'N1');

CREATE FOREIGN TABLE N6 (
    e1 integer NOT NULL PRIMARY KEY,
    e2 integer NOT NULL,
    e3 integer,
    FOREIGN KEY (e1) REFERENCES N5 (e1)
) OPTIONS(UPDATABLE 'TRUE', "teiid_mongo:MERGE" 'N5');

CREATE FOREIGN TABLE N7 (
    e1 integer NOT NULL PRIMARY KEY,
    e2 integer NOT NULL,
    e3 integer,
    FOREIGN KEY (e2) REFERENCES N5 (e1)
) OPTIONS(UPDATABLE 'TRUE', "teiid_mongo:MERGE" 'N5');

CREATE FOREIGN TABLE TIME_TEST (
    e1 integer NOT NULL,
    e2 timestamp,
    PRIMARY KEY (e1)
) OPTIONS(UPDATABLE 'TRUE');