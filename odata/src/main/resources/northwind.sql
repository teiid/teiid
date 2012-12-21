CREATE TABLE IF NOT EXISTS `categories` (
  `CategoryID` int(11) NOT NULL auto_increment,
  `CategoryName` varchar(15) default NULL,
  `Description` text,
  `Picture` varchar(40) default NULL,
  PRIMARY KEY  (`CategoryID`),
 UNIQUE CategoryName (`CategoryName`)
) ENGINE=InnoDB ;

--
-- Table structure for table `suppliers`
--

CREATE TABLE IF NOT EXISTS `suppliers` (
  `SupplierID` int(11) NOT NULL auto_increment,
  `CompanyName` varchar(40) default NULL,
  `ContactName` varchar(30) default NULL,
  `ContactTitle` varchar(30) default NULL,
  `Address` varchar(60) default NULL,
  `City` varchar(15) default NULL,
  `Region` varchar(15) default NULL,
  `PostalCode` varchar(10) default NULL,
  `Country` varchar(15) default NULL,
  `Phone` varchar(24) default NULL,
  `Fax` varchar(24) default NULL,
  `HomePage` text,
  PRIMARY KEY  (`SupplierID`)
) ENGINE=InnoDB ;

--
-- Table structure for table `shippers`
--

CREATE TABLE IF NOT EXISTS `shippers` (
  `ShipperID` int(11) NOT NULL auto_increment,
  `CompanyName` varchar(40) default NULL,
  `Phone` varchar(24) default NULL,
  PRIMARY KEY  (`ShipperID`)
) ENGINE=InnoDB;
--
-- Table structure for table `customers`
--

CREATE TABLE IF NOT EXISTS `customers` (
  `CustomerID` varchar(5) NOT NULL default '',
  `CompanyName` varchar(40) default NULL,
  `ContactName` varchar(30) default NULL,
  `ContactTitle` varchar(30) default NULL,
  `Address` varchar(60) default NULL,
  `City` varchar(15) default NULL,
  `Region` varchar(15) default NULL,
  `PostalCode` varchar(10) default NULL,
  `Country` varchar(15) default NULL,
  `Phone` varchar(24) default NULL,
  `Fax` varchar(24) default NULL,
  PRIMARY KEY  (`CustomerID`)
) ENGINE=InnoDB;


--
-- Table structure for table `employees`
--

CREATE TABLE IF NOT EXISTS `employees` (
  `EmployeeID` int(11) NOT NULL auto_increment,
  `LastName` varchar(20) default NULL,
  `FirstName` varchar(10) default NULL,
  `Title` varchar(30) default NULL,
  `TitleOfCourtesy` varchar(25) default NULL,
  `BirthDate` date default NULL,
  `HireDate` date default NULL,
  `Address` varchar(60) default NULL,
  `City` varchar(15) default NULL,
  `Region` varchar(15) default NULL,
  `PostalCode` varchar(10) default NULL,
  `Country` varchar(15) default NULL,
  `HomePhone` varchar(24) default NULL,
  `Extension` varchar(4) default NULL,
  `Photo` varchar(40) default NULL,
  `Notes` text,
  `ReportsTo` int(11) default NULL,
  PRIMARY KEY  (`EmployeeID`)
) ENGINE=InnoDB ;

--
-- Table structure for table `products`
--

CREATE TABLE IF NOT EXISTS `products` (
  `ProductID` int(11) NOT NULL auto_increment,
  `ProductName` varchar(40) default NULL,
  `SupplierID` int(11) NOT NULL,
  `CategoryID` int(11) NOT NULL,
  `QuantityPerUnit` varchar(20) default NULL,
  `UnitPrice` float(1,0) default '0',
  `UnitsInStock` smallint(6) default '0',
  `UnitsOnOrder` smallint(6) default '0',
  `ReorderLevel` smallint(6) default '0',
  `Discontinued` tinyint(1) default '0',
  PRIMARY KEY  (`ProductID`),
  FOREIGN KEY (`CategoryID`) REFERENCES categories (`CategoryID`),
  FOREIGN KEY (`SupplierID`) REFERENCES suppliers (`SupplierID`)
) ENGINE=InnoDB;
--
-- Table structure for table `orders`
--

CREATE TABLE IF NOT EXISTS `orders` (
  `OrderID` int(11) NOT NULL auto_increment,
  `CustomerID` varchar(5) default NULL,
  `EmployeeID` int(11) default NULL,
  `OrderDate` date default NULL,
  `RequiredDate` date default NULL,
  `ShippedDate` date default NULL,
  `ShipVia` int(11) default NULL,
  `Freight` float(1,0) default '0',
  `ShipName` varchar(40) default NULL,
  `ShipAddress` varchar(60) default NULL,
  `ShipCity` varchar(15) default NULL,
  `ShipRegion` varchar(15) default NULL,
  `ShipPostalCode` varchar(10) default NULL,
  `ShipCountry` varchar(15) default NULL,
  PRIMARY KEY  (`OrderID`),
  FOREIGN KEY (`CustomerID`) REFERENCES customers (`CustomerID`),
  FOREIGN KEY (`EmployeeID`) REFERENCES employees (`EmployeeID`),
  FOREIGN KEY (`ShipVia`) REFERENCES shippers (`ShipperID`)
) ENGINE=InnoDB ;



--
-- Table structure for table `order_details`
--

CREATE TABLE IF NOT EXISTS `order_details` (
  `odID` int(10),
  `OrderID` int(11) NOT NULL,
  `ProductID` int(11) NOT NULL,
  `UnitPrice` float(1,0) default '0',
  `Quantity` smallint(6) default '1',
  `Discount` float(1,0) default '0',
   FOREIGN KEY (`OrderID`) REFERENCES orders (`OrderID`),
   FOREIGN KEY (`ProductID`) REFERENCES products (`ProductID`),
  PRIMARY KEY (`OrderID`,`ProductID`)
) ENGINE=InnoDB ;
