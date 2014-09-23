/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.olingo;

import static org.junit.Assert.assertEquals;

import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.core.uri.parser.Parser;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.query.sql.symbol.GroupSymbol;

@SuppressWarnings("nls")
public class TestODataExpressionToSQLVisitor {

	private void te(String in, String expected) throws Exception {
	    Parser parser = new Parser();
	    UriInfo uriInfo = parser.parseUri("http://localhost:8080/odata4/vdb/model/table?$filter="+in, Mockito.mock(Edm.class));
		assertEquals(expected, null);
	}
	

	

	

	
	/*

	
	private void testSelect(String expected, String tableName, String filter, String select, String orderby, int top, String navProp, OEntityKey entityKey) throws Exception {
		TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl")), "northwind", "nw");		
		ODataSQLBuilder visitor = new ODataSQLBuilder(metadata.getMetadataStore(), false);
		QueryInfo qi = buildQueryInfo(filter, select, orderby, top);
		Query query = visitor.selectString(tableName, qi, entityKey, navProp, false);
		assertEquals(expected, query.toString()); // comma inserted by visitor
	}
	
	private void testSelectCountStar(String expected, String tableName, String filter, String select, String orderby, int top) throws Exception {
		TransformationMetadata metadata = RealMetadataFactory.example1();
		ODataSQLBuilder visitor = new ODataSQLBuilder(metadata.getMetadataStore(), false);
		QueryInfo qi = buildQueryInfo(filter, select, orderby, top);
		Query query = visitor.selectString(tableName, qi, null, null, true);
		assertEquals(expected, query.toString()); // comma inserted by visitor
	}	

	private QueryInfo buildQueryInfo(String filter, String select,
			String orderby, int top) {
		Builder b = QueryInfo.newBuilder();
		if (filter != null) {
			b.setFilter((BoolCommonExpression) ExpressionParser.parse(filter)).build();
		}
		if (select != null) {
			List<EntitySimpleProperty> esp = new ArrayList<EntitySimpleProperty>();
			StringTokenizer st = new StringTokenizer(select, ",");
			while (st.hasMoreTokens()) {
				esp.add(Expression.simpleProperty(st.nextToken().trim()));
			}
			b.setSelect(esp);
		}
		if (orderby != null) {
			b.setOrderBy(ExpressionParser.parseOrderBy(orderby));
		}

		if (top != -1) {
			b.setTop(top);
		}

		QueryInfo qi = b.build();
		return qi;
	}
	
	@Test(expected=NotFoundException.class)
	public void testBadSelect() throws Exception {
		testSelect("SELECT e1, x5 FROM pm1.g1", "nw.Shippers", "e1 ne 'foo'",
				"ShipperID,x5", "ShipperID desc, CompanyName", -1, null, null);
	}
	
	@Test
	public void testSelectCount() throws Exception {
		testSelectCountStar(
				"SELECT COUNT(*) FROM pm1.g1 AS g0 WHERE (g0.e1 >= 10) AND (g0.e1 < 20)",
				"pm1.g1", "e1 ge 10 and e1 lt 20", "e1,x5", "e1 desc, e2", 10);
	}
	
	@Test
	public void testSelectQuery() throws Exception {
		testSelect(
				"SELECT g0.ShipperID, g0.CompanyName, g0.Phone FROM nw.Shippers AS g0 WHERE (g0.ShipperID >= 10) AND (g0.ShipperID < 20) ORDER BY g0.ShipperID",
				"nw.Shippers", "ShipperID ge 10 and ShipperID lt 20", null,
				null, -1, null, null);
		testSelect(
				"SELECT g0.CompanyName, g0.Phone, g0.ShipperID FROM nw.Shippers AS g0 WHERE g0.CompanyName <> 'foo' ORDER BY g0.ShipperID",
				"nw.Shippers", "CompanyName ne 'foo'", "CompanyName,Phone",
				null, -1, null, null);
		testSelect(
				"SELECT g0.CompanyName, g0.Phone, g0.ShipperID FROM nw.Shippers AS g0 WHERE g0.CompanyName <> 'foo' ORDER BY g0.CompanyName DESC, g0.Phone",
				"nw.Shippers", "CompanyName ne 'foo'", "CompanyName,Phone",
				"CompanyName desc, Phone", -1, null, null);
		testSelect(
				"SELECT g0.ShipperID, g0.CompanyName, g0.Phone FROM nw.Shippers AS g0 ORDER BY g0.ShipperID",
				"nw.Shippers", null, null, null, 10, null, null);
	}	
	
	@Test
	public void testNavigationalQuery() throws Exception {
		testSelect(
				"SELECT g1.EmployeeID, g1.OrderID, g1.CustomerID, g1.ShipVia FROM nw.Customers AS g0 INNER JOIN Orders AS g1 ON g0.CustomerID = g1.CustomerID ORDER BY g1.OrderID",
				"nw.Customers", null, "EmployeeID", null, -1, "nw.Orders", null);
		testSelect(
				"SELECT g2.UnitPrice, g2.OrderID, g2.ProductID FROM (nw.Customers AS g0 INNER JOIN Orders AS g1 ON g0.CustomerID = g1.CustomerID) INNER JOIN OrderDetails AS g2 ON g1.OrderID = g2.OrderID WHERE g1.OrderID = 12 ORDER BY g2.OrderID, g2.ProductID",
				"nw.Customers", null, "UnitPrice", null, -1,
				"nw.Orders(12)/nw.OrderDetails", null);
		testSelect(
				"SELECT g2.UnitPrice, g2.OrderID, g2.ProductID FROM (nw.Customers AS g0 INNER JOIN Orders AS g1 ON g0.CustomerID = g1.CustomerID) INNER JOIN OrderDetails AS g2 ON g1.OrderID = g2.OrderID WHERE (g0.CustomerID = 33) AND (g1.OrderID = 12) ORDER BY g2.OrderID, g2.ProductID",
				"nw.Customers", null, "UnitPrice", null, -1,
				"nw.Orders(12)/nw.OrderDetails", OEntityKey.create(33));
	}
	
	
	@Test
	public void testEntityKeyQuery() throws Exception {
		testSelect("SELECT g0.ShipperID, g0.CompanyName, g0.Phone FROM nw.Shippers AS g0 WHERE g0.ShipperID = 12 ORDER BY g0.ShipperID", "nw.Shippers", null, null, null, -1, null, OEntityKey.create(12));
	}	
	
	@Test
	public void testFilterBasedAssosiation() throws Exception {
		testSelect(
				"SELECT g0.OrderID, g0.CustomerID, g0.EmployeeID, g0.ShipVia FROM nw.Orders AS g0 INNER JOIN Customers AS g1 ON g0.CustomerID = g1.CustomerID WHERE g1.ContactName = 'Fred' ORDER BY g0.OrderID",
				"nw.Orders", "Customers/ContactName eq 'Fred'", "OrderID",
				null, -1, null, null);
		
		testSelect(
				"SELECT g0.ContactName, g0.CustomerID FROM nw.Customers AS g0 INNER JOIN Orders AS g1 ON g0.CustomerID = g1.CustomerID WHERE g1.OrderID = 1 ORDER BY g0.CustomerID",
				"nw.Customers", "Orders/OrderID eq 1", "ContactName",
				null, -1, null, null);
		
	}	
	
	@Test
	public void testOrderByWithCriteria() throws Exception {
		testSelect(
				"SELECT g0.ShipperID, g0.CompanyName, g0.Phone FROM nw.Shippers AS g0 WHERE g0.ShipperID = 12 ORDER BY g0.ShipperID DESC",
				"nw.Shippers", null, null, "ShipperID eq 12 desc", -1, null,
				null);
	}	
	
	@Test
	public void testAny() throws Exception {	
		testSelect(
				"SELECT DISTINCT g0.OrderID, g0.CustomerID, g0.EmployeeID, g0.ShipVia FROM nw.Orders AS g0 INNER JOIN OrderDetails AS ol ON g0.OrderID = ol.OrderID WHERE ol.Quantity > 10 ORDER BY g0.OrderID",
				"nw.Orders", "OrderDetails/any(ol: ol/Quantity gt 10)",
				"OrderID", null, -1, null, null);
	}	
		
	@Test
	public void testAll() throws Exception {		
		testSelect(
				"SELECT g0.OrderID, g0.CustomerID, g0.EmployeeID, g0.ShipVia FROM nw.Orders AS g0 WHERE 10 < ALL (SELECT ol.Quantity FROM OrderDetails AS ol WHERE g0.OrderID = ol.OrderID) ORDER BY g0.OrderID",
				"nw.Orders", "OrderDetails/all(ol: ol/Quantity gt 10)",
				"OrderID", null, -1, null, null);
	}		
	
	@Test
	public void testMultiEntitykey() throws Exception {
		OEntityKey key = OEntityKey.parse("(11044)");
		testSelect("SELECT g1.OrderID, g1.ProductID FROM nw.Orders AS g0 INNER JOIN OrderDetails AS g1 ON g0.OrderID = g1.OrderID WHERE (g0.OrderID = 11044) AND ((g1.OrderID = 11044) AND (g1.ProductID = 62)) ORDER BY g1.OrderID, g1.ProductID",
				"nw.Orders", null,
				"OrderID", null, -1, "nw.OrderDetails(OrderID=11044L,ProductID=62L)", key);		
	}
	*/
}


