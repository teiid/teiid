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
package org.teiid.odata;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.junit.Test;
import org.mockito.Mockito;
import org.odata4j.core.OEntityKey;
import org.odata4j.exceptions.NotFoundException;
import org.odata4j.expression.BoolCommonExpression;
import org.odata4j.expression.CommonExpression;
import org.odata4j.expression.EntitySimpleProperty;
import org.odata4j.expression.Expression;
import org.odata4j.expression.ExpressionParser;
import org.odata4j.expression.OrderByExpression;
import org.odata4j.producer.QueryInfo;
import org.odata4j.producer.QueryInfo.Builder;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestODataSQLStringVisitor {

	private void te(String in, String expected) {
		CommonExpression expr = ExpressionParser.parse(in);
		ODataSQLBuilder visitor = new ODataSQLBuilder(Mockito.mock(MetadataStore.class), false);
		visitor.visitNode(expr);
		assertEquals(expected, visitor.getExpression().toString());
	}
	
	@Test
	public void testAnd() {
		te("a eq 1 and a eq 2", "(a = 1) AND (a = 2)");
		te("(x add 4) eq 3", "(x + 4) = 3");
		te("((x add y) sub z) mod x eq 0", "MOD(((x + y) - z), x) = 0");
	}
	
	@Test
	public void testEq() {
		te("a eq 1", "a = 1");
		te("a eq 4.5f", "a = 4.5");
		te("a eq 4.5d", "a = 4.5");
		te("a eq null", "a IS NULL");
		te("a eq 'foo'", "a = 'foo'");
		te("a eq true", "a = TRUE");
		te("a eq false", "a = FALSE");
		te("a eq time'PT13H20M'", "a = {t'13:20:00'}");
		te("a eq datetime'2008-10-13T00:00:00'", "a = {ts'2008-10-13 00:00:00.0'}");
		//te("a eq datetimeoffset'2008-10-13T00:00:00-04:00'", "a = {ts '2008-10-13T00:00:00.000'}");
	}
	
	@Test
	public void testNegate() {
		te("- a", "(-1 * a)");
		te("-4.5f", "-4.5");
	}	
	
	@Test
	public void testCast() {
		te("cast('foo', 'Edm.String')", "CONVERT('foo', string)");
		te("cast('foo', 'Edm.Int32')", "CONVERT('foo', integer)");
	}
	
	@Test
	public void testConcat() {
		te("concat('foo', 'bar')", "CONCAT2('foo', 'bar')");
	}
	
	@Test
	public void testEndsWith() {
		te("endswith(x, 'foo')", "ENDSWITH(x, 'foo') = TRUE");
	}	
	
	@Test
	public void testIndexOf() {
		te("indexof(x, 'foo')", "LOCATE('foo', x)");
	}	
	
	@Test
	public void testLength() {
		te("length(x)", "LENGTH(x)");
	}	
	
	@Test
	public void testOperator() {
		te("-1", "-1");
		te("not x", "NOT (x)");
		te("x mul y", "(x * y)");
		te("x div y", "(x / y)");
		te("x add y", "(x + y)");
		te("x sub y", "(x - y)");
		te("x mod y", "MOD(x, y)");
	}	
	
	@Test
	public void testComparisions() {
		te("x gt y", "x > y");
		te("x lt y", "x < y");
		te("x ge y", "x >= y");
		te("x le y", "x <= y");
		te("x eq y", "x = y");
		te("x ne y", "x <> y");
		te("x eq null", "x IS NULL");
		te("x ne null", "x IS NOT NULL");
	}	

	@Test
	public void testStringMethods() {
		te("replace(x, y, z)", "REPLACE(x, y, z)");
		
		te("substring(x, 'foo')", "SUBSTRING(x, 'foo')");
		te("substring(x, 'foo', 'bar')", "SUBSTRING(x, 'foo', 'bar')");
		te("tolower(x)", "LCASE(x)");
		te("toupper(x)", "UCASE(x)");
		te("trim('x')", "TRIM(' ' FROM 'x')");
		te("trim(x) ne 'foo' and toupper(y) eq 'bar'", "(TRIM(' ' FROM x) <> 'foo') AND (UCASE(y) = 'bar')");
	}	
	
	
	@Test
	public void testStartsWith() {
		te("startswith(x, 'foo')", "LOCATE('foo', x, 1) = 1");
	}
	
	@Test
	public void testTimeMethods() {
		te("year(x)", "YEAR(x)");
		te("year(datetime'2008-10-13T00:00:00')", "YEAR({ts'2008-10-13 00:00:00.0'})");
		te("month(x)", "MONTH(x)");
		te("day(x)", "DAYOFMONTH(x)");
		te("hour(x)", "HOUR(x)");
		te("minute(x)", "MINUTE(x)");
		te("second(x)", "SECOND(x)");
	}	
	
	@Test
	public void testRoundMethods() {
		te("round(x)", "ROUND(x, 0)");
		te("floor(x)", "FLOOR(x)");
		te("ceiling(x)", "CEILING(x)");
	}	
	
	@Test
	public void testOrderby() {
		List<OrderByExpression> expr = ExpressionParser.parseOrderBy("b desc, a");
		ODataSQLBuilder visitor = new ODataSQLBuilder(Mockito.mock(MetadataStore.class), false);
		visitor.visitNode(expr.get(0));
		assertEquals("ORDER BY b DESC", visitor.getOrderBy().toString());
		visitor.visitNode(expr.get(1));
		assertEquals("ORDER BY b DESC, a", visitor.getOrderBy().toString()); // comma inserted by visitor
	}
	
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
}


