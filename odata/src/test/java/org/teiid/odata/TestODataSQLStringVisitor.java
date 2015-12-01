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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.junit.Test;
import org.mockito.Mockito;
import org.odata4j.core.OEntities;
import org.odata4j.core.OEntity;
import org.odata4j.core.OEntityKey;
import org.odata4j.core.OProperties;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntitySet;
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
import org.teiid.metadata.Table;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestODataSQLStringVisitor {

	private void te(String in, String expected) {
		CommonExpression expr = ExpressionParser.parse(in);
		ODataSQLBuilder visitor = new ODataSQLBuilder(Mockito.mock(MetadataStore.class), false);
		visitor.resultNode = new DocumentNode((Table)null, null);
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
		te("endswith(x, 'foo')", "ENDSWITH('foo', x) = TRUE");
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
		te("substringof(x, 'foo')", "LOCATE(x, 'foo', 1) >= 1");
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
		visitor.resultNode = new DocumentNode((Table) null, null);
		visitor.visitNode(expr.get(0));
		assertEquals("ORDER BY b DESC", visitor.getOrderBy().toString());
		visitor.visitNode(expr.get(1));
		assertEquals("ORDER BY b DESC, a", visitor.getOrderBy().toString()); // comma inserted by visitor
	}
	
	private void testSelect(String expected, String tableName, String filter, String select, String orderby, int top, String navProp, OEntityKey entityKey) throws Exception {
		TransformationMetadata metadata = RealMetadataFactory.fromDDL("northwind", new RealMetadataFactory.DDLHolder("nw", ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl"))), new RealMetadataFactory.DDLHolder("nw1", ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl"))));		
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
				"SELECT g0.* FROM nw.Shippers AS g0 WHERE (g0.ShipperID >= 10) AND (g0.ShipperID < 20) ORDER BY g0.ShipperID",
				"nw.Shippers", "ShipperID ge 10 and ShipperID lt 20", null,
				null, -1, null, null);
		testSelect(
				"SELECT g0.ShipperID, g0.CompanyName, g0.Phone FROM nw.Shippers AS g0 WHERE g0.CompanyName <> 'foo' ORDER BY g0.ShipperID",
				"nw.Shippers", "CompanyName ne 'foo'", "CompanyName,Phone",
				null, -1, null, null);
		testSelect(
				"SELECT g0.ShipperID, g0.CompanyName, g0.Phone FROM nw.Shippers AS g0 WHERE g0.CompanyName <> 'foo' ORDER BY g0.CompanyName DESC, g0.Phone",
				"nw.Shippers", "CompanyName ne 'foo'", "CompanyName,Phone",
				"CompanyName desc, Phone", -1, null, null);
		testSelect(
				"SELECT g0.* FROM nw.Shippers AS g0 ORDER BY g0.ShipperID",
				"nw.Shippers", null, null, null, 10, null, null);
	}	
	
	@Test
	public void testNavigationalQuery() throws Exception {
		testSelect(
				"SELECT g1.OrderID, g1.CustomerID, g1.EmployeeID, g1.ShipVia FROM nw.Customers AS g0 INNER JOIN nw.Orders AS g1 ON g0.CustomerID = g1.CustomerID ORDER BY g1.OrderID",
				"nw.Customers", null, "EmployeeID", null, -1, "nw.Orders", null);
		testSelect(
				"SELECT g2.OrderID, g2.ProductID, g2.UnitPrice FROM (nw.Customers AS g0 INNER JOIN nw.Orders AS g1 ON g0.CustomerID = g1.CustomerID) INNER JOIN nw.OrderDetails AS g2 ON g1.OrderID = g2.OrderID WHERE g1.OrderID = 12 ORDER BY g2.OrderID, g2.ProductID",
				"nw.Customers", null, "UnitPrice", null, -1,
				"nw.Orders(12)/nw.OrderDetails", null);
		testSelect(
				"SELECT g2.OrderID, g2.ProductID, g2.UnitPrice FROM (nw.Customers AS g0 INNER JOIN nw.Orders AS g1 ON g0.CustomerID = g1.CustomerID) INNER JOIN nw.OrderDetails AS g2 ON g1.OrderID = g2.OrderID WHERE (g0.CustomerID = 33) AND (g1.OrderID = 12) ORDER BY g2.OrderID, g2.ProductID",
				"nw.Customers", null, "UnitPrice", null, -1,
				"nw.Orders(12)/nw.OrderDetails", OEntityKey.create(33));
	}
	
	@Test(expected=NotFoundException.class)
	public void testNavigationalQueryAmbiguous() throws Exception {
		testSelect(
				"SELECT g1.EmployeeID, g1.OrderID, g1.CustomerID, g1.ShipVia FROM nw.Customers AS g0 INNER JOIN nw.Orders AS g1 ON g0.CustomerID = g1.CustomerID ORDER BY g1.OrderID",
				"nw.Customers", null, "EmployeeID", null, -1, "Orders", null);
	}
	
	
	@Test
	public void testEntityKeyQuery() throws Exception {
		testSelect("SELECT g0.* FROM nw.Shippers AS g0 WHERE g0.ShipperID = 12 ORDER BY g0.ShipperID", "nw.Shippers", null, null, null, -1, null, OEntityKey.create(12));
	}	
	
	@Test
	public void testFilterBasedAssosiation() throws Exception {
		testSelect(
				"SELECT g0.OrderID, g0.CustomerID, g0.EmployeeID, g0.ShipVia FROM nw.Orders AS g0 INNER JOIN nw.Customers AS g1 ON g0.CustomerID = g1.CustomerID WHERE g1.ContactName = 'Fred' ORDER BY g0.OrderID",
				"nw.Orders", "nw.Customers/ContactName eq 'Fred'", "OrderID",
				null, -1, null, null);
		
		testSelect(
				"SELECT g0.CustomerID, g0.ContactName FROM nw.Customers AS g0 INNER JOIN nw.Orders AS g1 ON g0.CustomerID = g1.CustomerID WHERE g1.OrderID = 1 ORDER BY g0.CustomerID",
				"nw.Customers", "nw.Orders/OrderID eq 1", "ContactName",
				null, -1, null, null);
		
	}	
	
	@Test
	public void testOrderByWithCriteria() throws Exception {
		testSelect(
				"SELECT g0.* FROM nw.Shippers AS g0 ORDER BY g0.ShipperID = 12 DESC",
				"nw.Shippers", null, null, "ShipperID eq 12 desc", -1, null,
				null);
	}
	
	@Test
	public void testAny() throws Exception {	
		testSelect(
				"SELECT DISTINCT g0.OrderID, g0.CustomerID, g0.EmployeeID, g0.ShipVia FROM nw.Orders AS g0 INNER JOIN nw.OrderDetails AS ol ON g0.OrderID = ol.OrderID WHERE ol.Quantity > 10 ORDER BY g0.OrderID",
				"nw.Orders", "nw.OrderDetails/any(ol: ol/Quantity gt 10)",
				"OrderID", null, -1, null, null);
	}	
		
	@Test
	public void testAll() throws Exception {		
		testSelect(
				"SELECT g0.OrderID, g0.CustomerID, g0.EmployeeID, g0.ShipVia FROM nw.Orders AS g0 WHERE 10 < ALL (SELECT ol.Quantity FROM nw.OrderDetails AS ol WHERE g0.OrderID = ol.OrderID) ORDER BY g0.OrderID",
				"nw.Orders", "nw.OrderDetails/all(ol: ol/Quantity gt 10)",
				"OrderID", null, -1, null, null);
	}		
	
	@Test
	public void testMultiEntitykey() throws Exception {
		OEntityKey key = OEntityKey.parse("(11044)");
		testSelect("SELECT g1.OrderID, g1.ProductID FROM nw.Orders AS g0 INNER JOIN nw.OrderDetails AS g1 ON g0.OrderID = g1.OrderID WHERE (g0.OrderID = 11044) AND (g1.OrderID = 11044) AND (g1.ProductID = 62) ORDER BY g1.OrderID, g1.ProductID",
				"nw.Orders", null,
				"OrderID", null, -1, "nw.OrderDetails(OrderID=11044L,ProductID=62L)", key);		
	}
	
	@Test public void testUpdates() throws Exception {
		TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl")), "northwind", "nw");		
		ODataSQLBuilder visitor = new ODataSQLBuilder(metadata.getMetadataStore(), false);
		OEntityKey key = OEntityKey.parse("(11044)");
		EdmDataServices eds = LocalClient.buildMetadata(metadata.getVdbMetaData(), metadata.getMetadataStore());
		EdmEntitySet entitySet = eds.getEdmEntitySet("Categories");
		Delete delete = visitor.delete(entitySet, key);
		assertEquals("DELETE FROM nw.Categories WHERE nw.Categories.CategoryID = 11044", delete.toString()); 
		
		OEntity entity = OEntities.create(entitySet, key, (List)Arrays.asList(OProperties.string("Description", "foo")), null);
		Update update = visitor.update(entitySet, entity);
		assertEquals("UPDATE nw.Categories SET Description = ? WHERE nw.Categories.CategoryID = 11044", update.toString());
		
		Insert insert = visitor.insert(entitySet, entity);
		assertEquals("INSERT INTO nw.Categories (Description) VALUES (?)", insert.toString());
	}
	
}


