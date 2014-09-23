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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.apache.olingo.server.api.edm.provider.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.RealMetadataFactory.DDLHolder;

@SuppressWarnings("nls")
public class TestODataSQLBuilder {

	static class TestState{
		Client client;
		List<SQLParam> parameters;
		ArgumentCaptor<Query> arg1;
		ArgumentCaptor<EntityList> arg6;
		String response;
	}
	public TestState helpTest(String url, String sqlExpected) throws Exception {
	    ArgumentCaptor<Query> arg1 = ArgumentCaptor.forClass(Query.class);
	    ArgumentCaptor<EntityList> arg6 = ArgumentCaptor.forClass(EntityList.class);
	    Client client = Mockito.mock(Client.class);
	    List<SQLParam> parameters = new ArrayList<SQLParam>();
		
		
		String ddl = 
				"CREATE FOREIGN TABLE G1 (\n" + 
				"	e1 string, \n" + 
				"	e2 integer PRIMARY KEY, \n" + 
				"	e3 double\n" + 
				");\n" + 
				"\n" + 
				"CREATE FOREIGN TABLE G2 (\n" + 
				"	e1 string, \n" + 
				"	e2 integer PRIMARY KEY, \n" + 
				"	CONSTRAINT FK0 FOREIGN KEY (e2) REFERENCES G1 (e2)\n" + 
				") OPTIONS (UPDATABLE 'true');\n" +
				"CREATE FOREIGN TABLE G3 (\n" + 
				"	e1 string, \n" + 
				"	e2 integer,\n" + 
				"	CONSTRAINT PK PRIMARY KEY (e1,e2)\n" + 
				") OPTIONS (UPDATABLE 'true')" +
				"CREATE FOREIGN TABLE G4 (\n" + 
				"	e1 string PRIMARY KEY, \n" + 
				"	e2 integer,\n" + 
				"	CONSTRAINT FKX FOREIGN KEY (e2) REFERENCES G1(e2)\n" + 
				") OPTIONS (UPDATABLE 'true');";				
				
		DDLHolder model = new DDLHolder("PM1", ddl);
		TransformationMetadata metadata = RealMetadataFactory.fromDDL("vdb", model);
		MetadataStore store = metadata.getMetadataStore();
		//TranslationUtility utility = new TranslationUtility(metadata);
		
		OData odata = OData.newInstance();
		org.teiid.metadata.Schema teiidSchema = store.getSchema("PM1");
		Schema schema = OData4EntitySchemaBuilder.buildMetadata(teiidSchema);
		Edm edm = odata.createEdm(new TeiidEdmProvider(store, schema));
		
		Hashtable<String, String> headers = new Hashtable<String, String>();
		
		Mockito.stub(client.getMetadataStore()).toReturn(store);
		Mockito.stub(client.executeCount(Mockito.any(Query.class), Mockito.anyListOf(SQLParam.class))).toReturn(new CountResponse() {
			@Override
			public long getCount() {
				return 10;
			}
		});
		
	    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
	    Mockito.stub(request.getHeaderNames()).toReturn(headers.elements());
	    Mockito.stub(request.getMethod()).toReturn("GET");
	    Mockito.stub(request.getRequestURL()).toReturn(new StringBuffer(url));
	    Mockito.stub(request.getServletPath()).toReturn("");
	    Mockito.stub(request.getContextPath()).toReturn("/odata4/vdb/PM1");
	    
	    final StringBuffer sb = new StringBuffer();
	    ServletOutputStream out = new ServletOutputStream() {
			@Override
			public void write(int b) throws IOException {
				sb.append((char)b);
			}
		};
	    HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
	    Mockito.stub(response.getOutputStream()).toReturn(out);
	    
		ODataHttpHandler handler = odata.createHandler(edm);
	    handler.register(new TeiidProcessor(client, false));	    
	    handler.process(request, response);
	    
	    if (sqlExpected != null) {
	    	Query actualCommand = (Query) QueryParser.getQueryParser().parseCommand(sqlExpected, new ParseInfo());
	    	Mockito.verify(client).executeSQL(arg1.capture(), Mockito.eq(parameters), Mockito.eq(false), (Integer)Mockito.eq(null), (Integer)Mockito.eq(null), arg6.capture());
	    	Assert.assertEquals(actualCommand.toString(), arg1.getValue().toString());
	    }
	    
	    TestState state = new TestState();
	    state.client = client;
	    state.parameters = parameters;
	    state.arg1 = arg1;
	    state.arg6 = arg6;
	    state.response = sb.toString();
	    return state;
	}
	
	@Test
	public void testSimpleEntitySet() throws Exception {
		helpTest("/odata4/vdb/PM1/G1", "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 ORDER BY g0.e2");
	}
	
	@Test
	public void testSimpleEntitySetWithKey() throws Exception {
		helpTest("/odata4/vdb/PM1/G1(1)", "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e2 = 1 ORDER BY g0.e2");
	}
	
	@Test
	public void testEntitySet$Select() throws Exception {
		helpTest("/odata4/vdb/PM1/G1?$select=e1", "SELECT g0.e1 FROM PM1.G1 AS g0 ORDER BY g0.e2");
	}
	
	@Test 
	public void testEntitySet$SelectBad() throws Exception {
		TestState state = helpTest("/odata4/vdb/PM1/G1?$select=e1,x", null);
		Assert.assertEquals("{\"error\":{\"code\":null,\"message\":\"The type 'G1' has no property 'x'.\"}}", state.response);
	}	
	
	@Test
	public void testEntitySet$OrderBy() throws Exception {
		helpTest("/odata4/vdb/PM1/G1?$orderby=e1 desc, e2", 
				"SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 ORDER BY g0.e1 DESC, g0.e2");
		helpTest("/odata4/vdb/PM1/G1?$orderby=e1", 
				"SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 ORDER BY g0.e1");
	}
	
	@Test
	public void testEntitySet$OrderByNotIn$Select() throws Exception {
		helpTest("/odata4/vdb/PM1/G1?$orderby=e2&$select=e1", 
				"SELECT g0.e1, g0.e2 FROM PM1.G1 AS g0 ORDER BY g0.e2");
	}	
	
	@Test
	public void testEntitySet$filter() throws Exception {
		helpTest("/odata4/vdb/PM1/G1?$filter=e1 eq 1", 
				"SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e1 = 1 ORDER BY g0.e2");
	}
	
	@Test
	public void test$CountIsTrueEntitySet() throws Exception {
		String expected = "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 ORDER BY g0.e2";
		TestState state = helpTest("/odata4/vdb/PM1/G1?$count=true", null);
		
    	Mockito.verify(state.client).executeSQL(state.arg1.capture(), 
    			Mockito.eq(state.parameters), Mockito.eq(true), (Integer)Mockito.eq(null), 
    			(Integer)Mockito.eq(null), state.arg6.capture());
    	Assert.assertEquals(expected, state.arg1.getValue().toString());
		
	}
	
	@Test
	public void test$CountInEntitySet() throws Exception {
		String expected = "SELECT COUNT(*) FROM PM1.G1 AS g0";
		TestState state = helpTest("/odata4/vdb/PM1/G1/$count", null);
		
    	Mockito.verify(state.client).executeCount(state.arg1.capture(), Mockito.eq(state.parameters));
    	Assert.assertEquals(expected, state.arg1.getValue().toString());
	}
	
	@Test
	public void test$CountInNavigation() throws Exception {
		String expected = "SELECT COUNT(*) FROM PM1.G4 AS g0 INNER JOIN PM1.G1 AS g1 ON g0.e2 = g1.e2 WHERE g0.e1 = '1'";
		TestState state = helpTest("/odata4/vdb/PM1/G4('1')/FKX/$count", null);
		
    	Mockito.verify(state.client).executeCount(state.arg1.capture(), Mockito.eq(state.parameters));
    	Assert.assertEquals(expected, state.arg1.getValue().toString());
	}
	
	@Test
	public void test$CountIn$Filter() throws Exception {
		String expected = "SELECT g0.e1 FROM PM1.G4 AS g0 WHERE (SELECT COUNT(*) FROM PM1.G1 AS g1 WHERE g0.e2 = g1.e2) = 2 ORDER BY g0.e1";
		helpTest("/odata4/vdb/PM1/G4?$filter=FKX/$count eq 2&$select=e1", expected);
	}		
	
	@Test
	public void test$CountIn$OrderBy() throws Exception {
		helpTest("/odata4/vdb/PM1/G2?$orderby=G1/$count", "SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1");
	}
	
	@Test
	public void testAlias() throws Exception {
		helpTest("/odata4/vdb/PM1/G1?$filter=e1 eq @p1&@p1=1", 
				"SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE g0.e1 = 1 ORDER BY g0.e2");
	}	
	
	@Test
	public void testMultiEntitykey() throws Exception {
		helpTest("/odata4/vdb/PM1/G3(e1='1',e2=2)", 
				"SELECT g0.e1, g0.e2 FROM PM1.G3 AS g0 WHERE g0.e1 = '1' AND g0.e2 = 2 ORDER BY g0.e1, g0.e2");
	}
	
	private void te(String in, String expected) throws Exception {
	    helpTest("/odata4/vdb/PM1/G1?$filter="+in, 
	    		"SELECT g0.e1, g0.e2, g0.e3 FROM PM1.G1 AS g0 WHERE "+expected+" ORDER BY g0.e2");
	}
	
	@Test
	public void testAnd() throws Exception {
		te("e1 eq 1 and e1 eq 2", "(g0.e1 = 1) AND (g0.e1 = 2)");
		te("(e1 add 4) eq 3", "(g0.e1 + 4) = 3");
		te("((e1 add e2) sub e3) mod e1 eq 0", "MOD(((g0.e1 + g0.e2) - g0.e3), g0.e1) = 0");
	}	
	
	@Test
	public void testEq() throws Exception {
		te("e1 eq 1", "g0.e1 = 1");
		te("e1 eq 4.5", "g0.e1 = 4.5");
		te("e1 eq -4.5", "g0.e1 = -4.5");
		te("e1 eq null", "g0.e1 IS NULL");
		te("e1 eq 'foo'", "g0.e1 = 'foo'");
		te("e1 eq true", "g0.e1 = TRUE");
		te("e1 eq false", "g0.e1 = FALSE");
		te("e1 eq 13:20:00", "g0.e1 = {t'13:20:00'}");
		te("e1 eq 2008-10-13T00:00:00Z", "g0.e1 = {ts'2008-10-13 00:00:00.0'}");
//		te("e1 eq datetimeoffset'2008-10-13T00:00:00.1234-04:00'", "g0.e1 = {ts '2008-10-13T00:00:00.000'}");
	}
	
	@Test
	public void testCast() throws Exception {
		te("e1 eq cast('foo', Edm.String)", "g0.e1 = CONVERT('foo', string)");
		te("e1 eq cast('foo', Edm.Int32)", "g0.e1 = CONVERT('foo', integer)");
	}
	
	@Test
	public void testConcat() throws Exception {
		te("e1 eq concat('foo', 'bar')", "g0.e1 = CONCAT2('foo', 'bar')");
	}
	
	@Test
	public void testEndsWith() throws Exception {
		te("endswith(e1, 'foo')", "ENDSWITH(g0.e1, 'foo') = TRUE");
	}	
	
	@Test
	public void testIndexOf() throws Exception {
		te("indexof(e1, 'foo') eq 1", "LOCATE('foo', g0.e1) = 1");
	}	
	
	@Test
	public void testLength() throws Exception {
		te("length(e1) eq 2", "LENGTH(g0.e1) = 2");
	}	
	
	@Test
	public void testOperator() throws Exception {
		te("not (e1)", "NOT (g0.e1)");
		te("(e1 mul e2) gt 5", "(g0.e1 * g0.e2) > 5");
		te("(e1 div 5) gt 5", "(g0.e1 / 5) > 5");
		te("(e1 add 5) lt 5", "(g0.e1 + 5) < 5");
		te("(e1 sub 5) ne 0", "(g0.e1 - 5) != 0");
		te("(e1 mod 5) eq 0", "MOD(g0.e1, 5) = 0");
		te("(e1 mul -1) eq 0", "(g0.e1 * -1) = 0");
	}	
	
	@Test
	public void testComparisions() throws Exception {
		te("e1 gt e2", "g0.e1 > g0.e2");
		te("e1 lt e2", "g0.e1 < g0.e2");
		te("e1 ge e2", "g0.e1 >= g0.e2");
		te("e1 le e2", "g0.e1 <= g0.e2");
		te("e1 eq e2", "g0.e1 = g0.e2");
		te("e1 ne e2", "g0.e1 <> g0.e2");
		te("e1 eq null", "g0.e1 IS NULL");
		te("e1 ne null", "g0.e1 IS NOT NULL");
	}	

	@Test
	public void testStringMethods() throws Exception {
		//te("replace(x, y, z)", "REPLACE(x, y, z)");
		
		te("substring('foo', 1) eq 'f'", "SUBSTRING('foo', 1) = 'f'");
		te("substring('foo', 1, 2) eq 'f'", "SUBSTRING('foo', 1, 2) = 'f'");
		te("tolower(e1) eq 'foo'", "LCASE(g0.e1) = 'foo'");
		te("toupper(e1) eq 'FOO'", "UCASE(g0.e1) = 'FOO'");
		te("trim('x') eq e1", "TRIM(' ' FROM 'x') = g0.e1");
		te("trim(e1) ne 'foo' and toupper(e1) eq 'bar'", "(TRIM(' ' FROM g0.e1) <> 'foo') AND (UCASE(g0.e1) = 'bar')");
		te("contains(e1, 'foo')", "LOCATE('foo', g0.e1, 1) >= 1");
	}	
	
	
	@Test
	public void testStartsWith() throws Exception {
		te("startswith(e1, 'foo')", "LOCATE(g0.e1, 'foo', 1) = 1");
	}
	
	@Test
	public void testTimeMethods() throws Exception {
		te("year(e1) eq 2000", "YEAR(g0.e1) = 2000");
		te("year(2008-10-13T00:00:00Z) eq 2008", "YEAR({ts'2008-10-13 00:00:00.0'}) = 2008");
		te("month(e1) gt 1", "MONTH(g0.e1) > 1");
		te("day(e1) ne 1", "DAYOFMONTH(g0.e1) != 1");
		te("hour(e1) eq 12", "HOUR(g0.e1) = 12");
		te("minute(e1) lt 5", "MINUTE(g0.e1) < 5");
		te("second(e1) eq 3", "SECOND(g0.e1) = 3");
	}	
	
	@Test
	public void testRoundMethods() throws Exception {
		te("round(e1) eq 0", "ROUND(g0.e1, 0) = 0");
		te("floor(e1) eq 0", "FLOOR(g0.e1) = 0");
		te("ceiling(e1) eq 1", "CEILING(g0.e1) = 1");
	}	
	
	@Test
	public void testNavigationQuery() throws Exception {
		helpTest(
				"/odata4/vdb/PM1/G2(1)/FK0",
				"SELECT g1.e1, g1.e2, g1.e3 FROM PM1.G2 as g0 INNER JOIN PM1.G1 as g1 "
				+ "ON g0.e2 = g1.e2 WHERE g0.e2 = 1 ORDER BY g1.e2");
	}
	
	@Test
	public void testNavigationQuery$Select() throws Exception {
		helpTest(
				"/odata4/vdb/PM1/G2(1)/FK0?$select=e1",
				"SELECT g1.e1 FROM PM1.G2 as g0 INNER JOIN PM1.G1 as g1 "
				+ "ON g0.e2 = g1.e2 WHERE g0.e2 = 1 ORDER BY g1.e2");
	}
	
	
	/*
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
	

	*/	
	
}
