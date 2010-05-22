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
package org.teiid.translator.xml.streaming;

import static org.junit.Assert.assertEquals;

import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.resource.ResourceException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.ws.handler.MessageContext;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.xml.XMLExecutionFactory;
import org.teiid.translator.xml.streaming.TestSoapExecution.MyDispatch;

import com.metamatrix.cdk.api.ConnectorHost;

@SuppressWarnings("nls")
public class TestHttpExecution {

	
	public static BasicConnectionFactory getCF() {
		BasicConnectionFactory cf = new BasicConnectionFactory() {
			@Override
			public BasicConnection getConnection() throws ResourceException {		
				String usResult = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + 
						"<ns2:purchaseOrderList xmlns:ns2=\"http://www.example.com/PO1\" xmlns:ns3=\"http://www.example.org/PO1\">\n" + 
						"    <ns3:purchaseOrders orderDate=\"2010-05-22-04:00\">\n" + 
						"        <shipTo country=\"Ship_Country_6\">\n" + 
						"            <name>Ship_Name_6</name>\n" + 
						"            <street>Ship_Street_6</street>\n" + 
						"            <city>Ship_City_6</city>\n" + 
						"            <state>Ship_State_6</state>\n" + 
						"            <zip>6</zip>\n" + 
						"        </shipTo>\n" + 
						"        <billTo country=\"Bill_Country_6\">\n" + 
						"            <name>Bill_Name_6</name>\n" + 
						"            <street>Bill_Street_6</street>\n" + 
						"            <city>Bill_City_6</city>\n" + 
						"            <state>Bill_State_6</state>\n" + 
						"            <zip>6</zip>\n" + 
						"        </billTo>\n" + 
						"        <items>\n" + 
						"            <item partNum=\"0\">\n" + 
						"                <productName>Product0</productName>\n" + 
						"                <quantity>0</quantity>\n" + 
						"                <USPrice>0</USPrice>\n" + 
						"                <ns2:comment>Comment 0</ns2:comment>\n" + 
						"                <shipDate>2010-05-22-04:00</shipDate>\n" + 
						"            </item>\n" + 
						"            <item partNum=\"1\">\n" + 
						"                <productName>Product1</productName>\n" + 
						"                <quantity>1</quantity>\n" + 
						"                <USPrice>1</USPrice>\n" + 
						"                <ns2:comment>Comment 1</ns2:comment>\n" + 
						"                <shipDate>2010-05-22-04:00</shipDate>\n" + 
						"            </item>\n" + 
						"            <item partNum=\"2\">\n" + 
						"                <productName>Product2</productName>\n" + 
						"                <quantity>2</quantity>\n" + 
						"                <USPrice>2</USPrice>\n" + 
						"                <ns2:comment>Comment 2</ns2:comment>\n" + 
						"                <shipDate>2010-05-22-04:00</shipDate>\n" + 
						"            </item>\n" + 
						"        </items>\n" + 
						"    </ns3:purchaseOrders>\n" + 
						"    <ns3:purchaseOrders orderDate=\"2010-05-22-04:00\">\n" + 
						"        <shipTo country=\"Ship_Country_7\">\n" + 
						"            <name>Ship_Name_7</name>\n" + 
						"            <street>Ship_Street_7</street>\n" + 
						"            <city>Ship_City_7</city>\n" + 
						"            <state>Ship_State_7</state>\n" + 
						"            <zip>7</zip>\n" + 
						"        </shipTo>\n" + 
						"        <billTo country=\"Bill_Country_7\">\n" + 
						"            <name>Bill_Name_7</name>\n" + 
						"            <street>Bill_Street_7</street>\n" + 
						"            <city>Bill_City_7</city>\n" + 
						"            <state>Bill_State_7</state>\n" + 
						"            <zip>7</zip>\n" + 
						"        </billTo>\n" + 
						"        <items>\n" + 
						"            <item partNum=\"0\">\n" + 
						"                <productName>Product0</productName>\n" + 
						"                <quantity>0</quantity>\n" + 
						"                <USPrice>0</USPrice>\n" + 
						"                <ns2:comment>Comment 0</ns2:comment>\n" + 
						"                <shipDate>2010-05-22-04:00</shipDate>\n" + 
						"            </item>\n" + 
						"            <item partNum=\"1\">\n" + 
						"                <productName>Product1</productName>\n" + 
						"                <quantity>1</quantity>\n" + 
						"                <USPrice>1</USPrice>\n" + 
						"                <ns2:comment>Comment 1</ns2:comment>\n" + 
						"                <shipDate>2010-05-22-04:00</shipDate>\n" + 
						"            </item>\n" + 
						"            <item partNum=\"2\">\n" + 
						"                <productName>Product2</productName>\n" + 
						"                <quantity>2</quantity>\n" + 
						"                <USPrice>2</USPrice>\n" + 
						"                <ns2:comment>Comment 2</ns2:comment>\n" + 
						"                <shipDate>2010-05-22-04:00</shipDate>\n" + 
						"            </item>\n" + 
						"        </items>\n" + 
						"    </ns3:purchaseOrders>\n" + 
						"</ns2:purchaseOrderList>";

				
				HashMap<String, String> map = new HashMap<String, String>();
				map.put("result", usResult);
				return new HttpDispatch(map);
			}
			
		};
		return cf;
	}	
	
	static class HttpDispatch extends MyDispatch{
		
		public HttpDispatch(Map<String, String> result) {
			super(result);
		}
		
		@Override
		public Source invoke(Source msg) {
			String qt = (String)this.getRequestContext().get(MessageContext.QUERY_STRING);
			assertEquals("orderCount=2&itemCount=3&", qt);
			return new StreamSource(new StringReader(result.get("result"))); //$NON-NLS-1$
		}		
	}
	
	@Test
	public void testHttpExecution() throws Exception{
		XMLExecutionFactory factory  = new XMLExecutionFactory();
		
		String vdbPath = UnitTestUtil.getTestDataPath()+"/xmltest.vdb";
		ConnectorHost host = new ConnectorHost(factory, getCF(), vdbPath);
		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		Mockito.stub(context.getExecutionCountIdentifier()).toReturn("1.1");
		host.setExecutionContext(context);
		
		List result = host.executeCommand("SELECT queryParams.REQUEST.ResponseOut, queryParams.REQUEST.itemcount, queryParams.REQUEST.ordercount FROM queryParams.REQUEST WHERE (queryParams.REQUEST.itemcount = '3') AND (queryParams.REQUEST.ordercount = '2')", false);
		assertEquals(1, result.size());
		assertEquals(Arrays.asList(new Object[] {"1.1", "3", "2"}), result.get(0));
		
		result = host.executeCommand("SELECT queryParams.item.partNum, queryParams.item.productName, queryParams.item.quantity, queryParams.item.USPrice, queryParams.item.comment, queryParams.item.shipDate FROM queryParams.item WHERE queryParams.item.ResponseIn = '1.1'");
	
		assertEquals(6, result.size());
		assertEquals(Arrays.asList(new Object[] {"1", "Product1","1","1","Comment 1","2010-05-22-04:00"}), result.get(1));		
	}
}
