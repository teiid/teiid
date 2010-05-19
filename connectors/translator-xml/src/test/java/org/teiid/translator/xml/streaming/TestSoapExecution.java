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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import javax.resource.ResourceException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.ws.AsyncHandler;
import javax.xml.ws.Binding;
import javax.xml.ws.Dispatch;
import javax.xml.ws.EndpointReference;
import javax.xml.ws.Response;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.xml.XMLExecutionFactory;

import com.metamatrix.cdk.api.ConnectorHost;

@SuppressWarnings("nls")
public class TestSoapExecution {

	public static BasicConnectionFactory getCF() {
		BasicConnectionFactory cf = new BasicConnectionFactory() {
			@Override
			public BasicConnection getConnection() throws ResourceException {		
				String usResult = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
						+ "<m:FullCountryInfoResponse xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">"
						+ "<m:FullCountryInfoResult>"
						+ "<m:sISOCode xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">US</m:sISOCode>"
						+ "<m:sName xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">United States</m:sName>"
						+ "<m:sCapitalCity xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">Washington</m:sCapitalCity>"
						+ "<m:sPhoneCode xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">1</m:sPhoneCode>"
						+ "<m:sContinentCode xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">AM</m:sContinentCode>"
						+ "<m:sCurrencyISOCode xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">USD</m:sCurrencyISOCode>"
						+ "<m:sCountryFlag xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">http://www.oorsprong.org/WebSamples.CountryInfo/Images/USA.jpg</m:sCountryFlag>"
						+ "<m:Languages xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">"
						+ "<m:tLanguage xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">"
						+ "<m:sISOCode xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">eng</m:sISOCode>"
						+ "<m:sName xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">English</m:sName>"
						+ "</m:tLanguage>"
						+ "</m:Languages>"
						+ "</m:FullCountryInfoResult>"
						+ "</m:FullCountryInfoResponse>";

				String jpResult = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" 
					+ "<m:FullCountryInfoResponse xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">"
					+ "<m:FullCountryInfoResult>"
					+ "<m:sISOCode xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">JP</m:sISOCode>"
					+ "<m:sName xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">Japan</m:sName>"
					+ "<m:sCapitalCity xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">Tokyo</m:sCapitalCity>"
					+ "<m:sPhoneCode xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">12</m:sPhoneCode>"
					+ "<m:sContinentCode xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">AM</m:sContinentCode>"
					+ "<m:sCurrencyISOCode xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">YEN</m:sCurrencyISOCode>"
					+ "<m:sCountryFlag xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">http://www.oorsprong.org/WebSamples.CountryInfo/Images/JAPAN.jpg</m:sCountryFlag>"
					+ "<m:Languages xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">"
					+ "<m:tLanguage xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">"
					+ "<m:sISOCode xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">japanese</m:sISOCode>"
					+ "<m:sName xmlns:m=\"http://www.oorsprong.org/websamples.countryinfo\">Japan</m:sName>"
					+ "</m:tLanguage>"
					+ "</m:Languages>"
					+ "</m:FullCountryInfoResult>"
					+ "</m:FullCountryInfoResponse>";
				
				HashMap<String, String> map = new HashMap<String, String>();
				map.put("US", usResult);
				map.put("JP", jpResult);
				return new MyDispatch(map);
			}
			
		};
		return cf;
	}
	
	//@Test
	public void testSOAPSingleRequest() throws Exception {
		
		XMLExecutionFactory factory  = new XMLExecutionFactory();
		
		String vdbPath = UnitTestUtil.getTestDataPath()+"/cis.vdb";
		ConnectorHost host = new ConnectorHost(factory, getCF(), vdbPath);
		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		Mockito.stub(context.getExecutionCountIdentifier()).toReturn("1.1");
		host.setExecutionContext(context);
		
		List result = host.executeCommand("SELECT FullCountryInfoSoapRequest.ResponseOut FROM FullCountryInfoSoapRequest WHERE FullCountryInfoSoapRequest.sCountryISOCode = 'US'", false);
		assertEquals(1, result.size());
		assertEquals("1.1", ((List)result.get(0)).get(0));
		
		result = host.executeCommand("SELECT * FROM FullCountryInfoSoapResponse WHERE FullCountryInfoSoapResponse.ResponseIn = '1.1'");
	
		List row = (List)result.get(0);
		assertEquals("1.1", row.get(0));
		assertEquals("/m:FullCountryInfoResponse[0]", row.get(1));
		assertEquals("US", row.get(2));
		assertEquals("United States", row.get(3));
	}
	
	
	@Test
	public void testSOAPMultipleRequests() throws Exception {
		
		XMLExecutionFactory factory  = new XMLExecutionFactory();

		
		String vdbPath = UnitTestUtil.getTestDataPath()+"/cis.vdb";
		ConnectorHost host = new ConnectorHost(factory, getCF(), vdbPath);
		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		Mockito.stub(context.getExecutionCountIdentifier()).toReturn("1.2");
		host.setExecutionContext(context);
		
		List result = host.executeCommand("SELECT FullCountryInfoSoapRequest.ResponseOut FROM FullCountryInfoSoapRequest WHERE FullCountryInfoSoapRequest.sCountryISOCode IN('US', 'JP')", false);
		assertEquals(2, result.size());
		assertEquals("1.2", ((List)result.get(0)).get(0));
		
		
		result = host.executeCommand("SELECT * FROM FullCountryInfoSoapResponse WHERE FullCountryInfoSoapResponse.ResponseIn = '1.2'");
		assertEquals(2, result.size());
		
		List row = (List)result.get(0);
		assertEquals("1.2", row.get(0));
		assertEquals("/m:FullCountryInfoResponse[0]", row.get(1));
		assertEquals("US", row.get(2));
		assertEquals("United States", row.get(3));
		
		row = (List)result.get(1);
		assertEquals("1.2", row.get(0));
		assertEquals("/m:FullCountryInfoResponse[0]", row.get(1));
		assertEquals("JP", row.get(2));
		assertEquals("Japan", row.get(3));		
	}	
	
	
	static class MyDispatch extends BasicConnection implements Dispatch<Source>{
		private Map<String, Object> requestContext = new HashMap<String, Object>();
		private Map<String, String> result;
		
		public MyDispatch(Map<String, String> result) {
			this.result = result;
		}
				
		@Override
		public Source invoke(Source msg) {
			String content = getContent(msg);
			if (content.indexOf("<tns:sCountryISOCode>US</tns:sCountryISOCode>") != -1){
				return new StreamSource(new StringReader(this.result.get("US")));	
			}
			if (content.indexOf("<tns:sCountryISOCode>JP</tns:sCountryISOCode>") != -1){
				return new StreamSource(new StringReader(this.result.get("JP")));	
			}	
			return null;
		}

		@Override
		public Response<Source> invokeAsync(Source msg) {
			return null;
		}

		@Override
		public Future<?> invokeAsync(Source msg, AsyncHandler<Source> handler) {
			return null;
		}

		@Override
		public void invokeOneWay(Source msg) {
		}

		@Override
		public Binding getBinding() {
			return null;
		}

		@Override
		public EndpointReference getEndpointReference() {
			return null;
		}

		@Override
		public <T extends EndpointReference> T getEndpointReference(Class<T> clazz) {
			return null;
		}

		@Override
		public Map<String, Object> getRequestContext() {
			return this.requestContext;
		}

		@Override
		public Map<String, Object> getResponseContext() {
			return null;
		}

		@Override
		public void close() throws ResourceException {
		}
	}	
	
	public static String getContent(Source msg) {
		StreamSource ss = (StreamSource)msg;
		BufferedReader reader = new BufferedReader(ss.getReader());
		String line = null;
		String content = "";
		try {
			while ((line = reader.readLine()) != null) {
				content = content + line;
			}
		} catch (IOException e) {
		}
		return content;
	}
}
