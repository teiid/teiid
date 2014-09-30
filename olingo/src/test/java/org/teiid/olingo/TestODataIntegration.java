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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.http.HttpClientTransportOverHTTP;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletHandler;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.json.simple.ContentHandler;
import org.teiid.json.simple.ParseException;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.loopback.LoopbackExecutionFactory;

@SuppressWarnings("nls")
public class TestODataIntegration {

	private static final class JSONValueExtractor implements ContentHandler {
		Object value;
		boolean next;
		String key;

		private JSONValueExtractor(String key) {
			this.key = key;
		}

		@Override
		public boolean startObjectEntry(String key) throws ParseException,
				IOException {
			if (key.equals(this.key)) {
				next = true;
			}
			return true;
		}

		@Override
		public boolean startObject() throws ParseException, IOException {
			return true;
		}

		@Override
		public void startJSON() throws ParseException, IOException {

		}

		@Override
		public boolean startArray() throws ParseException, IOException {
			return true;
		}

		@Override
		public boolean primitive(Object value) throws ParseException, IOException {
			if (next) {
				this.value = value;
				next = false;
			}
			return true;
		}

		@Override
		public boolean endObjectEntry() throws ParseException, IOException {
			return true;
		}

		@Override
		public boolean endObject() throws ParseException, IOException {
			return true;
		}

		@Override
		public void endJSON() throws ParseException, IOException {

		}

		@Override
		public boolean endArray() throws ParseException, IOException {
			return true;
		}
	}

	private static int port;
	private static EmbeddedServer teiid;
	private static Server server;

	@BeforeClass
	public static void before() throws Exception {
		teiid = new EmbeddedServer();
		teiid.start(new EmbeddedConfiguration());
		teiid.addTranslator(LoopbackExecutionFactory.class);

		server = new Server();

		ServerConnector connector = new ServerConnector(server);
		server.setConnectors(new Connector[] {connector});

		ServletHandler handler = new ServletHandler();
        server.setHandler(handler);

        handler.addFilterWithMapping(new FilterHolder(new ODataFilter() {
        	@Override
        	public Client buildClient(String vdbName, int version, Properties props) {
        		LocalClient c=  new LocalClient(vdbName, version, props);
        		c.setDriver(teiid.getDriver());
        		return c;
        	}
        }), "/*", 0);
        handler.addServletWithMapping(ODataServlet.class, "/*");
        server.start();
        port = connector.getLocalPort();
        deployVDB();
	}

	@AfterClass
	public static void after() throws Exception {
		server.stop();
		teiid.stop();
	}

	private static void deployVDB() throws IOException, ConnectorManagerException, VirtualDatabaseException, TranslatorException {
		teiid.deployVDB(new FileInputStream(UnitTestUtil.getTestDataFile("loopy-vdb.xml")));
	}

	@Test
	public void testMetadata() throws Exception {
		// for usage see
		// http://www.eclipse.org/jetty/documentation/current/http-client-api.html
		HttpClientTransportOverHTTP transport = new HttpClientTransportOverHTTP();
		HttpClient http = new HttpClient(transport, null);
		transport.setHttpClient(http);
		http.start();

		ContentResponse response = http.GET("http://localhost:"+port+"/odata4/loopy/vm1/$metadata");
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("loopy-edmx-metadata.xml")), response.getContentAsString());
	}

	@Test
	public void tesSystemMetadata() throws Exception {
		// for usage see
		// http://www.eclipse.org/jetty/documentation/current/http-client-api.html
		HttpClientTransportOverHTTP transport = new HttpClientTransportOverHTTP();
		HttpClient http = new HttpClient(transport, null);
		transport.setHttpClient(http);
		http.start();

		ContentResponse response = http.GET("http://localhost:"+port+"/odata4/loopy/SYS/$metadata");
        Assert.assertEquals(200, response.getStatus());
		response = http.GET("http://localhost:"+port+"/odata4/loopy/SYSADMIN/$metadata");
        Assert.assertEquals(200, response.getStatus());
	}

	@Test
	public void tesServiceMetadata() throws Exception {
		// for usage see
		// http://www.eclipse.org/jetty/documentation/current/http-client-api.html
		HttpClientTransportOverHTTP transport = new HttpClientTransportOverHTTP();
		HttpClient http = new HttpClient(transport, null);
		transport.setHttpClient(http);
		http.start();

		ContentResponse response = http.GET("http://localhost:"+port+"/odata4/loopy/VM1");
        Assert.assertEquals(200, response.getStatus());
        //TODO: match the document here.. port is being random
	}

	@Test
	public void testEntitySet() throws Exception {
		// for usage see
		// http://www.eclipse.org/jetty/documentation/current/http-client-api.html
		HttpClientTransportOverHTTP transport = new HttpClientTransportOverHTTP();
		HttpClient http = new HttpClient(transport, null);
		transport.setHttpClient(http);
		http.start();

		ContentResponse response = http.GET("http://localhost:"+port+"/odata4/loopy/vm1/G1");
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals("{\"@odata.context\":\"$metadata#G1\",\"value\":[{\"e1\":\"ABCDEFGHIJ\",\"e2\":0,\"e3\":0.0}]}", response.getContentAsString());
	}

	@Test
	public void testEntitySetWithKey() throws Exception {
		// for usage see
		// http://www.eclipse.org/jetty/documentation/current/http-client-api.html
		HttpClientTransportOverHTTP transport = new HttpClientTransportOverHTTP();
		HttpClient http = new HttpClient(transport, null);
		transport.setHttpClient(http);
		http.start();

		ContentResponse response = http.GET("http://localhost:"+port+"/odata4/loopy/vm1/G1(0)");
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals("{\"@odata.context\":\"$metadata#G1/$entity\",\"value\":[{\"e1\":\"ABCDEFGHIJ\",\"e2\":0,\"e3\":0.0}]}", response.getContentAsString());
	}

    @Test
    public void testIndividualProperty() throws Exception {
        // for usage see
        // http://www.eclipse.org/jetty/documentation/current/http-client-api.html
        HttpClientTransportOverHTTP transport = new HttpClientTransportOverHTTP();
        HttpClient http = new HttpClient(transport, null);
        transport.setHttpClient(http);
        http.start();

        ContentResponse response = http.GET("http://localhost:"+port+"/odata4/loopy/vm1/G1(1)/e1");
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals("{\"@odata.context\":\"$metadata#Edm.String\",\"value\":\"ABCDEFGHIJ\"}", response.getContentAsString());
    }

	/*
    protected Client mockClient2() {
        Client client = mock(Client.class);
        VDBMetaData vdb = mock(VDBMetaData.class);
        ModelMetaData model = mock(ModelMetaData.class);
        stub(model.isVisible()).toReturn(false);
        stub(vdb.getModel("nw")).toReturn(model);
        stub(client.getMetadataStore()).toReturn(metadata.getMetadataStore());
        EdmDataServices eds = LocalClient.buildMetadata(vdb, metadata.getMetadataStore());
        stub(client.getMetadata()).toReturn(eds);
        return client;
    }

    @Test
    public void testMetadataVisibility() throws Exception {
        Client client = mockClient2();
        MockProvider.CLIENT = client;

        StringWriter sw = new StringWriter();

        EdmxFormatWriter.write(client.getMetadata(), sw);

        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/$metadata"));
        ClientResponse<String> response = request.get(String.class);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals(sw.toString(), response.getEntity());
        String edm = "<?xml version=\"1.0\" encoding=\"utf-8\"?><edmx:Edmx Version=\"1.0\" xmlns:edmx=\"http://schemas.microsoft.com/ado/2007/06/edmx\"><edmx:DataServices m:DataServiceVersion=\"2.0\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\"></edmx:DataServices></edmx:Edmx>";
        Assert.assertEquals(edm, response.getEntity());
    }



	@Test
	public void testProjectedColumns() throws Exception {
		Client client = mockClient();
		MockProvider.CLIENT = client;
		ArgumentCaptor<Query> sql = ArgumentCaptor.forClass(Query.class);
		ArgumentCaptor<EdmEntitySet> entitySet = ArgumentCaptor.forClass(EdmEntitySet.class);

		OEntity entity = createCustomersEntity(client.getMetadata());
		ArrayList<OEntity> list = new ArrayList<OEntity>();
		list.add(entity);

		EntityList result = Mockito.mock(EntityList.class);
		when(result.get(0)).thenReturn(entity);
		when(result.size()).thenReturn(1);
		when(result.iterator()).thenReturn(list.iterator());

		when(client.executeSQL(any(Query.class), anyListOf(SQLParam.class), any(EdmEntitySet.class), (LinkedHashMap<String, Boolean>) any(), any(QueryInfo.class))).thenReturn(result);

        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/Customers?$select=CustomerID,CompanyName,Address"));
        ClientResponse<String> response = request.get(String.class);
        verify(client).executeSQL(sql.capture(),  anyListOf(SQLParam.class), entitySet.capture(), (LinkedHashMap<String, Boolean>) any(), any(QueryInfo.class));

        Assert.assertEquals("SELECT g0.CustomerID, g0.CompanyName, g0.Address FROM Customers AS g0 ORDER BY g0.CustomerID", sql.getValue().toString());
        Assert.assertEquals(200, response.getStatus());
        //Assert.assertEquals("", response.getEntity());
	}

	@Test
	public void testCheckGeneratedColumns() throws Exception {
		Client client = mockClient();
		MockProvider.CLIENT = client;
		ArgumentCaptor<Command> insertCmd = ArgumentCaptor.forClass(Command.class);
		ArgumentCaptor<Query> sql = ArgumentCaptor.forClass(Query.class);
		ArgumentCaptor<EdmEntitySet> entitySet = ArgumentCaptor.forClass(EdmEntitySet.class);

		OEntity entity = createCustomersEntity(client.getMetadata());
		ArrayList<OEntity> list = new ArrayList<OEntity>();
		list.add(entity);

		EntityList result = Mockito.mock(EntityList.class);
		when(result.get(0)).thenReturn(entity);
		when(result.size()).thenReturn(1);
		when(result.iterator()).thenReturn(list.iterator());

		when(client.executeSQL(any(Query.class), anyListOf(SQLParam.class), any(EdmEntitySet.class), (LinkedHashMap<String, Boolean>) any(), any(QueryInfo.class))).thenReturn(result);

		UpdateResponse respose = new UpdateResponse() {
			@Override
			public int getUpdateCount() {
				return 1;
			}
			@Override
			public Map<String, Object> getGeneratedKeys() {
				HashMap<String, Object> map = new HashMap<String, Object>();
				map.put("id", 1234);
				return map;
			}
		};

		String post = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
				"<entry xml:base=\"http://host/service.svc/\"\n" +
				"xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\"\n" +
				"xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\"\n" +
				"xmlns=\"http://www.w3.org/2005/Atom\">\n" +
				"    <content type=\"application/xml\">\n" +
				"        <m:properties>\n" +
				"            <d:CompanyName>JBoss</d:CompanyName>\n" +
				"            <d:ContactName>Joe</d:ContactName>\n" +
				"            <d:ContactTitle>1970</d:ContactTitle>\n" +
				"            <d:Address>123 Main Street</d:Address>\n" +
				"            <d:City>STL</d:City>\n" +
				"            <d:Region>MidWest</d:Region>\n" +
				"            <d:PostalCode>12345</d:PostalCode>\n" +
				"            <d:Country>USA</d:Country>\n" +
				"            <d:Phone>123234</d:Phone>\n" +
				"        </m:properties>\n" +
				"    </content>\n" +
				"</entry>";

		when(client.executeUpdate(any(Command.class), anyListOf(SQLParam.class))).thenReturn(respose);

        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/Customers"));
        request.body(MediaType.APPLICATION_ATOM_XML, post);

        ClientResponse<String> response = request.post();

        verify(client).executeUpdate(insertCmd.capture(),  anyListOf(SQLParam.class));

        // post after insert pulls the entity inserted. In above XML there is customer id, but
        // below selection is based on primary key 1234
        verify(client).executeSQL(sql.capture(),  anyListOf(SQLParam.class), entitySet.capture(), (LinkedHashMap<String, Boolean>) any(), any(QueryInfo.class));

        Assert.assertEquals("INSERT INTO Customers (CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", insertCmd.getValue().toString());
        Assert.assertEquals("SELECT g0.CustomerID, g0.CompanyName, g0.ContactName, g0.ContactTitle, g0.Address, g0.City, g0.Region, g0.PostalCode, g0.Country, g0.Phone, g0.Fax FROM Customers AS g0 WHERE g0.CustomerID = 1234 ORDER BY g0.CustomerID", sql.getValue().toString());
        Assert.assertEquals(201, response.getStatus());
	}

	@Test
	public void testProcedure() throws Exception {
		Client client = mockClient();
		MockProvider.CLIENT = client;
		ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<List> params = ArgumentCaptor.forClass(List.class);

		when(client.executeCall(any(String.class), anyListOf(SQLParam.class), any(EdmType.class))).thenReturn(Responses.simple(EdmSimpleType.INT32, "return", null));

        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/getCustomers?p2=datetime'2011-09-11T00:00:00'&p3=2.0M"));
        ClientResponse<String> response = request.get(String.class);
        verify(client).executeCall(sql.capture(), params.capture(), any(EdmType.class));

        Assert.assertEquals("{? = call nw.getCustomers(p2=>?,p3=>?)}", sql.getValue().toString());
        Assert.assertEquals(TimestampUtil.createTimestamp(111, 8, 11, 0, 0, 0, 0), ((SQLParam)params.getValue().get(0)).value);
        Assert.assertEquals(BigDecimal.valueOf(2.0), ((SQLParam)params.getValue().get(1)).value);
        Assert.assertEquals(200, response.getStatus());
	}

	@Test
	public void testSkipNoPKTable() throws Exception {
		Client client = mockClient();
		MockProvider.CLIENT = client;

        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/NoPKTable"));
        ClientResponse<String> response = request.get(String.class);

        Assert.assertEquals(404, response.getStatus());
        Assert.assertTrue(response.getEntity().endsWith("<error xmlns=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\"><code>NotFoundException</code><message lang=\"en-US\">TEIID16011 EntitySet \"NoPKTable\" is not found; Check the spelling, use modelName.tableName; The table that representing the Entity type must either have a PRIMARY KEY or UNIQUE key(s)</message></error>"));
	}

	@Test
	public void testError() throws Exception {
		Client client = mockClient();
		MockProvider.CLIENT = client;

		when(client.executeSQL(any(Query.class), anyListOf(SQLParam.class), any(EdmEntitySet.class), (LinkedHashMap<String, Boolean>) any(), any(QueryInfo.class))).thenThrow(new NullPointerException());

        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/Customers?$select=CustomerID,CompanyName,Address"));
        ClientResponse<String> response = request.get(String.class);

        Assert.assertEquals(500, response.getStatus());
        Assert.assertTrue(response.getEntity().endsWith("<error xmlns=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\"><code>ServerErrorException</code><message lang=\"en-US\">Internal Server Error</message></error>"));
	}

	@Test
	public void testProcedureCall() throws Exception {
		Client client = mockClient();
		MockProvider.CLIENT = client;
		ArgumentCaptor<Query> sql = ArgumentCaptor.forClass(Query.class);
		ArgumentCaptor<EdmEntitySet> entitySet = ArgumentCaptor.forClass(EdmEntitySet.class);

		OEntity entity = createCustomersEntity(client.getMetadata());
		ArrayList<OEntity> list = new ArrayList<OEntity>();
		list.add(entity);

		EntityList result = Mockito.mock(EntityList.class);
		when(result.get(0)).thenReturn(entity);
		when(result.size()).thenReturn(1);
		when(result.iterator()).thenReturn(list.iterator());

		when(client.executeSQL(any(Query.class), anyListOf(SQLParam.class), any(EdmEntitySet.class), (LinkedHashMap<String, Boolean>) any(), any(QueryInfo.class))).thenReturn(result);

        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/Customers?$select=CustomerID,CompanyName,Address"));
        ClientResponse<String> response = request.get(String.class);
        verify(client).executeSQL(sql.capture(),  anyListOf(SQLParam.class), entitySet.capture(), (LinkedHashMap<String, Boolean>) any(), any(QueryInfo.class));

        Assert.assertEquals("SELECT g0.CustomerID, g0.CompanyName, g0.Address FROM Customers AS g0 ORDER BY g0.CustomerID", sql.getValue().toString());
        Assert.assertEquals(200, response.getStatus());
        //Assert.assertEquals("", response.getEntity());

	}

	@Test public void testInvalidCharacterReplacement() throws Exception {
		EmbeddedServer es = new EmbeddedServer();
		es.start(new EmbeddedConfiguration());
		try {
			ModelMetaData mmd = new ModelMetaData();
			mmd.setName("vw");
			mmd.setSchemaSourceType("ddl");
			mmd.setModelType(Type.VIRTUAL);
			mmd.setSchemaText("create view x (a string primary key, b char, c string[], d integer) as select 'ab\u0000cd\u0001', char(22), ('a\u00021','b1'), 1;");
			es.deployVDB("northwind", mmd);

			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			props.setProperty(LocalClient.INVALID_CHARACTER_REPLACEMENT, " ");
			LocalClient lc = new LocalClient("northwind", 1, props);
			lc.setDriver(td);
			MockProvider.CLIENT = lc;

	        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x"));
	        ClientResponse<String> response = request.get(String.class);
	        Assert.assertEquals(200, response.getStatus());
	        assertTrue(response.getEntity().contains("ab cd "));
	        assertTrue(response.getEntity().contains("a 1"));
		} finally {
			es.stop();
		}
	}

	@Test public void testArrayResults() throws Exception {
		EmbeddedServer es = new EmbeddedServer();
		es.start(new EmbeddedConfiguration());
		try {
			ModelMetaData mmd = new ModelMetaData();
			mmd.setName("vw");
			mmd.setSchemaSourceType("ddl");
			mmd.setModelType(Type.VIRTUAL);
			mmd.setSchemaText("create view x (a string primary key, b integer[], c string[][]) as select 'x', (1, 2, 3), (('a','b'),('c','d'));");
			es.deployVDB("northwind", mmd);

			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			LocalClient lc = new LocalClient("northwind", 1, props);
			lc.setDriver(td);
			MockProvider.CLIENT = lc;

	        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x?$format=json&$select=a,b"));
	        ClientResponse<String> response = request.get(String.class);
	        assertEquals(200, response.getStatus());
	        assertTrue(response.getEntity().contains("1, 2, 3"));

	        request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x?$format=json"));
	        response = request.get(String.class);
	        assertEquals(500, response.getStatus());
		} finally {
			es.stop();
		}
	}

	@Test public void testSkipToken() throws Exception {
		EmbeddedServer es = new EmbeddedServer();
		es.start(new EmbeddedConfiguration());
		try {
			ModelMetaData mmd = new ModelMetaData();
			mmd.setName("vw");
			mmd.setSchemaSourceType("ddl");
			mmd.setModelType(Type.VIRTUAL);
			mmd.setSchemaText("create view x (a string primary key, b integer) as select 'xyz', 123 union all select 'abc', 456;");
			es.deployVDB("northwind", mmd);

			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			props.setProperty("batch-size", "1");
			LocalClient lc = new LocalClient("northwind", 1, props);
			lc.setDriver(td);
			MockProvider.CLIENT = lc;

	        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x?$format=json"));
	        ClientResponse<String> response = request.get(String.class);
	        assertEquals(200, response.getStatus());
	        JSONParser parser = new JSONParser();
	        JSONValueExtractor contentHandler = new JSONValueExtractor("__next");
			parser.parse(response.getEntity(), contentHandler);
	        assertNotNull(contentHandler.next);
	        assertTrue(response.getEntity().contains("abc"));
	        assertTrue(!response.getEntity().contains("xyz"));

	        //follow the skip
	        request = new ClientRequest((String) contentHandler.value);
	        response = request.get(String.class);
	        assertEquals(200, response.getStatus());

	        assertTrue(!response.getEntity().contains("abc"));
	        assertTrue(response.getEntity().contains("xyz"));

	        contentHandler.value = null;
	        parser.parse(response.getEntity(), contentHandler);
	        assertNull(contentHandler.value);
		} finally {
			es.stop();
		}
	}

	@Test public void testCount() throws Exception {
		EmbeddedServer es = new EmbeddedServer();
		es.start(new EmbeddedConfiguration());
		try {
			ModelMetaData mmd = new ModelMetaData();
			mmd.setName("vw");
			mmd.setSchemaSourceType("ddl");
			mmd.setModelType(Type.VIRTUAL);
			mmd.setSchemaText("create view x (a string primary key, b integer) as select 'xyz', 123 union all select 'abc', 456;");
			es.deployVDB("northwind", mmd);

			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			props.setProperty("batch-size", "1");
			LocalClient lc = new LocalClient("northwind", 1, props);
			lc.setDriver(td);
			MockProvider.CLIENT = lc;

	        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x?$format=json&$inlinecount=allpages&$top=1&$skip=1"));
	        ClientResponse<String> response = request.get(String.class);
	        assertEquals(200, response.getStatus());
	        JSONParser parser = new JSONParser();
	        JSONValueExtractor contentHandler = new JSONValueExtractor("__count");
			parser.parse(response.getEntity(), contentHandler);
	        assertEquals("2", contentHandler.value);
	        assertTrue(response.getEntity().contains("xyz"));
	        assertTrue(!response.getEntity().contains("abc"));

	        //effectively the same as above
	        request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x?$format=json&$inlinecount=allpages&$skip=1"));
	        response = request.get(String.class);
	        assertEquals(200, response.getStatus());
	        contentHandler.value = null;
			parser.parse(response.getEntity(), contentHandler);
	        assertEquals("2", contentHandler.value);
	        contentHandler.key = "__next";
	        contentHandler.value = null;
	        parser.parse(response.getEntity(), contentHandler);
	        assertNull(contentHandler.value);

	        //now there should be a next
	        request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x?$format=json&$inlinecount=allpages"));
	        response = request.get(String.class);
	        assertEquals(200, response.getStatus());
	        contentHandler.value = null;
			parser.parse(response.getEntity(), contentHandler);
	        assertNotNull(contentHandler.value);

	        request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x/$count"));
	        response = request.get(String.class);
	        assertEquals(200, response.getStatus());
	        assertEquals("2", response.getEntity());
		} finally {
			es.stop();
		}
	}

	@Test public void testCompositeKeyUpdates() throws Exception {
		EmbeddedServer es = new EmbeddedServer();
		HardCodedExecutionFactory hc = new HardCodedExecutionFactory() {
			@Override
			public boolean supportsCompareCriteriaEquals() {
				return true;
			}
		};
		hc.addUpdate("DELETE FROM x WHERE x.a = 'a' AND x.b = 'b'", new int[] {1});
		hc.addUpdate("UPDATE x SET c = 5 WHERE x.a = 'a' AND x.b = 'b'", new int[] {1});
		es.addTranslator("x", hc);
		es.start(new EmbeddedConfiguration());
		try {
			ModelMetaData mmd = new ModelMetaData();
			mmd.setName("m");
			mmd.setSchemaSourceType("ddl");
			mmd.setSchemaText("create foreign table x (a string, b string, c integer, primary key (a, b)) options (updatable true);");
			mmd.addSourceMapping("x", "x", null);
			es.deployVDB("northwind", mmd);

			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			LocalClient lc = new LocalClient("northwind", 1, props);
			lc.setDriver(td);
			MockProvider.CLIENT = lc;

	        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x(a='a',b='b')"));
	        ClientResponse<String> response = request.delete(String.class);
	        assertEquals(200, response.getStatus());

	        //partial key
	        request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x('a')"));
	        response = request.delete(String.class);
	        assertEquals(404, response.getStatus());

	        //partial key
	        request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x(a='a',a='b')"));
	        response = request.delete(String.class);
	        assertEquals(404, response.getStatus());

	        //not supported
	        //request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x(a='a',b='b')/c/$value"));
	        //request.body("text/plain", "5");
	        //response = request.put(String.class);

	        request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x(a='a',b='b')"));
	        request.body("application/json", "{\"c\":5}");
	        response = request.put(String.class);
	        assertEquals(200, response.getStatus());
		} finally {
			es.stop();
		}
	}

	@Test public void testBatch() throws Exception {
		EmbeddedServer es = new EmbeddedServer();
		HardCodedExecutionFactory hc = new HardCodedExecutionFactory() {
			@Override
			public boolean supportsCompareCriteriaEquals() {
				return true;
			}
		};
		hc.addUpdate("DELETE FROM x WHERE x.a = 'a' AND x.b = 'b'", new int[] {1});
		es.addTranslator("x", hc);
		es.start(new EmbeddedConfiguration());
		try {
			ModelMetaData mmd = new ModelMetaData();
			mmd.setName("m");
			mmd.setSchemaSourceType("ddl");
			mmd.setSchemaText("create foreign table x (a string, b string, c integer, primary key (a, b)) options (updatable true);");
			mmd.addSourceMapping("x", "x", null);
			es.deployVDB("northwind", mmd);

			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			LocalClient lc = new LocalClient("northwind", 1, props);
			lc.setDriver(td);
			MockProvider.CLIENT = lc;

			String post = "Content-Type: application/http\n"
					+ "Content-Transfer-Encoding:binary\n"
					+ "\nDELETE /odata/northwind/x(a='a',b='b') HTTP/1.1\n"
					//+ "Host: host\n"
					+ "--batch_36522ad7-fc75-4b56-8c71-56071383e77b\n";

	        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/Customers/$batch"));
	        request.body(ODataBatchProvider.MULTIPART_MIXED, post);

	        ClientResponse<String> response = request.post(String.class);
	        assertEquals(202, response.getStatus());
		} finally {
			es.stop();
		}
	}

	@Test public void testJsonProcedureResultSet() throws Exception {
		EmbeddedServer es = new EmbeddedServer();
		HardCodedExecutionFactory hc = new HardCodedExecutionFactory();
		hc.addData("EXEC x()", Arrays.asList(Arrays.asList("x"), Arrays.asList("y")));
		es.addTranslator("x", hc);
		es.start(new EmbeddedConfiguration());
		try {
			ModelMetaData mmd = new ModelMetaData();
			mmd.setName("m");
			mmd.setSchemaSourceType("ddl");
			mmd.setSchemaText("create foreign procedure x () returns table(y string);");
			mmd.addSourceMapping("x", "x", null);
			es.deployVDB("northwind", mmd);

			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			LocalClient lc = new LocalClient("northwind", 1, props);
			lc.setDriver(td);
			MockProvider.CLIENT = lc;

	        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x?$format=json"));
	        ClientResponse<String> response = request.get(String.class);
	        assertEquals(200, response.getStatus());
		} finally {
			es.stop();
		}
	}

	@Test public void testBasicTypes() throws Exception {
		EmbeddedServer es = new EmbeddedServer();
		es.start(new EmbeddedConfiguration());
		try {
			ModelMetaData mmd = new ModelMetaData();
			mmd.setName("m");
			mmd.setSchemaSourceType("ddl");

			mmd.addSourceMapping("x", "x", null);
			MetadataStore ms = RealMetadataFactory.exampleBQTStore();
			Schema s = ms.getSchema("BQT1");
			KeyRecord pk = new KeyRecord(KeyRecord.Type.Primary);
			Table smalla = s.getTable("SmallA");
			pk.setName("pk");
			pk.addColumn(smalla.getColumnByName("IntKey"));
			smalla.setPrimaryKey(pk);
			String ddl = DDLStringVisitor.getDDLString(s, EnumSet.allOf(SchemaObjectType.class), "SmallA");
			mmd.setSchemaText(ddl);
			HardCodedExecutionFactory hc = new HardCodedExecutionFactory() {
				@Override
				protected List<? extends List<?>> getData(
						QueryExpression command) {
					Class<?>[] colTypes = command.getProjectedQuery().getColumnTypes();
					List<Expression> cols = new ArrayList<Expression>();
					for (int i = 0; i < colTypes.length; i++) {
						ElementSymbol elementSymbol = new ElementSymbol("X");
						elementSymbol.setType(colTypes[i]);
						cols.add(elementSymbol);
					}
					return (List)Arrays.asList(AutoGenDataService.createResults(cols, 1, false));
				}
			};

			es.addTranslator("x", hc);
			es.deployVDB("northwind", mmd);

			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			LocalClient lc = new LocalClient("northwind", 1, props);
			lc.setDriver(td);
			MockProvider.CLIENT = lc;

	        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/SmallA?$format=json&$select=TimeValue"));
	        ClientResponse<String> response = request.get(String.class);
	        assertEquals(200, response.getStatus());
		} finally {
			es.stop();
		}
	}

	@Test public void testConnectionProperties() throws Exception {
		EmbeddedServer es = new EmbeddedServer();
		es.start(new EmbeddedConfiguration());
		try {
			ModelMetaData mmd = new ModelMetaData();
			mmd.setName("m");
			mmd.setModelType(Type.VIRTUAL);
			mmd.setSchemaSourceType("ddl");
			mmd.setSchemaText("create view v as select 1");

			Properties props = new Properties();
			props.setProperty(ODBCServerRemoteImpl.CONNECTION_PROPERTY_PREFIX + ExecutionProperties.RESULT_SET_CACHE_MODE, "true");

			es.deployVDB("northwind", mmd);

			TeiidDriver td = es.getDriver();

			LocalClient lc = new LocalClient("northwind", 1, props);
			lc.setDriver(td);
			ConnectionImpl impl = lc.getConnection();

			assertEquals("true", impl.getExecutionProperty(ExecutionProperties.RESULT_SET_CACHE_MODE));
		} finally {
			es.stop();
		}
	}

	private OEntity createCustomersEntity(EdmDataServices metadata) {
		EdmEntitySet entitySet = metadata.findEdmEntitySet("Customers");
		OEntityKey entityKey = OEntityKey.parse("CustomerID='12'");
		ArrayList<OProperty<?>> properties = new ArrayList<OProperty<?>>();
		properties.add(OProperties.string("CompanyName", "teiid"));
		properties.add(OProperties.string("ContactName", "contact-name"));
		properties.add(OProperties.string("ContactTitle", "contact-title"));
		properties.add(OProperties.string("Address", "address"));
		properties.add(OProperties.string("City", "city"));
		properties.add(OProperties.string("Region", "region"));
		properties.add(OProperties.string("PostalCode", "postal-code"));
		properties.add(OProperties.string("Country", "country"));
		properties.add(OProperties.string("Phone", "555-1212"));
		properties.add(OProperties.string("Fax", "555-1212"));
		OEntity entity = OEntities.create(entitySet, entityKey, properties,null);
		return entity;
	}
	*/
}
