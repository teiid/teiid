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
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.ws.rs.core.MediaType;

import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.test.BaseResourceTest;
import org.jboss.resteasy.test.EmbeddedContainer;
import org.jboss.resteasy.test.TestPortProvider;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.odata4j.core.OCollection;
import org.odata4j.core.OComplexObject;
import org.odata4j.core.OEntities;
import org.odata4j.core.OEntity;
import org.odata4j.core.OEntityKey;
import org.odata4j.core.OProperties;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmSimpleType;
import org.odata4j.edm.EdmType;
import org.odata4j.format.xml.EdmxFormatWriter;
import org.odata4j.producer.QueryInfo;
import org.odata4j.producer.Responses;
import org.odata4j.producer.resources.EntitiesRequestResource;
import org.odata4j.producer.resources.EntityRequestResource;
import org.odata4j.producer.resources.MetadataResource;
import org.odata4j.producer.resources.ODataBatchProvider;
import org.odata4j.producer.resources.ServiceDocumentResource;
import org.teiid.adminapi.Admin.SchemaObjectType;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.service.AutoGenDataService;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.ExecutionProperties;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.json.simple.ContentHandler;
import org.teiid.json.simple.JSONParser;
import org.teiid.json.simple.ParseException;
import org.teiid.json.simple.SimpleContentHandler;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.odbc.ODBCServerRemoteImpl;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.HardCodedExecutionFactory;

@SuppressWarnings("nls")
public class TestODataIntegration extends BaseResourceTest {
	
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

	private static TransformationMetadata metadata;
    
	@BeforeClass
	public static void before() throws Exception {	    
		deployment = EmbeddedContainer.start("/odata/northwind");
		dispatcher = deployment.getDispatcher();
		deployment.getRegistry().addPerRequestResource(EntitiesRequestResource.class);
		deployment.getRegistry().addPerRequestResource(EntityRequestResource.class);
		deployment.getRegistry().addPerRequestResource(MetadataResource.class);
		deployment.getRegistry().addPerRequestResource(ServiceDocumentResource.class);
		deployment.getProviderFactory().registerProvider(ODataBatchProvider.class);
		deployment.getProviderFactory().registerProvider(ODataExceptionMappingProvider.class);
		deployment.getProviderFactory().registerProvider(MockProvider.class);		
		metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl")),"northwind", "nw");		
	}	
	
	@Test
	public void testMetadata() throws Exception {
		Client client = mockClient();
		MockProvider.CLIENT = client;
		
		StringWriter sw = new StringWriter();
		
		EdmxFormatWriter.write(client.getMetadata(), sw);
		
        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/$metadata"));
        ClientResponse<String> response = request.get(String.class);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals(sw.toString(), response.getEntity());
	}
	
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

    protected Client mockClient() {
        Client client = mock(Client.class);
        VDBMetaData vdb = mock(VDBMetaData.class);
		stub(client.getMetadataStore()).toReturn(metadata.getMetadataStore());	
		EdmDataServices eds = LocalClient.buildMetadata(vdb, metadata.getMetadataStore());
		stub(client.getMetadata()).toReturn(eds);
        return client;
    }

	@Test
	public void testProjectedColumns() throws Exception {
		Client client = mockClient();
		MockProvider.CLIENT = client;
		ArgumentCaptor<Query> sql = ArgumentCaptor.forClass(Query.class);
		
		OEntity entity = createCustomersEntity(client.getMetadata());
		ArrayList<OEntity> list = new ArrayList<OEntity>();
		list.add(entity);
		
		EntityList result = Mockito.mock(EntityList.class);
        EdmEntitySet entitySet = client.getMetadata().getEdmEntitySet("nw.Customers");
        when(result.getEntitySet()).thenReturn(entitySet);
        when(result.getEntities()).thenReturn(Arrays.asList(entity));
		when(result.size()).thenReturn(1);
		when(result.iterator()).thenReturn(list.iterator());
		
		when(client.executeSQL(any(Query.class), anyListOf(SQLParam.class), 
		        any(QueryInfo.class), any(EntityCollector.class))).thenReturn(result);
		
        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/Customers?$select=CustomerID,CompanyName,Address"));
        ClientResponse<String> response = request.get(String.class);
        verify(client).executeSQL(sql.capture(),  anyListOf(SQLParam.class), 
                any(QueryInfo.class), any(EntityCollector.class));
        
        Assert.assertEquals("SELECT g0.CustomerID, g0.CompanyName, g0.Address FROM nw.Customers AS g0 ORDER BY g0.CustomerID", sql.getValue().toString());
        Assert.assertEquals(200, response.getStatus());
        Assert.assertTrue(response.getEntity().contains("nw.Customer"));		
        Assert.assertTrue(!response.getEntity().contains("//Customer"));
	}	
	
	@Test
	public void testCheckGeneratedColumns() throws Exception {
		Client client = mockClient();
		MockProvider.CLIENT = client;
		ArgumentCaptor<Command> insertCmd = ArgumentCaptor.forClass(Command.class);
		ArgumentCaptor<Query> sql = ArgumentCaptor.forClass(Query.class);
		
		OEntity entity = createCustomersEntity(client.getMetadata());
		ArrayList<OEntity> list = new ArrayList<OEntity>();
		list.add(entity);
		
		EntityList result = Mockito.mock(EntityList.class);
        EdmEntitySet entitySet = client.getMetadata().getEdmEntitySet("nw.Customers");
        when(result.getEntitySet()).thenReturn(entitySet);
        when(result.getEntities()).thenReturn(list);
		when(result.size()).thenReturn(1);
		when(result.iterator()).thenReturn(list.iterator());
		
		when(client.executeSQL(any(Query.class), anyListOf(SQLParam.class), 
		        any(QueryInfo.class), any(EntityCollector.class))).thenReturn(result);
		
		UpdateResponse respose = new UpdateResponse() {
			@Override
			public int getUpdateCount() {
				return 1;
			}
			@Override
			public Map<String, Object> getGeneratedKeys() {
				HashMap<String, Object> map = new HashMap<String, Object>();
				map.put("CustomerID", 1234);
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
        verify(client).executeSQL(sql.capture(),  anyListOf(SQLParam.class), 
                any(QueryInfo.class), any(EntityCollector.class));
        
        Assert.assertEquals("INSERT INTO nw.Customers (CompanyName, ContactName, ContactTitle, "
                + "Address, City, Region, PostalCode, Country, Phone) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                insertCmd.getValue().toString());
        Assert.assertEquals("SELECT g0.* FROM nw.Customers AS g0 "
                + "WHERE g0.CustomerID = 1234 ORDER BY g0.CustomerID", sql.getValue().toString());
        Assert.assertEquals(201, response.getStatus());
	}	
	
	@Test
	@SuppressWarnings("unchecked")
	public void testProcedureOptions() throws Exception {
		Client client = mockClient();
		MockProvider.CLIENT = client;
		ArgumentCaptor<Query> sql = ArgumentCaptor.forClass(Query.class);
		ArgumentCaptor<List> params = ArgumentCaptor.forClass(List.class);
		
		OCollection<OComplexObject> collection = mock(OCollection.class);
		stub(collection.getType()).toReturn(mock(EdmType.class));
		stub(collection.iterator()).toReturn(mock(Iterator.class));
		
		ComplexCollection result = mock(ComplexCollection.class);
		stub(result.getCollection()).toReturn(collection);
		stub(result.getCollectionName()).toReturn("any");
		
		when(client.executeSQL(any(Query.class), anyListOf(SQLParam.class), 
                    any(QueryInfo.class), any(EntityCollector.class))).thenReturn(result);
		
		String url = TestPortProvider.generateURL("/odata/northwind/getSuppliers?p3=2&$filter=SupplierID eq 1");
        ClientRequest request = new ClientRequest(url);
        ClientResponse<String> response = request.get(String.class);
        verify(client).executeSQL(sql.capture(), params.capture(),  any(QueryInfo.class),any(EntityCollector.class));
        
        Assert.assertEquals("SELECT g0.* FROM LATERAL(EXEC nw.getSuppliers(p3 => ?)) AS g0 WHERE g0.SupplierID = ?", 
                sql.getValue().toString());        
        Assert.assertEquals(Integer.valueOf(2), ((SQLParam)params.getValue().get(0)).value);
        Assert.assertEquals(Integer.valueOf(1), ((SQLParam)params.getValue().get(1)).value);
        Assert.assertEquals(200, response.getStatus());
	}
	
    @Test
    @SuppressWarnings("unchecked")
    public void testProcedureOptions2() throws Exception {
        Client client = mockClient();
        MockProvider.CLIENT = client;
        ArgumentCaptor<Query> sql = ArgumentCaptor.forClass(Query.class);
        ArgumentCaptor<List> params = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<QueryInfo> queryInfo = ArgumentCaptor.forClass(QueryInfo.class);
        
        OCollection<OComplexObject> collection = mock(OCollection.class);
        stub(collection.getType()).toReturn(mock(EdmType.class));
        stub(collection.iterator()).toReturn(mock(Iterator.class));
        
        ComplexCollection result = mock(ComplexCollection.class);
        stub(result.getCollection()).toReturn(collection);
        stub(result.getCollectionName()).toReturn("any");
        
        when(client.executeSQL(any(Query.class), anyListOf(SQLParam.class), 
                    any(QueryInfo.class), any(EntityCollector.class))).thenReturn(result);
        
        String url = TestPortProvider.generateURL("/odata/northwind/getSuppliers?p3=2&$select=SupplierID&$top=10&$orderby=SupplierID");
        ClientRequest request = new ClientRequest(url);
        ClientResponse<String> response = request.get(String.class);
        verify(client).executeSQL(sql.capture(), params.capture(),  queryInfo.capture(),any(EntityCollector.class));
        
        Assert.assertEquals("SELECT g0.SupplierID FROM LATERAL(EXEC nw.getSuppliers(p3 => ?)) AS g0 ORDER BY g0.SupplierID", 
                sql.getValue().toString());        
        Assert.assertEquals(Integer.valueOf(2), ((SQLParam)params.getValue().get(0)).value);
        Assert.assertEquals(Integer.valueOf(10), queryInfo.getValue().top);
        Assert.assertEquals(200, response.getStatus());
    }	
	
    @Test
    @SuppressWarnings("unchecked")
    public void testProcedure() throws Exception {
        Client client = mockClient();
        MockProvider.CLIENT = client;
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<List> params = ArgumentCaptor.forClass(List.class);
        
        when(client.executeCall(any(String.class), anyListOf(SQLParam.class), any(EdmType.class))).thenReturn(Responses.simple(EdmSimpleType.INT32, "return", null));
        
        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/getCustomers?p2=datetime'2011-09-11T00:00:00'&p3=2.0M"));
        ClientResponse<String> response = request.get(String.class);
        verify(client).executeCall(sql.capture(), params.capture(), any(EdmType.class));
        
        Assert.assertEquals("{ ? = CALL nw.getCustomers(p2 => ?, p3 => ?) }", sql.getValue().toString());
        Assert.assertEquals(TimestampUtil.createTimestamp(111, 8, 11, 0, 0, 0, 0), ((SQLParam)params.getValue().get(0)).value);
        Assert.assertEquals(BigDecimal.valueOf(2.0), ((SQLParam)params.getValue().get(1)).value);
        Assert.assertEquals(200, response.getStatus());
    }
    
	
	@Test
	public void testProcedureNoReturn() throws Exception {
		EmbeddedServer es = new EmbeddedServer();
		es.start(new EmbeddedConfiguration());
		try {
			ModelMetaData mmd = new ModelMetaData();
			mmd.setName("vw");
			mmd.addSourceMetadata("ddl", "create procedure proc () as BEGIN END");
			mmd.setModelType(Type.VIRTUAL);
			
			es.deployVDB("northwind", mmd);
			
			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			LocalClient lc = new LocalClient("northwind", "1", props);
			lc.setDriver(td);
			MockProvider.CLIENT = lc;
			
	        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/proc"));
	        ClientResponse<String> response = request.post(String.class);
	        Assert.assertEquals(204, response.getStatus());
	        
		} finally {
			es.stop();
		}
	}
	
	@Test
	public void testSkipNoPKTable() throws Exception {
		Client client = mockClient();
		MockProvider.CLIENT = client;
		
        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/NoPKTable"));
        ClientResponse<String> response = request.get(String.class);
        
        Assert.assertEquals(404, response.getStatus());
        String endsWith = "<error xmlns=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\">"
                + "<code>NotFoundException</code>"
                + "<message lang=\"en-US\">TEIID16011 EntitySet \"NoPKTable\" is not found; "
                + "Check the spelling, use modelName.tableName; The table that representing "
                + "the Entity type must either have a PRIMARY KEY or UNIQUE key(s)</message>"
                + "</error>";
        Assert.assertTrue(response.getEntity().endsWith(endsWith));
	}	
	
	@Test
	public void testError() throws Exception {
		Client client = mockClient();
		MockProvider.CLIENT = client;
		
		when(client.executeSQL(any(Query.class), anyListOf(SQLParam.class),  
		        any(QueryInfo.class), any(EntityCollector.class))).thenThrow(new NullPointerException());
		
        ClientRequest request = new ClientRequest(TestPortProvider.generateURL(
                "/odata/northwind/Customers?$select=CustomerID,CompanyName,Address"));
        ClientResponse<String> response = request.get(String.class);
        
        Assert.assertEquals(500, response.getStatus());
        String endsWith = "<error xmlns=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\">"
                + "<code>ServerErrorException</code>"
                + "<message lang=\"en-US\">Internal Server Error</message></error>";
        Assert.assertTrue(response.getEntity().endsWith(endsWith));
	}	
	
	@Test
	public void testSelect() throws Exception {
		Client client = mockClient();
		MockProvider.CLIENT = client;
		ArgumentCaptor<Query> sql = ArgumentCaptor.forClass(Query.class);
		
		OEntity entity = createCustomersEntity(client.getMetadata());
		ArrayList<OEntity> list = new ArrayList<OEntity>();
		list.add(entity);
		
		EntityList result = Mockito.mock(EntityList.class);
		EdmEntitySet entitySet = client.getMetadata().getEdmEntitySet("nw.Customers");
		when(result.getEntitySet()).thenReturn(entitySet);
		when(result.getEntities()).thenReturn(Arrays.asList(entity));
		when(result.size()).thenReturn(1);
		when(result.iterator()).thenReturn(list.iterator());
		
		when(client.executeSQL(any(Query.class), anyListOf(SQLParam.class), any(QueryInfo.class), 
		        any(EntityCollector.class))).thenReturn(result);
		
        ClientRequest request = new ClientRequest(
                TestPortProvider.generateURL("/odata/northwind/Customers?$select=CustomerID,CompanyName,Address"));
        ClientResponse<String> response = request.get(String.class);
        verify(client).executeSQL(sql.capture(),  anyListOf(SQLParam.class), any(QueryInfo.class), 
                any(EntityCollector.class));
        
        Assert.assertEquals("SELECT g0.CustomerID, g0.CompanyName, g0.Address FROM nw.Customers "
                + "AS g0 ORDER BY g0.CustomerID", sql.getValue().toString());
        Assert.assertEquals(200, response.getStatus());
        
	}
	
    @Test
    public void testSelectWithExpand() throws Exception {
        Client client = mockClient();
        MockProvider.CLIENT = client;
        ArgumentCaptor<Query> sql = ArgumentCaptor.forClass(Query.class);
        
        OEntity entity = createCustomersEntity(client.getMetadata());
        ArrayList<OEntity> list = new ArrayList<OEntity>();
        list.add(entity);
        
        EntityList result = Mockito.mock(EntityList.class);
        EdmEntitySet entitySet = client.getMetadata().getEdmEntitySet("nw.Customers");
        when(result.getEntitySet()).thenReturn(entitySet);
        when(result.getEntities()).thenReturn(Arrays.asList(entity));
        when(result.size()).thenReturn(1);
        when(result.iterator()).thenReturn(list.iterator());
        
        when(client.executeSQL(any(Query.class), anyListOf(SQLParam.class), any(QueryInfo.class), 
                any(EntityCollector.class))).thenReturn(result);
        
        ClientRequest request = new ClientRequest(
                TestPortProvider.generateURL("/odata/northwind/Customers?$expand=Orders"));
        ClientResponse<String> response = request.get(String.class);
        verify(client).executeSQL(sql.capture(),  anyListOf(SQLParam.class), any(QueryInfo.class), 
                any(EntityCollector.class));
        
        Assert.assertEquals("SELECT g0.*, g1.* FROM nw.Customers AS g0 "
                + "INNER JOIN nw.Orders AS g1 ON g0.CustomerID = g1.CustomerID "
                + "ORDER BY g0.CustomerID", sql.getValue().toString());
        Assert.assertEquals(200, response.getStatus());
    }   	
	
	@Test
	public void testGetEntity() throws Exception {
		EmbeddedServer es = new EmbeddedServer();
		es.start(new EmbeddedConfiguration());
		try {
			ModelMetaData mmd = new ModelMetaData();
			mmd.setName("vw");
			mmd.addSourceMetadata("ddl", 
			        "create view x ("
			        + "a string primary key, "
			        + "b string) "
			        + "as select 'a', 'b' union all select 'c', 'd';"
			        +"\n"
					+ "create view y ("
					+ "a1 string primary key, "
					+ "b1 string, "
					+ "foreign key (a1) references x (a)) "
					+ "as select 'a', 'b' union all select 'c', 'd';"
					+"\n"
                    + "create view z ("
                    + "a1 string, "
                    + "b1 string primary key,"
                    + "foreign key (a1) references x (a)) "
                    + "as select 'a', 'b' union all select 'c', 'd';"					
			        );
			mmd.setModelType(Type.VIRTUAL);
			es.deployVDB("northwind", mmd);
			
			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			LocalClient lc = new LocalClient("northwind", "1", props);
			lc.setDriver(td);
			MockProvider.CLIENT = lc;
			
	        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x('a')"));
	        ClientResponse<String> response = request.get(String.class);
	        assertTrue(response.getEntity().contains("('a')"));
	        Assert.assertEquals(200, response.getStatus());
	        
	        request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x?$skip=2&$format=json"));
	        response = request.get(String.class);
	        Assert.assertEquals(200, response.getStatus());
	        assertFalse(response.getEntity().contains("('a')"));
	        
	        //a missing entity should be a 404
	        request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x('v')"));
	        response = request.get(String.class);
	        Assert.assertEquals(404, response.getStatus());
	        
	        //filter is not applicable to getEntity
	        request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x('a')?$filter=b eq 'd'"));
	        response = request.get(String.class);
	        assertTrue(response.getEntity().contains("('a')"));
	        Assert.assertEquals(200, response.getStatus());
	        
	        //ensure that a child is nav property works - one -to-one
	        request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x('a')/y"));
	        response = request.get(String.class);
	        assertTrue(!response.getEntity().contains("<feed"));
	        Assert.assertEquals(200, response.getStatus());
	        
            //ensure that a child is nav property works - one -to-many
            request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x('a')/z"));
            response = request.get(String.class);
            assertTrue(response.getEntity().contains("<feed"));
            Assert.assertEquals(200, response.getStatus());
	        
            // invalid entity - should be 404 a1 = a can't be also c
	        request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x('a')/y?$filter=a1 eq 'c'"));
	        response = request.get(String.class);
	        assertFalse(response.getEntity().contains("('c')"));
	        Assert.assertEquals(404, response.getStatus());
		} finally {
			es.stop();
		}
	}
	
	@Test public void testInvalidCharacterReplacement() throws Exception {
		EmbeddedServer es = new EmbeddedServer();
		es.start(new EmbeddedConfiguration());
		try {
			ModelMetaData mmd = new ModelMetaData();
			mmd.setName("vw");
			mmd.addSourceMetadata("ddl", "create view x (a string primary key, b char, c string[], d integer) as select 'ab\u0000cd\u0001', char(22), ('a\u00021','b1'), 1;");
			mmd.setModelType(Type.VIRTUAL);
			es.deployVDB("northwind", mmd);
			
			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			props.setProperty(LocalClient.INVALID_CHARACTER_REPLACEMENT, " ");
			LocalClient lc = new LocalClient("northwind", "1", props);
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
			mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer[], c string[][]) as select 'x', (1, 2, 3), (('a','b'),('c','d'));");
			mmd.setModelType(Type.VIRTUAL);
			es.deployVDB("northwind", mmd);
			
			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			LocalClient lc = new LocalClient("northwind", "1", props);
			lc.setDriver(td);
			MockProvider.CLIENT = lc;
			
	        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x?$format=json&$select=a,b"));
	        ClientResponse<String> response = request.get(String.class);
	        assertEquals(200, response.getStatus());	       
	        assertTrue(response.getEntity().contains("1, 2, 3"));
	        
	        String expected = "{\n" + 
	                "\"d\" : {\n" + 
	                "\"results\" : [\n" + 
	                "{\n" + 
	                "\"__metadata\" : {\n" + 
	                "\"uri\" : \"http://localhost:8081/odata/northwind/vw.x('x')\", \"type\" : \"vw.x\"\n" + 
	                "}, \"a\" : \"x\", \"b\" : {\n" + 
	                "\"results\" : [\n" + 
	                "1, 2, 3\n" + 
	                "]\n" + 
	                "}, \"c\" : {\n" + 
	                "\"results\" : [\n" + 
	                "{\n" + 
	                "\"results\" : [\n" + 
	                "\"a\", \"b\"\n" + 
	                "]\n" + 
	                "}, {\n" + 
	                "\"results\" : [\n" + 
	                "\"c\", \"d\"\n" + 
	                "]\n" + 
	                "}\n" + 
	                "]\n" + 
	                "}\n" + 
	                "}\n" + 
	                "]\n" + 
	                "}\n" + 
	                "}";
	        request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x?$format=json"));
	        response = request.get(String.class);
	        assertEquals(expected, response.getEntity());
	        assertEquals(200, response.getStatus());
		} finally {
			es.stop();
		}
	}
	
    @Test 
    public void testArrayResultsInAtom() throws Exception {
        EmbeddedServer es = new EmbeddedServer();
        es.start(new EmbeddedConfiguration());
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("vw");
            mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer[], c string[][]) as select 'x', (1, 2, 3), (('a','b'),('c','d'));");
            mmd.setModelType(Type.VIRTUAL);
            es.deployVDB("northwind", mmd);
            
            TeiidDriver td = es.getDriver();
            Properties props = new Properties();
            LocalClient lc = new LocalClient("northwind", "1", props);
            lc.setDriver(td);
            MockProvider.CLIENT = lc;
            
            ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x?$select=a,b"));
            ClientResponse<String> response = request.get(String.class);
            assertEquals(200, response.getStatus());             
            String expected = 
                    "<content type=\"application/xml\">" + 
                        "<m:properties>" + 
                            "<d:a>x</d:a>" + 
                            "<d:b m:type=\"Collection(Edm.Int32)\">" + 
                                "<d:element>1</d:element>" + 
                                "<d:element>2</d:element>" + 
                                "<d:element>3</d:element>" + 
                            "</d:b>" + 
                        "</m:properties>" + 
                    "</content>";
            assertTrue(response.getEntity().contains(expected));
            
            expected = "<m:properties>" +
                        "<d:a>x</d:a>" + 
                        "<d:b m:type=\"Collection(Edm.Int32)\">" + 
                            "<d:element>1</d:element>" + 
                            "<d:element>2</d:element>" + 
                            "<d:element>3</d:element>" + 
                        "</d:b>" + 
                        "<d:c m:type=\"Collection(Collection(Edm.String))\">" + 
                            "<d:element m:type=\"Collection(Edm.String)\">" + 
                                "<d:element>a</d:element>" + 
                                "<d:element>b</d:element>" + 
                            "</d:element>" + 
                            "<d:element m:type=\"Collection(Edm.String)\">" + 
                                "<d:element>c</d:element>" + 
                                "<d:element>d</d:element>" + 
                            "</d:element>" + 
                        "</d:c>" +
                    "</m:properties>";
            request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x"));
            response = request.get(String.class);
            assertTrue(response.getEntity(), response.getEntity().contains(expected));
            assertEquals(200, response.getStatus());
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
			mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer) as select 'xyz', 123 union all select 'abc', 456;");
			mmd.setModelType(Type.VIRTUAL);
			es.deployVDB("northwind", mmd);
			
			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			props.setProperty("batch-size", "1");
			LocalClient lc = new LocalClient("northwind", "1", props);
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
	        URL url = new URL((String) contentHandler.value);
	        String skip = getQueryParameter(URLDecoder.decode(url.getQuery(), "UTF-8"), "$skiptoken");
	        assertTrue(skip.indexOf(LocalClient.DELIMITER) != -1);
	        
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
	
    public String getQueryParameter(String queryPath, String param) {
        if (queryPath != null) {
            StringTokenizer st = new StringTokenizer(queryPath, "&");
            while (st.hasMoreTokens()) {
                String token = st.nextToken();
                int index = token.indexOf('=');
                if (index != -1) {
                    String key = token.substring(0, index);
                    String value = token.substring(index + 1);
                    if (key.equals(param)) {
                        return value;
                    }
                }
            }
        }
        return null;
    }	
	
    @Test public void testNoSkipToken() throws Exception {
        EmbeddedServer es = new EmbeddedServer();
        es.start(new EmbeddedConfiguration());
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("vw");
            mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer) as select 'xyz', 123 union all select 'abc', 456;");
            mmd.setModelType(Type.VIRTUAL);
            es.deployVDB("northwind", mmd);
            
            TeiidDriver td = es.getDriver();
            Properties props = new Properties();
            props.setProperty("batch-size", "0");
            LocalClient lc = new LocalClient("northwind", "1", props);
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
            assertTrue(response.getEntity().contains("xyz"));
            
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
			mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer) as select 'xyz', 123 union all select 'abc', 456;");
			mmd.setModelType(Type.VIRTUAL);
			es.deployVDB("northwind", mmd);
			
			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			props.setProperty("batch-size", "1");
			LocalClient lc = new LocalClient("northwind", "1", props);
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
			mmd.addSourceMetadata("ddl", "create foreign table x (a string, b string, c integer, primary key (a, b)) options (updatable true);");
			mmd.addSourceMapping("x", "x", null);
			es.deployVDB("northwind", mmd);
			
			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			LocalClient lc = new LocalClient("northwind", "1", props);
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
	        assertEquals(204, response.getStatus());
		} finally {
			es.stop();
		}
	}

    @Test public void testUpdates() throws Exception {
        EmbeddedServer es = new EmbeddedServer();
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory() {
            @Override
            public boolean supportsCompareCriteriaEquals() {
                return true;
            }
        };
        hc.addUpdate("DELETE FROM x WHERE x.a = 'a'", new int[] {0});
        hc.addUpdate("UPDATE x SET c = 5 WHERE x.a = 'a'", new int[] {0});
        es.addTranslator("x", hc);
        es.start(new EmbeddedConfiguration());
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("m");
            mmd.addSourceMetadata("ddl", "create foreign table x (a string, b string, c integer, primary key (a)) options (updatable true);");
            mmd.addSourceMapping("x", "x", null);
            es.deployVDB("northwind", mmd);
            
            TeiidDriver td = es.getDriver();
            Properties props = new Properties();
            LocalClient lc = new LocalClient("northwind", "1", props);
            lc.setDriver(td);
            MockProvider.CLIENT = lc;
            
            ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x(a='a')"));
            ClientResponse<String> response = request.delete(String.class);
            assertEquals(404, response.getStatus());
                        
            //not supported
            //request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x(a='a',b='b')/c/$value"));
            //request.body("text/plain", "5");
            //response = request.put(String.class);
            
            request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x(a='a',b='b')"));
            request.body("application/json", "{\"c\":5}");
            response = request.put(String.class);
            assertEquals(404, response.getStatus());
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
			mmd.addSourceMetadata("ddl", "create foreign table x (a string, b string, c integer, primary key (a, b)) options (updatable true);");
			mmd.addSourceMapping("x", "x", null);
			es.deployVDB("northwind", mmd);
			
			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			LocalClient lc = new LocalClient("northwind", "1", props);
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
			mmd.addSourceMetadata("ddl", "create foreign procedure x () returns table(y string);");
			mmd.addSourceMapping("x", "x", null);
			es.deployVDB("northwind", mmd);
			
			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			LocalClient lc = new LocalClient("northwind", "1", props);
			lc.setDriver(td);
			MockProvider.CLIENT = lc;
			
			String expected = "{\n" + 
			        "\"d\" : [\n" + 
			        "{\n" + 
			        "\"y\" : \"x\"\n" + 
			        "}, {\n" + 
			        "\"y\" : \"y\"\n" + 
			        "}\n" + 
			        "]\n" + 
			        "}";
	        ClientRequest clientRequest = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x?$format=json"));
	        ClientResponse<String> response = clientRequest.get(String.class);
	        assertEquals(200, response.getStatus());
	        assertEquals(expected, response.getEntity());
		} finally {
			es.stop();
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test 
	public void testBasicTypes() throws Exception {
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
			LocalClient lc = new LocalClient("northwind", "1", props);
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
			mmd.addSourceMetadata("ddl", "create view v as select 1");
			
			Properties props = new Properties();
			props.setProperty(ODBCServerRemoteImpl.CONNECTION_PROPERTY_PREFIX + ExecutionProperties.RESULT_SET_CACHE_MODE, "true");
			
			es.deployVDB("northwind", mmd);
			
			TeiidDriver td = es.getDriver();
			
			LocalClient lc = new LocalClient("northwind", "1", props);
			lc.setDriver(td);
			ConnectionImpl impl = lc.getConnection();
			
			assertEquals("true", impl.getExecutionProperty(ExecutionProperties.RESULT_SET_CACHE_MODE));
		} finally {
			es.stop();
		}
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void testEmbeddedComplexType() throws Exception {
		EmbeddedServer es = new EmbeddedServer();
		es.start(new EmbeddedConfiguration());
		try {
			ModelMetaData mmd = new ModelMetaData();
			mmd.setName("m");
			mmd.setModelType(Type.VIRTUAL);
			mmd.addSourceMetadata("ddl", "CREATE VIEW Employees ( EmployeeID integer primary key, "
					+ "LastName varchar(20), FirstName varchar(10), "
					+ "Address varchar(60) options (\"teiid_odata:columngroup\" 'Address', \"teiid_odata:complextype\" 'NorthwindModel.Address'),"
					+ "City varchar(15) options (\"teiid_odata:columngroup\" 'Address', \"teiid_odata:complextype\" 'NorthwindModel.Address')) as "
					+ "select 1, 'wayne', 'john', '123 place', 'hollywood'");
			
			es.deployVDB("northwind", mmd);
			
			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			LocalClient lc = new LocalClient("northwind", "1", props);
			lc.setDriver(td);
			MockProvider.CLIENT = lc;
			
	        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/Employees?$format=json&$select=Address"));
	        ClientResponse<String> response = request.get(String.class);
	        assertEquals(200, response.getStatus());
	        JSONParser parser = new JSONParser();
	        SimpleContentHandler sch = new SimpleContentHandler();
	        parser.parse(response.getEntity(), sch);
	        Map<String, ?> result = (Map<String, ?>)sch.getResult();
	        List<Object> results = (List<Object>)((Map<String, ?>)result.get("d")).get("results");
	        result = (Map<String, ?>)results.get(0);
	        assertEquals("123 place", result.get("Address"));
	        assertNull(result.get("City"));
		} finally {
			es.stop();
		}
	}
	
	@Test
	public void testAmbiguities() throws Exception {
		EmbeddedServer es = new EmbeddedServer();
		es.start(new EmbeddedConfiguration());
		try {
			ModelMetaData mmd = new ModelMetaData();
			mmd.setName("vw");
			mmd.addSourceMetadata("ddl", "create view x (a string primary key) as select 'a'; create virtual procedure y () returns table(y string) as select 'a';");
			mmd.setModelType(Type.VIRTUAL);
			
			ModelMetaData mmd1 = new ModelMetaData();
			mmd1.setName("vw1");
			mmd1.addSourceMetadata("ddl", "create view x (a string primary key) as select 'a'; create virtual procedure y () returns table(y string) as select 'a';");
			mmd1.setModelType(Type.VIRTUAL);
			es.deployVDB("northwind", mmd, mmd1);
			
			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			LocalClient lc = new LocalClient("northwind", "1", props);
			lc.setDriver(td);
			MockProvider.CLIENT = lc;
			
	        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x('a')"));
	        ClientResponse<String> response = request.get(String.class);
	        Assert.assertEquals(404, response.getStatus());
	        
	        request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/y"));
	        response = request.get(String.class);
	        Assert.assertEquals(404, response.getStatus());
		} finally {
			es.stop();
		}
	}
	
	@Test
	public void testUnselectable() throws Exception {
		EmbeddedServer es = new EmbeddedServer();
		es.start(new EmbeddedConfiguration());
		try {
			ModelMetaData mmd = new ModelMetaData();
			mmd.setName("vw");
			mmd.addSourceMetadata("ddl", "create view x (a string primary key, b string options (selectable false)) as select 'a', 'hello';");
			mmd.setModelType(Type.VIRTUAL);
			es.deployVDB("northwind", mmd);
			
			TeiidDriver td = es.getDriver();
			Properties props = new Properties();
			LocalClient lc = new LocalClient("northwind", "1", props);
			lc.setDriver(td);
			MockProvider.CLIENT = lc;
			
	        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x('a')"));
	        ClientResponse<String> response = request.get(String.class);
	        Assert.assertEquals(200, response.getStatus());
	        assertFalse(response.getEntity().contains("hello"));
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
	
    @Test
    @SuppressWarnings("unchecked")
    public void testProperty() throws Exception {
        EmbeddedServer es = new EmbeddedServer();
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory() {
            @Override
            public boolean supportsCompareCriteriaEquals() {
                return true;
            }
        };
        hc.addData("SELECT x.a FROM x WHERE x.a = 'a'", Arrays.asList(Arrays.asList("x"), Arrays.asList("y")));
        es.addTranslator("x", hc);
        es.start(new EmbeddedConfiguration());
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("m");
            mmd.addSourceMetadata("ddl", "create foreign table x (a string, b string, c integer, primary key (a)) options (updatable true);");
            mmd.addSourceMapping("x", "x", null);
            es.deployVDB("northwind", mmd);
            
            TeiidDriver td = es.getDriver();
            Properties props = new Properties();
            LocalClient lc = new LocalClient("northwind", "1", props);
            lc.setDriver(td);
            MockProvider.CLIENT = lc;
            
            ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x('a')/a?$format=json"));
            ClientResponse<String> response = request.get(String.class);
            assertEquals(200, response.getStatus());
            String expected = "{\n" + 
                    "\"d\" : {\n" + 
                    "\"a\" : \"x\"\n" + 
                    "}\n" + 
                    "}";
            assertEquals(expected, response.getEntity());
            
            request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x('a')/a/$value?$format=json"));
            response = request.get(String.class);
            assertEquals(200, response.getStatus());
            expected = "x";
            assertEquals(expected, response.getEntity());            

            request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x('a')/a/$value"));
            response = request.get(String.class);
            assertEquals(200, response.getStatus());
            expected = "x";
            assertEquals(expected, response.getEntity());            
            
        } finally {
            es.stop();
        }
    }	
    
    @Test
    @SuppressWarnings("unchecked")
    public void testExpandJson() throws Exception {
        EmbeddedServer es = new EmbeddedServer();
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory() {
            @Override
            public boolean supportsCompareCriteriaEquals() {
                return true;
            }
            public boolean supportsInnerJoins() {
                return true;
            }
            public boolean supportsOuterJoins() {
                return true;
            }            
        };
        hc.addData("SELECT x.a, x.b, y.a1, y.b1 FROM x, y WHERE x.a = y.a1", 
                Arrays.asList(Arrays.asList("xa", "xb", "xa", "yb"), 
                        Arrays.asList("xa1", "xb1", "xa1", "yb1"),
                        Arrays.asList("xa2", "xb2", "xa2", "yb2")
                        ));
        hc.addData("SELECT x.a, x.b, z.a1, z.b1 FROM x, z WHERE x.a = z.a1", 
                Arrays.asList(Arrays.asList("xa", "xb", "xa", "zb"), 
                        Arrays.asList("xa", "xb", "xa", "zb1"),
                        Arrays.asList("xa", "xb", "xa", "zb2")
                        ));

        es.addTranslator("x", hc);
        es.start(new EmbeddedConfiguration());
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("m");
            mmd.addSourceMetadata("ddl", 
                    "create foreign table x ("
                    + "a string primary key, "
                    + "b string);"
                    +"\n"
                    + "create foreign table y ("
                    + "a1 string primary key, "
                    + "b1 string, "
                    + "foreign key (a1) references x (a)); "
                    +"\n"
                    + "create foreign table z ("
                    + "a1 string, "
                    + "b1 string primary key,"
                    + "foreign key (a1) references x (a));");
            
            mmd.addSourceMapping("x", "x", null);
            es.deployVDB("northwind", mmd);
            
            TeiidDriver td = es.getDriver();
            Properties props = new Properties();
            LocalClient lc = new LocalClient("northwind", "1", props);
            lc.setDriver(td);
            MockProvider.CLIENT = lc;

            // in one-2-one
            String url = TestPortProvider.generateURL("/odata/northwind/x?$expand=y&$format=json&$inlinecount=allpages");
            ClientRequest request = new ClientRequest(url);
            ClientResponse<String> response = request.get(String.class);
            assertEquals(200, response.getStatus());
            String expected = "{\n" + 
                    "\"d\" : {\n" + 
                    "\"results\" : [\n" + 
                    "{\n" + 
                    "\"__metadata\" : {\n" + 
                    "\"uri\" : \"http://localhost:8081/odata/northwind/m.x('xa')\", \"type\" : \"m.x\"\n" + 
                    "}, \"a\" : \"xa\", \"b\" : \"xb\", \"y\" : {\n" + 
                    "\"__metadata\" : {\n" + 
                    "\"uri\" : \"http://localhost:8081/odata/northwind/m.y('xa')\", \"type\" : \"m.y\"\n" + 
                    "}, \"a1\" : \"xa\", \"b1\" : \"yb\"\n" + 
                    "}, \"z\" : {\n" + 
                    "\"__deferred\" : {\n" + 
                    "\"uri\" : \"http://localhost:8081/odata/northwind/m.x('xa')/z\"\n" + 
                    "}\n" + 
                    "}\n" + 
                    "}, {\n" + 
                    "\"__metadata\" : {\n" + 
                    "\"uri\" : \"http://localhost:8081/odata/northwind/m.x('xa1')\", \"type\" : \"m.x\"\n" + 
                    "}, \"a\" : \"xa1\", \"b\" : \"xb1\", \"y\" : {\n" + 
                    "\"__metadata\" : {\n" + 
                    "\"uri\" : \"http://localhost:8081/odata/northwind/m.y('xa1')\", \"type\" : \"m.y\"\n" + 
                    "}, \"a1\" : \"xa1\", \"b1\" : \"yb1\"\n" + 
                    "}, \"z\" : {\n" + 
                    "\"__deferred\" : {\n" + 
                    "\"uri\" : \"http://localhost:8081/odata/northwind/m.x('xa1')/z\"\n" + 
                    "}\n" + 
                    "}\n" + 
                    "}, {\n" + 
                    "\"__metadata\" : {\n" + 
                    "\"uri\" : \"http://localhost:8081/odata/northwind/m.x('xa2')\", \"type\" : \"m.x\"\n" + 
                    "}, \"a\" : \"xa2\", \"b\" : \"xb2\", \"y\" : {\n" + 
                    "\"__metadata\" : {\n" + 
                    "\"uri\" : \"http://localhost:8081/odata/northwind/m.y('xa2')\", \"type\" : \"m.y\"\n" + 
                    "}, \"a1\" : \"xa2\", \"b1\" : \"yb2\"\n" + 
                    "}, \"z\" : {\n" + 
                    "\"__deferred\" : {\n" + 
                    "\"uri\" : \"http://localhost:8081/odata/northwind/m.x('xa2')/z\"\n" + 
                    "}\n" + 
                    "}\n" + 
                    "}\n" + 
                    "]\n" + 
                    "}\n" + 
                    "}";
            assertEquals(expected, response.getEntity());
            
            // in one-2-many
            url = TestPortProvider.generateURL("/odata/northwind/x?$expand=z&$format=json");
            request = new ClientRequest(url);
            response = request.get(String.class);
            assertEquals(200, response.getStatus());
            expected = "{\n" + 
                    "\"d\" : {\n" + 
                    "\"results\" : [\n" + 
                    "{\n" + 
                    "\"__metadata\" : {\n" + 
                    "\"uri\" : \"http://localhost:8081/odata/northwind/m.x('xa')\", \"type\" : \"m.x\"\n" + 
                    "}, \"a\" : \"xa\", \"b\" : \"xb\", \"y\" : {\n" + 
                    "\"__deferred\" : {\n" + 
                    "\"uri\" : \"http://localhost:8081/odata/northwind/m.x('xa')/y\"\n" + 
                    "}\n" +                     
                    "}, \"z\" : {\n" + 
                    "\"results\" : [\n" + 
                    "{\n" + 
                    "\"__metadata\" : {\n" + 
                    "\"uri\" : \"http://localhost:8081/odata/northwind/m.z('zb')\", \"type\" : \"m.z\"\n" + 
                    "}, \"a1\" : \"xa\", \"b1\" : \"zb\"\n" + 
                    "}, {\n" + 
                    "\"__metadata\" : {\n" + 
                    "\"uri\" : \"http://localhost:8081/odata/northwind/m.z('zb1')\", \"type\" : \"m.z\"\n" + 
                    "}, \"a1\" : \"xa\", \"b1\" : \"zb1\"\n" + 
                    "}, {\n" + 
                    "\"__metadata\" : {\n" + 
                    "\"uri\" : \"http://localhost:8081/odata/northwind/m.z('zb2')\", \"type\" : \"m.z\"\n" + 
                    "}, \"a1\" : \"xa\", \"b1\" : \"zb2\"\n" + 
                    "}\n" + 
                    "]\n" + 
                    "}\n" +
                    "}\n" + 
                    "]\n" + 
                    "}\n" + 
                    "}";
            assertEquals(expected, response.getEntity());
        } finally {
            es.stop();
        }
    }
    
    @SuppressWarnings("unchecked")
    @Test 
    public void testExpandXML() throws Exception {
        EmbeddedServer es = new EmbeddedServer();
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory() {
            @Override
            public boolean supportsCompareCriteriaEquals() {
                return true;
            }
            public boolean supportsInnerJoins() {
                return true;
            }
            public boolean supportsOuterJoins() {
                return true;
            }            
        };
        hc.addData("SELECT x.a, x.b, y.a1, y.b1 FROM x, y WHERE x.a = y.a1", 
                Arrays.asList(Arrays.asList("xa", "xb", "xa", "yb"), 
                        Arrays.asList("xa1", "xb1", "xa1", "yb1"),
                        Arrays.asList("xa2", "xb2", "xa2", "yb2")
                        ));
        hc.addData("SELECT x.a, x.b, z.a1, z.b1 FROM x, z WHERE x.a = z.a1", 
                Arrays.asList(Arrays.asList("xa", "xb", "xa", "zb"), 
                        Arrays.asList("xa", "xb", "xa", "zb1"),
                        Arrays.asList("xa", "xb", "xa", "zb2")
                        ));

        es.addTranslator("x", hc);
        es.start(new EmbeddedConfiguration());
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("m");
            mmd.addSourceMetadata("ddl", 
                    "create foreign table x ("
                    + "a string primary key, "
                    + "b string);"
                    +"\n"
                    + "create foreign table y ("
                    + "a1 string primary key, "
                    + "b1 string, "
                    + "foreign key (a1) references x (a)); "
                    +"\n"
                    + "create foreign table z ("
                    + "a1 string, "
                    + "b1 string primary key,"
                    + "foreign key (a1) references x (a));");
            
            mmd.addSourceMapping("x", "x", null);
            es.deployVDB("northwind", mmd);
            
            TeiidDriver td = es.getDriver();
            Properties props = new Properties();
            LocalClient lc = new LocalClient("northwind", "1", props);
            lc.setDriver(td);
            MockProvider.CLIENT = lc;

            // in one-2-one
            String url = TestPortProvider.generateURL("/odata/northwind/x?$expand=y");
            ClientRequest request = new ClientRequest(url);
            ClientResponse<String> response = request.get(String.class);
            assertEquals(200, response.getStatus());
            assertTrue(response.getEntity().contains("<m:inline>"));
            assertTrue(response.getEntity().contains("<id>http://localhost:8081/odata/northwind/m.y('xa')</id>"));
            assertTrue(response.getEntity().contains("<id>http://localhost:8081/odata/northwind/m.x('xa2')</id>"));
            
            // in one-2-many
            url = TestPortProvider.generateURL("/odata/northwind/x?$expand=z");
            request = new ClientRequest(url);
            response = request.get(String.class);
            assertEquals(200, response.getStatus());
            // atom has all dates so can't assert properly
            assertTrue(response.getEntity().contains("<m:inline>"));
            assertTrue(response.getEntity().contains("<id>http://localhost:8081/odata/northwind/m.z('zb')</id>"));
            assertTrue(response.getEntity().contains("<id>http://localhost:8081/odata/northwind/m.z('zb2')</id>"));
            
        } finally {
            es.stop();
        }
    }
    
    @SuppressWarnings("unchecked")
    @Test 
    public void testMultipleNavigationsBetweenSameEntities() throws Exception {
        EmbeddedServer es = new EmbeddedServer();
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory() {
            @Override
            public boolean supportsCompareCriteriaEquals() {
                return true;
            }
            public boolean supportsInnerJoins() {
                return true;
            }
            public boolean supportsOuterJoins() {
                return true;
            }            
        };
        hc.addData("SELECT x.a, x.b, y.a1, y.b1 FROM x, y WHERE x.a = y.a1", 
                Arrays.asList(Arrays.asList("xa", "xb", "xa", "yb"), 
                        Arrays.asList("xa1", "xb1", "xa1", "yb1"),
                        Arrays.asList("xa2", "xb2", "xa2", "yb2")
                        ));
        hc.addData("SELECT x.a, x.b, z.a1, z.b1 FROM x, z WHERE x.a = z.a1", 
                Arrays.asList(Arrays.asList("xa", "xb", "xa", "zb"), 
                        Arrays.asList("xa", "xb", "xa", "zb1"),
                        Arrays.asList("xa", "xb", "xa", "zb2")
                        ));

        es.addTranslator("x", hc);
        es.start(new EmbeddedConfiguration());
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("m");
            mmd.addSourceMetadata("ddl", 
                    "create foreign table x ("
                    + "a string primary key, "
                    + "b string);"
                    +"\n"
                    + "create foreign table y ("
                    + "a1 string primary key, "
                    + "b1 string, "
                    + "foreign key (a1) references x (a),"
                    + "foreign key (b1) references x (a));");
            
            mmd.addSourceMapping("x", "x", null);
            es.deployVDB("northwind", mmd);
            
            TeiidDriver td = es.getDriver();
            Properties props = new Properties();
            LocalClient lc = new LocalClient("northwind", "1", props);
            lc.setDriver(td);
            MockProvider.CLIENT = lc;

            // in one-2-one
            String payload = "<NavigationProperty Name=\"y\" Relationship=\"m.y_FK0\" FromRole=\"x\" ToRole=\"y\">"
                    + "</NavigationProperty><NavigationProperty Name=\"y1\" Relationship=\"m.y_FK1\" FromRole=\"x\" ToRole=\"y\">";
            String url = TestPortProvider.generateURL("/odata/northwind/$metadata");
            ClientRequest request = new ClientRequest(url);
            ClientResponse<String> response = request.get(String.class);
            assertEquals(200, response.getStatus());
            assertTrue(response.getEntity().contains(payload));
            
        } finally {
            es.stop();
        }
    }     
}
