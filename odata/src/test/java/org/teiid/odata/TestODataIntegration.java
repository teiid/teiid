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

import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

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
import org.odata4j.producer.Responses;
import org.odata4j.producer.resources.EntitiesRequestResource;
import org.odata4j.producer.resources.EntityRequestResource;
import org.odata4j.producer.resources.MetadataResource;
import org.odata4j.producer.resources.ODataBatchProvider;
import org.odata4j.producer.resources.ServiceDocumentResource;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.odata.ODataEntitySchemaBuilder;

@SuppressWarnings("nls")
public class TestODataIntegration extends BaseResourceTest {
	
	private static TransformationMetadata metadata;
	private static EdmDataServices eds;
	
	@BeforeClass
	public static void before() throws Exception {
		deployment = EmbeddedContainer.start("/odata/northwind");
		dispatcher = deployment.getDispatcher();
		deployment.getRegistry().addPerRequestResource(EntitiesRequestResource.class);
		deployment.getRegistry().addPerRequestResource(EntityRequestResource.class);
		deployment.getRegistry().addPerRequestResource(MetadataResource.class);
		deployment.getRegistry().addPerRequestResource(ServiceDocumentResource.class);
		deployment.getProviderFactory().registerProviderInstance(ODataBatchProvider.class);
		deployment.getProviderFactory().addExceptionMapper(ODataExceptionMappingProvider.class);
		deployment.getProviderFactory().addContextResolver(org.teiid.odata.MockProvider.class);		
		metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl")),"northwind", "nw");
		eds = ODataEntitySchemaBuilder.buildMetadata(metadata.getMetadataStore());
	}	
	
	@Test
	public void testMetadata() throws Exception {
		Client client = mock(Client.class);
		stub(client.getMetadataStore()).toReturn(metadata.getMetadataStore());	
		stub(client.getMetadata()).toReturn(eds);
		MockProvider.CLIENT = client;
		
		StringWriter sw = new StringWriter();
		
		EdmxFormatWriter.write(eds, sw);
		
        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/$metadata"));
        ClientResponse<String> response = request.get(String.class);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals(sw.toString(), response.getEntity());		
	}

	@Test
	public void testProjectedColumns() throws Exception {
		Client client = mock(Client.class);
		stub(client.getMetadataStore()).toReturn(metadata.getMetadataStore());
		stub(client.getMetadata()).toReturn(eds);
		MockProvider.CLIENT = client;
		ArgumentCaptor<Query> sql = ArgumentCaptor.forClass(Query.class);
		ArgumentCaptor<EdmEntitySet> entitySet = ArgumentCaptor.forClass(EdmEntitySet.class);
		
		OEntity entity = createCustomersEntity(eds);
		ArrayList<OEntity> list = new ArrayList<OEntity>();
		list.add(entity);
		
		EntityList result = Mockito.mock(EntityList.class);
		when(result.get(0)).thenReturn(entity);
		when(result.size()).thenReturn(1);
		when(result.iterator()).thenReturn(list.iterator());
		
		when(client.executeSQL(any(Query.class), anyListOf(SQLParam.class), any(EdmEntitySet.class), (LinkedHashMap<String, Boolean>) any(), any(Boolean.class), any(String.class), any(Boolean.class))).thenReturn(result);
		
        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/Customers?$select=CustomerID,CompanyName,Address"));
        ClientResponse<String> response = request.get(String.class);
        verify(client).executeSQL(sql.capture(),  anyListOf(SQLParam.class), entitySet.capture(), (LinkedHashMap<String, Boolean>) any(), any(Boolean.class), any(String.class), any(Boolean.class));
        
        Assert.assertEquals("SELECT g0.CustomerID, g0.CompanyName, g0.Address FROM Customers AS g0 ORDER BY g0.CustomerID", sql.getValue().toString());
        Assert.assertEquals(200, response.getStatus());
        //Assert.assertEquals("", response.getEntity());		
	}	
	
	@Test
	public void testProcedure() throws Exception {
		Client client = mock(Client.class);
		stub(client.getMetadataStore()).toReturn(metadata.getMetadataStore());
		stub(client.getMetadata()).toReturn(eds);
		MockProvider.CLIENT = client;
		ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<List> params = ArgumentCaptor.forClass(List.class);
		
		when(client.executeCall(any(String.class), anyListOf(SQLParam.class), any(EdmType.class))).thenReturn(Responses.simple(EdmSimpleType.INT32, "return", org.odata4j.expression.Expression.null_()));
		
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
		Client client = mock(Client.class);
		stub(client.getMetadataStore()).toReturn(metadata.getMetadataStore());
		stub(client.getMetadata()).toReturn(eds);
		MockProvider.CLIENT = client;
		
        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/NoPKTable"));
        ClientResponse<String> response = request.get(String.class);
        
        Assert.assertEquals(404, response.getStatus());
        Assert.assertTrue(response.getEntity().endsWith("<error xmlns=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\"><code>NotFoundException</code><message lang=\"en-US\">TEIID16011 EntitySet \"NoPKTable\" is not found; Check the spelling, use modelName.tableName; The table that representing the Entity type must either have a PRIMARY KEY or UNIQUE key(s)</message></error>"));
	}	
	
	@Test
	public void testError() throws Exception {
		Client client = mock(Client.class);
		stub(client.getMetadataStore()).toReturn(metadata.getMetadataStore());
		stub(client.getMetadata()).toReturn(eds);
		MockProvider.CLIENT = client;
		
		when(client.executeSQL(any(Query.class), anyListOf(SQLParam.class), any(EdmEntitySet.class), (LinkedHashMap<String, Boolean>) any(), any(Boolean.class), any(String.class), any(Boolean.class))).thenThrow(new NullPointerException());
		
        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/Customers?$select=CustomerID,CompanyName,Address"));
        ClientResponse<String> response = request.get(String.class);
        
        Assert.assertEquals(500, response.getStatus());
        Assert.assertTrue(response.getEntity().endsWith("<error xmlns=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\"><code>ServerErrorException</code><message lang=\"en-US\">Internal Server Error</message></error>"));
	}	
	
	@Test
	public void testProcedureCall() throws Exception {
		Client client = mock(Client.class);
		stub(client.getMetadataStore()).toReturn(metadata.getMetadataStore());
		stub(client.getMetadata()).toReturn(eds);
		MockProvider.CLIENT = client;
		ArgumentCaptor<Query> sql = ArgumentCaptor.forClass(Query.class);
		ArgumentCaptor<EdmEntitySet> entitySet = ArgumentCaptor.forClass(EdmEntitySet.class);
		
		OEntity entity = createCustomersEntity(eds);
		ArrayList<OEntity> list = new ArrayList<OEntity>();
		list.add(entity);
		
		EntityList result = Mockito.mock(EntityList.class);
		when(result.get(0)).thenReturn(entity);
		when(result.size()).thenReturn(1);
		when(result.iterator()).thenReturn(list.iterator());
		
		when(client.executeSQL(any(Query.class), anyListOf(SQLParam.class), any(EdmEntitySet.class), (LinkedHashMap<String, Boolean>) any(), any(Boolean.class), any(String.class), any(Boolean.class))).thenReturn(result);
		
        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/Customers?$select=CustomerID,CompanyName,Address"));
        ClientResponse<String> response = request.get(String.class);
        verify(client).executeSQL(sql.capture(),  anyListOf(SQLParam.class), entitySet.capture(), (LinkedHashMap<String, Boolean>) any(), any(Boolean.class), any(String.class), any(Boolean.class));
        
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
			mmd.setSchemaText("create view x (a string primary key, b char) as select 'ab\u0000cd\u0001', char(22);");
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
}
