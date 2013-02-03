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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.test.BaseResourceTest;
import org.jboss.resteasy.test.EmbeddedContainer;
import org.jboss.resteasy.test.TestPortProvider;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;
import org.mockito.ArgumentCaptor;
import org.odata4j.core.*;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.format.xml.EdmxFormatWriter;
import org.odata4j.producer.resources.*;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.odata.Client.Cursor;
import org.teiid.odata.LocalClient.LocalCursor;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.unittest.RealMetadataFactory;

@Ignore
public class TestODataIntegration extends BaseResourceTest {
	
	@BeforeClass
	public static void before() throws Exception {
		deployment = EmbeddedContainer.start("/odata/northwind");
		dispatcher = deployment.getDispatcher();
		deployment.getRegistry().addPerRequestResource(EntitiesRequestResource.class);
		deployment.getRegistry().addPerRequestResource(EntityRequestResource.class);
		deployment.getRegistry().addPerRequestResource(MetadataResource.class);
		deployment.getRegistry().addPerRequestResource(ServiceDocumentResource.class);
		deployment.getProviderFactory().registerProviderInstance(ODataBatchProvider.class);
		deployment.getProviderFactory().registerProviderInstance(ExceptionMappingProvider.class);
		deployment.getProviderFactory().addContextResolver(org.teiid.odata.MockProvider.class);		
	}	
	
	@Test
	public void testMetadata() throws Exception {
		TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl")),"northwind", "nw");
		EdmDataServices eds = ODataEntitySchemaBuilder.buildMetadata(metadata.getMetadataStore());
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
		TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("northwind.ddl")),"northwind", "nw");
		EdmDataServices eds = ODataEntitySchemaBuilder.buildMetadata(metadata.getMetadataStore());
		Client client = mock(Client.class);
		stub(client.getMetadataStore()).toReturn(metadata.getMetadataStore());
		stub(client.getMetadata()).toReturn(eds);
		MockProvider.CLIENT = client;
		ArgumentCaptor<Query> sql = ArgumentCaptor.forClass(Query.class);
		ArgumentCaptor<EdmEntitySet> entitySet = ArgumentCaptor.forClass(EdmEntitySet.class);
		
		OEntity entity = createCustomersEntity(eds);
		List<OEntity> result = new ArrayList<OEntity>();
		result.add(entity);
		
		LocalCursor cursor = new LocalCursor("foo", "#Temp1", 1000, 10, 123);
		
		when(client.createCursor(any(Query.class), anyListOf(SQLParam.class), any(EdmEntitySet.class))).thenReturn(cursor);
		when(client.fetchCursor(any(Cursor.class), any(EdmEntitySet.class))).thenReturn(result);
		
        ClientRequest request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/Customers?$select=CustomerID,CompanyName,Address"));
        ClientResponse<String> response = request.get(String.class);
        verify(client).createCursor(sql.capture(),  anyListOf(SQLParam.class), entitySet.capture());
        verify(client).fetchCursor(any(LocalCursor.class), entitySet.capture());
        
        Assert.assertEquals("SELECT g0.Address, g0.CustomerID, g0.CompanyName FROM Customers AS g0", sql.getValue().toString());
        Assert.assertEquals(200, response.getStatus());
        //Assert.assertEquals("", response.getEntity());		
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
