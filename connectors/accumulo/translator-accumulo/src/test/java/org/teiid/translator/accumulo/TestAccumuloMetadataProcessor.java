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
package org.teiid.translator.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.metadata.*;
import org.teiid.metadata.Column.SearchType;
import org.teiid.query.metadata.SystemMetadata;

@SuppressWarnings("nls")
public class TestAccumuloMetadataProcessor {
	
	private static Connector connector;
	
	@Before
	public void setup() throws Exception {
		MockInstance instance = new MockInstance("teiid-test");
		
		connector = instance.getConnector("root", new PasswordToken(""));
		try {
			connector.tableOperations().create("Customer");
			connector.tableOperations().create("Category");
		} catch (Exception e) {
		}

		BatchWriter writer = connector.createBatchWriter("Customer",new BatchWriterConfig());
		Mutation m = new Mutation("1");
		m.put("Customer", "CompanyName", new Value("teiid.org".getBytes()));
		m.put("Customer", "ContactName", new Value("helpdesk".getBytes()));
		m.put("Customer", "ContactTitle", new Value("dude".getBytes()));
		writer.addMutation(m);
		writer.close();

		writer = connector.createBatchWriter("Category",new BatchWriterConfig());
		m = new Mutation("1");
		m.put("Category", "CategoryName", new Value("software".getBytes()));
		m.put("Category", "CategoryDescription", new Value("Data Virtualization Software".getBytes()));
		writer.addMutation(m);
		writer.close();
	}
	
	@Test
	public void testDefaultImportPropertiesMetadata() throws Exception {
		AccumuloConnection conn = Mockito.mock(AccumuloConnection.class);
		Mockito.stub(conn.getInstance()).toReturn(connector);
		Mockito.stub(conn.getAuthorizations()).toReturn(new Authorizations("public"));
		MetadataFactory mf = new MetadataFactory("vdb", 1, "accumulo", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
		AccumuloMetadataProcessor processor = new AccumuloMetadataProcessor();
		processor.process(mf,conn);


		Schema schema = mf.getSchema();
		Map<String, Table> tables = schema.getTables();
		assertNotNull(tables);
		assertEquals("wrong table size="+tables, 2, tables.size());

		Table customer = tables.get("Customer");
		assertNotNull(customer);
		List<Column> columns = customer.getColumns();
		assertEquals(4, columns.size());
		assertNotNull(customer.getColumnByName("rowid"));
		assertNotNull(customer.getColumnByName("Customer_CompanyName"));
		assertEquals("string",customer.getColumnByName("Customer_CompanyName").getDatatype().getName());

		Column column = customer.getColumnByName("Customer_CompanyName");
		assertEquals("Customer",column.getProperty(AccumuloMetadataProcessor.CF, false));
		assertEquals("CompanyName",column.getProperty(AccumuloMetadataProcessor.CQ, false));
		assertEquals("{VALUE}",column.getProperty(AccumuloMetadataProcessor.VALUE_IN, false));
	}
	
	@Test
	public void testImportPropertiesMetadata() throws Exception {
		Properties props = new Properties();
		
		props.put("importer.ColumnNamePattern", "{CQ}");
		props.put("importer.ValueIn", "{ROWID}");
		
		AccumuloConnection conn = Mockito.mock(AccumuloConnection.class);
		Mockito.stub(conn.getInstance()).toReturn(connector);
		Mockito.stub(conn.getAuthorizations()).toReturn(new Authorizations("public"));
		MetadataFactory mf = new MetadataFactory("vdb", 1, "accumulo", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
		AccumuloMetadataProcessor processor = new AccumuloMetadataProcessor();
		PropertiesUtils.setBeanProperties(processor, mf.getModelProperties(), "importer"); //$NON-NLS-1$
		processor.process(mf,conn);

		Schema schema = mf.getSchema();
		Map<String, Table> tables = schema.getTables();
		assertNotNull(tables);
		assertEquals(2, tables.size());

		Table customer = tables.get("Customer");
		assertNotNull(customer);
		List<Column> columns = customer.getColumns();
		assertEquals(4, columns.size());
		assertNotNull(customer.getColumnByName("rowid"));
		assertNotNull(customer.getColumnByName("CompanyName"));
		assertEquals("string",customer.getColumnByName("CompanyName").getDatatype().getName());

		Column column = customer.getColumnByName("CompanyName");
		assertEquals("Customer",column.getProperty(AccumuloMetadataProcessor.CF, false));
		assertEquals("CompanyName",column.getProperty(AccumuloMetadataProcessor.CQ, false));
		assertEquals("{ROWID}",column.getProperty(AccumuloMetadataProcessor.VALUE_IN, false));
		assertEquals(SearchType.All_Except_Like, column.getSearchType());
		
		column = customer.getColumnByName("rowid");
		assertEquals(SearchType.All_Except_Like, column.getSearchType());
		
		assertNotNull(customer.getPrimaryKey());
	}	

}