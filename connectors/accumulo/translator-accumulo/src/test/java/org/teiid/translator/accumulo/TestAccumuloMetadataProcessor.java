/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.accumulo;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.util.FileUtils;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.SystemMetadata;

@SuppressWarnings("nls")
public class TestAccumuloMetadataProcessor {

    private static AccumuloClient client;
    private static Connector connector;
    private static MiniAccumuloCluster cluster;

    @BeforeClass
    public static void setup() throws Exception {
        File f = UnitTestUtil.getTestScratchFile("accumulo");
        FileUtils.removeDirectoryAndChildren(f);
        MiniAccumuloConfig cfg = new MiniAccumuloConfig(f, "password");
        cluster = new MiniAccumuloCluster(cfg);
        cluster.start();

        client = cluster.createAccumuloClient("root", new PasswordToken("password"));
        client.securityOperations().changeUserAuthorizations("root", new Authorizations("public"));

        connector = Connector.from(client);
        client.tableOperations().create("Customer");
        client.tableOperations().create("Category");

        BatchWriter writer = client.createBatchWriter("Customer",new BatchWriterConfig());
        Mutation m = new Mutation("1");
        m.put("Customer", "CompanyName", new Value("teiid.org".getBytes()));
        m.put("Customer", "ContactName", new Value("helpdesk".getBytes()));
        m.put("Customer", "ContactTitle", new Value("dude".getBytes()));
        writer.addMutation(m);
        writer.close();

        writer = client.createBatchWriter("Category",new BatchWriterConfig());
        m = new Mutation("1");
        m.put("Category", "CategoryName", new Value("software".getBytes()));
        m.put("Category", "CategoryDescription", new Value("Data Virtualization Software".getBytes()));
        writer.addMutation(m);
        writer.close();
    }

    @AfterClass
    public static void teardown() throws Exception {
        cluster.stop();
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