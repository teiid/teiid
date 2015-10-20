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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;

import javax.servlet.DispatcherType;
import javax.transaction.TransactionManager;

import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.core.Encoder;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.GeneratedKeys;
import org.teiid.adminapi.Admin.SchemaObjectType;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.dqp.service.AutoGenDataService;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.odata.api.Client;
import org.teiid.olingo.service.LocalClient;
import org.teiid.olingo.web.ODataFilter;
import org.teiid.olingo.web.ODataServlet;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.HardCodedExecutionFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.loopback.LoopbackExecutionFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@SuppressWarnings("nls")
public class TestODataIntegration {
    private static final String CRLF = "\r\n";
      private static final String MIME_HEADERS = "Content-Type: application/http" + CRLF
              + "Content-Transfer-Encoding: binary" + CRLF;
    private static EmbeddedServer teiid;
    private static Server server = new Server();
    private static String baseURL;
    private static HttpClient http = new HttpClient();
    private static LocalClient localClient;

    @Before
    public void before() throws Exception {
        teiid = new EmbeddedServer();
        EmbeddedConfiguration config = new EmbeddedConfiguration();
        config.setTransactionManager(Mockito.mock(TransactionManager.class));
        teiid.start(config);
        teiid.addTranslator(LoopbackExecutionFactory.class);

        ServerConnector connector = new ServerConnector(server);
        server.setConnectors(new Connector[] { connector });

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/odata4");
        context.addServlet(new ServletHolder(new ODataServlet()), "/*");
        context.addFilter(new FilterHolder(new ODataFilter() {
            @Override
            public Client buildClient(String vdbName, int version, Properties props) {
                if (localClient != null) {
                    return localClient;
                }
                return getClient(teiid.getDriver(), vdbName, version, props);       
            }
        }), "/*", EnumSet.allOf(DispatcherType.class));
        server.setHandler(context);
        server.start();
        int port = connector.getLocalPort();
        http.start();
        baseURL = "http://localhost:"+port+"/odata4";        

        deployVDB();
    }

    @After
    public void after() throws Exception {
        server.stop();
        teiid.stop();
    }

    private static void deployVDB() throws IOException, ConnectorManagerException, VirtualDatabaseException, TranslatorException {
        teiid.deployVDB(new FileInputStream(UnitTestUtil.getTestDataFile("loopy-vdb.xml")));
    }

    private JsonNode getJSONNode(ContentResponse response) throws IOException,
            JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree(response.getContent());
        return node;
    }    
    
    @Test
    public void testMetadata() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/$metadata");
        assertEquals(200, response.getStatus());
        assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("loopy-edmx-metadata.xml")),
                response.getContentAsString());        
    }
    @Test
    public void testSystemMetadata() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/SYS/$metadata");
        assertEquals(200, response.getStatus());
    }

    @Test
    public void testServiceMetadata() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/VM1");
        assertEquals(200, response.getStatus());
        String expected = "{" +  
                "\"@odata.context\":\""+baseURL+"/loopy/VM1/$metadata\"," +
                "\"value\":[{" +
                "\"name\":\"G1\"," +
                "\"url\":\"G1\"" +
                "},{" +
                "\"name\":\"G2\"," +
                "\"url\":\"G2\"" +
                "},{" +
                "\"name\":\"G4\"," +
                "\"url\":\"G4\"" +
                "}]" +
                "}";
        assertEquals(expected, response.getContentAsString());
    }

    @Test
    public void testEntitySet() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#G1\",\"value\":[{\"e1\":\"ABCDEFGHIJ\",\"e2\":0,\"e3\":0.0}]}", 
                response.getContentAsString());
    }

    @Test
    public void testEntitySetWithKey() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1(0)");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#G1/$entity\",\"e1\":\"ABCDEFGHIJ\",\"e2\":0,\"e3\":0.0}", 
                response.getContentAsString());        
    }

    @Test
    public void testIndividualProperty() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1(0)/e1");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#G1(0)/e1\",\"value\":\"ABCDEFGHIJ\"}", 
                response.getContentAsString());        
    }
    
    @Test
    public void testIndividualProperty$Value() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1(0)/e1/$value");
        assertEquals(200, response.getStatus());
        assertEquals("ABCDEFGHIJ",response.getContentAsString());        
    }
    
    @Test
    public void testNavigation_1_to_1() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/pm1/G2(0)/FK0");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#G1/$entity\",\"e1\":\"ABCDEFGHIJ\",\"e2\":0,\"e3\":0.0}", 
                response.getContentAsString());
    }
    
    @Test
    public void testNavigation_1_to_many() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1(0)/G2_FK0");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#G2\",\"value\":[{\"e1\":\"ABCDEFGHIJ\",\"e2\":0}]}", 
                response.getContentAsString());
    }     
    
    @Test
    public void testInsert() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("INSERT INTO x (a, b) VALUES ('teiid', 'dv')", new int[] {1});
        teiid.addTranslator("x10", hc);
        
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("m");
            mmd.addSourceMetadata("ddl", "create foreign table x ("
                    + " a string, "
                    + " b string, "
                    + " primary key (a)"
                    + ") options (updatable true);");
            mmd.addSourceMapping("x10", "x10", null);
            teiid.deployVDB("northwind", mmd);

            localClient = getClient(teiid.getDriver(), "northwind", 1, new Properties());
            
            String payload = "{\n" +
                    "  \"a\":\"teiid\",\n" +
                    "  \"b\":\"dv\"\n" +
                    "}";
            ContentResponse response = http.newRequest(baseURL + "/northwind/m/x")
                .method("POST")
                .content(new StringContentProvider(payload))
                .header("Content-Type", "application/json")
                .send();
            assertEquals(204, response.getStatus());
            assertTrue(response.getHeaders().get("OData-EntityId").endsWith("northwind/m/x('ABCDEFG')"));
            //assertEquals("ABCDEFGHIJ",response.getContentAsString()); 
        } finally {
            localClient = null;
            teiid.undeployVDB("northwind");
        }
        
    }    
    
    @Test 
    public void testDeepInsert() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("INSERT INTO x (a, b) VALUES ('teiid', 'dv')", new int[] {1});
        hc.addUpdate("INSERT INTO y (a, b) VALUES ('odata', 'olingo')", new int[] {1});
        hc.addUpdate("INSERT INTO y (a, b) VALUES ('odata4', 'olingo4')", new int[] {1});
        teiid.addTranslator("x10", hc);
        
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("m");
            mmd.addSourceMetadata("ddl", "create foreign table x ("
                    + " a string, "
                    + " b string, "
                    + " primary key (a)"
                    + ") options (updatable true);"
                    + "create foreign table y ("
                    + " a string, "
                    + " b string, "
                    + " primary key (a),"
                    + " CONSTRAINT FKX FOREIGN KEY (b) REFERENCES x(a)"                    
                    + ") options (updatable true);");
            mmd.addSourceMapping("x10", "x10", null);
            teiid.deployVDB("northwind", mmd);

            localClient = getClient(teiid.getDriver(), "northwind", 1, new Properties());
            
            // update to collection based reference
            String payload = "{\n" +
                    "  \"a\":\"teiid\",\n" +
                    "  \"b\":\"dv\",\n" +
                    "     \"y_FKX\": [\n"+
                    "        {"+
                    "          \"a\":\"odata\",\n" +
                    "          \"b\":\"olingo\"\n" +                    
                    "        },\n"+
                    "        {\n"+
                    "          \"a\":\"odata4\",\n" +
                    "          \"b\":\"olingo4\"\n" +                    
                    "        }\n"+                    
                    "     ]\n"+
                    "}";
            ContentResponse response = http.newRequest(baseURL + "/northwind/m/x")
                    .method("POST")
                    .content(new StringContentProvider(payload), ContentType.APPLICATION_JSON.toString())
                    // when this header is defined the return should be expanded, but due to way olingo 
                    // designed it is going to be a big refactoring.
                    .header("Prefer", "return=representation")
                    .send();
            assertEquals(201, response.getStatus());
            assertEquals("{\"@odata.context\":\"$metadata#x\",\"a\":\"ABCDEFG\",\"b\":\"ABCDEFG\"}", response.getContentAsString());

        } finally {
            localClient = null;
            teiid.undeployVDB("northwind");
        }
    }     

    @Test
    public void testFunction() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/proc(x='foo')");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#Edm.String\",\"value\":\"foo\"}", 
                response.getContentAsString());        
    }

    @Test
    public void testFunctionDate() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/getCustomers(p2='2011-09-11T00:00:00',p3=2.0)");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#Edm.DateTimeOffset\",\"value\":\"2011-09-11T00:00:00Z\"}", 
                response.getContentAsString());        
    }    
    
    @Test
    public void testFunctionReturningResultSet() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/procResultSet(x='foo',y=1)");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#Collection(Loopy.1.VM1.procResultSet_RSParam)\",\"value\":[{\"x\":\"foo\",\"y\":1}]}", 
                response.getContentAsString());        
    }
    
    @Test
    public void testFunctionReturningStream() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/procXML(x='foo')");
        assertEquals(200, response.getStatus());
        assertEquals("<name>foo</name>", 
                response.getContentAsString());        
    }
    
    @Test
    public void testFunctionReturningStreamDesignedToReturnTable() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/procComposableXML(x='foo')");
        assertEquals(200, response.getStatus());
        assertEquals("<name>foo</name>", 
                response.getContentAsString());        
    }    
    
    @Test
    public void testActionStream() throws Exception {
        ContentResponse response = http.newRequest(baseURL + "/loopy/vm1/actionXML")
                .method("POST")
                .content(new StringContentProvider("<name>foo2</name>"), "application/xml")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("<name>foo2</name>", 
                response.getContentAsString());        
    } 
    @Test
    public void testActionSimpleParameters() throws Exception {
        ContentResponse response = http.newRequest(baseURL + "/loopy/vm1/procActionJSON")
                .method("POST")
                .content(new StringContentProvider("{\"x\": \"foo\", \"y\": 4.5}"), "application/json")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"x1\":\"foo\",\"y1\":4.5}", 
                response.getContentAsString());        
    }     

    @Test
    public void testMetadataVisibility() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/PM2/G1");
        assertEquals(500, response.getStatus());
    }
        
    @Test 
    public void testCheckGeneratedColumns() throws Exception {
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory() {
            @Override
            public UpdateExecution createUpdateExecution(
                    org.teiid.language.Command command,
                    ExecutionContext executionContext,
                    RuntimeMetadata metadata, Object connection)
                    throws TranslatorException {
                GeneratedKeys keys = executionContext.getCommandContext().returnGeneratedKeys(new String[] {"a"}, new Class[] {String.class});
                keys.addKey(Arrays.asList("ax"));
                return super.createUpdateExecution(command, executionContext, metadata,
                        connection);
            }
            @Override
            public boolean supportsCompareCriteriaEquals() {
                return true;
            }
        };
        hc.addUpdate("INSERT INTO x (b, c) VALUES ('b', 5)", new int[] {1});
        // this gets called right after insert.
        hc.addData("SELECT x.a, x.b, x.c FROM x WHERE x.a = 'ax'", Arrays.asList(Arrays.asList("a", "b", 2)));
        teiid.addTranslator("x", hc);
        
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("m");
            mmd.addSourceMetadata("ddl", "create foreign table x (a string, b string, c integer, primary key (a)) options (updatable true);");
            mmd.addSourceMapping("x", "x", null);
            teiid.deployVDB("northwind", mmd);

            localClient = getClient(teiid.getDriver(), "northwind", 1, new Properties());

            ContentResponse response = http.newRequest(baseURL + "/northwind/m/x")
                    .method("POST")
                    .content(new StringContentProvider("{\"b\":\"b\", \"c\":5}"), "application/json")
                    .send();                        
            assertEquals(204, response.getStatus());
        } finally {
            localClient = null;
            teiid.undeployVDB("northwind");
        }
    }    
    
    private LocalClient getClient(final TeiidDriver driver, final String vdb, final int version, final Properties properties) {
        return new LocalClient(vdb, 1, properties) {
            ConnectionImpl conn;
            
            @Override
            public ConnectionImpl getConnection() {
                return conn;
            }
            
            @Override
            public void close() throws SQLException {
                if (conn != null) {
                    conn.close();
                }
            }
            
            @Override
            public Connection open() throws SQLException {
                try {
                    conn = LocalClient.buildConnection(driver, vdb, 1, properties);
                    return conn;
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
    }
    
    @Test
    public void testSkipNoPKTable() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/PM1/NoPKTable");
        assertEquals(404, response.getStatus());
        assertEquals("{\"error\":{\"code\":null,\"message\":\"Cannot find EntitySet, Singleton, ActionImport or FunctionImport with name 'NoPKTable'.\"}}", 
                response.getContentAsString());
    }
    
    @Test 
    public void testInvalidCharacterReplacement() throws Exception {
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("vw");
            mmd.addSourceMetadata("DDL", "create view x (a string primary key, b char, c string[], d integer) as select 'ab\u0000cd\u0001', char(22), ('a\u00021','b1'), 1;");
            mmd.setModelType(Model.Type.VIRTUAL);
            teiid.deployVDB("northwind", mmd);

            Properties props = new Properties();
            props.setProperty(LocalClient.INVALID_CHARACTER_REPLACEMENT, " ");
            localClient = getClient(teiid.getDriver(), "northwind", 1, props);

            ContentResponse response = http.GET(baseURL + "/northwind/vw/x");
            assertEquals(200, response.getStatus());
            assertEquals("{\"@odata.context\":\"$metadata#x\",\"value\":[{\"a\":\"ab cd \",\"b\":\" \",\"c\":[\"a 1\",\"b1\"],\"d\":1}]}", 
                    response.getContentAsString());
        } finally {
            localClient = null;
            teiid.undeployVDB("northwind");
        }
    }
    
    @Test 
    public void testArrayResults() throws Exception {
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("vw");
            mmd.addSourceMetadata("DDL", "create view x (a string primary key, b integer[], c string[][]) "
                    + "as select 'x', (1, 2, 3), (('a','b'),('c','d')) union "
                    + "select 'y', (4, 5, 6), (('x','y'),('z','u'));");
            mmd.setModelType(Model.Type.VIRTUAL);
            teiid.deployVDB("northwind", mmd);

            localClient = getClient(teiid.getDriver(), "northwind", 1, new Properties());

            ContentResponse response = http.GET(baseURL + "/northwind/vw/x?$format=json&$select=a,b");
            assertEquals(200, response.getStatus());
            assertEquals(
                    "{\"@odata.context\":\"$metadata#x(a,b)\","
                    + "\"value\":[{\"a\":\"x\",\"b\":[1,2,3]},{\"a\":\"y\",\"b\":[4,5,6]}]}",
                    response.getContentAsString());

            response = http.GET(baseURL + "/northwind/vw/x?$format=json");
            assertEquals(500, response.getStatus());
        } finally {
            localClient = null;
            teiid.undeployVDB("northwind");
        }
    }
    
    @SuppressWarnings("unchecked")
    @Test 
    public void test$ItFilter() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addData("SELECT x.c, x.a FROM x WHERE x.a = 'x'", Arrays.asList(Arrays.asList(new String[] {"google.net", "google.com"}, 'x')));
        hc.addData("SELECT x.c, x.a FROM x WHERE x.a = 'y'", Arrays.asList(Arrays.asList(new String[] {"example.net", "example.com"}, 'y')));
        
        teiid.addTranslator("x8", hc);
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("vw");
            mmd.addSourceMetadata("DDL", "create foreign table x  (a string primary key, b integer[], c string[]);");
            mmd.setModelType(Model.Type.PHYSICAL);
            mmd.addSourceMapping("x8", "x8", null);
            teiid.deployVDB("northwind", mmd);

            localClient = getClient(teiid.getDriver(), "northwind", 1, new Properties());

            ContentResponse response = http.GET(baseURL + "/northwind/vw/x('x')/c?$filter=endswith($it,'com')");
            assertEquals(200, response.getStatus());
            assertEquals("{\"@odata.context\":\"$metadata#x('x')/c\",\"value\":[\"google.com\"]}", response.getContentAsString());

            response = http.GET(baseURL + "/northwind/vw/x('y')/c?$filter=startswith($it,'example')");
            assertEquals(200, response.getStatus());
            assertEquals("{\"@odata.context\":\"$metadata#x('y')/c\",\"value\":[\"example.net\",\"example.com\"]}", response.getContentAsString());
        } finally {
            localClient = null;
            teiid.undeployVDB("northwind");
        }
    }    
    
    @Test 
    public void testArrayInsertResults() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("INSERT INTO x (a, b) VALUES ('x', (1, 2, 3))", new int[] {1});
        teiid.addTranslator("x5", hc);

        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("m");
            mmd.addSourceMetadata("DDL", "create foreign table x (a string primary key, b integer[], c string[][]) OPTIONS (updatable true);");
            mmd.setModelType(Model.Type.PHYSICAL);
            mmd.addSourceMapping("x5", "x5", null);
            teiid.deployVDB("northwind", mmd);

            localClient = getClient(teiid.getDriver(), "northwind", 1, new Properties());

            ContentResponse response = http.newRequest(baseURL + "/northwind/m/x")
                    .method("POST")
                    .content(new StringContentProvider("{\"a\":\"x\",\"b\":[1,2,3]}"), "application/json")
                    .send();                        
            assertEquals(204, response.getStatus());
        } finally {
            localClient = null;
            teiid.undeployVDB("northwind");
        }
    }
    
    @Test 
    public void testArrayUpdateResults() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("UPDATE x SET b = (1, 2, 3) WHERE x.a = 'x'", new int[] {1});
        teiid.addTranslator("x6", hc);

        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("m");
            mmd.addSourceMetadata("DDL", "create foreign table x (a string primary key, b integer[], c string[][]) OPTIONS (updatable true);");
            mmd.setModelType(Model.Type.PHYSICAL);
            mmd.addSourceMapping("x6", "x6", null);
            teiid.deployVDB("northwind", mmd);

            localClient = getClient(teiid.getDriver(), "northwind", 1, new Properties());
            
            ContentResponse response = http.newRequest(baseURL + "/northwind/m/x('x')")
                    .method("PATCH")
                    .content(new StringContentProvider("{\"a\":\"x\",\"b\":[1,2,3]}"), "application/json")
                    .send();                        
            assertEquals(204, response.getStatus());            
        
        } finally {
            localClient = null;
            teiid.undeployVDB("northwind");
        }
    }    
    
    
    @Test 
    public void testSkipToken() throws Exception {
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("vw");
            mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer) as select 'xyz', 123 union all select 'abc', 456;");
            mmd.setModelType(Model.Type.VIRTUAL);
            teiid.deployVDB("northwind", mmd);

            Properties props = new Properties();
            props.setProperty("batch-size", "1");
            localClient = getClient(teiid.getDriver(), "northwind", 1, props);
            
            ContentResponse response = http.GET(baseURL + "/northwind/vw/x?$format=json");
            assertEquals(200, response.getStatus());
            String starts = "{\"@odata.context\":\"$metadata#x\",\"value\":[{\"a\":\"abc\",\"b\":456}],\"@odata.nextLink\":\""+baseURL+"/northwind/vw/x?$format=json&$skiptoken=";
            String ends = "--1\"}";
            System.out.println(baseURL);
            assertTrue(response.getContentAsString(), response.getContentAsString().startsWith(starts));
            assertTrue(response.getContentAsString().endsWith(ends));
            
            JsonNode node = getJSONNode(response);
            String nextLink = node.get("@odata.nextLink").asText();
            response = http.GET(nextLink);
            assertEquals(200, response.getStatus());
            assertEquals("{\"@odata.context\":\"$metadata#x\",\"value\":[{\"a\":\"xyz\",\"b\":123}]}", 
                    response.getContentAsString());
        } finally {
            localClient = null;
            teiid.undeployVDB("northwind");
        }
    }
    
    @Test 
    public void testSkipTokenWithPageSize() throws Exception {
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("vw");
            mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer) as select 'xyz', 123 union all select 'abc', 456;");
            mmd.setModelType(Model.Type.VIRTUAL);
            teiid.deployVDB("northwind", mmd);

            ContentResponse response = http.newRequest(baseURL + "/northwind/vw/x?$format=json")
                .header("Prefer", "odata.maxpagesize=1")
                .send();

            assertEquals(200, response.getStatus());
            String starts = "{\"@odata.context\":\"$metadata#x\",\"value\":[{\"a\":\"abc\",\"b\":456}],\"@odata.nextLink\":\""+baseURL+"/northwind/vw/x?$format=json&$skiptoken=";
            String ends = "--1\"}";
            assertTrue(response.getContentAsString().startsWith(starts));
            assertTrue(response.getContentAsString().endsWith(ends));
            assertEquals("odata.maxpagesize=1", getHeader(response, "Preference-Applied"));
            
            JsonNode node = getJSONNode(response);
            String nextLink = node.get("@odata.nextLink").asText();
            response = http.GET(nextLink);
            assertEquals(200, response.getStatus());
            assertEquals("{\"@odata.context\":\"$metadata#x\",\"value\":[{\"a\":\"xyz\",\"b\":123}]}", 
                    response.getContentAsString());
        } finally {
            teiid.undeployVDB("northwind");
        }
    }    

    private String getHeader(ContentResponse response, String header) {
        return response.getHeaders().get(header);
    }
    
    @Test 
    public void testCount() throws Exception {
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("vw");
            mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer) as select 'xyz', 123 union all select 'abc', 456;");
            mmd.setModelType(Model.Type.VIRTUAL);
            teiid.deployVDB("northwind", mmd);

            Properties props = new Properties();
            props.setProperty("batch-size", "1");
            localClient = getClient(teiid.getDriver(), "northwind", 1, props);

            ContentResponse response = http.GET(baseURL + "/northwind/vw/x?$format=json&$count=true&$top=1&$skip=1");
            assertEquals(200, response.getStatus());
            assertEquals("{\"@odata.context\":\"$metadata#x\",\"@odata.count\":2,\"value\":[{\"a\":\"xyz\",\"b\":123}]}", 
                    response.getContentAsString());
            

            //effectively the same as above
            response = http.GET(baseURL + "/northwind/vw/x?$format=json&$count=true&$skip=1");
            assertEquals(200, response.getStatus());
            assertEquals("{\"@odata.context\":\"$metadata#x\",\"@odata.count\":2,\"value\":[{\"a\":\"xyz\",\"b\":123}]}", 
                    response.getContentAsString());
            
            //now there should be a next
            response = http.GET(baseURL + "/northwind/vw/x?$format=json&$count=true");
            assertEquals(200, response.getStatus());
            String ends = "--1\"}";
            assertTrue(response.getContentAsString().endsWith(ends));

            response = http.GET(baseURL + "/northwind/vw/x/$count");
            assertEquals(200, response.getStatus());            
            assertEquals("2", response.getContentAsString());
        } finally {
            localClient = null;
            teiid.undeployVDB("northwind");
        }
    }

    @Test 
    public void testCompositeKeyUpdates() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("DELETE FROM x WHERE x.a = 'a' AND x.b = 'b'", new int[] {1});
        hc.addUpdate("INSERT INTO x (a, b, c) VALUES ('a', 'b', 5)", new int[] {1});
        hc.addUpdate("UPDATE x SET c = 10 WHERE x.a = 'a' AND x.b = 'b'", new int[] {1});
        hc.addData("SELECT x.a, x.b, x.c FROM x WHERE x.a = 'a' AND x.b = 'b'", Arrays.asList(Arrays.asList("a", "b", 1)));
        teiid.addTranslator("x1", hc);
        
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("m");
            mmd.addSourceMetadata("ddl", "create foreign table x (a string, b string, c integer, primary key (a, b)) options (updatable true);");
            mmd.addSourceMapping("x1", "x1", null);
            teiid.deployVDB("northwind", mmd);

            localClient = getClient(teiid.getDriver(), "northwind", 1, new Properties());

            ContentResponse response = http.newRequest(baseURL + "/northwind/m/x(a='a',b='b')")
                    .method("DELETE")
                    .send();
            assertEquals(204, response.getStatus());

            //partial key
            response = http.newRequest(baseURL + "/northwind/m/x('a')")
                    .method("DELETE")
                    .send();            
            assertEquals(400, response.getStatus());

            //partial key
            response = http.newRequest(baseURL + "/northwind/m/x(a='a',a='b')")
                    .method("DELETE")
                    .send();                        
            assertEquals(400, response.getStatus());

            //not supported
            //request = new ClientRequest(TestPortProvider.generateURL("/odata/northwind/x(a='a',b='b')/c/$value"));
            //request.body("text/plain", "5");
            //response = request.put(String.class);

            response = http.newRequest(baseURL + "/northwind/m/x")
                    .method("POST")
                    .content(new StringContentProvider("{\"a\":\"a\", \"b\":\"b\", \"c\":5}"), "application/json")
                    .send();                        
            assertEquals(204, response.getStatus());
            
            response = http.newRequest(baseURL + "/northwind/m/x(a='a',b='b')")
                    .method("PATCH")
                    .content(new StringContentProvider("{\"c\":10}"), "application/json")
                    .send();                        
            assertEquals(204, response.getStatus());

        } finally {
            localClient = null;
            teiid.undeployVDB("northwind");
        }
    }
    
    @Test 
    public void testCrossJoin() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("UPDATE y SET b = 'a' WHERE y.a = 'a'", new int[] {1});
        teiid.addTranslator("x9", hc);
        
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("m");
            mmd.addSourceMetadata("ddl", "create foreign table x ("
                    + " a string, "
                    + " b string, "
                    + " primary key (a)"
                    + ") options (updatable true);"
                    + "create foreign table y ("
                    + " a string, "
                    + " b string, "
                    + " primary key (a)"
                    + ") options (updatable true);");
            mmd.addSourceMapping("x9", "x9", null);
            teiid.deployVDB("northwind", mmd);

            localClient = getClient(teiid.getDriver(), "northwind", 1, new Properties());

            ContentResponse response = http.newRequest(baseURL + "/northwind/m/$crossjoin(x,y)")
                    .method("GET")
                    .send();
            assertEquals(200, response.getStatus());
            assertEquals("{\"@odata.context\":\"$metadata#Collection(Edm.ComplexType)\",\"value\":[{\"x@odata.navigationLink\":\"x('ABCDEFG')\",\"y@odata.navigationLink\":\"y('ABCDEFG')\"}]}", response.getContentAsString());

            response = http.newRequest(baseURL + "/northwind/m/$crossjoin(x,y)?$expand=x")
                    .method("GET")
                    .send();
            assertEquals(200, response.getStatus());
            assertEquals("{\"@odata.context\":\"$metadata#Collection(Edm.ComplexType)\",\"value\":[{\"x\":{\"a\":\"ABCDEFG\",\"b\":\"ABCDEFG\"},\"y@odata.navigationLink\":\"y('ABCDEFG')\"}]}", response.getContentAsString());            
        } finally {
            localClient = null;
            teiid.undeployVDB("northwind");
        }
    }     
    
    @Test 
    public void testNavigationLinks() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("UPDATE y SET b = 'a' WHERE y.a = 'a'", new int[] {1});
        teiid.addTranslator("x4", hc);
        
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("m");
            mmd.addSourceMetadata("ddl", "create foreign table x ("
                    + " a string, "
                    + " b string, "
                    + " primary key (a)"
                    + ") options (updatable true);"
                    + "create foreign table y ("
                    + " a string, "
                    + " b string, "
                    + " primary key (a),"
                    + " CONSTRAINT FKX FOREIGN KEY (b) REFERENCES x(a)"                    
                    + ") options (updatable true);");
            mmd.addSourceMapping("x4", "x4", null);
            teiid.deployVDB("northwind", mmd);

            localClient = getClient(teiid.getDriver(), "northwind", 1, new Properties());

            ContentResponse response = http.newRequest(baseURL + "/northwind/m/x('a')/y_FKX/$ref")
                    .method("GET")
                    .send();
            assertEquals(200, response.getStatus());
            assertEquals("{\"@odata.context\":\"$metadata#Collection($ref)\",\"value\":[{\"@odata.id\":\"y('ABCDEFG')\"}]}", response.getContentAsString());
            
            // update to collection based reference
            String payload = "{\n" +
                    "\"@odata.id\": \"/odata4/northwind/m/y('a')\"\n" +
                    "}";            
            response = http.newRequest(baseURL + "/northwind/m/x('a')/y_FKX/$ref")
                    .method("POST")
                    .content(new StringContentProvider(payload), ContentType.APPLICATION_JSON.toString())
                    .send();
            assertEquals(204, response.getStatus());
            

        } finally {
            localClient = null;
            teiid.undeployVDB("northwind");
        }
    }    
    
    @SuppressWarnings("unchecked")
    @Test 
    public void testExpandSimple() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addData("SELECT x.a, x.b FROM x WHERE x.a = 'xa1'", Arrays.asList(Arrays.asList("xa1", "xb")));
        hc.addData("SELECT y.a, y.b FROM y WHERE y.b = 'xa1'", Arrays.asList(Arrays.asList("ya1", "xa1"), Arrays.asList("ya2", "xa1")));
        teiid.addTranslator("x7", hc);
        
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("m");
            mmd.addSourceMetadata("ddl", "create foreign table x ("
                    + " a string, "
                    + " b string, "
                    + " primary key (a)"
                    + ") options (updatable true);"
                    + "create foreign table y ("
                    + " a string, "
                    + " b string, "
                    + " primary key (a),"
                    + " CONSTRAINT FKX FOREIGN KEY (b) REFERENCES x(a)"                    
                    + ") options (updatable true);"
                    + "create foreign table z ("
                    + " a string, "
                    + " b string, "
                    + " primary key (a),"
                    + " CONSTRAINT FKX FOREIGN KEY (a) REFERENCES x(a)"                    
                    + ") options (updatable true);");
            
            mmd.addSourceMapping("x7", "x7", null);
            teiid.deployVDB("northwind", mmd);

            localClient = getClient(teiid.getDriver(), "northwind", 1, new Properties());

            ContentResponse response = null;
            
            response = http.newRequest(baseURL + "/northwind/m/x?$expand=y_FKX")
                    .method("GET")
                    .send();
            assertEquals(200, response.getStatus());
            assertEquals("{\"@odata.context\":\"$metadata#x\",\"value\":[{\"a\":\"ABCDEFG\",\"b\":\"ABCDEFG\",\"y_FKX\":[{\"a\":\"ABCDEFG\",\"b\":\"ABCDEFG\"}]}]}", response.getContentAsString());

            response = http.newRequest(baseURL + "/northwind/m/z?$expand=FKX&$select=a")
                    .method("GET")
                    .send();
            assertEquals(200, response.getStatus());
            assertEquals("{\"@odata.context\":\"$metadata#z(a)\",\"value\":[{\"a\":\"ABCDEFG\",\"FKX\":{\"a\":\"ABCDEFG\",\"b\":\"ABCDEFG\"}}]}", response.getContentAsString());
            
            // explictly selecting and expanding
            response = http.newRequest(baseURL + "/northwind/m/z?$expand=FKX&$select=a")
                    .method("GET")
                    .send();
            assertEquals(200, response.getStatus());
            assertEquals("{\"@odata.context\":\"$metadata#z(a)\",\"value\":[{\"a\":\"ABCDEFG\",\"FKX\":{\"a\":\"ABCDEFG\",\"b\":\"ABCDEFG\"}}]}", response.getContentAsString());

            response = http.newRequest(baseURL + "/northwind/m/x?$expand="+Encoder.encode("y_FKX($filter=b eq 'xa1')"))
                    .method("GET")
                    .send();
            assertEquals(200, response.getStatus());
            assertEquals("{\"@odata.context\":\"$metadata#x\",\"value\":[{\"a\":\"xa1\",\"b\":\"xb\",\"y_FKX\":[{\"a\":\"ya1\",\"b\":\"xa1\"},{\"a\":\"ya2\",\"b\":\"xa1\"}]}]}", response.getContentAsString());
            

            response = http.newRequest(baseURL + "/northwind/m/z?$expand=FKX")
                    .method("GET")
                    .send();
            assertEquals(200, response.getStatus());
            assertEquals("{\"@odata.context\":\"$metadata#z\",\"value\":[{\"a\":\"ABCDEFG\",\"b\":\"ABCDEFG\",\"FKX\":{\"a\":\"ABCDEFG\",\"b\":\"ABCDEFG\"}}]}", response.getContentAsString());
            
            response = http.newRequest(baseURL + "/northwind/m/z?$expand=FKX/a&$select=a")
                    .method("GET")
                    .send();
            assertEquals(200, response.getStatus());
            assertEquals("{\"@odata.context\":\"$metadata#z(a,FKX/a)\",\"value\":[{\"a\":\"ABCDEFG\",\"FKX\":{\"a\":\"ABCDEFG\",\"b\":\"ABCDEFG\"}}]}", response.getContentAsString());
        } finally {
            localClient = null;
            teiid.undeployVDB("northwind");
        }
    }    
    
    @Test
    public void testBatch() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("DELETE FROM x WHERE x.a = 'a' AND x.b = 'b'", new int[] {1});
        teiid.addTranslator("x", hc);
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("m");
            mmd.addSourceMetadata("ddl", "create foreign table x (a string, b string, c integer, primary key (a, b)) options (updatable true);");
            mmd.addSourceMapping("x", "x", null);
            teiid.deployVDB("northwind", mmd);

            localClient = getClient(teiid.getDriver(), "northwind", 1, new Properties());

            final String batch = ""
                    + "--batch_8194-cf13-1f56" + CRLF
                    + MIME_HEADERS
                    + CRLF
                    + "GET "+baseURL+"/northwind/m/x HTTP/1.1" + CRLF
                    + "Accept: application/json" + CRLF
                    + "MaxDataServiceVersion: 4.0" + CRLF
                    + CRLF
                    + CRLF
                    + "--batch_8194-cf13-1f56" + CRLF
                    + "Content-Type: multipart/mixed; boundary=changeset_f980-1cb6-94dd" + CRLF
                    + CRLF
                    + "--changeset_f980-1cb6-94dd" + CRLF
                    + "content-type:     Application/http" + CRLF
                    + "content-transfer-encoding: Binary" + CRLF
                    + "Content-ID: 1" + CRLF
                    + CRLF
                    + "DELETE "+baseURL+"/northwind/m/x(a='a',b='b') HTTP/1.1" + CRLF
                    + "Content-type: application/json" + CRLF
                    + CRLF
                    + CRLF
                    + "--changeset_f980-1cb6-94dd--" + CRLF
                    + "--batch_8194-cf13-1f56--";
            
            ContentResponse response = http.newRequest(baseURL + "/northwind/m/$batch")
                    .method("POST")
                    .content(new StringContentProvider(batch), "multipart/mixed;boundary=batch_8194-cf13-1f56")
                    .send();

            assertEquals(202, response.getStatus());
            /*
            String expected = "--batch_d06279e4-c510-46ed-a778-e4e941dfd6f1\n" + 
                    "Content-Type: application/http\n" + 
                    "Content-Transfer-Encoding: binary\n" + 
                    "\n" + 
                    "HTTP/1.1 200 OK\n" + 
                    "Content-Type: application/json;odata.metadata=minimal\n" + 
                    "Content-Length: 78\n" + 
                    "\n" + 
                    "{\"@odata.context\":\"$metadata#x\",\"value\":[{\"a\":\"ABCDEFG\",\"b\":\"ABCDEFG\",\"c\":0}]}\n" + 
                    "--batch_d06279e4-c510-46ed-a778-e4e941dfd6f1\n" + 
                    "Content-Type: multipart/mixed; boundary=changeset_5a1cba47-b51f-46c2-b0ac-ead23fa7706d\n" + 
                    "\n" + 
                    "--changeset_5a1cba47-b51f-46c2-b0ac-ead23fa7706d\n" + 
                    "Content-Type: application/http\n" + 
                    "Content-Transfer-Encoding: binary\n" + 
                    "Content-Id: 1\n" + 
                    "\n" + 
                    "HTTP/1.1 204 No Content\n" + 
                    "Content-Length: 0\n" + 
                    "\n" + 
                    "\n" + 
                    "--changeset_5a1cba47-b51f-46c2-b0ac-ead23fa7706d--\n" + 
                    "--batch_d06279e4-c510-46ed-a778-e4e941dfd6f1--";
            assertEquals(expected, response.getContentAsString());
            */
        } finally {
            localClient = null;
            teiid.undeployVDB("northwind");
        }
    }

    private HardCodedExecutionFactory buildHardCodedExecutionFactory() {
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory() {
            @Override
            public boolean supportsCompareCriteriaEquals() {
                return true;
            }
            @Override
            protected List<? extends List<?>> getData(
                    QueryExpression command) {
                
                if (super.getData(command) != null) {
                    return super.getData(command);
                }
                
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
        return hc;
    }
    
    @Test 
    public void testJsonProcedureResultSet() throws Exception {
        try {
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory();
        hc.addData("EXEC x()", Arrays.asList(Arrays.asList("x"), Arrays.asList("y")));
        teiid.addTranslator("x2", hc);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("ddl", "create foreign procedure x () returns table(y string) OPTIONS(UPDATECOUNT 0);");
        mmd.addSourceMapping("x2", "x2", null);
        teiid.deployVDB("northwind", mmd);

        localClient = getClient(teiid.getDriver(), "northwind", 1, new Properties());

        ContentResponse response =  http.GET(baseURL + "/northwind/m/x()?$format=json");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#Collection(northwind.1.m.x_RSParam)\",\"value\":[{\"y\":\"x\"},{\"y\":\"y\"}]}", response.getContentAsString());
        } finally {
            localClient = null;
            teiid.undeployVDB("northwind");
        }
    }

    @Test 
    public void testBasicTypes() throws Exception {
        try {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMapping("x3", "x3", null);
        
        MetadataStore ms = RealMetadataFactory.exampleBQTStore();
        Schema s = ms.getSchema("BQT1");
        KeyRecord pk = new KeyRecord(KeyRecord.Type.Primary);
        Table smalla = s.getTable("SmallA");
        pk.setName("pk");
        pk.addColumn(smalla.getColumnByName("IntKey"));
        smalla.setPrimaryKey(pk);
        String ddl = DDLStringVisitor.getDDLString(s, EnumSet.allOf(SchemaObjectType.class), "SmallA");
        
        mmd.addSourceMetadata("DDL", ddl);
        
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        teiid.addTranslator("x3", hc);
        teiid.deployVDB("northwind", mmd);

        localClient = getClient(teiid.getDriver(), "northwind", 1, new Properties());

        ContentResponse response= http.GET(baseURL + "/northwind/m/SmallA?$format=json&$select=TimeValue");
        assertEquals(200, response.getStatus());
        } finally {
            localClient = null;
            teiid.undeployVDB("northwind");
        }
    }
}
