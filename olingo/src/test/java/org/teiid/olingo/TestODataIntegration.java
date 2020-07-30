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
package org.teiid.olingo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import javax.servlet.DispatcherType;

import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.core.Encoder;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.GeneratedKeys;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.Admin.SchemaObjectType;
import org.teiid.adminapi.CacheStatistics;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.core.types.BinaryType;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.dqp.service.AutoGenDataService;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.language.Literal;
import org.teiid.language.QueryExpression;
import org.teiid.language.Update;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.odata.api.Client;
import org.teiid.odbc.ODBCServerRemoteImpl;
import org.teiid.olingo.service.LocalClient;
import org.teiid.olingo.web.ODataFilter;
import org.teiid.olingo.web.ODataServlet;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.HardCodedExecutionFactory;
import org.teiid.runtime.TestEmbeddedServer;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.loopback.LoopbackExecutionFactory;
import org.teiid.transport.SocketConfiguration;
import org.teiid.transport.WireProtocol;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@SuppressWarnings("nls")
public class TestODataIntegration {

    interface CommandValidator {
        void validate(org.teiid.language.Command c);
    }

    private static final class AutoUpdateHardCodedExecutionFactory
            extends HardCodedExecutionFactory {
        CommandValidator validator;

        @Override
        public boolean supportsCompareCriteriaEquals() {
            return true;
        }

        @Override
        public UpdateExecution createUpdateExecution(
                org.teiid.language.Command command,
                ExecutionContext executionContext,
                RuntimeMetadata metadata, Object connection)
                throws TranslatorException {
            addUpdate(command.toString(), new int[] {1});
            if (validator != null) {
                validator.validate(command);
            }
            return super.createUpdateExecution(command, executionContext, metadata,connection);
        }
    }

    static final class UnitTestLocalClient extends LocalClient {
        private final Properties properties;
        private final TeiidDriver driver;
        private final String vdb;
        private boolean rollback;

        UnitTestLocalClient(String vdbName, String vdbVersion,
                Properties properties, Properties properties2,
                TeiidDriver driver, String vdb, Map<Object, Future<Boolean>> loading) {
            super(vdbName, vdbVersion, properties, loading);
            this.properties = properties2;
            this.driver = driver;
            this.vdb = vdb;
        }

        @Override
        public Connection open() throws SQLException {
            connection = LocalClient.buildConnection(driver, vdb, "1", properties);
            ODBCServerRemoteImpl.setConnectionProperties(connection, this.properties);
            return connection;
        }

        @Override
        public void rollback(String txnId) throws SQLException {
            rollback = true;
            super.rollback(txnId);
        }

        public boolean isRollback() {
            return rollback;
        }
    }

    private static final String CRLF = "\r\n";
      private static final String MIME_HEADERS = "Content-Type: application/http" + CRLF
              + "Content-Transfer-Encoding: binary" + CRLF;
    private static EmbeddedServer teiid;
    private static Server server = new Server();
    private static String baseURL;
    private static HttpClient http = new HttpClient();
    private UnitTestLocalClient localClient;
    private LoopbackExecutionFactory ef;
    private int port;

    @Before
    public void before() throws Exception {
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("UTC"));
        teiid = new EmbeddedServer();
        EmbeddedConfiguration config = new EmbeddedConfiguration();
        config.setTransactionManager(new TestEmbeddedServer.MockTransactionManager());
        SocketConfiguration sc = new SocketConfiguration();
        sc.setBindAddress("localhost");
        sc.setPortNumber(31000);
        sc.setProtocol(WireProtocol.teiid);
        config.addTransport(sc);
        teiid.start(config);
        ef = new LoopbackExecutionFactory();
        ef.setDelegate(new JDBCExecutionFactory());
        teiid.addTranslator("loopback", ef);

        createContext("/odata4", null);

        deployVDB();
    }

    private void createContext(String contextPath, Map<String, String> properties) throws Exception {
        server.stop();

        ServerConnector connector = new ServerConnector(server);
        server.setConnectors(new Connector[] { connector });

        ServletContextHandler context = new ServletContextHandler();
        if (properties != null) {
            for (Map.Entry<String, String> prop : properties.entrySet()) {
                context.setInitParameter(prop.getKey(), prop.getValue());
            }
        }
        context.setContextPath(contextPath);
        context.addServlet(new ServletHolder(new ODataServlet()), "/*");
        context.addFilter(new FilterHolder(new ODataFilter() {
            @Override
            public Client buildClient(String vdbName, String version, Properties props) {
                localClient = getClient(teiid.getDriver(), vdbName, props);
                return localClient;
            }
        }), "/*", EnumSet.allOf(DispatcherType.class));
        server.setHandler(context);
        server.start();
        port = connector.getLocalPort();
        baseURL = "http://localhost:"+port+contextPath;
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        http.setStopTimeout(5000);
        http.start();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        http.stop();
    }

    @After
    public void after() throws Exception {
        TimestampWithTimezone.resetCalendar(null);
        server.stop();
        teiid.stop();
        assertEquals(0, loading.size());
    }

    private static void deployVDB() throws IOException,
            ConnectorManagerException, VirtualDatabaseException,
            TranslatorException {
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
        assertEquals(ObjectConverterUtil.convertFileToString(
                UnitTestUtil.getTestDataFile("loopy-edmx-metadata.xml")),
                response.getContentAsString());
    }

    @Test
    public void testOpenApi2Metadata() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/swagger.json");
        assertEquals(200, response.getStatus());
        assertEquals(ObjectConverterUtil.convertFileToString(
                UnitTestUtil.getTestDataFile("loopy-vm1-metadata-swagger.json")).replace("${host}", "localhost:"+port),
                response.getContentAsString());

        //cached fetch
        response = http.GET(baseURL + "/loopy/vm1/openapi.json");
        assertEquals(200, response.getStatus());
        assertEquals(ObjectConverterUtil.convertFileToString(
                UnitTestUtil.getTestDataFile("loopy-vm1-metadata-swagger.json")).replace("${host}", "localhost:"+port),
                response.getContentAsString());

        response = http.GET(baseURL + "/loopy/pm1/swagger.json");
        assertEquals(200, response.getStatus());
        assertEquals(ObjectConverterUtil.convertFileToString(
                UnitTestUtil.getTestDataFile("loopy-pm1-metadata-swagger.json")).replace("${host}", "localhost:"+port),
                response.getContentAsString());
    }

    @Test
    public void testOpenApi3Metadata() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/openapi.json?version=3");
        assertEquals(200, response.getStatus());
        assertEquals(ObjectConverterUtil.convertFileToString(
                UnitTestUtil.getTestDataFile("loopy-vm1-metadata-openapi.json")).replace("${host}", "http://localhost:"+port),
                response.getContentAsString());

        //cached fetch
        response = http.GET(baseURL + "/loopy/vm1/openapi.json?version=3");
        assertEquals(200, response.getStatus());
        assertEquals(ObjectConverterUtil.convertFileToString(
                UnitTestUtil.getTestDataFile("loopy-vm1-metadata-openapi.json")).replace("${host}", "http://localhost:"+port),
                response.getContentAsString());

        response = http.GET(baseURL + "/loopy/pm1/openapi.json?version=3");
        assertEquals(200, response.getStatus());
        assertEquals(ObjectConverterUtil.convertFileToString(
                UnitTestUtil.getTestDataFile("loopy-pm1-metadata-openapi.json")).replace("${host}", "http://localhost:"+port),
                response.getContentAsString());
    }

    @Test
    public void testSystemMetadata() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/SYS/$metadata");
        assertEquals(404, response.getStatus());
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
                "},{\"name\":\"LobTable\",\"url\":\"LobTable\"}]" +
                "}";
        assertEquals(expected, response.getContentAsString());
    }

    @Test
    public void testFilterExpression() throws Exception {
        //won't resolve
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1?$filter=e1");
        assertEquals(400, response.getStatus());

        response = http.GET(baseURL + "/loopy/vm1/G1?$filter=true");
        assertEquals(200, response.getStatus());
    }

    @Test
    public void testFilterIsNotNull() throws Exception {
        //should work, but does not due to https://issues.apache.org/jira/browse/OLINGO-1245
        //ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1?$filter=not(" + Encoder.encode("e1 eq null") +")");

        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1?$filter=" + Encoder.encode("e1 ne null"));
        assertEquals(response.getContentAsString(), 200, response.getStatus());
    }

    @Test
    public void testEntitySet() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/loopy/vm1/$metadata#G1\",\"value\":[{\"e1\":\"ABCDEFGHIJ\",\"e2\":0,\"e3\":0.0}]}",
                response.getContentAsString());
    }

    @Test
    public void testEntitySetWithTrailingSlash() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1/");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/loopy/vm1/$metadata#G1\",\"value\":[{\"e1\":\"ABCDEFGHIJ\",\"e2\":0,\"e3\":0.0}]}",
                response.getContentAsString());
    }

    @Test
    public void testEntitySetSkipOnly() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1/?$skip=1");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL + "/loopy/vm1/$metadata#G1\",\"value\":[]}",
                response.getContentAsString());
    }

    @Test
    public void testEntitySetWithKey() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1(0)/");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/loopy/vm1/$metadata#G1/$entity\",\"e1\":\"ABCDEFGHIJ\",\"e2\":0,\"e3\":0.0}",
                response.getContentAsString());
    }

    @Test
    public void testIndividualProperty() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1(0)/e1");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/loopy/vm1/$metadata#G1(0)/e1\",\"value\":\"ABCDEFGHIJ\"}",
                response.getContentAsString());
    }

    @Test
    public void testEntity() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1(0)");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/loopy/vm1/$metadata#G1/$entity\",\"e1\":\"ABCDEFGHIJ\",\"e2\":0,\"e3\":0.0}",
                response.getContentAsString());
    }

    @Test
    public void testIndividualProperty$Value() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1(0)/e1/$value");
        assertEquals(200, response.getStatus());
        assertEquals("ABCDEFGHIJ",response.getContentAsString());
    }

    @Test
    public void testIndividualProperty$ValueNoRow() throws Exception {
        ef.setRowCount(0);
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1(0)/e1/$value");
        assertEquals(404, response.getStatus());
    }

    @Test
    public void testNavigation_1_to_1() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/pm1/G2(0)/FK0");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/loopy/pm1/$metadata#G1/$entity\",\"e1\":\"ABCDEFGHIJ\",\"e2\":0,\"e3\":0.0}",
                response.getContentAsString());
    }

    @Test
    public void testNavigation_1_to_many() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1(0)/G2_FK0");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/loopy/vm1/$metadata#G2\",\"value\":[{\"e1\":\"ABCDEFGHIJ\",\"e2\":0}]}",
                response.getContentAsString());
    }

    @Test
    public void testInsert() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("INSERT INTO x (a, b) VALUES ('teiid', 'dv')", new int[] {1});
        teiid.addTranslator("x10", hc);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("ddl", "create foreign table x ("
                + " a string, "
                + " b string, "
                + " primary key (a)"
                + ") options (updatable true);");
        mmd.addSourceMapping("x10", "x10", null);
        teiid.deployVDB("northwind", mmd);

        String payload = "{\n" +
                "  \"a\":\"teiid\",\n" +
                "  \"b\":\"dv\"\n" +
                "}";
        ContentResponse response = http.newRequest(baseURL + "/northwind/m/x")
            .method("POST")
            .content(new StringContentProvider(payload))
            .header("Content-Type", "application/json")
            .header("Prefer", "return=minimal")
            .send();
        assertEquals(204, response.getStatus());
        assertTrue(response.getHeaders().get("OData-EntityId").endsWith("northwind/m/x('ABCDEFG')"));
        //assertEquals("ABCDEFGHIJ",response.getContentAsString());
    }

    @Test
    public void testInsertDifferentTypes() throws Exception {

        AutoUpdateHardCodedExecutionFactory hc = new AutoUpdateHardCodedExecutionFactory();

        hc.addUpdate("INSERT INTO PostTable (intkey, intnum, stringkey, stringval, booleanval, "
                + "decimalval, timeval, dateval, timestampval) "
                + "VALUES (4, 4, '4', 'value_4', FALSE, -20.4, {t '00:00:04'}, "
                + "{d '2004-04-04'}, {ts '2004-01-01 00:00:04.0'})",
                new int[] {1});
        hc.addData("SELECT PostTable.intkey, PostTable.intnum, PostTable.stringkey, "
                + "PostTable.stringval, PostTable.booleanval, PostTable.decimalval, "
                + "PostTable.timeval, PostTable.dateval, PostTable.timestampval, "
                + "PostTable.clobval FROM PostTable "
                + "WHERE PostTable.intkey = 4",
                Arrays.asList(Arrays.asList(4, 4, "4", "value_4", false, new BigDecimal("-20.4"),
                        new java.sql.Time(0), new java.sql.Date(0),
                        new java.sql.Timestamp(0), null)));
        teiid.addTranslator("x11", hc);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("ddl", "CREATE foreign TABLE PostTable(\n"
                + "intkey integer PRIMARY KEY,\n"
                + "intnum integer,\n"
                + "stringkey varchar(20),\n"
                + "stringval varchar(20),\n"
                + "booleanval boolean,\n" +
                "  decimalval decimal(20, 10),\n"
                + "timeval time,\n"
                + "dateval date,\n"
                + "timestampval timestamp,\n"
                + "clobval clob) options (updatable true);");
        mmd.addSourceMapping("x11", "x11", null);
        teiid.deployVDB("northwind", mmd);


        String payload = "\n" +
                "<entry xmlns=\"http://www.w3.org/2005/Atom\" xmlns:d=\"http://docs.oasis-open.org/odata/ns/data\" "
                + "xmlns:georss=\"http://www.georss.org/georss\" "
                + "xmlns:gml=\"http://www.opengis.net/gml\" "
                + "xmlns:m=\"http://docs.oasis-open.org/odata/ns/metadata\">\n" +
                "   <category scheme=\"http://docs.oasis-open.org/odata/ns/scheme\" />\n" +
                "   <content type=\"application/xml\">\n" +
                "      <m:properties>\n" +
                "         <d:intkey m:type=\"Int32\">4</d:intkey>\n" +
                "         <d:intnum m:type=\"Int32\">4</d:intnum>\n" +
                "         <d:stringkey>4</d:stringkey>\n" +
                "         <d:stringval>value_4</d:stringval>\n" +
                "         <d:booleanval m:type=\"Boolean\">false</d:booleanval>\n" +
                "         <d:decimalval m:type=\"Double\">-20.4</d:decimalval>\n" +
                "         <d:timeval m:type=\"TimeOfDay\">00:00:04</d:timeval>\n" +
                "         <d:dateval m:type=\"Date\">2004-04-04</d:dateval>\n" +
                "         <d:timestampval m:type=\"DateTimeOffset\">2004-01-01T00:00:04Z</d:timestampval>\n" +
                "      </m:properties>\n" +
                "   </content>\n" +
                "</entry>";
        ContentResponse response = http.newRequest(baseURL + "/northwind/m/PostTable")
            .method("POST")
            .content(new StringContentProvider(payload))
            .header("Content-Type", "application/xml")
            .header("Prefer", "return=minimal")
            .send();
        assertEquals(204, response.getStatus());
        assertTrue(response.getHeaders().get("OData-EntityId"),
                response.getHeaders().get("OData-EntityId").endsWith("northwind/m/PostTable(4)"));

        String jsonPlayload= "{\n" +
                "  \"intkey\":4,\n" +
                "  \"intnum\":4,\n" +
                "  \"stringkey\":\"4\",\n" +
                "  \"stringval\":\"value_4\",\n" +
                "  \"booleanval\":false,\n" +
                "  \"decimalval\":-20.4,\n" +
                "  \"timeval\":\"00:00:04\",\n" +
                "  \"dateval\":\"2004-04-04\",\n" +
                "  \"timestampval\":\"2004-01-01T00:00:04Z\"" +
                "}";
        response = http.newRequest(baseURL + "/northwind/m/PostTable")
                .method("POST")
                .content(new StringContentProvider(jsonPlayload))
                .header("Content-Type", "application/json")
                .header("Prefer", "return=representation")
                .send();
        assertEquals(201, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#PostTable\","
                + "\"intkey\":4,"
                + "\"intnum\":4,"
                + "\"stringkey\":\"4\","
                + "\"stringval\":\"value_4\","
                + "\"booleanval\":false,"
                + "\"decimalval\":-20.4,"
                + "\"timeval\":\"00:00:00\","
                + "\"dateval\":\"1970-01-01\","
                + "\"timestampval\":\"1970-01-01T00:00:00Z\""
                + "}", response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/PostTable(4)/clobval")
                .method("POST")
                .content(new StringContentProvider("clob value"))
                .send();
        assertEquals(405, response.getStatus());

        hc.validator = (org.teiid.language.Command c) -> {
            Update update = (Update) c;
            Literal value = (Literal) update.getChanges().get(0).getValue();
            Clob clob = (Clob) value.getValue();
            try {
                assertEquals("clob value", clob.getSubString(1, (int)clob.length()));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };

        response = http.newRequest(baseURL + "/northwind/m/PostTable(4)/clobval")
                .method("PUT")
                .content(new StringContentProvider("clob value"))
                .send();
        assertEquals(204, response.getStatus());

        hc.validator = null;

        response = http.newRequest(baseURL + "/northwind/m/PostTable(4)/clobval")
                .method("DELETE")
                .send();
        assertEquals(204, response.getStatus());
    }

    @Test
    public void testDeepInsert() throws Exception {
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory();
        hc.addUpdate("INSERT INTO x (a, b) VALUES ('teiid', 'dv')", new int[] {1});
        hc.addUpdate("INSERT INTO y (a, b) VALUES ('odata', 'teiid')", new int[] {1});
        hc.addUpdate("INSERT INTO y (a, b) VALUES ('odata4', 'teiid')", new int[] {1});
        hc.addUpdate("INSERT INTO z (a, b) VALUES ('odata', 'teiid')", new int[] {1});
        hc.addUpdate("INSERT INTO z (a, b) VALUES ('odata4', 'olingo4')", new int[] {1});
        hc.addData("SELECT x.a, x.b FROM x", Arrays.asList(Arrays.asList("teiid", "dv")));
        hc.addData("SELECT y.b, y.a FROM y", Arrays.asList(Arrays.asList("teiid", "odata"), Arrays.asList("teiid", "odata4")));
        hc.addData("SELECT z.b, z.a FROM z", Arrays.asList(Arrays.asList("teiid", "odata"), Arrays.asList("olingo4", "odata4")));
        teiid.addTranslator("x10", hc);

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
                + " CONSTRAINT FKX FOREIGN KEY (b) REFERENCES x(a)"
                + ") options (updatable true);");
        mmd.addSourceMapping("x10", "x10", null);
        teiid.deployVDB("northwind", mmd);



        // update to collection based reference
        String payload = "{\n" +
                "  \"a\":\"teiid\",\n" +
                "  \"b\":\"dv\",\n" +
                "     \"y_FKX\": [\n"+
                "        {"+
                "          \"a\":\"odata\",\n" +
                "          \"b\":\"teiid\"\n" +
                "        },\n"+
                "        {\n"+
                "          \"a\":\"odata4\",\n" +
                "          \"b\":\"teiid\"\n" +
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
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#x\",\"a\":\"teiid\",\"b\":\"dv\"}",
                response.getContentAsString());

        // update to collection based reference
        payload = "{\n" +
                "  \"a\":\"teiid\",\n" +
                "  \"b\":\"dv\",\n" +
                "    \"y_FKX\": [\n"+
                "        {"+
                "          \"a\":\"odata\",\n" +
                "          \"b\":\"teiid\"\n" +
                "        },\n"+
                "        {\n"+
                "          \"a\":\"odata4\",\n" +
                "          \"b\":\"teiid\"\n" +
                "        }\n"+
                "     ],\n"+
                "    \"z_FKX\": [\n"+
                "        {"+
                "          \"a\":\"odata\",\n" +
                "          \"b\":\"teiid\"\n" +
                "        },\n"+
                "        {\n"+
                "          \"a\":\"odata4\",\n" +
                "          \"b\":\"olingo4\"\n" +
                "        }\n"+
                "     ]\n"+
                "}";
        response = http.newRequest(baseURL + "/northwind/m/x")
                .method("POST")
                .content(new StringContentProvider(payload), ContentType.APPLICATION_JSON.toString())
                // when this header is defined the return should be expanded, but due to way olingo
                // designed it is going to be a big refactoring.
                .header("Prefer", "return=representation")
                .send();
        assertEquals(201, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#x\",\"a\":\"teiid\",\"b\":\"dv\"}",
                response.getContentAsString());
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
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/getCustomers(p2=2011-09-11T00:00:00Z,p3=2.0)");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#Edm.DateTimeOffset\",\"value\":\"2011-09-11T00:00:00Z\"}",
                response.getContentAsString());
    }

    @Test
    public void testFunctionReturningResultSet() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/procResultSet(x='foo''bar',y=1)");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#Collection(Loopy.1.VM1.procResultSet_RSParam)\","
                + "\"value\":[{\"x\":\"foo'bar\",\"y\":1},"
                + "{\"x\":\"second\",\"y\":2},"
                + "{\"x\":\"third\",\"y\":3}]}",
                response.getContentAsString());

        response = http.GET(baseURL + "/loopy/vm1/procResultSet(x='foo',y='1'");
        assertEquals(400, response.getStatus());

        response = http.GET(baseURL + "/loopy/vm1/procResultSet(x='foo''bar',y=1)?$filter="+Encoder.encode("y eq 3"));
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#Collection(Loopy.1.VM1.procResultSet_RSParam)\","
                + "\"value\":[{\"x\":\"third\",\"y\":3}]}", response.getContentAsString());

        response = http.GET(baseURL + "/loopy/vm1/procResultSet(x='foo''bar',y=1)?$skip=1&$top=1");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#Collection(Loopy.1.VM1.procResultSet_RSParam)\","
                + "\"value\":[{\"x\":\"second\",\"y\":2}]}", response.getContentAsString());

        response = http.GET(baseURL + "/loopy/vm1/procResultSet(x='foo''bar',y=1)?$orderby=y%20desc");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#Collection(Loopy.1.VM1.procResultSet_RSParam)\","
                + "\"value\":[{\"x\":\"third\",\"y\":3},"
                + "{\"x\":\"second\",\"y\":2},"
                + "{\"x\":\"foo'bar\",\"y\":1}]}",
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
        //can't handle multiple lob rows
        assertEquals(404, response.getStatus());
    }

    @Test
    public void testFunctionReturningReturningArray() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/getCustomerIds(p1=1)");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#Collection(Edm.Int32)\",\"value\":[1,2]}",
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
    public void testAllowHeaderOnMethodNotSupported() throws Exception {
        ContentResponse response = http.newRequest(baseURL + "/loopy/vm1/actionXML")
                .method("GET")
                .content(new StringContentProvider("<name>foo2</name>"), "application/xml")
                .send();
        assertEquals(405, response.getStatus());
        assertEquals("POST", getHeader(response, "Allow"));
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
        assertEquals(404, response.getStatus());
        assertEquals("{ \"error\": { \"code\": \"\", \"message\": \"TEIID16022 Unknown Schema or Schema is not visible in VDB\" } }", response.getContentAsString());

        response = http.newRequest(baseURL + "/loopy/PM2/G1?$format=xml").send();
        assertEquals(404, response.getStatus());
        assertEquals("<m:error xmlns:m=\"http://docs.oasis-open.org/odata/ns/metadata\"><m:code></m:code><m:message>"
                + "TEIID16022 Unknown Schema or Schema is not visible in VDB</m:message></m:error>", response.getContentAsString());
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
                GeneratedKeys keys = executionContext.getCommandContext().returnGeneratedKeys(
                        new String[] { "a" },new Class[] { String.class });
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

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("ddl", "create foreign table x (a string, b string, c integer, "
                + "primary key (a)) options (updatable true);");
        mmd.addSourceMapping("x", "x", null);
        teiid.deployVDB("northwind", mmd);



        ContentResponse response = http.newRequest(baseURL + "/northwind/m/x")
                .method("POST")
                .content(new StringContentProvider("{\"b\":\"b\", \"c\":5}"), "application/json")
                .send();
        assertEquals(201, response.getStatus());
    }

    Map<Object, Future<Boolean>> loading = new HashMap<>();

    private UnitTestLocalClient getClient(final TeiidDriver driver, final String vdb, final Properties properties) {
        return new UnitTestLocalClient(vdb, "1", properties, properties, driver, vdb, loading);
    }

    @Test
    public void testSkipNoPKTable() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/PM1/NoPKTable");
        assertEquals(404, response.getStatus());
        assertEquals("{\"error\":{\"code\":null,\"message\":\"Cannot find EntitySet, Singleton, "
                + "ActionImport or FunctionImport with name 'NoPKTable'.\"}}",
                response.getContentAsString());
    }

    @Test
    public void testInvalidCharacterReplacement() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("DDL", "create view x (a string primary key, b char, "
                + "c string[], d integer) as select 'ab\u0000cd\u0001', char(22), "
                + "('a\u00021','b1'), 1;");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        Map<String, String> props = new HashMap<String, String>();
        props.put(LocalClient.INVALID_CHARACTER_REPLACEMENT, " ");
        createContext("/odata4", props);

        ContentResponse response = null;
        response = http.newRequest(baseURL + "/northwind/vw/x")
                .method("GET")
                .header("Accept", "application/xml")
                .send();

        assertEquals(200, response.getStatus());
        String payload =
                "<a:content type=\"application/xml\">"
                    + "<m:properties><d:a>ab cd </d:a>"
                        + "<d:b> </d:b>"
                        + "<d:c m:type=\"#Collection(String)\">"
                            + "<m:element>a 1</m:element>"
                            + "<m:element>b1</m:element>"
                        + "</d:c>"
                        + "<d:d m:type=\"Int32\">1</d:d>"
                    + "</m:properties>"
                + "</a:content>";

        assertTrue(response.getContentAsString().contains(payload));

        response = http.GET(baseURL + "/northwind/vw/x");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL + "/northwind/vw/$metadata#x\",\"value\":["
                + "{\"a\":\"ab\\u0000cd\\u0001\",\"b\":\"\\u0016\",\"c\":[\"a\\u00021\",\"b1\"],\"d\":1}]}",
                response.getContentAsString());
    }

    @Test
    public void testArrayResults() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("DDL", "create view x (a string primary key, b integer[], c string[][]) "
                + "as select 'x', (1, 2, 3), (('a','b'),('c','d')) union "
                + "select 'y', (4, 5, 6), (('x','y'),('z','u'));");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.GET(baseURL + "/northwind/vw/x?$format=json&$select=a,b");
        assertEquals(200, response.getStatus());
        assertEquals(
                "{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x(a,b)\","
                + "\"value\":[{\"a\":\"x\",\"b\":[1,2,3]},{\"a\":\"y\",\"b\":[4,5,6]}]}",
                response.getContentAsString());

        // since there are multi-dimentional array it will be not supported
        response = http.GET(baseURL + "/northwind/vw/x?$format=json");
        assertEquals(501, response.getStatus());
    }

    @Test
    public void test$ItFilter() throws Exception {
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory() {
            @Override
            public boolean supportsCompareCriteriaEquals() {
                return true;
            }
        };
        hc.addData("SELECT x.c, x.a FROM x WHERE x.a = 'x'", Arrays.asList(Arrays.asList(new String[] {"google.net", "google.com"}, 'x')));
        hc.addData("SELECT x.c, x.a FROM x WHERE x.a = 'y'", Arrays.asList(Arrays.asList(new String[] {"example.net", "example.com"}, 'y')));

        teiid.addTranslator("x8", hc);
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("DDL", "create foreign table x  (a string primary key, b integer[], c string[]);");
        mmd.setModelType(Model.Type.PHYSICAL);
        mmd.addSourceMapping("x8", "x8", null);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.GET(baseURL + "/northwind/vw/x('x')/c?$filter=endswith($it,'com')");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL + "/northwind/vw/$metadata#x('x')/c\",\"value\":[\"google.com\"]}", response.getContentAsString());

        response = http.GET(baseURL + "/northwind/vw/x('y')/c?$filter=startswith($it,'example')");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x('y')/c\",\"value\":[\"example.net\",\"example.com\"]}", response.getContentAsString());

        response = http.GET(baseURL + "/northwind/vw/x('y')/c?$filter=startswith($it,'example')&$orderby=$it");
        assertEquals(501, response.getStatus());

        response = http.GET(baseURL + "/northwind/vw/x('x')/c?$filter=endswith($it,'com')%20or%20endswith($it,'net')");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x('x')/c\",\"value\":[\"google.net\",\"google.com\"]}", response.getContentAsString());
    }

    @Test
    public void testArrayInsertResults() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("INSERT INTO x (a, b) VALUES ('x', (1, 2, 3))", new int[] {1});
        teiid.addTranslator("x5", hc);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("DDL", "create foreign table x (a string primary key, b integer[], c string[][]) OPTIONS (updatable true);");
        mmd.setModelType(Model.Type.PHYSICAL);
        mmd.addSourceMapping("x5", "x5", null);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.newRequest(baseURL + "/northwind/m/x")
                .method("POST")
                .content(new StringContentProvider("{\"a\":\"x\",\"b\":[1,2,3]}"), "application/json")
                .send();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testArrayUpdateResults() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("UPDATE x SET b = (1, 2, 3) WHERE x.a = 'x'", new int[] {1});
        teiid.addTranslator("x6", hc);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("DDL", "create foreign table x (a string primary key, b integer[], c string[][]) OPTIONS (updatable true);");
        mmd.setModelType(Model.Type.PHYSICAL);
        mmd.addSourceMapping("x6", "x6", null);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.newRequest(baseURL + "/northwind/m/x('x')")
                .method("PATCH")
                .content(new StringContentProvider("{\"a\":\"x\",\"b\":[1,2,3]}"), "application/json")
                .send();
        assertEquals(204, response.getStatus());
    }

    @Test
    public void testResultsetCaching() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer) "
                + "as select 'xyz', 123 union all select 'abc', 456;");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        Map<String, String> props = new HashMap<String, String>();
        props.put("batch-size", "1");
        props.put("connection.resultSetCacheMode", "true");
        createContext("/odata4", props);

        ContentResponse response = http.GET(baseURL + "/northwind/vw/x?$format=json");
        assertEquals(200, response.getStatus());
        String starts = "{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x\",\"value\":[{\"a\":\"abc\",\"b\":456}],"
                + "\"@odata.nextLink\":\""+baseURL+"/northwind/vw/x?$format=json&$skiptoken=";
        String ends = ",1\"}";
        assertTrue(response.getContentAsString(), response.getContentAsString().startsWith(starts));
        assertTrue(response.getContentAsString(), response.getContentAsString().endsWith(ends));

        JsonNode node = getJSONNode(response);
        String nextLink = node.get("@odata.nextLink").asText();
        response = http.GET(nextLink);
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x\",\"value\":[{\"a\":\"xyz\",\"b\":123}]}",
                response.getContentAsString());
        CacheStatistics stats = teiid.getAdmin().getCacheStats(Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE.name()).iterator().next();
        //first query misses, second hits
        assertEquals(50, stats.getHitRatio(), 0);

        response = http.GET(baseURL + "/northwind/vw/x?$format=json");
        assertEquals(200, response.getStatus());
        stats = teiid.getAdmin().getCacheStats(Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE.name()).iterator().next();
        //third should reuse as well
        assertEquals(66, stats.getHitRatio(), 1);
    }

    @Test
    public void testNextWithProcedure() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", "create procedure x (i integer) returns table (b integer) options (updatecount 0) as select i union all select i+1;");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        Map<String, String> props = new HashMap<String, String>();
        props.put("batch-size", "1");
        createContext("/odata4", props);

        ContentResponse response = http.GET(baseURL + "/northwind/vw/x(i=1)");
        assertEquals(200, response.getStatus());
        String starts = "{\"@odata.context\":\"$metadata#Collection(Edm.ComplexType)\",\"value\":[{\"b\":1}],";
        assertTrue(response.getContentAsString(), response.getContentAsString().startsWith(starts));
        JsonNode node = getJSONNode(response);
        String nextLink = node.get("@odata.nextLink").asText();
        response = http.GET(nextLink);
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#Collection(northwind.1.vw.x_RSParam)\",\"value\":[{\"b\":2}]}",
                response.getContentAsString());

        //xml not supported
        response = http.GET(baseURL + "/northwind/vw/x(i=1)?$format=xml");
        assertEquals(400, response.getStatus());
    }

    /**
     * With an update count (above) this is mapped as a function, without as an action
     * @throws Exception
     */
    @Test
    public void testActionMappingWithoutUpdatecount() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", "create procedure x (i integer) returns table (b integer) as select i union all select i+1;");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.newRequest(baseURL + "/northwind/vw/x")
                .method("POST")
                .content(new StringContentProvider("{\"i\":1}"), "application/json")
                .send();
        assertEquals(200, response.getStatus());
    }

    @Test
    public void testSkipToken() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer) "
                + "as select 'xyz', 123 union all select 'abc', 456;");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        Map<String, String> props = new HashMap<>();
        props.put("batch-size", "1");
        createContext("/odata4", props);

        ContentResponse response = http.GET(baseURL + "/northwind/vw/x?$format=json");
        assertEquals(200, response.getStatus());
        String starts = "{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x\",\"value\":[{\"a\":\"abc\",\"b\":456}],"
                + "\"@odata.nextLink\":\""+baseURL+"/northwind/vw/x?$format=json&$skiptoken=";
        String ends = ",1\"}";
        assertTrue(response.getContentAsString(), response.getContentAsString().startsWith(starts));
        assertTrue(response.getContentAsString(), response.getContentAsString().endsWith(ends));

        JsonNode node = getJSONNode(response);
        String nextLink = node.get("@odata.nextLink").asText();
        response = http.GET(nextLink);
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x\",\"value\":[{\"a\":\"xyz\",\"b\":123}]}",
                response.getContentAsString());
        CacheStatistics stats = teiid.getAdmin().getCacheStats(Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE.name()).iterator().next();
        //first query misses, second hits
        assertEquals(50, stats.getHitRatio(), 0);

        response = http.GET(baseURL + "/northwind/vw/x?$format=json");
        assertEquals(200, response.getStatus());
        stats = teiid.getAdmin().getCacheStats(Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE.name()).iterator().next();
        //third should miss as it's a new session
        assertEquals(33, stats.getHitRatio(), 1);

        //invalid
        response = http.GET(baseURL + "/northwind/vw/x?$skiptoken=a");
        assertEquals(500, response.getStatus());
        assertTrue(response.getContentAsString(), response.getContentAsString().contains("TEIID16062"));
    }

    @Test
    public void testSkipTokenNoSystemOptions() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer) "
                + "as select 'xyz', 123 union all select 'abc', 456;");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        Map<String, String> props = new HashMap<String, String>();
        props.put("batch-size", "1");
        createContext("/odata4", props);

        ContentResponse response = http.GET(baseURL + "/northwind/vw/x");
        assertEquals(200, response.getStatus());
    }

    @Test
    public void test$SkipWithNegitive() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer) "
                + "as select 'xyz', 123 union all select 'abc', 456;");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.GET(baseURL + "/northwind/vw/x?$skip=-1");
        assertEquals(400, response.getStatus());
    }

    @Test
    public void testAlias() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer) "
                + "as select 'xyz', 123 union all select 'abc', 456;");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.GET(baseURL
                + "/northwind/vw/x?$filter=" + Encoder.encode("a eq @a")
                + "&" + Encoder.encode("@a")+"="+Encoder.encode("'xyz'"));
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x\",\"value\":[{\"a\":\"xyz\",\"b\":123}]}",
                response.getContentAsString());
    }

    @Test
    public void testNegitive$Top() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer) "
                + "as select 'xyz', 123 union all select 'abc', 456;");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.GET(baseURL+ "/northwind/vw/x?$top=-1");
        assertEquals(400, response.getStatus());
    }

    @Test
    public void testAliasNoValue() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer) "
                + "as select 'xyz', 123 union all select 'abc', 456;");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.GET(baseURL + "/northwind/vw/x?$filter="+Encoder.encode("a eq @a"));
        assertEquals(200, response.getStatus());
    }

    @Test
    public void testSkipTokenWithPageSize() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer) as "
                + "select 'xyz', 123 union all select 'abc', 456;");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.newRequest(baseURL + "/northwind/vw/x?$format=json")
            .header("Prefer", "odata.maxpagesize=1")
            .send();

        assertEquals(200, response.getStatus());
        String starts = "{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x\",\"value\":[{\"a\":\"abc\",\"b\":456}],"
                + "\"@odata.nextLink\":\""+baseURL+"/northwind/vw/x?$format=json&$skiptoken=";
        String ends = ",1\"}";
        assertTrue(response.getContentAsString(), response.getContentAsString().startsWith(starts));
        assertTrue(response.getContentAsString(), response.getContentAsString().endsWith(ends));
        assertEquals("odata.maxpagesize=1", getHeader(response, "Preference-Applied"));

        JsonNode node = getJSONNode(response);
        String nextLink = node.get("@odata.nextLink").asText();
        response = http.GET(nextLink);
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x\",\"value\":[{\"a\":\"xyz\",\"b\":123}]}",
                response.getContentAsString());
    }

    private String getHeader(ContentResponse response, String header) {
        return response.getHeaders().get(header);
    }

    @Test
    public void testCount() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer) "
                + "as select 'a', 123 "
                + "union all select 'b', 456 "
                + "union all select 'c', 789 "
                + "union all select 'd', 012;");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        Map<String, String> props = new HashMap<String, String>();
        props.put("batch-size", "1");
        createContext("/odata4", props);

        ContentResponse response = http.GET(baseURL + "/northwind/vw/x?$format=json&$count=true&$top=1&$skip=1");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x\",\"@odata.count\":4,\"value\":[{\"a\":\"b\",\"b\":456}]}",
                response.getContentAsString());

        response = http.GET(baseURL + "/northwind/vw/x?$format=json&$count=true&$filter="+Encoder.encode("a eq 'a'"));
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x\",\"@odata.count\":1,\"value\":[{\"a\":\"a\",\"b\":123}]}",
                response.getContentAsString());

        //effectively the same as above
        response = http.GET(baseURL + "/northwind/vw/x?$format=json&$count=true&$skip=1");
        assertEquals(200, response.getStatus());
        String r = "{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x\",\"@odata.count\":4,"
                + "\"value\":[{\"a\":\"b\",\"b\":456}],\"@odata.nextLink\":";
        assertTrue(response.getContentAsString(), response.getContentAsString().startsWith(r));

        //now there should be a next
        response = http.GET(baseURL + "/northwind/vw/x?$format=json&$count=true");
        assertEquals(200, response.getStatus());
        String ends = ",1,4\"}";
        String responseStr = response.getContentAsString();
        assertTrue(responseStr, responseStr.endsWith(ends));

        //Next string
        JsonParser parser = new JsonFactory(new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY, true))
                .createParser(responseStr);
        JsonNode node = parser.getCodec().readTree(parser);

        response = http.newRequest(node.get("@odata.nextLink").asText())
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        responseStr = response.getContentAsString();
        assertTrue(responseStr, responseStr.startsWith("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x\","
                + "\"@odata.count\":4,"
                + "\"value\":[{\"a\":\"b\",\"b\":456}],"
                + "\"@odata.nextLink\":\""+baseURL+"/northwind/vw/x?$format=json&$count=true&$skiptoken="));

        response = http.GET(baseURL + "/northwind/vw/x/$count");
        assertEquals(200, response.getStatus());
        assertEquals("4", response.getContentAsString());
    }

    @Test
    public void testCompositeKeyUpdates() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("DELETE FROM x WHERE x.a = 'a' AND x.b = 'b'", new int[] {1});
        hc.addUpdate("INSERT INTO x (a, b, c) VALUES ('a', 'b', 5)", new int[] {1});
        hc.addUpdate("UPDATE x SET c = 10 WHERE x.a = 'a' AND x.b = 'b'", new int[] {1});
        hc.addData("SELECT x.a, x.b, x.c FROM x WHERE x.a = 'a' AND x.b = 'b'",
                Arrays.asList(Arrays.asList("a", "b", 1)));
        teiid.addTranslator("x1", hc);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("ddl", "create foreign table x (a string, b string, c integer, "
                + "primary key (a, b)) options (updatable true);");
        mmd.addSourceMapping("x1", "x1", null);
        teiid.deployVDB("northwind", mmd);



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
        assertEquals(201, response.getStatus());

        response = http.newRequest(baseURL + "/northwind/m/x(a='a',b='b')")
                .method("PATCH")
                .content(new StringContentProvider("{\"c\":10}"), "application/json")
                .send();
        assertEquals(204, response.getStatus());

        response = http.newRequest(baseURL + "/northwind/m/x(a='a',b='b')")
                .method("PUT")
                .content(new StringContentProvider("{\"a\":\"a\", \"b\":\"b\", \"c\":5}"), "application/json")
                .send();
        assertEquals(204, response.getStatus());
    }

    @Test
    public void testPutFailure() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("DELETE FROM x WHERE x.a = 'a' AND x.b = 'b'", new TranslatorException());
        teiid.addTranslator("x1", hc);
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("ddl", "create foreign table x (a string, b string, c integer, "
                + "primary key (a, b)) options (updatable true);");
        mmd.addSourceMapping("x1", "x1", null);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.newRequest(baseURL + "/northwind/m/x(a='a',b='b')")
                .method("PUT")
                .content(new StringContentProvider("{\"a\":\"a\", \"b\":\"b\", \"c\":5}"), "application/json")
                .send();
        assertEquals(500, response.getStatus());
        assertTrue(localClient.isRollback());
    }

    @Test
    public void testPutRawValue() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("UPDATE x SET c = 6 WHERE x.a = 'a'", new int[] {1});
        hc.addUpdate("UPDATE x SET b = '6' WHERE x.a = 'a'", new int[] {1});
        teiid.addTranslator("x1", hc);
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("ddl", "create foreign table x (a string, b string, c integer, "
                + "primary key (a)) options (updatable true);");
        mmd.addSourceMapping("x1", "x1", null);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.newRequest(baseURL + "/northwind/m/x('a')/c/$value")
                .method("PUT")
                .content(new BytesContentProvider("6".getBytes()))
                .send();
        assertEquals(204, response.getStatus());

        response = http.newRequest(baseURL + "/northwind/m/x('a')/b/$value")
                .method("PUT")
                .content(new BytesContentProvider("6".getBytes()))
                .send();
        assertEquals(204, response.getStatus());
    }

    @Test
    public void testEntityId() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("UPDATE x SET c = 6 WHERE x.a = 'a'", new int[] {1});
        hc.addUpdate("UPDATE x SET b = '6' WHERE x.a = 'a'", new int[] {1});
        teiid.addTranslator("x1", hc);
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("ddl", "create foreign table x (a string, b string, c integer, "
                + "primary key (a)) options (updatable true);");
        mmd.addSourceMapping("x1", "x1", null);
        teiid.deployVDB("northwind", mmd);



        ContentResponse response = http.newRequest(baseURL + "/northwind/m/$entity?$id="+baseURL+"/northwind/m/x('a')&$select=b")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#x(a,b)/$entity\","
                + "\"@odata.id\":\""+baseURL+"/northwind/m/x('ABCDEFG')\","
                + "\"a\":\"ABCDEFG\","
                + "\"b\":\"ABCDEFG\"}", response.getContentAsString());
    }

    @Test
    public void testCrossJoin() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("UPDATE y SET b = 'a' WHERE y.a = 'a'", new int[] {1});
        teiid.addTranslator("x9", hc);

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
                + ") options (updatable true);"
                + "create foreign table z ("
                + " a1 string, "
                + " b1 string, "
                + " primary key (a1)"
                + ") options (updatable true);");
        mmd.addSourceMapping("x9", "x9", null);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.newRequest(baseURL + "/northwind/m/$crossjoin(x,y)")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        String u = baseURL + "/northwind/m/";
        assertEquals("{\"@odata.context\":\"$metadata#Collection(Edm.ComplexType)\","
                + "\"value\":[{\"x@odata.navigationLink\":\""+u+"x('ABCDEFG')\","
                        + "\"y@odata.navigationLink\":\""+u+"y('ABCDEFG')\"}]}",
                        response.getContentAsString());

        // TODO: OLINGO-904
        response = http.newRequest(baseURL + "/northwind/m/$crossjoin(x,y)?$expand=x")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#Collection(Edm.ComplexType)\",\"value\":[{\"x\":{\"a\":\"ABCDEFG\",\"b\":\"ABCDEFG\"},\"y@odata.navigationLink\":\""+u+"y('ABCDEFG')\"}]}", response.getContentAsString());

        // xml is not supported
        response = http.newRequest(baseURL + "/northwind/m/$crossjoin(x,y)?$format=xml")
                .method("GET")
                .send();
        assertEquals(400, response.getStatus());

        response = http.newRequest(baseURL + "/northwind/m/$crossjoin(x,z)?$expand=z")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        u = baseURL + "/northwind/m/";
        assertTrue(response.getContentAsString().contains("\"z\":{\"a1\""));
    }

    @Test
    public void testNavigationLinks() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("UPDATE y SET b = 'a' WHERE y.a = 'a'", new int[] {1});
        teiid.addTranslator("x4", hc);

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



        ContentResponse response = http.newRequest(baseURL + "/northwind/m/x('a')/y_FKX/$ref")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        String url = baseURL + "/northwind/m/";
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#Collection($ref)\","
                + "\"value\":[{\"@odata.id\":\""+url+"y('ABCDEFG')\"}]}",
                response.getContentAsString());

        // update to collection based reference
        String payload = "{\n" +
                "\"@odata.id\": \"/odata4/northwind/m/y('a')\"\n" +
                "}";
        response = http.newRequest(baseURL + "/northwind/m/x('a')/y_FKX/$ref")
                .method("POST")
                .content(new StringContentProvider(payload), ContentType.APPLICATION_JSON.toString())
                .send();
        assertEquals(204, response.getStatus());
    }

    @Test
    public void testCreateViaNavigation() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("INSERT INTO y (a) VALUES ('val')", new int[] {1});
        hc.addData("SELECT y.a, y.b FROM y WHERE y.a = 'val'", Arrays.asList(Arrays.asList("val", null)));
        hc.addUpdate("UPDATE y SET b = 'a' WHERE y.a = 'val'", new int[] {1});
        teiid.addTranslator("x4", hc);

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

        // update to collection based reference
        String payload = "{\n" +
                "\"a\": \"val\"\n" +
                "}";
        ContentResponse response = http.newRequest(baseURL + "/northwind/m/x('a')/y_FKX")
                .method("POST")
                .content(new StringContentProvider(payload), ContentType.APPLICATION_JSON.toString())
                .send();
        assertEquals(201, response.getStatus());
    }

    @Test
    public void testRelatedEntities() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addData("SELECT x.a, x.b FROM x WHERE x.a = 'xa1'", Arrays.asList(Arrays.asList("xa1", "xb")));
        hc.addData("SELECT y.a, y.b FROM y WHERE y.b = 'xa1'", Arrays.asList(Arrays.asList("ya1", "xa1"),
                Arrays.asList("ya2", "xa1")));

        // 1-many
        hc.addData("SELECT x.a FROM x WHERE x.a = 'xa2'", Arrays.asList(Arrays.asList("xa2")));
        hc.addData("SELECT y.a, y.b FROM y WHERE y.b = 'xa2'", new ArrayList<List<?>>());

        // 1-1
        hc.addData("SELECT z.a FROM z WHERE z.a = 'xa3'", Arrays.asList(Arrays.asList("xa3")));
        hc.addData("SELECT x.a, x.b FROM x WHERE x.a = 'xa3'", new ArrayList<List<?>>());

        teiid.addTranslator("x7", hc);

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



        ContentResponse response = null;

        // single 1-many relation
        response = http.newRequest(baseURL + "/northwind/m/x('xa2')/y_FKX")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#y\",\"value\":[]}", response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/z('xa3')/FKX")
                .method("GET")
                .send();
        assertEquals(204, response.getStatus());
    }

    @Test
    public void testExpandSimple() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        teiid.addTranslator("x7", hc);

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



        ContentResponse response = null;
        response = http.newRequest(baseURL + "/northwind/m/x?$expand=y_FKX")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#x(y_FKX())\",\"value\":[{\"a\":\"ABCDEFG\","
                + "\"b\":\"ABCDEFG\",\"y_FKX\":[{\"a\":\"ABCDEFG\",\"b\":\"ABCDEFG\"}]}]}",
                response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/z?$expand=FKX&$select=a")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#z(a,FKX())\",\"value\":[{\"a\":\"ABCDEFG\","
                + "\"FKX\":{\"a\":\"ABCDEFG\",\"b\":\"ABCDEFG\"}}]}",
                response.getContentAsString());

        // explictly selecting and expanding
        response = http.newRequest(baseURL + "/northwind/m/z?$expand=FKX&$select=a")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#z(a,FKX())\",\"value\":[{\"a\":\"ABCDEFG\","
                + "\"FKX\":{\"a\":\"ABCDEFG\",\"b\":\"ABCDEFG\"}}]}", response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/z?$expand=FKX($select=a)")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#z(FKX(a))\",\"value\":[{\"a\":\"ABCDEFG\","
                + "\"b\":\"ABCDEFG\",\"FKX\":{\"a\":\"ABCDEFG\"}}]}", response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/z?$expand=FKX")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#z(FKX())\",\"value\":[{\"a\":\"ABCDEFG\","
                + "\"b\":\"ABCDEFG\",\"FKX\":{\"a\":\"ABCDEFG\",\"b\":\"ABCDEFG\"}}]}", response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/z?$expand=FKX($top=1)")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());

        /* TODO
        response = http.newRequest(baseURL + "/northwind/m/z?$expand=FKX/a&$select=a")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#z(a,FKX/a)\",\"value\":[{\"a\":\"ABCDEFG\",\"FKX\":{\"a\":\"ABCDEFG\",\"b\":\"ABCDEFG\"}}]}", response.getContentAsString());
        */
    }

    @Test
    public void testExpandSimple2() throws Exception {
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory();
        hc.addData("SELECT x.a, x.b FROM x", Arrays.asList(Arrays.asList("xa1", "xb"),
                Arrays.asList("xa2", "xb2")));
        hc.addData("SELECT y.b, y.a FROM y", Arrays.asList(Arrays.asList("xa1", "ya1"),
                Arrays.asList("xa1", "ya2")));
        teiid.addTranslator("x7", hc);

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



        ContentResponse response = null;
        response = http.newRequest(baseURL + "/northwind/m/x?$expand="+Encoder.encode("y_FKX($filter=b eq 'xa1')"))
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#x(y_FKX())\","
                + "\"value\":["
                + "{\"a\":\"xa1\",\"b\":\"xb\","
                + "\"y_FKX\":[{\"a\":\"ya1\",\"b\":\"xa1\"},"
                + "{\"a\":\"ya2\",\"b\":\"xa1\"}]},"
                + "{\"a\":\"xa2\",\"b\":\"xb2\","
                + "\"y_FKX\":[]}]}",
                response.getContentAsString());
    }

    @Test
    public void testExpandComplexSelf() throws Exception {
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory();
        hc.addData("SELECT tree.a, tree.b, tree.c FROM tree", Arrays.asList(Arrays.asList("1", "null", "x"), Arrays.asList("2", "1", "y"), Arrays.asList("3", "1", "z")));
        hc.addData("SELECT tree.b, tree.a, tree.c FROM tree", Arrays.asList(Arrays.asList("null", "1", "x"), Arrays.asList("1", "2", "y"), Arrays.asList("1", "3", "z")));

        teiid.addTranslator("x7", hc);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("ddl", "create foreign table tree ("
                + " a string, "
                + " b string, "
                + " c string, "
                + " primary key (a),"
                + " CONSTRAINT parent FOREIGN KEY (b) REFERENCES tree(a)"
                + ");");

        mmd.addSourceMapping("x7", "x7", null);
        teiid.deployVDB("northwind", mmd);



        ContentResponse response = null;
        response = http.newRequest(baseURL + "/northwind/m/tree?$expand=tree_parent($filter=$it/c%20eq%20%27x%27)")
                .method("GET")
                .send();

        assertEquals(response.getContentAsString(), 200, response.getStatus());

        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#tree(tree_parent())\",\"value\":["
                + "{\"a\":\"1\",\"b\":\"null\",\"c\":\"x\",\"tree_parent\":[{\"a\":\"2\",\"b\":\"1\",\"c\":\"y\"},{\"a\":\"3\",\"b\":\"1\",\"c\":\"z\"}]},"
                + "{\"a\":\"2\",\"b\":\"1\",\"c\":\"y\",\"tree_parent\":[]},"
                + "{\"a\":\"3\",\"b\":\"1\",\"c\":\"z\",\"tree_parent\":[]}]}",
                response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/tree?$expand=parent($filter=$it/c%20eq%20%27x%27)")
                .method("GET")
                .send();

        assertEquals(response.getContentAsString(), 200, response.getStatus());

        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#tree(parent())\",\"value\":[{\"a\":\"1\",\"b\":\"null\",\"c\":\"x\",\"parent\":null},"
                + "{\"a\":\"2\",\"b\":\"1\",\"c\":\"y\",\"parent\":null},"
                + "{\"a\":\"3\",\"b\":\"1\",\"c\":\"z\",\"parent\":null}]}",
                response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/tree?$expand=parent($filter=$it/c%20eq%20%27y%27)")
                .method("GET")
                .send();

        assertEquals(response.getContentAsString(), 200, response.getStatus());

        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#tree(parent())\",\"value\":[{\"a\":\"1\",\"b\":\"null\",\"c\":\"x\",\"parent\":null},"
                + "{\"a\":\"2\",\"b\":\"1\",\"c\":\"y\",\"parent\":{\"a\":\"1\",\"b\":\"null\",\"c\":\"x\"}},"
                + "{\"a\":\"3\",\"b\":\"1\",\"c\":\"z\",\"parent\":null}]}",
                response.getContentAsString());
    }

    @Test
    public void testExpandComplex() throws Exception {
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory();
        hc.addData("SELECT x.a, x.b FROM x", Arrays.asList(Arrays.asList("a", "b")));
        hc.addData("SELECT y.b, y.a FROM y", Arrays.asList(Arrays.asList("a", "y"), Arrays.asList("a", "y1")));
        hc.addData("SELECT y.a, y.b FROM y", Arrays.asList(Arrays.asList("y", "a"), Arrays.asList("y1","a")));
        hc.addData("SELECT z.a, z.b FROM z", Arrays.asList(Arrays.asList("a", "y")));
        hc.addData("SELECT z.b, z.a FROM z", Arrays.asList(Arrays.asList("y", "a")));

        teiid.addTranslator("x7", hc);

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
                + " CONSTRAINT FKX FOREIGN KEY (a) REFERENCES x(a),"
                + " CONSTRAINT FKY FOREIGN KEY (b) REFERENCES y(a)"
                + ") options (updatable true);");

        mmd.addSourceMapping("x7", "x7", null);
        teiid.deployVDB("northwind", mmd);



        ContentResponse response = null;
        response = http.newRequest(baseURL + "/northwind/m/x?$expand=y_FKX($expand=z_FKY)")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#x(y_FKX(z_FKY()))\",\"value\":["
                + "{\"a\":\"a\",\"b\":\"b\",\"y_FKX\":"
                    + "[{\"a\":\"y\",\"b\":\"a\",\"z_FKY\":[{\"a\":\"a\",\"b\":\"y\"}]},{\"a\":\"y1\",\"b\":\"a\",\"z_FKY\":[]}]}"
                + "]}",
                response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/x?$expand=y_FKX,z_FKX")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#x(y_FKX(),z_FKX())\",\"value\":["
                + "{\"a\":\"a\",\"b\":\"b\",\"y_FKX\":"
                    + "[{\"a\":\"y\",\"b\":\"a\"},{\"a\":\"y1\",\"b\":\"a\"}],"
                    + "\"z_FKX\":{\"a\":\"a\",\"b\":\"y\"}}"
                + "]}",
                response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/x?$expand=*")
            .method("GET")
            .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#x(y_FKX(),z_FKX())\",\"value\":["
                + "{\"a\":\"a\",\"b\":\"b\",\"y_FKX\":"
                    + "[{\"a\":\"y\",\"b\":\"a\"},{\"a\":\"y1\",\"b\":\"a\"}],"
                    + "\"z_FKX\":{\"a\":\"a\",\"b\":\"y\"}}"
                + "]}",
                response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/x?$expand=y_FKX($filter=a%20eq%20'y1'),*")
                .method("GET")
                .send();
            assertEquals(200, response.getStatus());
            assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#x(y_FKX(),z_FKX())\",\"value\":["
                    + "{\"a\":\"a\",\"b\":\"b\",\"y_FKX\":"
                        + "[{\"a\":\"y1\",\"b\":\"a\"}],"
                        + "\"z_FKX\":{\"a\":\"a\",\"b\":\"y\"}}"
                    + "]}",
                    response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/x?$expand=y_FKX,y_FKX")
                .method("GET")
                .send();
            assertEquals(400, response.getStatus());

        response = http.newRequest(baseURL + "/northwind/m/x?$expand=*($levels=3)")
                .method("GET")
                .send();
            assertEquals(200, response.getStatus());
            String expected = "{" +
                    "\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#x(y_FKX(),z_FKX())\"," +
                    "\"value\":[" +
                        "{" +
                        "\"a\":\"a\"," +
                          "\"b\":\"b\"," +
                          "\"y_FKX\":[" +
                            "{" +
                              "\"a\":\"y\"," +
                              "\"b\":\"a\"," +
                              "\"FKX\":" +
                                "{" +
                                  "\"@odata.id\":\""+baseURL+"/northwind/m/x('a')\"" +
                                "}" +
                              "," +
                              "\"z_FKY\":[" +
                                "{" +
                                  "\"a\":\"a\"," +
                                  "\"b\":\"y\"," +
                                  "\"FKX\":{" +
                                    "\"@odata.id\":\""+baseURL+"/northwind/m/x('a')\"" +
                                  "}," +
                                  "\"FKY\":" +
                                    "{" +
                                      "\"@odata.id\":\""+baseURL+"/northwind/m/y('y')\"" +
                                    "}" +
                                  "" +
                                "}" +
                              "]" +
                            "}," +
                            "{" +
                              "\"a\":\"y1\"," +
                              "\"b\":\"a\"," +
                              "\"FKX\":" +
                                "{" +
                                  "\"@odata.id\":\""+baseURL+"/northwind/m/x('a')\"" +
                                "}" +
                              "," +
                              "\"z_FKY\":[" +
                              "]" +
                            "}" +
                          "]," +
                          "\"z_FKX\":{" +
                            "\"a\":\"a\"," +
                            "\"b\":\"y\"," +
                            "\"FKX\":{" +
                              "\"@odata.id\":\""+baseURL+"/northwind/m/x('a')\"" +
                            "}," +
                            "\"FKY\":" +
                              "{" +
                                "\"a\":\"y\"," +
                                "\"b\":\"a\"," +
                                "\"FKX\":" +
                                  "{" +
                                    "\"@odata.id\":\""+baseURL+"/northwind/m/x('a')\"" +
                                  "}" +
                                "," +
                                "\"z_FKY\":[" +
                                  "{" +
                                    "\"@odata.id\":\""+baseURL+"/northwind/m/z('a')\"" +
                                  "}" +
                                "]" +
                              "}" +
                            "" +
                          "}" +
                        "}" +
                      "]" +
                    "}";
            assertEquals(expected, response.getContentAsString());

        //invalid it's not a self relationship
        response = http.newRequest(baseURL + "/northwind/m/x?$expand=y_FKX($levels=1)")
                .method("GET")
                .send();
            assertEquals(400, response.getStatus());

        response = http.newRequest(baseURL + "/northwind/m/x?$expand=y_FKX($filter=$it/b%20eq%20a)")
                .method("GET")
                .send();
            assertEquals(200, response.getStatus());
            assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#x(y_FKX())\",\"value\":["
                    + "{\"a\":\"a\",\"b\":\"b\",\"y_FKX\":"
                        + "[]}"
                    + "]}",
                    response.getContentAsString());
    }

    @Test
    public void testBatch() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addUpdate("DELETE FROM x WHERE x.a = 'a' AND x.b = 'b'", new int[] {1});
        teiid.addTranslator("x", hc);
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("ddl", "create foreign table x (a string, b string, c integer, "
                + "primary key (a, b)) options (updatable true);");
        mmd.addSourceMapping("x", "x", null);
        teiid.deployVDB("northwind", mmd);



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
    }

    static int ROW_COUNT = 1;

    static class ODataHardCodedExecutionFactory extends HardCodedExecutionFactory{
        @Override
        public boolean supportsCompareCriteriaEquals() {
            return true;
        }
        @Override
        protected List<? extends List<?>> getData(
                QueryExpression command) {

            if (super.getData(command) != null) {
                List<? extends List<?>> results = super.getData(command);
                return results;
            }

            Class<?>[] colTypes = command.getProjectedQuery().getColumnTypes();
            List<Expression> cols = new ArrayList<Expression>();
            for (int i = 0; i < colTypes.length; i++) {
                ElementSymbol elementSymbol = new ElementSymbol("X");
                elementSymbol.setType(colTypes[i]);
                cols.add(elementSymbol);
            }
            return (List)Arrays.asList(AutoGenDataService.createResults(cols, ROW_COUNT, false));
        }
    }

    private HardCodedExecutionFactory buildHardCodedExecutionFactory() {
        return new ODataHardCodedExecutionFactory();
    }

    @Test
    public void testJsonProcedureResultSet() throws Exception {
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory();
        hc.addData("EXEC x()", Arrays.asList(Arrays.asList("x"), Arrays.asList("y")));
        teiid.addTranslator("x2", hc);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("ddl", "create foreign procedure x () returns table(y string) OPTIONS(UPDATECOUNT 0);");
        mmd.addSourceMapping("x2", "x2", null);
        teiid.deployVDB("northwind", mmd);



        ContentResponse response =  http.GET(baseURL + "/northwind/m/x()?$format=json");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#Collection(northwind.1.m.x_RSParam)\","
                + "\"value\":[{\"y\":\"x\"},{\"y\":\"y\"}]}", response.getContentAsString());
    }

    @Test
    public void testBasicTypes() throws Exception {
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

        ddl += "; create foreign table bin_test (pk integer primary key, bin varbinary)";

        mmd.addSourceMetadata("DDL", ddl);

        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        teiid.addTranslator("x3", hc);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response= http.GET(baseURL + "/northwind/m/SmallA?$format=json&$select=TimeValue");
        assertEquals(200, response.getStatus());

        hc.addData("SELECT bin_test.pk, bin_test.bin FROM bin_test", Arrays.asList(Arrays.asList(1, new BinaryType("hello".getBytes("UTF-8")))));

        response= http.GET(baseURL + "/northwind/m/bin_test?$format=json&$select=bin");
        assertEquals(200, response.getStatus());
        assertTrue(response.getContentAsString().contains("\"bin\":\"aGVsbG8=\""));
    }

    @Test
    public void testCompositeKeyTimestamp() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        hc.addData("SELECT x.a, x.b, x.c FROM x WHERE x.a = 'a' AND x.b = {ts '2011-09-11T00:00:00'}",
                Arrays.asList(Arrays.asList("a", TimestampUtil.createTimestamp(111, 8, 11, 0, 0, 0, 0), 1)));
        hc.addUpdate("INSERT INTO x (a, b) VALUES ('b', {ts '2000-02-02 22:22:22.0'})", new int[] {1});

        teiid.addTranslator("x1", hc);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("ddl", "create foreign table x (a string, b timestamp, c integer, "
                + "primary key (a, b)) options (updatable true);");
        mmd.addSourceMapping("x1", "x1", null);
        teiid.deployVDB("northwind", mmd);



        ContentResponse response = http.newRequest(baseURL + "/northwind/m/x")
                .method("POST")
                .content(new StringContentProvider("{\"a\":\"b\", \"b\":\"2000-02-02T22:22:22Z\"}"),
                        "application/json")
                .send();
        assertEquals(201, response.getStatus());

        response = http.newRequest(baseURL + "/northwind/m/x(a='a',b=2011-09-11T00:00:00Z)")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
    }

    @Test
    public void testStreamProperties() throws Exception {
        ContentResponse response = http.newRequest(baseURL + "/loopy/vm1/LobTable(2)/e2")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("<name>content2</name>",
                response.getContentAsString());

        response = http.newRequest(baseURL + "/loopy/vm1/LobTable")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        String string = response.getContentAsString();
        // stream properties are shown in the metadata.
        assertTrue(string.contains("{\"@odata.mediaEditLink\":\""+baseURL+"/loopy/vm1/LobTable(1)/e2\""));
        assertTrue(string.contains("{\"@odata.mediaEditLink\":\""+baseURL+"/loopy/vm1/LobTable(2)/e2\""));
    }

    @Test
    public void testWithAlternateContext() throws Exception {
        Map<String, String> props = new HashMap<String, String>();
        props.put("vdb-name", "loopy");
        props.put("vdb-version", "1");

        createContext("/other", props);

        ContentResponse response = http.newRequest(baseURL + "/vm1/LobTable(2)/e2")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("<name>content2</name>",
                response.getContentAsString());
    }

    @Test
    public void testNonExistentEntity() throws Exception {
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory();
        hc.addData("SELECT x.a, x.b FROM x", Arrays.asList(Arrays.asList("a", 1)));

        teiid.addTranslator("x1", hc);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("ddl", "create foreign table x (a string, b integer, primary key (a));");
        mmd.addSourceMapping("x1", "x1", null);
        teiid.deployVDB("northwind", mmd);



        ContentResponse response = http.newRequest(baseURL + "/northwind/m/x('b')")
                .method("GET")
                .send();
        assertEquals(404, response.getStatus());

        response = http.newRequest(baseURL + "/northwind/m/x('b')/b")
                .method("GET")
                .send();
        assertEquals(404, response.getStatus());
    }

    @Test
    public void testInvalidResource() throws Exception {
        server.stop();
        Map<String, String> props = new HashMap<String, String>();
        props.put("vdb-name", "loopy");
        props.put("vdb-version", "1");

        createContext("/other", props);

        ContentResponse response = http.newRequest(baseURL + "/vm1/Foo")
                .method("GET")
                .send();
        assertEquals(404, response.getStatus());
        assertEquals("{\"error\":{\"code\":null,\"message\":\"Cannot find EntitySet, Singleton, "
                + "ActionImport or FunctionImport with name 'Foo'.\"}}",
                response.getContentAsString());
    }

    @Test
    public void testErrorCodes() throws Exception {
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory() {
            @Override
            public ResultSetExecution createResultSetExecution(
                    final QueryExpression command, ExecutionContext executionContext,
                    RuntimeMetadata metadata, Object connection)
                    throws TranslatorException {
                List<? extends List<?>> list = getData(command);
                if (list == null) {
                    throw new RuntimeException(command.toString());
                }
                final Iterator<? extends List<?>> result = list.iterator();
                return new ResultSetExecution() {

                    @Override
                    public void execute() throws TranslatorException {
                        throw new TranslatorException(ODataPlugin.Event.TEIID16001, "execution failed");
                    }

                    @Override
                    public void close() {

                    }

                    @Override
                    public void cancel() throws TranslatorException {

                    }

                    @Override
                    public List<?> next() throws TranslatorException, DataNotAvailableException {
                        if (result.hasNext()) {
                            return result.next();
                        }
                        return null;
                    }
                };
            }
        };
        hc.addData("SELECT x.a, x.b FROM x", Arrays.asList(Arrays.asList("a", 1)));

        teiid.addTranslator("x1", hc);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("ddl", "create foreign table x (a string, b integer, primary key (a));");
        mmd.addSourceMapping("x1", "x1", null);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.newRequest(baseURL + "/northwind/m/x")
                .method("GET")
                .send();
        assertEquals(500, response.getStatus());
        assertEquals("{\"error\":{\"code\":\"TEIID30504\","
                + "\"message\":\"TEIID30504 x1: TEIID16001 execution failed\"}}",
                response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/x?$format=xml")
                .method("GET")
                .send();
        assertEquals(500, response.getStatus());
        assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<error xmlns=\"http://docs.oasis-open.org/odata/ns/metadata\">"
                +   "<code>TEIID30504</code>"
                +   "<message>TEIID30504 x1: TEIID16001 execution failed</message>"
                + "</error>",
                response.getContentAsString());
    }

    @Test
    public void testFilterNull() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer) as "
                + "select 'xyz', 123 union all "
                + "select 'abc', null;");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.GET(baseURL + "/northwind/vw/x?$filter="+Encoder.encode("b eq null"));
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x\",\"value\":[{\"a\":\"abc\",\"b\":null}]}",
                response.getContentAsString());
    }


    @Test
    public void testMultipleAirthamatic() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/pm1/G1?$filter="+Encoder.encode("e2 eq 1 add 1 add 1"));
        assertEquals(200, response.getStatus());
    }

    @Test
    public void testFloor() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/pm1/G1?$filter="+Encoder.encode("e2 eq floor(4.2)"));
        assertEquals(200, response.getStatus());
    }

    @Test
    public void testRound() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/pm1/G1?$filter="+Encoder.encode("e2 eq round(4.2)"));
        assertEquals(200, response.getStatus());
    }

    @Test
    public void test$allNotImplemented() throws Exception {
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/$all");
        assertEquals(501, response.getStatus());
    }

    @Test
    public void testExpand() throws Exception {
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory();
        hc.addData("SELECT Customers.id, Customers.name FROM Customers",
                Arrays.asList(Arrays.asList(1, "customer1"), Arrays.asList(2, "customer2"),
                Arrays.asList(3, "customer3"), Arrays.asList(4, "customer4")));
        hc.addData("SELECT Orders.customerid, Orders.id, Orders.place FROM Orders",
                Arrays.asList(Arrays.asList(1, 1, "town"), Arrays.asList(1, 2, "state"),
                Arrays.asList(1, 3, "country"), Arrays.asList(1,4, "abroad"),
                Arrays.asList(2, 5, "state"), Arrays.asList(2, 6, "country"),
                Arrays.asList(3,7,"town"), Arrays.asList(3, 8, "town")));
        hc.addData("SELECT Orders.customerid, Orders.place, Orders.id FROM Orders",
                Arrays.asList(Arrays.asList(1,"town", 1), Arrays.asList(1,"state", 2),
                Arrays.asList(1,"country", 3), Arrays.asList(1,"abroad", 4),
                Arrays.asList(2,"state", 5), Arrays.asList(2,"country", 6),
                Arrays.asList(3,"town", 7), Arrays.asList(3,"town", 8)));


        teiid.addTranslator("x12", hc);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("ddl",
                "CREATE FOREIGN TABLE Customers (\n" +
                "  id integer PRIMARY KEY OPTIONS (NAMEINSOURCE 'id'),\n" +
                "  name varchar(10));\n" +
                "CREATE FOREIGN TABLE Orders (\n" +
                "  id integer PRIMARY KEY OPTIONS (NAMEINSOURCE 'id'),\n" +
                "  customerid integer,\n" +
                "  place varchar(10),\n" +
                "  FOREIGN KEY (customerid) REFERENCES Customers(id));");

        mmd.addSourceMapping("x12", "x12", null);
        teiid.deployVDB("northwind", mmd);



        ContentResponse response = null;

        response = http.newRequest(baseURL + "/northwind/m/Customers?$expand=Orders_FK0&$count=true")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#Customers(Orders_FK0())\","
                + "\"@odata.count\":4,\""
                + "value\":["
                + "{\"id\":1,\"name\":\"customer1\","
                    + "\"Orders_FK0\":["
                        + "{\"id\":1,\"customerid\":1,\"place\":\"town\"},"
                        + "{\"id\":2,\"customerid\":1,\"place\":\"state\"},"
                        + "{\"id\":3,\"customerid\":1,\"place\":\"country\"},"
                        + "{\"id\":4,\"customerid\":1,\"place\":\"abroad\"}"
                + "]},"
                + "{\"id\":2,\"name\":\"customer2\","
                    + "\"Orders_FK0\":["
                        + "{\"id\":5,\"customerid\":2,\"place\":\"state\"},"
                        + "{\"id\":6,\"customerid\":2,\"place\":\"country\"}"
                + "]},"
                + "{\"id\":3,\"name\":\"customer3\","
                    + "\"Orders_FK0\":["
                        + "{\"id\":7,\"customerid\":3,\"place\":\"town\"},"
                        + "{\"id\":8,\"customerid\":3,\"place\":\"town\"}"
                + "]},"
                + "{\"id\":4,\"name\":\"customer4\","
                    + "\"Orders_FK0\":["
                    + "]}]}",
                response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/Customers?$expand=Orders_FK0&$count=true&$skip=3")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#Customers(Orders_FK0())\","
                + "\"@odata.count\":4,\"value\":["
                + "{\"id\":4,\"name\":\"customer4\","
                + "\"Orders_FK0\":[]"
                + "}]}", response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/Customers?$expand=Orders_FK0&$skip=2")
                .method("GET")
                .header("Prefer", "odata.maxpagesize=1")
                .send();
        assertEquals(200, response.getStatus());
        assertTrue(response.getContentAsString().startsWith("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#Customers(Orders_FK0())\","
                + "\"value\":[{\"id\":3,\"name\":\"customer3\","
                + "\"Orders_FK0\":["
                + "{\"id\":7,\"customerid\":3,\"place\":\"town\"},"
                + "{\"id\":8,\"customerid\":3,\"place\":\"town\"}"
                + "]}],"
                + "\"@odata.nextLink\":\"http://localhost:"));
        assertTrue(response.getContentAsString(),
                response.getContentAsString().endsWith(",1\"}"));

        JsonParser parser = new JsonFactory(new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY, true))
            .createParser(response.getContentAsString());
        JsonNode node = parser.getCodec().readTree(parser);

        response = http.newRequest(node.get("@odata.nextLink").asText())
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#Customers(Orders_FK0())\","
                + "\"value\":["
                + "{\"id\":4,\"name\":\"customer4\","
                + "\"Orders_FK0\":[]"
                + "}]}", response.getContentAsString());

        // system options
        response = http.newRequest(baseURL + "/northwind/m/Customers?$expand=Orders_FK0($skip=2)&$count=true")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#Customers(Orders_FK0())\","
                + "\"@odata.count\":4,\""
                + "value\":["
                + "{\"id\":1,\"name\":\"customer1\","
                    + "\"Orders_FK0\":["
                        + "{\"id\":3,\"customerid\":1,\"place\":\"country\"},"
                        + "{\"id\":4,\"customerid\":1,\"place\":\"abroad\"}"
                + "]},"
                + "{\"id\":2,\"name\":\"customer2\","
                    + "\"Orders_FK0\":["
                + "]},"
                + "{\"id\":3,\"name\":\"customer3\","
                    + "\"Orders_FK0\":["
                + "]},"
                + "{\"id\":4,\"name\":\"customer4\","
                    + "\"Orders_FK0\":["
                    + "]}]}",
                response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/Customers?$expand=Orders_FK0($top=2;$count=true)&$count=true")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#Customers(Orders_FK0())\","
                + "\"@odata.count\":4,\""
                + "value\":["
                    + "{\"id\":1,\"name\":\"customer1\","
                        + "\"Orders_FK0@odata.count\":4,"
                        + "\"Orders_FK0\":["
                            + "{\"id\":1,\"customerid\":1,\"place\":\"town\"},"
                            + "{\"id\":2,\"customerid\":1,\"place\":\"state\"}"
                        + "]},"
                    + "{\"id\":2,\"name\":\"customer2\","
                    + "\"Orders_FK0@odata.count\":2,"
                    + "\"Orders_FK0\":["
                        + "{\"id\":5,\"customerid\":2,\"place\":\"state\"},"
                        + "{\"id\":6,\"customerid\":2,\"place\":\"country\"}"
                    + "]},"
                    + "{\"id\":3,\"name\":\"customer3\","
                    + "\"Orders_FK0@odata.count\":2,"
                    + "\"Orders_FK0\":["
                    + "{\"id\":7,\"customerid\":3,\"place\":\"town\"},"
                    + "{\"id\":8,\"customerid\":3,\"place\":\"town\"}"
                    + "]},"
                    + "{\"id\":4,\"name\":\"customer4\","
                    + "\"Orders_FK0@odata.count\":0,"
                    + "\"Orders_FK0\":["
                    + "]}]}",
                response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/Customers?$expand=Orders_FK0($top=1;$select=place)&$count=true")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#Customers(Orders_FK0(id,place))\","
                + "\"@odata.count\":4,\""
                + "value\":["
                + "{\"id\":1,\"name\":\"customer1\","
                    + "\"Orders_FK0\":["
                    + "{\"@odata.id\":\""+baseURL+"/northwind/m/Orders(1)\",\"id\":1,\"place\":\"town\"}"
                + "]},"
                + "{\"id\":2,\"name\":\"customer2\","
                    + "\"Orders_FK0\":["
                    + "{\"@odata.id\":\""+baseURL+"/northwind/m/Orders(5)\",\"id\":5,\"place\":\"state\"}"
                + "]},"
                + "{\"id\":3,\"name\":\"customer3\","
                    + "\"Orders_FK0\":["
                    + "{\"@odata.id\":\""+baseURL+"/northwind/m/Orders(7)\",\"id\":7,\"place\":\"town\"}"
                + "]},"
                + "{\"id\":4,\"name\":\"customer4\","
                    + "\"Orders_FK0\":["
                    + "]}]}",
                response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/Customers?$expand=Orders_FK0($filter="+Encoder.encode("place eq ")+"'town')")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#Customers(Orders_FK0())\","
                + "\"value\":[{\"id\":1,\"name\":\"customer1\","
                +   "\"Orders_FK0\":[{\"id\":1,\"customerid\":1,\"place\":\"town\"}]},"
                + "{\"id\":2,\"name\":\"customer2\",\"Orders_FK0\":[]},"
                + "{\"id\":3,\"name\":\"customer3\","
                +  "\"Orders_FK0\":[{\"id\":7,\"customerid\":3,\"place\":\"town\"},"
                + "{\"id\":8,\"customerid\":3,\"place\":\"town\"}]},"
                + "{\"id\":4,\"name\":\"customer4\",\"Orders_FK0\":[]}]}",
                response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/Customers?$expand=Orders_FK0($top=0;$count=true)&$count=true")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#Customers(Orders_FK0())\","
                + "\"@odata.count\":4,\""
                + "value\":["
                    + "{\"id\":1,\"name\":\"customer1\","
                    + "\"Orders_FK0@odata.count\":4,"
                    + "\"Orders_FK0\":[]},"
                    + "{\"id\":2,\"name\":\"customer2\","
                    + "\"Orders_FK0@odata.count\":2,"
                    + "\"Orders_FK0\":[]},"
                    + "{\"id\":3,\"name\":\"customer3\","
                    + "\"Orders_FK0@odata.count\":2,"
                    + "\"Orders_FK0\":[]},"
                    + "{\"id\":4,\"name\":\"customer4\","
                    + "\"Orders_FK0@odata.count\":0,"
                    + "\"Orders_FK0\":["
                    + "]}]}",
                response.getContentAsString());

        //apply not yet implemented for expand
        response = http.newRequest(baseURL + "/northwind/m/Customers?$apply=expand(Orders_FK0,filter(place%20gt%20'a'))")
                .method("GET")
                .send();
        assertEquals(501, response.getStatus());

        response = http.newRequest(baseURL + "/northwind/m/Customers?$expand=Orders_FK0($apply=aggregate(place%20with%20min%20as%20first))")
                .method("GET")
                .send();
        assertEquals(501, response.getStatus());
    }

    @Test
    public void testIndexingOfStringFunctions() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", "create view x (a string primary key, b integer) as "
                + "select 'xyz', 123 union all select 'abc', 456;");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.GET(baseURL + "/northwind/vw/x?$filter="
                +Encoder.encode("indexof(a,'y') eq 1"));
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x\",\"value\":[{\"a\":\"xyz\",\"b\":123}]}",
                response.getContentAsString());

        response = http.GET(baseURL + "/northwind/vw/x?$filter="
                +Encoder.encode("indexof(a,'y') eq 2"));
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x\",\"value\":[]}",
                response.getContentAsString());

        response = http.GET(baseURL + "/northwind/vw/x?$filter="
                +Encoder.encode("substring(a,1) eq 'yz'"));
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x\",\"value\":[{\"a\":\"xyz\",\"b\":123}]}",
                response.getContentAsString());

        response = http.GET(baseURL + "/northwind/vw/x?$filter="
                +Encoder.encode("substring(a,1,2) eq 'yz'"));
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x\",\"value\":[{\"a\":\"xyz\",\"b\":123}]}",
                response.getContentAsString());

        response = http.GET(baseURL + "/northwind/vw/x?$filter="
                +Encoder.encode("substring(a,0,1) eq 'a'"));
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#x\",\"value\":[{\"a\":\"abc\",\"b\":456}]}",
                response.getContentAsString());
    }

    @Test
    public void testMonthFunctions() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", "CREATE VIEW SimpleTable(\n" +
                "    intkey integer PRIMARY KEY,\n" +
                "    intnum integer,\n" +
                "    stringkey varchar(20),\n" +
                "    stringval varchar(20),\n" +
                "    booleanval boolean,\n" +
                "    decimalval decimal(20, 10),\n" +
                "    timeval time,\n" +
                "    dateval date,\n" +
                "    timestampval timestamp,\n" +
                "    clobval clob) as select 1,1, '1','1',true,1.0,{t '00:01:01'}, "
                + "{d '2001-01-01'},{ts '2001-01-01 00:01:01.01'},null;");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.GET(baseURL + "/northwind/vw/SimpleTable?$filter="
                +Encoder.encode("month(2001-01-01T00:01:01.01Z) eq intkey")+"&$select=intkey");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#SimpleTable(intkey)\",\"value\":[{\"intkey\":1}]}",
                response.getContentAsString());
    }

    @Ignore("Not supported")
    @Test
    public void testArrayParam() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", "CREATE virtual procedure x(y string[]) returns string[] as return y;");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.GET(baseURL + "/northwind/vw/x(y="+Encoder.encode("[\"a\",\"b\"]")+")");
        assertEquals(200, response.getStatus());
    }

    @Ignore("Not supported")
    @Test
    public void testArrayFilter() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", "CREATE virtual view x(a integer primary key, b string[]) as select 1, ('c','d');");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.GET(baseURL + "/northwind/vw/x?$filter=" + Encoder.encode("b eq [\"c\",\"d\"]"));
        assertEquals(200, response.getStatus());
    }

    @Test
    public void testActionNoReturn() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", "CREATE virtual procedure x() as begin --do nothing\nend");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.newRequest(baseURL + "/northwind/vw/x")
                .method("POST")
                .send();
        assertEquals(204, response.getStatus());
    }

    @Test
    public void testReverseNavigation() throws Exception {
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory();
        hc.addData("SELECT Customers.id, Customers.name FROM Customers",
                Arrays.asList(Arrays.asList(1, "customer1"), Arrays.asList(2, "customer2"),
                Arrays.asList(3, "customer3"), Arrays.asList(4, "customer4")));


        hc.addData("SELECT Customers.id FROM Customers",
                Arrays.asList(Arrays.asList(1), Arrays.asList(2),
                Arrays.asList(3), Arrays.asList(4)));
        hc.addData("SELECT Orders.id, Orders.customerid FROM Orders",
                Arrays.asList(Arrays.asList(1, 1), Arrays.asList(2, 1),
                Arrays.asList(3,1), Arrays.asList(4,1),
                Arrays.asList(5,2), Arrays.asList(6,2),
                Arrays.asList(7,3), Arrays.asList(8,3)));
        hc.addData("SELECT Orders.id, Orders.customerid, Orders.place FROM Orders",
                Arrays.asList(Arrays.asList(1, 1, "town"), Arrays.asList(2, 1, "state"),
                Arrays.asList(3, 1,"country"), Arrays.asList(4, 1, "abroad"),
                Arrays.asList(5,2, "state"), Arrays.asList(6,2, "country"),
                Arrays.asList(7,3,"town"), Arrays.asList(8,3, "town")));

        hc.addData("SELECT Orders.customerid, Orders.id, Orders.place FROM Orders",
                Arrays.asList(Arrays.asList(1, 1, "town"), Arrays.asList(1, 2, "state"),
                Arrays.asList(1,3, "country"), Arrays.asList(1, 4, "abroad"),
                Arrays.asList(2,5, "state"), Arrays.asList(2, 6, "country"),
                Arrays.asList(3,7,"town"), Arrays.asList(3,8, "town")));


        teiid.addTranslator("x12", hc);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("ddl",
                "CREATE FOREIGN TABLE Customers (\n" +
                "  id integer PRIMARY KEY OPTIONS (NAMEINSOURCE 'id'),\n" +
                "  name varchar(10));\n" +
                "CREATE FOREIGN TABLE Orders (\n" +
                "  id integer PRIMARY KEY OPTIONS (NAMEINSOURCE 'id'),\n" +
                "  customerid integer,\n" +
                "  place varchar(10),\n" +
                "  CONSTRAINT Customer FOREIGN KEY (customerid) REFERENCES Customers(id));");

        mmd.addSourceMapping("x12", "x12", null);
        teiid.deployVDB("northwind", mmd);



        ContentResponse response = null;
        response = http.newRequest(baseURL + "/northwind/m/Orders(1)/Customer")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#Customers/$entity\","
                + "\"id\":1,\"name\":\"customer1\"}",
                response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/Orders(1)?$expand=Customer")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#Orders(Customer())/$entity\","
                + "\"id\":1,\"customerid\":1,\"place\":\"town\","
                + "\"Customer\":{\"id\":1,\"name\":\"customer1\"}}",
                response.getContentAsString());
        response = http.newRequest(baseURL + "/northwind/m/Orders(1)/Customer/Orders_Customer")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#Customers\","
                + "\"value\":[{\"id\":1,\"customerid\":1,\"place\":\"town\"},"
                + "{\"id\":2,\"customerid\":1,\"place\":\"state\"},"
                + "{\"id\":3,\"customerid\":1,\"place\":\"country\"},"
                + "{\"id\":4,\"customerid\":1,\"place\":\"abroad\"}]}",
                response.getContentAsString());
    }

    @Test
    public void testReverseNavigationWithUniqueKey() throws Exception {
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory();
        hc.addData("SELECT Customers.id, Customers.name FROM Customers",
                Arrays.asList(Arrays.asList(1, "customer1"), Arrays.asList(2, "customer2"),
                Arrays.asList(3, "customer3"), Arrays.asList(4, "customer4")));


        hc.addData("SELECT Customers.id FROM Customers",
                Arrays.asList(Arrays.asList(1), Arrays.asList(2),
                Arrays.asList(3), Arrays.asList(4)));
        hc.addData("SELECT Orders.id, Orders.customerid FROM Orders",
                Arrays.asList(Arrays.asList(1, 1), Arrays.asList(2, 1),
                Arrays.asList(3,1), Arrays.asList(4,1),
                Arrays.asList(5,2), Arrays.asList(6,2),
                Arrays.asList(7,3), Arrays.asList(8,3)));
        hc.addData("SELECT Orders.id, Orders.customerid, Orders.place FROM Orders",
                Arrays.asList(Arrays.asList(1, 1, "town"), Arrays.asList(2, 1, "state"),
                Arrays.asList(3, 1,"country"), Arrays.asList(4, 1, "abroad"),
                Arrays.asList(5,2, "state"), Arrays.asList(6,2, "country"),
                Arrays.asList(7,3,"town"), Arrays.asList(8,3, "town")));

        hc.addData("SELECT Orders.customerid, Orders.id, Orders.place FROM Orders",
                Arrays.asList(Arrays.asList(1, 1, "town"), Arrays.asList(1, 2, "state"),
                Arrays.asList(1,3, "country"), Arrays.asList(1, 4, "abroad"),
                Arrays.asList(2,5, "state"), Arrays.asList(2, 6, "country"),
                Arrays.asList(3,7,"town"), Arrays.asList(3,8, "town")));


        teiid.addTranslator("x12", hc);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("ddl",
                "CREATE FOREIGN TABLE Customers (\n" +
                "  id integer UNIQUE OPTIONS (NAMEINSOURCE 'id'),\n" +
                "  name varchar(10));\n" +
                "CREATE FOREIGN TABLE Orders (\n" +
                "  id integer PRIMARY KEY OPTIONS (NAMEINSOURCE 'id'),\n" +
                "  customerid integer,\n" +
                "  place varchar(10),\n" +
                "  CONSTRAINT Customer FOREIGN KEY (customerid) REFERENCES Customers(id));");

        mmd.addSourceMapping("x12", "x12", null);
        teiid.deployVDB("northwind", mmd);



        ContentResponse response = null;
        response = http.newRequest(baseURL + "/northwind/m/Orders(1)/Customer")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#Customers/$entity\","
                + "\"id\":1,\"name\":\"customer1\"}",
                response.getContentAsString());

        response = http.newRequest(baseURL + "/northwind/m/Orders(1)?$expand=Customer")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#Orders(Customer())/$entity\","
                + "\"id\":1,\"customerid\":1,\"place\":\"town\","
                + "\"Customer\":{\"id\":1,\"name\":\"customer1\"}}",
                response.getContentAsString());
        response = http.newRequest(baseURL + "/northwind/m/Orders(1)/Customer/Orders_Customer")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#Customers\","
                + "\"value\":[{\"id\":1,\"customerid\":1,\"place\":\"town\"},"
                + "{\"id\":2,\"customerid\":1,\"place\":\"state\"},"
                + "{\"id\":3,\"customerid\":1,\"place\":\"country\"},"
                + "{\"id\":4,\"customerid\":1,\"place\":\"abroad\"}]}",
                response.getContentAsString());
    }

    @Test
    public void testReverseBidirectionalNavigation() throws Exception {
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory();
        hc.addData("SELECT EmployeeMasterEntity.EmployeeID, EmployeeMasterEntity.Department FROM EmployeeMasterEntity",
                Arrays.asList(Arrays.asList(3, 10000001)));

        hc.addData("SELECT OrganizationalUnitEntity.OrganizationaUnitID, OrganizationalUnitEntity.UnitManager FROM OrganizationalUnitEntity",
                Arrays.asList(Arrays.asList(10000001, 1)));

        teiid.addTranslator("x12", hc);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("ddl",
                "CREATE FOREIGN TABLE EmployeeMasterEntity (\n" +
                "  EmployeeID integer primary key,\n" +
                "  Department integer,"
                + "CONSTRAINT Departments FOREIGN KEY (Department) REFERENCES OrganizationalUnitEntity(OrganizationaUnitID));\n" +
                "CREATE FOREIGN TABLE OrganizationalUnitEntity (\n" +
                "  OrganizationaUnitID integer PRIMARY KEY,\n" +
                "  UnitManager integer,\n" +
                "  CONSTRAINT Managers FOREIGN KEY (UnitManager) REFERENCES EmployeeMasterEntity(EmployeeID));");
        mmd.addSourceMapping("x12", "x12", null);
        teiid.deployVDB("northwind", mmd);



        ContentResponse response = null;
        response = http.newRequest(baseURL + "/northwind/m/EmployeeMasterEntity(3)/Departments?$format=json")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/m/$metadata#OrganizationalUnitEntity/$entity\",\"OrganizationaUnitID\":10000001,\"UnitManager\":1}",
                response.getContentAsString());
    }

    @Test
    public void testDecimalPrecisionScale() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", "CREATE VIEW SimpleTable(\n" +
                "    intkey integer PRIMARY KEY,\n" +
                "    decimalval decimal(3, 1), bigintegerval biginteger(40)) as select 1,12.30,cast(1 as biginteger) union all select 2, 1.000,2 union all select 3, 123.0,3;");
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.GET(baseURL + "/northwind/vw/SimpleTable");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#SimpleTable\",\"value\":[{\"intkey\":1,\"decimalval\":12.3,\"bigintegerval\":1},{\"intkey\":2,\"decimalval\":1.0,\"bigintegerval\":2},{\"intkey\":3,\"decimalval\":123,\"bigintegerval\":3}]}",
                response.getContentAsString());

        response = http.GET(baseURL + "/northwind/vw/$metadata");
        assertTrue(response.getContentAsString().contains("Name=\"bigintegerval\" Type=\"Edm.Decimal\" Precision=\"40\" Scale=\"0\""));
    }

    @Test public void testReferentialConstraints() throws Exception {
        String ddl = "CREATE VIEW A(a_id integer PRIMARY KEY, a_value string) AS SELECT 1, 'a1' UNION ALL SELECT 2, 'a2';\n" +
                "            CREATE VIEW B(b_id integer PRIMARY KEY, b_value string) AS SELECT 3, 'b1' UNION ALL SELECT 4, 'b2';\n" +
                "            CREATE VIEW C(c_id integer PRIMARY KEY, a_ref integer, b_ref integer,\n" +
                "            FOREIGN KEY (a_ref) REFERENCES A(a_id),\n" +
                "            FOREIGN KEY (b_ref) REFERENCES B(b_id))\n" +
                "            AS SELECT 5, 1, 3 UNION ALL SELECT 6, 2, 4;";

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", ddl);
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.GET(baseURL + "/northwind/vw/$metadata");
        assertEquals(200, response.getStatus());
        assertTrue(response.getContentAsString().contains("<NavigationProperty Name=\"FK1\" Type=\"vw.B\">"
                + "<ReferentialConstraint Property=\"b_ref\" ReferencedProperty=\"b_id\"></ReferentialConstraint>"));
    }

    @Test public void testConcatNull() throws Exception {
        String ddl = "CREATE VIEW A(a_id integer PRIMARY KEY, a_value string, b_value string) AS SELECT 1, 'a', null";

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", ddl);
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.GET(baseURL + "/northwind/vw/A?$filter=concat(a_value,b_value)%20eq%20%27a%27");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#A\",\"value\":[]}",
                response.getContentAsString());
    }

    @Test public void testSubstring() throws Exception {
        String ddl = "CREATE VIEW A(a_id integer PRIMARY KEY, a_value string) AS SELECT 1, 'abc'";

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("vw");
        mmd.addSourceMetadata("ddl", ddl);
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.GET(baseURL + "/northwind/vw/A?$filter=substring(a_value,1)%20eq%20%27bc%27");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#A\",\"value\":[{\"a_id\":1,\"a_value\":\"abc\"}]}",
                response.getContentAsString());

        response = http.GET(baseURL + "/northwind/vw/A?$filter=substring(a_value,0,1)%20eq%20%27a%27");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\""+baseURL+"/northwind/vw/$metadata#A\",\"value\":[{\"a_id\":1,\"a_value\":\"abc\"}]}",
                response.getContentAsString());
    }

    @Test public void testGeometry() throws Exception {
        String ddl = "CREATE view geo_view (id integer primary key,"
                + "location geometry options (\"teiid_spatial:coord_dimension\" 2, \"teiid_spatial:srid\" 4326, \"teiid_spatial:type\" 'point'),"
                + "poly geometry options (\"teiid_spatial:coord_dimension\" 2, \"teiid_spatial:srid\" 4326, \"teiid_spatial:type\" 'polygon'))"
                + " AS select 1, ST_GEOMFROMTEXT('POINT (1 3)'), ST_GEOMFROMTEXT('POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),\n" +
                "(20 30, 35 35, 30 20, 20 30))');"
                + "CREATE view geo_view_multipolygon \n" +
                "(id integer primary key,\n" +
                "   location geometry options \n" +
                "   (\"teiid_spatial:coord_dimension\" 2, \n" +
                "    \"teiid_spatial:srid\" 4326,\n" +
                "    \"teiid_spatial:type\" 'multipolygon')\n" +
                ")\n" +
                "AS select 1, ST_GeomFromEWKT('SRID=4326;MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),\n" +
                "((15 5, 40 10, 10 20, 5 10, 15 5)))');"
                + "CREATE view geo_view_polygon \n" +
                "(id integer primary key,\n" +
                "   location geometry options \n" +
                "   (\"teiid_spatial:coord_dimension\" 2, \n" +
                "    \"teiid_spatial:srid\" 4326,\n" +
                "    \"teiid_spatial:type\" 'polygon')\n" +
                ")\n" +
                "AS select 1, ST_GeomFromEWKT('SRID=4326;POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))');"
                + "CREATE view geo_view_multilinestring\n" +
                "(id integer primary key,\n" +
                "   location geometry options \n" +
                "   (\"teiid_spatial:coord_dimension\" 2, \n" +
                "    \"teiid_spatial:srid\" 4326,\n" +
                "    \"teiid_spatial:type\" 'multilinestring')\n" +
                ")\n" +
                "as select 1, ST_GeomFromEWKT('SRID=4326;MULTILINESTRING ((10 10, 20 20, 10 40),\n" +
                "(40 40, 30 30, 40 20, 30 10))');"
                + "CREATE view geo_view_multipoint \n" +
                "(id integer primary key,\n" +
                "   location geometry options \n" +
                "   (\"teiid_spatial:coord_dimension\" 2, \n" +
                "    \"teiid_spatial:srid\" 4326,\n" +
                "    \"teiid_spatial:type\" 'multipoint')\n" +
                ")\n" +
                "AS select 1, ST_GEOMfromEWKT('SRID=4326;MULTIPOINT ((10 40), (40 30), (20 20), (30 10))');";

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("phy");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("ddl", ddl);

        teiid.deployVDB("northwind", mmd);

        String url = "/northwind/phy/geo_view";
        String value = get(url);
        assertEquals("[{\"id\":1,\"location\":{\"type\":\"Point\",\"coordinates\":[1.0,3.0]},\"poly\":{\"type\":\"Polygon\",\"coordinates\":[[[35.0,10.0],[45.0,45.0],[15.0,40.0],[10.0,20.0],[35.0,10.0]],[[20.0,30.0],[35.0,35.0],[30.0,20.0],[20.0,30.0]]]}}]", value);

        ContentResponse response = http.GET(baseURL + "/northwind/phy/$metadata");
        assertTrue(response.getContentAsString(), response.getContentAsString().contains("<Property Name=\"location\" "
                + "Type=\"Edm.GeometryPoint\" SRID=\"4326\">"));


        value = get("/northwind/phy/geo_view_multipolygon");
        assertEquals("[{\"id\":1,\"location\":{\"type\":\"MultiPolygon\",\"coordinates\":[[[[30.0,20.0],[45.0,40.0],[10.0,40.0],[30.0,20.0]]],[[[15.0,5.0],[40.0,10.0],[10.0,20.0],[5.0,10.0],[15.0,5.0]]]]}}]", value);

        value = get("/northwind/phy/geo_view_polygon");
        assertEquals("[{\"id\":1,\"location\":{\"type\":\"Polygon\",\"coordinates\":[[[30.0,10.0],[40.0,40.0],[20.0,40.0],[10.0,20.0],[30.0,10.0]]]}}]", value);

        value = get("/northwind/phy/geo_view_multilinestring");
        assertEquals("[{\"id\":1,\"location\":{\"type\":\"MultiLineString\",\"coordinates\":[[[10.0,10.0],[20.0,20.0],[10.0,40.0]],[[40.0,40.0],[30.0,30.0],[40.0,20.0],[30.0,10.0]]]}}]", value);

        value = get("/northwind/phy/geo_view_multipoint");
        assertEquals("[{\"id\":1,\"location\":{\"type\":\"MultiPoint\",\"coordinates\":[[10.0,40.0],[40.0,30.0],[20.0,20.0],[30.0,10.0]]}}]", value);
    }

    private String get(String url)
            throws InterruptedException, ExecutionException, TimeoutException,
            IOException, JsonProcessingException {
        ContentResponse response = http.GET(baseURL + url);
        assertEquals(200, response.getStatus());

        JsonNode node = getJSONNode(response);
        String value = node.get("value").toString();
        return value;
    }

    @Test public void testGeography() throws Exception {
        String ddl = "CREATE foreign table geo (id integer primary key, location string);"
                + "CREATE view geo_view (id integer primary key,"
                + "location geography options (\"teiid_spatial:coord_dimension\" 2, \"teiid_spatial:srid\" 4326, \"teiid_spatial:type\" 'point'))"
                + " AS select id, ST_GEOGFROMTEXT(location) from geo;";

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("phy");
        mmd.addSourceMetadata("ddl", ddl);
        mmd.addSourceMapping("x", "x", null);
        HardCodedExecutionFactory hef = new HardCodedExecutionFactory();

        hef.addData("SELECT geo.id, geo.location FROM geo", Arrays.asList(Arrays.asList(1, "POINT (1 3)")));

        teiid.addTranslator("x", hef);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.GET(baseURL + "/northwind/phy/geo_view");
        assertEquals(200, response.getStatus());

        JsonNode node = getJSONNode(response);
        String value = node.get("value").toString();
        assertEquals("[{\"id\":1,\"location\":{\"type\":\"Point\",\"coordinates\":[1.0,3.0]}}]", value);

        response = http.GET(baseURL + "/northwind/phy/$metadata");
        assertTrue(response.getContentAsString().contains("<Property Name=\"location\" Type=\"Edm.GeographyPoint\" "
                + "SRID=\"4326\">"));
    }

    @Test
    public void testAnnotationMetadata() throws Exception {
        HardCodedExecutionFactory hc = buildHardCodedExecutionFactory();
        teiid.addTranslator("x5", hc);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("m");
        mmd.addSourceMetadata("DDL", "create foreign table x (a string primary key) OPTIONS (ANNOTATION 'hello', foo 'bar');");
        mmd.setModelType(Model.Type.PHYSICAL);
        mmd.addSourceMapping("x5", "x5", null);
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.newRequest(baseURL + "/northwind/m/$metadata")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
    }

    @Test public void testCrossSchemaFk() throws Exception {
        String ddl = "CREATE VIEW pktable (\n" +
                "id integer,\n" +
                "keyvalue string,\n" +
                "CONSTRAINT pkid PRIMARY KEY(id)\n" +
                ") OPTIONS (UPDATABLE 'TRUE') \n" +
                "AS \n" +
                "SELECT 1 as id, 'a' as keyvalue\n";

        String fkddl = "CREATE VIEW fktable (\n" +
                "id integer,\n" +
                "fkvalue integer,\n" +
                "CONSTRAINT pkid PRIMARY KEY(id),\n" +
                "CONSTRAINT fkid FOREIGN KEY(fkvalue) REFERENCES pkmodel.pktable(id)\n" +
                ") OPTIONS(UPDATABLE 'TRUE') \n" +
                "AS\n" +
                "SELECT 1 as id, 1 as fkvalue";

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("pkmodel");
        mmd.addSourceMetadata("DDL", ddl);
        mmd.setModelType(Model.Type.VIRTUAL);
        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("fkmodel");
        mmd1.addSourceMetadata("DDL", fkddl);
        mmd1.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd, mmd1);



        ContentResponse response = http.newRequest(baseURL + "/northwind/fkmodel/$metadata")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        String content = response.getContentAsString();
        assertTrue(content.contains("Namespace=\"northwind.1.fkmodel\""));
        assertTrue(content.contains("Namespace=\"northwind.1.pkmodel\""));

        response = http.newRequest(baseURL + "/northwind/pkmodel/pktable?$expand=*")
                .method("GET")
                .send();
        assertTrue(response.getContentAsString().contains("fktable_fkid"));

        response = http.newRequest(baseURL + "/northwind/pkmodel/$metadata")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());

        content = response.getContentAsString();
        assertTrue(content.contains("Namespace=\"northwind.1.pkmodel\""));
        assertTrue(content.contains("Namespace=\"northwind.1.pkmodel\""));
    }

    @Test public void testCicularCrossSchemaFk() throws Exception {
        String ddl = "CREATE VIEW pktable (\n" +
                "id integer,\n" +
                "keyvalue string,\n" +
                "CONSTRAINT pkid PRIMARY KEY(id),\n" +
                "CONSTRAINT fkid FOREIGN KEY(keyvalue) REFERENCES fkmodel.fktable(id)\n" +
                ") OPTIONS (UPDATABLE 'TRUE') \n" +
                "AS \n" +
                "SELECT 1 as id, 'a' as keyvalue\n";

        String fkddl = "CREATE VIEW fktable (\n" +
                "id integer,\n" +
                "fkvalue integer,\n" +
                "CONSTRAINT pkid PRIMARY KEY(id),\n" +
                "CONSTRAINT fkid FOREIGN KEY(fkvalue) REFERENCES pkmodel.pktable(id)\n" +
                ") OPTIONS(UPDATABLE 'TRUE') \n" +
                "AS\n" +
                "SELECT 1 as id, 1 as fkvalue";

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("pkmodel");
        mmd.addSourceMetadata("DDL", ddl);
        mmd.setModelType(Model.Type.VIRTUAL);
        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("fkmodel");
        mmd1.addSourceMetadata("DDL", fkddl);
        mmd1.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd, mmd1);

        ContentResponse response = http.newRequest(baseURL + "/northwind/fkmodel/$metadata")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        String content = response.getContentAsString();
        assertTrue(content.contains("Namespace=\"northwind.1.fkmodel\""));
        assertTrue(content.contains("Namespace=\"northwind.1.pkmodel\""));

        response = http.newRequest(baseURL + "/northwind/pkmodel/$metadata")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
        content = response.getContentAsString();
        assertTrue(content.contains("Namespace=\"northwind.1.fkmodel\""));
        assertTrue(content.contains("Namespace=\"northwind.1.pkmodel\""));
    }

    @Test public void testMultipleUnique() throws Exception {
        String ddl = "CREATE VIEW pktable (\n" +
                "id integer primary key,\n" +
                "keyvalue string UNIQUE\n" +
                ") AS \n" +
                "SELECT 1 as id, 'a' as keyvalue\n" +
                "CREATE VIEW fktable (\n" +
                "id integer primary key,\n" +
                "fkvalue string,\n" +
                //reference the unique key, not the pk
                "CONSTRAINT fkid FOREIGN KEY(fkvalue) REFERENCES pktable(keyvalue)\n" +
                ") OPTIONS(UPDATABLE 'TRUE') \n" +
                "AS\n" +
                "SELECT 1 as id, 'a' as fkvalue";

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("model");
        mmd.addSourceMetadata("DDL", ddl);
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);



        ContentResponse response = http.newRequest(baseURL + "/northwind/model/fktable(1)/fkid")
                .method("GET")
                .send();
        //if things get crossed up we'll either get a 400 or 404
        assertEquals(200, response.getStatus());
    }

    @Test public void testReservedSchemaName() throws Exception {
        String ddl = "CREATE VIEW pktable (\n" +
                "id integer primary key,\n" +
                "keyvalue string UNIQUE\n" +
                ") AS \n" +
                "SELECT 1 as id, 'a' as keyvalue\n";

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("teiid");
        mmd.addSourceMetadata("DDL", ddl);
        mmd.setModelType(Model.Type.VIRTUAL);
        teiid.deployVDB("northwind", mmd);



        ContentResponse response = http.GET(baseURL + "/northwind/teiid/$metadata");

        //fails if it collides with the teiid extension
        assertEquals(200, response.getStatus());

        response = http.newRequest(baseURL + "/northwind/teiid/pktable")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());

        //escaped
        response = http.newRequest(baseURL + "/northwind/te%69id/pktable")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
    }

    @Test public void testTypeMetadata() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("x");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("ddl", "create view v (col1, col2, col3, col4, col5 primary key, col6, col7, col8, col9, col10,"
                + "col11, col12, col13, col14, col15, col16, col17, col18, col19, col20,"
                + "col21, col22, col23, col24, col25, col26, col27, col28, col29, col30,"
                + "col31, col32, col33, col34, col35, col36, col37, col38, col39) as select null, cast(null as xml), cast(null as boolean), cast(null as byte), cast(null as short), cast(null as integer) as pk, cast(null as long),"
                + " cast(null as float), cast(null as double), cast(null as bigdecimal), cast(null as biginteger), cast(null as time), cast(null as date), cast(null as timestamp), cast(null as varbinary), "
                + " cast(null as char), cast(null as string), cast(null as clob), cast(null as blob), cast(null as json), "
                + " cast(null as xml[]), cast(null as boolean[]), cast(null as byte[]), cast(null as short[]), cast(null as integer[]), cast(null as long[]), cast(null as bigdecimal[]), cast(null as biginteger[]), "
                + " cast(null as float[]), cast(null as double[]), cast(null as time[]), cast(null as date[]), cast(null as timestamp[]), cast(null as varbinary[]), "
                + " cast(null as char[]), cast(null as string[]), cast(null as clob[]), cast(null as blob[]), cast(null as json[]);");
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.GET(baseURL + "/northwind/x/$metadata");

        assertEquals(200, response.getStatus());

        response = http.GET(baseURL + "/northwind/x/swagger.json");

        assertEquals(ObjectConverterUtil.convertFileToString(
                UnitTestUtil.getTestDataFile("all-types-swagger.json")).replace("${host}", "localhost:"+port),
                response.getContentAsString());
    }

    @Test public void testSumAggregate() throws Exception {
        ef.setRowCount(2);
        ef.setIncrementRows(true);
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1?$apply=aggregate(e3%20with%20sum%20as%20Total)");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#G1%28Total%29\",\"value\":[{\"@odata.id\":\"null\",\"Total\":0.1}]}",  response.getContentAsString());
    }

    @Test public void testFilteredSumAggregate() throws Exception {
        ef.setSupportsCompareCriteriaEquals(false);
        ef.setRowCount(2);
        ef.setIncrementRows(true);
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1?$apply=filter(e2%20gt%200)/aggregate(e3%20with%20sum%20as%20Total)");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#G1%28Total%29\",\"value\":[{\"@odata.id\":\"null\",\"Total\":0.1}]}",  response.getContentAsString());
    }

    @Test public void testSumAggregateFiltered() throws Exception {
        ef.setSupportsCompareCriteriaEquals(false);
        ef.setRowCount(2);
        ef.setIncrementRows(true);
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1?$apply=aggregate(e3%20with%20sum%20as%20Total)/filter(Total%20gt%200)");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#G1%28Total%29\",\"value\":[{\"@odata.id\":\"null\",\"Total\":0.1}]}",  response.getContentAsString());
    }

    @Test public void testCountDistinctAggregate() throws Exception {
        ef.setRowCount(2);
        ef.setIncrementRows(true);
        ContentResponse response = http.GET(baseURL + "/loopy/vm1/G1?$apply=aggregate(e3%20with%20countdistinct%20as%20Total)");
        assertEquals(200, response.getStatus());
        assertEquals("{\"@odata.context\":\"$metadata#G1%28Total%29\",\"value\":[{\"@odata.id\":\"null\",\"Total\":2}]}",  response.getContentAsString());
        ef.setRowCount(2);
        ef.setIncrementRows(false);
        response = http.GET(baseURL + "/loopy/vm1/G1?$apply=aggregate(e3%20with%20countdistinct%20as%20Total)");
        assertEquals(200, response.getStatus());
        //same value, so should just be 1 distinct
        assertEquals("{\"@odata.context\":\"$metadata#G1%28Total%29\",\"value\":[{\"@odata.id\":\"null\",\"Total\":1}]}",  response.getContentAsString());
    }

    @Test public void testJson() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("x");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("ddl", "create view v (col1 integer primary key, col2 json) as SELECT 1, JSONARRAY_AGG(JSONOBJECT('a')) from (select 1) as x");
        teiid.deployVDB("northwind", mmd);

        ContentResponse response = http.newRequest(baseURL + "/northwind/x/v")
                .method("GET")
                .send();
        assertEquals(200, response.getStatus());
    }

}