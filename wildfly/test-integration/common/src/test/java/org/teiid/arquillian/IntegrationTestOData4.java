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

package org.teiid.arquillian;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.ws.rs.core.Response;

import org.apache.cxf.jaxrs.client.WebClient;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.jboss.AdminFactory;
import org.teiid.core.util.Base64;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.TeiidDriver;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestOData4 extends AbstractMMQueryTestCase {

    private Admin admin;

    @Before
    public void setup() throws Exception {
        admin = AdminFactory.getInstance().createAdmin("localhost", AdminUtil.MANAGEMENT_PORT, "admin", "admin".toCharArray());
    }

    @After
    public void teardown() throws AdminException {
        AdminUtil.cleanUp(admin);
        admin.close();
    }
    @Test
    public void testOdata() throws Exception {
        String vdb = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<vdb name=\"Loopy\" version=\"1\">\n" +
                "    <model name=\"MarketData\">\n" +
                "        <source name=\"text-connector2\" translator-name=\"loopback\" />\n" +
                "         <metadata type=\"DDL\"><![CDATA[\n" +
                "                CREATE FOREIGN TABLE G1 (e1 string, e2 integer PRIMARY KEY);\n" +
                "                CREATE FOREIGN TABLE G2 (e1 string, e2 integer PRIMARY KEY) OPTIONS (UPDATABLE 'true');\n" +
                "        ]]> </metadata>\n" +
                "    </model>\n" +
                "</vdb>";

        admin.deploy("loopy-vdb.xml", new ReaderInputStream(new StringReader(vdb), Charset.forName("UTF-8")));

        assertTrue(AdminUtil.waitForVDBLoad(admin, "Loopy", 1));

        WebClient client = WebClient.create("http://localhost:8080/odata4/loopy.1/MarketData/$metadata");
        client.header("Authorization", "Basic " + Base64.encodeBytes(("user:user").getBytes())); //$NON-NLS-1$ //$NON-NLS-2$
        Response response = client.invoke("GET", null);

        int statusCode = response.getStatus();
        assertEquals(200, statusCode);

        Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:loopy@mm://localhost:31000;user=user;password=user", null);

        PreparedStatement ps = conn.prepareCall("select t.* from xmltable('/*:Edmx/*:DataServices/*:Schema[@Alias=\"MarketData\"]' passing xmlparse(document cast(? as clob))) as t");
        ps.setAsciiStream(1, (InputStream)response.getEntity());

        ResultSet rs = ps.executeQuery();
        rs.next();

        assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("loopy-metadata4-results.txt")), rs.getString(1));

        conn.close();

        //try an invalid url
        client = WebClient.create("http://localhost:8080/odata4/x/y$metadata");
        client.header("Authorization", "Basic " + Base64.encodeBytes(("user:user").getBytes())); //$NON-NLS-1$ //$NON-NLS-2$
        response = client.invoke("GET", null);
        assertEquals(404, response.getStatus());

        admin.undeploy("loopy-vdb.xml");
    }

    @Test
    public void testReadOdataMetadata() throws Exception {
        String vdb = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<vdb name=\"Loopy\" version=\"1\">\n" +
                "    <model name=\"MarketData\">\n" +
                "        <source name=\"text-connector2\" translator-name=\"loopback\" />\n" +
                "         <metadata type=\"DDL\"><![CDATA[\n" +
                "                CREATE FOREIGN TABLE G1 (e1 string[], e2 integer PRIMARY KEY);\n" +
                "                CREATE FOREIGN TABLE G2 (e1 string, e2 integer PRIMARY KEY) OPTIONS (UPDATABLE 'true');\n" +
                "        ]]> </metadata>\n" +
                "    </model>\n" +
                "</vdb>";

        admin.deploy("loopy-vdb.xml", new ReaderInputStream(new StringReader(vdb), Charset.forName("UTF-8")));

        assertTrue(AdminUtil.waitForVDBLoad(admin, "Loopy", 1));

        String vdb2 = "<?xml version='1.0' encoding='UTF-8'?>\n" +
                "<vdb name=\"TestOData\" version=\"1\">\n" +
                "    <model name=\"TestOData\" type=\"PHYSICAL\" visible=\"true\">\n" +
                "     <property name=\"importer.entityContainer\" value=\"MarketData\"/>\n"+
                "     <property name=\"importer.schemaNamespace\" value=\"MarketData\"/>\n"+
                "     <source name=\"TestOData\" translator-name=\"odata4\" connection-jndi-name=\"java:/TestOData4\"/>\n" +
                "    </model>\n" +
                "</vdb>";

        Properties p = new Properties();
        p.setProperty("class-name", "org.teiid.resource.adapter.ws.WSManagedConnectionFactory");
        p.setProperty("EndPoint", "http://localhost:8080/odata4/Loopy.1/MarketData");
        p.setProperty("SecurityType", "HTTPBasic");
        p.setProperty("AuthUserName", "user");
        p.setProperty("AuthPassword", "user");
        admin.createDataSource("TestOData4", "webservice", p);

        admin.deploy("test-vdb.xml", new ReaderInputStream(new StringReader(vdb2), Charset.forName("UTF-8")));
        assertTrue(AdminUtil.waitForVDBLoad(admin, "TestOData", 1));

        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:TestOData@mm://localhost:31000;user=user;password=user", null);

        execute("SELECT Name FROM Sys.Tables Where name='G1'"); //$NON-NLS-1$
        assertResultsSetEquals("Name[string]\nG1");

        execute("SELECT * from G1"); //$NON-NLS-1$
        assertResultsSetEquals("e1[string[]]    e2[integer]\n[ABCDEFGHIJ]    0");

        admin.undeploy("loopy-vdb.xml");
        admin.undeploy("test-vdb.xml");
    }

    @Test public void testCompositeKeyTimestamp() throws Exception {
        String vdb = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<vdb name=\"Loopy\" version=\"1\">\n" +
                "    <model name=\"m\">\n" +
                "        <source name=\"x1\" translator-name=\"loopback\" />\n" +
                "         <metadata type=\"DDL\"><![CDATA[\n" +
                "                CREATE FOREIGN TABLE x (a string, b timestamp, c integer, primary key (a, b)) options (updatable true);\n" +
                "        ]]> </metadata>\n" +
                "    </model>\n" +
                "</vdb>";

        admin.deploy("loopy-vdb.xml", new ReaderInputStream(new StringReader(vdb), Charset.forName("UTF-8")));
        assertTrue(AdminUtil.waitForVDBLoad(admin, "Loopy", 1));

        WebClient client = WebClient.create("http://localhost:8080/odata4/Loopy/m/x(a='a',b=2011-09-11T00:00:00Z)");
        client.header("Authorization", "Basic " + Base64.encodeBytes(("user:user").getBytes())); //$NON-NLS-1$ //$NON-NLS-2$
        Response response = client.invoke("GET", null);
        assertEquals(200, response.getStatus());

        client = WebClient.create("http://localhost:8080/odata4/Loopy/m/x");
        client.header("Authorization", "Basic " + Base64.encodeBytes(("user:user").getBytes())); //$NON-NLS-1$ //$NON-NLS-2$
        client.header("Content-Type", "application/json");
        response = client.post("{\"a\":\"b\", \"b\":\"2000-02-02T22:22:22Z\"}");
        assertEquals(304, response.getStatus());


        client = WebClient.create("http://localhost:8080/odata4/Loopy/m/swagger.json");
        client.header("Authorization", "Basic " + Base64.encodeBytes(("user:user").getBytes())); //$NON-NLS-1$ //$NON-NLS-2$
        response = client.get();
        String metadata = ObjectConverterUtil.convertToString((InputStream)response.getEntity());
        assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("loopy-metadata4-swagger.json")), metadata);

        admin.undeploy("loopy-vdb.xml");
    }

    @Test
    public void testOdataMetadataError() throws Exception {
        String vdb = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<vdb name=\"Loopy\" version=\"1\">\n" +
                "    <model name=\"MarketData\">\n" +
                "        <source name=\"text-connector2\" translator-name=\"loopback\" />\n" +
                "         <metadata type=\"DDL\"><![CDATA[\n" +
                "                CREATE FOREIGN TABLE G1 (e1 string, e2 integer PRIMARY KEY);\n" +
                "                CREATE FOREIGN TABLE G1 (e1 string, e2 integer PRIMARY KEY) OPTIONS (UPDATABLE 'true');\n" +
                "        ]]> </metadata>\n" +
                "    </model>\n" +
                "</vdb>";

        admin.deploy("loopy-vdb.xml", new ReaderInputStream(new StringReader(vdb), Charset.forName("UTF-8")));

        assertTrue(AdminUtil.waitForVDBLoad(admin, "Loopy", 1));

        WebClient client = WebClient.create("http://localhost:8080/odata4/loopy.1/MarketData/$metadata");
        client.header("Authorization", "Basic " + Base64.encodeBytes(("user:user").getBytes())); //$NON-NLS-1$ //$NON-NLS-2$
        Response response = client.invoke("GET", null);

        int statusCode = response.getStatus();
        assertEquals(404, statusCode);
    }

    @Test
    public void testCORS() throws Exception {
        String vdb = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<vdb name=\"Loopy\" version=\"1\">\n" +
                "    <model name=\"MarketData\">\n" +
                "        <source name=\"text-connector2\" translator-name=\"loopback\" />\n" +
                "         <metadata type=\"DDL\"><![CDATA[\n" +
                "                CREATE FOREIGN TABLE G1 (e1 string, e2 integer PRIMARY KEY);\n" +
                "                CREATE FOREIGN TABLE G2 (e1 string, e2 integer PRIMARY KEY) OPTIONS (UPDATABLE 'true');\n" +
                "        ]]> </metadata>\n" +
                "    </model>\n" +
                "</vdb>";

        admin.deploy("loopy-vdb.xml", new ReaderInputStream(new StringReader(vdb), Charset.forName("UTF-8")));

        assertTrue(AdminUtil.waitForVDBLoad(admin, "Loopy", 1));

        WebClient client = WebClient.create("http://localhost:8080/odata4/loopy.1/MarketData/$metadata");
        Response response = client.invoke("OPTIONS", null);

        int statusCode = response.getStatus();
        assertEquals(400, statusCode);

        /* setting of Origin header is blocked HttpUrlConnection
        client = WebClient.create("http://localhost:8080/odata4/loopy.1/MarketData/$metadata");
        client.header("Origin", "foo.bar"); //$NON-NLS-1$ //$NON-NLS-2$
        response = client.invoke("OPTIONS", null);

        assertEquals(204, response.getStatus());
        assertEquals("GET,POST,PUT,PATCH,DELETE", response.getHeaderString("Access-Control-Allow-Methods"));
        assertEquals("foo.bar", response.getHeaderString("Access-Control-Allow-Origin"));
        assertEquals("true", response.getHeaderString("Access-Control-Allow-Credentials"));
        assertEquals("Content-Type,Accept,Origin,Authorization", response.getHeaderString("Access-Control-Allow-Headers"));
        */
    }

    @Test
    public void testStaticServlet() throws Exception {
        WebClient client = WebClient.create("http://localhost:8080/odata4/static/org.apache.olingo.v1.xml");
        client.header("Authorization", "Basic " + Base64.encodeBytes(("user:user").getBytes())); //$NON-NLS-1$ //$NON-NLS-2$
        Response response = client.invoke("GET", null);

        int statusCode = response.getStatus();
        assertEquals(200, statusCode);

        client = WebClient.create("http://localhost:8080/odata4/static/pom.xml");
        client.header("Authorization", "Basic " + Base64.encodeBytes(("user:user").getBytes())); //$NON-NLS-1$ //$NON-NLS-2$
        response = client.invoke("GET", null);

        statusCode = response.getStatus();
        assertEquals(404, statusCode);

        client = WebClient.create("http://localhost:8080/odata4/static/META-INF/keycloak.json");
        client.header("Authorization", "Basic " + Base64.encodeBytes(("user:user").getBytes())); //$NON-NLS-1$ //$NON-NLS-2$
        response = client.invoke("GET", null);

        statusCode = response.getStatus();
        assertEquals(404, statusCode);
    }

    @Test
    public void testGetDataInGzip() throws AdminException, IOException{
        String vdb = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<vdb name=\"loopy\" version=\"1\">\n" +
                "    <model name=\"gzip\">\n" +
                "        <source name=\"loopback_src\" translator-name=\"loopback\" />\n" +
                "         <metadata type=\"DDL\"><![CDATA[\n" +
                "                CREATE FOREIGN TABLE t (e integer PRIMARY KEY) OPTIONS (UPDATABLE 'true');\n" +
                "        ]]> </metadata>\n" +
                "    </model>\n" +
                "</vdb>";

        admin.deploy("loopy-vdb.xml", new ByteArrayInputStream(vdb.getBytes()));

        assertTrue(AdminUtil.waitForVDBLoad(admin, "Loopy", 1));

        WebClient client = WebClient.create("http://localhost:8080/odata4/loopy/gzip/t");
        client.header("Authorization", "Basic " + Base64.encodeBytes(("user:user").getBytes()));
        client.header("Accept", "application/json");
        Response response = client.invoke("GET", null);

        assertEquals("The request should succeed.", 200, response.getStatus());
        assertNull("Content-Encoding response header is not expected.", response.getHeaderString("Content-Encoding"));
        InputStream is = (InputStream)response.getEntity();
        byte[] buff = new byte[1000];
        assertEquals("Expected entity.", "{\"@odata.context\":\"http://localhost:8080/odata4/loopy/gzip/$metadata#t\",\"value\":[{\"e\":0}]}", new String(buff, 0, is.read(buff)));

        client.header("Accept-Encoding", "gzip");
        response = client.invoke("GET", null);
        is = (InputStream)response.getEntity();
        is = new GZIPInputStream(is);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int read;
        while((read = is.read(buff)) != -1){
            bos.write(buff, 0, read);
        }
        assertEquals("The request should succeed.", 200, response.getStatus());
        assertNotNull("Content-Encoding response header is expected.", response.getHeaderString("Content-Encoding"));
        assertEquals("Content-Encoding response header.", "gzip", response.getHeaderString("Content-Encoding").toLowerCase());
        assertEquals("Expected entity.", "{\"@odata.context\":\"http://localhost:8080/odata4/loopy/gzip/$metadata#t\",\"value\":[{\"e\":0}]}", new String(bos.toByteArray()));
    }

    @Test
    public void testPutDataInGzip() throws AdminException, IOException{
        String vdb = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<vdb name=\"loopy\" version=\"1\">\n" +
                "    <model name=\"gzip\">\n" +
                "        <source name=\"loopback_src\" translator-name=\"loopback\" />\n" +
                "         <metadata type=\"DDL\"><![CDATA[\n" +
                "                CREATE FOREIGN TABLE t (e integer PRIMARY KEY) OPTIONS (UPDATABLE 'true');\n" +
                "        ]]> </metadata>\n" +
                "    </model>\n" +
                "</vdb>";

        admin.deploy("loopy-vdb.xml", new ByteArrayInputStream(vdb.getBytes()));

        assertTrue(AdminUtil.waitForVDBLoad(admin, "Loopy", 1));

        String data = "{\"e\":1}";
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        GZIPOutputStream gos = new GZIPOutputStream(bos);
        gos.write(data.getBytes());
        gos.finish();
        gos.close();
        byte[] gzipData = bos.toByteArray();

        WebClient client = WebClient.create("http://localhost:8080/odata4/loopy/gzip/t(1)");
        client.header("Authorization", "Basic " + Base64.encodeBytes(("user:user").getBytes()));
        client.header("Content-Type", "application/json");

        Response response = client.put(data);
        assertEquals("The request should succeed." + response.readEntity(String.class), 304, response.getStatus());

        response = client.put(new ByteArrayInputStream(gzipData));
        assertEquals("The request should not succeed. Data is in GZIP format but header not specified.", 400, response.getStatus());

        client.header("Content-Encoding", "gzip");
        response = client.put(data);
        assertEquals("The request should not succeed. Data is not in GZIP format.", 400, response.getStatus());

        response = client.put(new ByteArrayInputStream(gzipData));
        assertEquals("The request should succeed.", 304, response.getStatus());
    }

    @Test
    public void testOdataGeo() throws Exception {
        String vdb = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<vdb name=\"Loopy\" version=\"1\">\n" +
                "    <model name=\"MarketData\">\n" +
                "        <source name=\"text-connector2\" translator-name=\"loopback\" />\n" +
                "         <metadata type=\"DDL\"><![CDATA[\n" +
                "            CREATE view geo_view (id integer primary key," +
                "            location geometry options (\"teiid_spatial:coord_dimension\" 2, \"teiid_spatial:srid\" 4326, \"teiid_spatial:type\" 'point'))" +
                "            AS select 1, ST_POINT(1.0, 2.0);" +
                "        ]]> </metadata>\n" +
                "    </model>\n" +
                "</vdb>";

        admin.deploy("loopy-vdb.xml", new ReaderInputStream(new StringReader(vdb), Charset.forName("UTF-8")));

        assertTrue(AdminUtil.waitForVDBLoad(admin, "Loopy", 1));

        WebClient client = WebClient.create("http://localhost:8080/odata4/loopy.1/MarketData/$metadata");
        client.header("Authorization", "Basic " + Base64.encodeBytes(("user:user").getBytes())); //$NON-NLS-1$ //$NON-NLS-2$
        Response response = client.invoke("GET", null);

        int statusCode = response.getStatus();
        assertEquals(200, statusCode);

        client = WebClient.create("http://localhost:8080/odata4/loopy/MarketData/geo_view");
        client.header("Authorization", "Basic " + Base64.encodeBytes(("user:user").getBytes()));
        client.header("Content-Type", "application/json");

        response = client.get();

        String result = ObjectConverterUtil.convertToString((InputStream)response.getEntity());

        assertTrue(result.contains("\"value\":[{\"id\":1,\"location\":{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}}]"));

        admin.undeploy("loopy-vdb.xml");
    }
}
