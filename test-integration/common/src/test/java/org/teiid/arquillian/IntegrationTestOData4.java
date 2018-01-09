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

package org.teiid.arquillian;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.ws.rs.core.MediaType;
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
import org.teiid.core.util.ApplicationInfo;
import org.teiid.core.util.Base64;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.TeiidDriver;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestOData4 extends AbstractMMQueryTestCase {

	private Admin admin;

	@Before
	public void setup() throws Exception {
		admin = AdminFactory.getInstance().createAdmin("localhost", 9999, "admin", "admin".toCharArray());
		admin.deploy("teiid-olingo-" + ApplicationInfo.getInstance().getReleaseNumber() + "-odata4.war", new FileInputStream("target/jboss-eap-6.4/dataVirtualization/vdb/teiid-olingo-odata4.war"));
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
		
		assertTrue(AdminUtil.waitForVDBLoad(admin, "Loopy", 1, 3));
		
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
        
        assertTrue(AdminUtil.waitForVDBLoad(admin, "Loopy", 1, 3));
        
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
        assertTrue(AdminUtil.waitForVDBLoad(admin, "TestOData", 1, 30000));
        
        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:TestOData@mm://localhost:31000;user=user;password=user", null);
        
        execute("SELECT Name FROM Sys.Tables Where name='G1'"); //$NON-NLS-1$
        assertResultsSetEquals("Name[string]\nG1");

        execute("SELECT * from G1"); //$NON-NLS-1$
        assertResultsSetEquals("e1[string[]]    e2[integer]\n[ABCDEFGHIJ]    0");        
        
        admin.undeploy("loopy-vdb.xml");
        admin.undeploy("test-vdb.xml");
    }
    
    // TEIID-3914 - test the olingo-patch work
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
        assertTrue(AdminUtil.waitForVDBLoad(admin, "Loopy", 1, 3));
        
        WebClient client = WebClient.create("http://localhost:8080/odata4/Loopy/m/x(a='a',b=2011-09-11T00:00:00Z)");
        client.header("Authorization", "Basic " + Base64.encodeBytes(("user:user").getBytes())); //$NON-NLS-1$ //$NON-NLS-2$
        Response response = client.invoke("GET", null);
        assertEquals(200, response.getStatus());
        
        client = WebClient.create("http://localhost:8080/odata4/Loopy/m/x");
        client.header("Authorization", "Basic " + Base64.encodeBytes(("user:user").getBytes())); //$NON-NLS-1$ //$NON-NLS-2$
        client.header("Content-Type", "application/json");
        response = client.post("{\"a\":\"b\", \"b\":\"2000-02-02T22:22:22Z\"}");
        assertEquals(304, response.getStatus());
        
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
		
		assertTrue(AdminUtil.waitForVDBLoad(admin, "Loopy", 1, 3));
		
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
                "                CREATE FOREIGN TABLE G1 (e1 string, e2 integer PRIMARY KEY) OPTIONS (UPDATABLE 'true');\n" + 
                "        ]]> </metadata>\n" + 
                "    </model>\n" + 
                "</vdb>";
        
        admin.deploy("loopy-vdb.xml", new ReaderInputStream(new StringReader(vdb), Charset.forName("UTF-8")));
        
        assertTrue(AdminUtil.waitForVDBLoad(admin, "Loopy", 1, 3));
        
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
    public void testExponentialDecimalValue() throws Exception {
        String vdb = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" + 
                "<vdb name=\"expo\" version=\"1\">\n" + 
                "    <model name=\"test\">\n" + 
                "        <source name=\"text-connector2\" translator-name=\"loopback\" />\n" + 
                "         <metadata type=\"DDL\"><![CDATA[\n" + 
                "                CREATE FOREIGN TABLE test_tbl(\n" + 
                "                  id integer NOT NULL AUTO_INCREMENT,\n" + 
                "                  title VARCHAR(100) NOT NULL, \n" + 
                "                  author VARCHAR(40) NOT NULL, \n" + 
                "                  price DECIMAL(15,15) NOT NULL, \n" + 
                "                  submission_date DATE, \n" + 
                "                  PRIMARY KEY ( id ) \n" + 
                "                ) OPTIONS (UPDATABLE true);\n" + 
                "        ]]> </metadata>\n" + 
                "    </model>\n" + 
                "</vdb>";
        
        admin.deploy("expo-vdb.xml", new ReaderInputStream(new StringReader(vdb), Charset.forName("UTF-8")));
        
        assertTrue(AdminUtil.waitForVDBLoad(admin, "expo", 1, 3));
        
        WebClient client = WebClient.create("http://localhost:8080/odata4/expo.1/test/$metadata");
        client.header("Authorization", "Basic " + Base64.encodeBytes(("user:user").getBytes())); //$NON-NLS-1$ //$NON-NLS-2$
        Response response = client.invoke("GET", null);
        
        int statusCode = response.getStatus();
        assertEquals(200, statusCode);
        
        String payload = "{\"id\":3,\"title\":\"title 3\",\"author\":\"author 3\",\"price\":9.9999999999E8,\"submission_date\":\"2017-12-08\"}";
        List<Object> providers = new ArrayList<Object>();
        providers.add( new JacksonJaxbJsonProvider() );
        
        client = WebClient.create("http://localhost:8080/odata4/expo.1/test/test_tbl", providers);
        client.header("Authorization", "Basic " + Base64.encodeBytes(("user:user").getBytes())); //$NON-NLS-1$ //$NON-NLS-2$
        client.type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON);
        
        response = client.invoke("POST", payload);
        assertEquals(304, response.getStatus());
        
        admin.undeploy("expo-vdb.xml");
    }	
}
