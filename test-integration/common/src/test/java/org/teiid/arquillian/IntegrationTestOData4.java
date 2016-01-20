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

import static org.junit.Assert.*;

import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

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
		admin = AdminFactory.getInstance().createAdmin("localhost", 9999, "admin", "admin".toCharArray());
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
		client = WebClient.create("http://localhost:8080/odata/x/y$metadata");
		client.header("Authorization", "Basic " + Base64.encodeBytes(("user:user").getBytes())); //$NON-NLS-1$ //$NON-NLS-2$
		response = client.invoke("GET", null);
		assertEquals(500, response.getStatus());
		
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
}
