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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminFactory;
import org.teiid.adminapi.AdminFactory.AdminImpl;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.TeiidDriver;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestRestWebserviceGeneration extends AbstractMMQueryTestCase {

	private static Admin admin;
	
	@BeforeClass
	public static void setup() throws Exception {
		admin = AdminFactory.getInstance().createAdmin("localhost", 9999,	"admin", "admin".toCharArray());
		admin.deploy("sample-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("sample-vdb.xml")));
	}
	
	@AfterClass
	public static void teardown() throws AdminException {
		AdminUtil.cleanUp(admin);
		admin.close();
	}
	
	@Test
    public void testGetOperation() throws Exception {
		
		assertTrue(AdminUtil.waitForVDBLoad(admin, "sample", 1, 3));
		
		this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:sample@mm://localhost:31000;user=user;password=user", null);
		
		execute("SELECT * FROM Txns.G1"); //$NON-NLS-1$
		this.internalResultSet.next();
		
		assertTrue(((AdminImpl)admin).getDeployments().contains("sample_1.war"));
		
		// get based call
		String response = httpCall("http://localhost:8080/sample_1/view/g1/123", "GET", null);
		assertEquals("<rows p1=\"123\"><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></rows>", response);
    }
	
	@Test
    public void testPostOperation() throws Exception {
		
		assertTrue(AdminUtil.waitForVDBLoad(admin, "sample", 1, 3));
		
		this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:sample@mm://localhost:31000;user=user;password=user", null);
		
		execute("SELECT * FROM Txns.G1"); //$NON-NLS-1$
		this.internalResultSet.next();
		
		assertTrue(((AdminImpl)admin).getDeployments().contains("sample_1.war"));
		
		String params = URLEncoder.encode("p1", "UTF-8") + "=" + URLEncoder.encode("456", "UTF-8");
		
		// post based call
		String response = httpCall("http://localhost:8080/sample_1/view/g1post", "POST", params);
		assertEquals("<rows p1=\"456\"><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></rows>", response);
		
		// ad-hoc procedure
		params = URLEncoder.encode("sql", "UTF-8") + "=" + URLEncoder.encode("SELECT XMLELEMENT(NAME \"rows\", XMLAGG(XMLELEMENT(NAME \"row\", XMLFOREST(e1, e2)))) AS xml_out FROM Txns.G1", "UTF-8");
		response = httpCall("http://localhost:8080/sample_1/view/query", "POST", params);
		assertEquals("<rows><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></rows>", response);
    }	
	
	
	private String httpCall(String url, String method, String params) throws Exception {
		StringBuffer buff = new StringBuffer();
		HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
		connection.setRequestMethod(method);
		connection.setDoOutput(true);
		
		if (method.equalsIgnoreCase("post")) {
			OutputStreamWriter wr = new OutputStreamWriter(connection.getOutputStream());
		    wr.write(params);
		    wr.flush();
		}
		
		BufferedReader serverResponse = new BufferedReader(new InputStreamReader(connection.getInputStream()));
		String line;
		while ((line = serverResponse.readLine()) != null) {
			buff.append(line);
		}
		return buff.toString();
	}		
}
