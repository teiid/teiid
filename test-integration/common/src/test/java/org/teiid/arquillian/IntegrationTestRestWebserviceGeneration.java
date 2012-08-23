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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
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

	private Admin admin;
	
	@Before
	public void setup() throws Exception {
		admin = AdminFactory.getInstance().createAdmin("localhost", 9999,	"admin", "admin".toCharArray());
	}
	
	@After
	public void teardown() throws AdminException {
		AdminUtil.cleanUp(admin);
		admin.close();
	}
	
	@Test
    public void testReuse() throws Exception {
		admin.deploy("sample-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("sample-vdb.xml")));
		
		assertTrue(AdminUtil.waitForVDBLoad(admin, "sample", 1, 3));
		
		this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:sample@mm://localhost:31000;user=user;password=user", null);
		
		execute("SELECT * FROM Txns.G1"); //$NON-NLS-1$
		this.internalResultSet.next();
		
		assertTrue(((AdminImpl)admin).getDeployments().contains("sample_1.war"));
		
		String response = httpCall("http://localhost:8080/sample_1/view/g1/123", "GET");
		assertEquals("<sample.g1Table.p1 p1=\"123\"><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></sample.g1Table.p1>", response);
    }
	
	private String httpCall(String url, String method) throws Exception {
		StringBuffer buff = new StringBuffer();
		HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
		connection.setRequestMethod(method);
		connection.setDoOutput(true);
		BufferedReader serverResponse = new BufferedReader(new InputStreamReader(connection.getInputStream()));
		String line;
		while ((line = serverResponse.readLine()) != null) {
			buff.append(line);
		}
		return buff.toString();
	}		
}
