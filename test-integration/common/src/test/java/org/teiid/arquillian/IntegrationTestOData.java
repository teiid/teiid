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

import java.io.StringReader;
import java.nio.charset.Charset;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.GetMethod;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminFactory;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestOData extends AbstractMMQueryTestCase {

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
				"    <property name=\"UseConnectorMetadata\" value=\"true\" />\n" + 
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
		
		HttpClient client = new HttpClient();
		client.getState().setCredentials(
            AuthScope.ANY,
            new UsernamePasswordCredentials("user", "user")
		);		
		
		HttpMethod method = new GetMethod("http://localhost:8080/odata/loopy.1/$metadata");
		int statusCode = client.executeMethod(method);
		assertTrue(statusCode == HttpStatus.SC_OK);
		String stringResults = ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("loopy-metadata-results.txt"));
		String response=method.getResponseBodyAsString();
		System.out.println(response);
//		if (response.indexOf("Edm.Int64") >  -1 ) {
//		  stringResults = stringResults.replace("Edm.Int32", "Edm.Int64");    
//		}
		assertEquals(stringResults, response);
		method.releaseConnection();
	}
}