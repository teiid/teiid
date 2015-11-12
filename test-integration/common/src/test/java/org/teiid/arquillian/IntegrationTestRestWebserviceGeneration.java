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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.jboss.AdminFactory;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.TeiidDriver;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestRestWebserviceGeneration extends AbstractMMQueryTestCase {

	private Admin admin;
	
	@Before
	public void setup() throws Exception {
		admin = AdminFactory.getInstance().createAdmin("localhost", AdminUtil.MANAGEMENT_PORT,	"admin", "admin".toCharArray());
		
	}
	
	@After
	public void teardown() throws AdminException {
		AdminUtil.cleanUp(admin);
		admin.close();
	}
	
	@Test
    public void testGetOperation() throws Exception {
		Properties p = new Properties();
		p.setProperty("class-name", "org.teiid.resource.adapter.ws.WSManagedConnectionFactory");
		p.setProperty("EndPoint", "http://localhost:8080");
		p.setProperty("RequestTimeout", "20000");
		AdminUtil.createDataSource(admin, "sample-ws", "webservice", p);

		admin.deploy("sample-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("sample-vdb.xml")));
		assertTrue(AdminUtil.waitForVDBLoad(admin, "sample", 1, 3));
		
		admin.deploy("sample-ws-vdb.xml",new ReaderInputStream(new StringReader(
				//simple ws vdb
				"<vdb name=\"sample-ws\" version=\"1\">"
				+ "<model name=\"ws\"><source name=\"ws\" translator-name=\"ws\" connection-jndi-name=\"java:/sample-ws\"/></model>"
				+"</vdb>"), Charset.forName("UTF-8")));
		assertTrue(AdminUtil.waitForVDBLoad(admin, "sample-ws", 1, 3));
		
		this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:sample@mm://localhost:31000;user=user;password=user", null);
		
		execute("SELECT * FROM Txns.G1"); //$NON-NLS-1$
		this.internalResultSet.next();
		
		assertTrue("sample_1.war not found", AdminUtil.waitForDeployment(admin, "sample_1.war", 5));
		
		// get based call
		String response = httpCall("http://localhost:8080/sample_1/view/g1/123?p2=test", "GET", null);
		String expected = "<rows p1=\"123\" p2=\"test\"><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></rows>";
		assertEquals("response did not match expected", expected, response);

		this.internalConnection.close();
		
		//try the same thing through a vdb
		this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:sample-ws@mm://localhost:31000;user=user;password=user", null);
		execute("select to_chars(x.result, 'UTF-8') from (call invokeHttp(action=>'GET',endpoint=>'sample_1/view/g1/123?p2=test')) as x");
		this.internalResultSet.next();
		assertEquals(expected, this.internalResultSet.getString(1));

		//test a large doc
		response = httpCall("http://localhost:8080/sample_1/view/largedoc", "GET", null);
		assertEquals(327801, response.length());
		
		//test streaming xmltable
		execute("select * from xmltable('/rows/row/e1' passing xmlparse(document (select result from (call invokeHttp(action=>'GET',endpoint=>'sample_1/view/g1/123?p2=test')) as d))) as x");
		this.internalResultSet.next();
		assertEquals("<e1>ABCDEFGHIJ</e1>", this.internalResultSet.getString(1));
		
		admin.undeploy("sample-vdb.xml");
		Thread.sleep(2000);
    }
	
    @Test
    public void testPostOperation() throws Exception {
        admin.deploy("sample-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("sample-vdb.xml")));
        assertTrue(AdminUtil.waitForVDBLoad(admin, "sample", 1, 3));
        
        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:sample@mm://localhost:31000;user=user;password=user", null);
        
        execute("SELECT * FROM Txns.G1"); //$NON-NLS-1$
        this.internalResultSet.next();
        
        assertTrue("sample_1.war not found", AdminUtil.waitForDeployment(admin, "sample_1.war", 5));
        
        String params = URLEncoder.encode("p1", "UTF-8") + "=" + URLEncoder.encode("456", "UTF-8");
        
        // post based call with default
        String response = httpCall("http://localhost:8080/sample_1/view/g1simplepost", "POST", params);
        assertEquals("response did not match expected", "<rows p1=\"456\" p2=\"1\"><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></rows>", response);

        // post based call
        params += "&" + URLEncoder.encode("p2", "UTF-8") + "=" + URLEncoder.encode("2", "UTF-8");
        params += "&" + URLEncoder.encode("p3", "UTF-8") + "=" + URLEncoder.encode("string value", "UTF-8");
        response = httpCall("http://localhost:8080/sample_1/view/g1simplepost", "POST", params);
        assertEquals("response did not match expected", "<rows p1=\"456\" p2=\"2\" p3=\"string value\"><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></rows>", response);
        
        // ad-hoc procedure
        params = URLEncoder.encode("sql", "UTF-8") + "=" + URLEncoder.encode("SELECT XMLELEMENT(NAME \"rows\", XMLAGG(XMLELEMENT(NAME \"row\", XMLFOREST(e1, e2)))) AS xml_out FROM Txns.G1", "UTF-8");
        response = httpCall("http://localhost:8080/sample_1/view/query", "POST", params);
        assertEquals("response did not match expected", "<rows><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></rows>", response);
        admin.undeploy("sample-vdb.xml");
        Thread.sleep(2000);
    }
    
	@Test
    public void testMultipartPostOperation() throws Exception {
		admin.deploy("sample-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("sample-vdb.xml")));
		assertTrue(AdminUtil.waitForVDBLoad(admin, "sample", 1, 3));
		
		this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:sample@mm://localhost:31000;user=user;password=user", null);
		
		execute("SELECT * FROM Txns.G1"); //$NON-NLS-1$
		this.internalResultSet.next();
		
		assertTrue("sample_1.war not found", AdminUtil.waitForDeployment(admin, "sample_1.war", 5));
		
		String params = URLEncoder.encode("p1", "UTF-8") + "=" + URLEncoder.encode("456", "UTF-8");
        HttpEntity entity = MultipartEntityBuilder.create()
                .addTextBody("p1", "456")
                .build();
		
		
		// post based call with default
		String response = httpMultipartPost(entity, "http://localhost:8080/sample_1/view/g1post");
		assertEquals("response did not match expected", "<rows p1=\"456\" p2=\"1\"><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></rows>", response);

		// post based call
		entity = MultipartEntityBuilder.create()
                .addTextBody("p1", "456")
                .addTextBody("p2", "2")
                .addTextBody("p3", "string value")
                .addBinaryBody("p4", "<root><p4>bar</p4></root>".getBytes("UTF-8"), ContentType.create("application/xml", "UTF-8"), "foo.xml")
                .build();		
		response = httpMultipartPost(entity, "http://localhost:8080/sample_1/view/g1post");
		assertEquals("response did not match expected", "<rows p1=\"456\" p2=\"2\" p3=\"string value\" p4=\"bar\"><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></rows>", response);
		
		// ad-hoc procedure
		params = URLEncoder.encode("sql", "UTF-8") + "=" + URLEncoder.encode("SELECT XMLELEMENT(NAME \"rows\", XMLAGG(XMLELEMENT(NAME \"row\", XMLFOREST(e1, e2)))) AS xml_out FROM Txns.G1", "UTF-8");
		response = httpCall("http://localhost:8080/sample_1/view/query", "POST", params);
		assertEquals("response did not match expected", "<rows><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></rows>", response);
		admin.undeploy("sample-vdb.xml");
		Thread.sleep(2000);
    }	
	
	private String httpMultipartPost(HttpEntity entity, String url ) throws ClientProtocolException, IOException {
        HttpPost httpPost = new HttpPost(url);
        //httpPost.addHeader("Authorization", "Basic "+Base64.encodeBytes(("user:user").getBytes()));
        httpPost.setEntity(entity);
        HttpClient httpClient = new DefaultHttpClient();
        HttpResponse response = httpClient.execute(httpPost);
        return ObjectConverterUtil.convertToString(new InputStreamReader(response.getEntity().getContent(), Charset
                .forName("UTF-8")));
	}
	
	private String httpCall(String url, String method, String params) throws Exception {
		HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
		connection.setRequestMethod(method);
		connection.setDoOutput(true);
		
		if (method.equalsIgnoreCase("post")) {
			OutputStreamWriter wr = new OutputStreamWriter(connection.getOutputStream());
		    wr.write(params);
		    wr.flush();
		}
		return ObjectConverterUtil.convertToString(new InputStreamReader(connection.getInputStream(), Charset.forName("UTF-8")));
	}		
}
