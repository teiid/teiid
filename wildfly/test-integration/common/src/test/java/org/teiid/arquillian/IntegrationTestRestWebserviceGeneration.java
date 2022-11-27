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

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.Base64;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.ReaderInputStream;
import org.teiid.core.util.StringUtil;
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
        deployVDB();
        assertTrue(AdminUtil.waitForVDBLoad(admin, "sample", 1));

        admin.deploy("sample-ws-vdb.xml",new ReaderInputStream(new StringReader(
                //simple ws vdb
                "<vdb name=\"sample-ws\" version=\"1\">"
                + "<model name=\"ws\"><source name=\"ws\" translator-name=\"ws\" connection-jndi-name=\"java:/sample-ws\"/></model>"
                +"</vdb>"), Charset.forName("UTF-8")));
        assertTrue(AdminUtil.waitForVDBLoad(admin, "sample-ws", 1));

        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:sample@mm://localhost:31000;user=user;password=user", null);

        execute("SELECT * FROM Txns.G1"); //$NON-NLS-1$
        this.internalResultSet.next();

        assertTrue("sample_1.war not found", AdminUtil.waitForDeployment(admin, "sample_1.war", 5));

        // get based call
        String response = httpCall("http://localhost:8080/sample_1/View/g1/123?p2=test", "GET", null);
        String expected = "<rows p1=\"123\" p2=\"test\"><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></rows>";
        assertEquals("response did not match expected", expected, response);

        response = httpCall("http://localhost:8080/sample_1/View/any/1", "GET", null);
        assertEquals("response did not match expected", "1", response);

        this.internalConnection.close();

        //try the same thing through a vdb
        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:sample-ws@mm://localhost:31000;user=user;password=user", null);
        execute("select to_chars(x.result, 'UTF-8') from (call invokeHttp(action=>'GET',endpoint=>'sample_1/View/g1/123?p2=test')) as x");
        this.internalResultSet.next();
        assertEquals(expected, this.internalResultSet.getString(1));

        //test a large doc
        response = httpCall("http://localhost:8080/sample_1/View/largedoc", "GET", null);
        assertEquals(327801, response.length());

        //test streaming xmltable
        execute("select * from xmltable('/rows/row/e1' passing xmlparse(document (select result from (call invokeHttp(headers=>jsonObject('application/xml' as \"Content-Type\"), action=>'GET',endpoint=>'sample_1/View/g1/123?p2=test')) as d))) as x");
        this.internalResultSet.next();
        assertEquals("<e1>ABCDEFGHIJ</e1>", this.internalResultSet.getString(1));

        admin.deploy("sample2-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("sample2-vdb.xml")));
        assertTrue(AdminUtil.waitForVDBLoad(admin, "sample2", 1));

        //test swagger
        response = httpCall("http://localhost:8080/sample_1/swagger.yaml", "GET", null);

        Thread.sleep(2000); //wait for the war to come up

        int retries = 10;
        for (int i = 1; i <= retries; i++) {
            try {
                String response1 = httpCall("http://localhost:8080/sample2_1/swagger.yaml", "GET", null);
                assertNotEquals(response, response1);
                break;
            } catch (TeiidRuntimeException e) {
                if (i == retries) {
                    throw e;
                }
                Thread.sleep(1000); //wait for the war to come up
            }
        }

        admin.undeploy("sample-vdb.xml");
        Thread.sleep(2000);
    }

    @Test
    public void testPostOperation() throws Exception {
        deployVDB();

        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:sample@mm://localhost:31000;user=user;password=user", null);

        execute("SELECT * FROM Txns.G1"); //$NON-NLS-1$
        this.internalResultSet.next();

        assertTrue("sample_1.war not found", AdminUtil.waitForDeployment(admin, "sample_1.war", 5));

        String params = URLEncoder.encode("p1", "UTF-8") + "=" + URLEncoder.encode("456", "UTF-8");

        // post based call with default
        String response = httpCall("http://localhost:8080/sample_1/View/g1simplepost", "POST", params);
        assertEquals("response did not match expected", "<rows p1=\"456\" p2=\"1\"><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></rows>", response);

        // post based call
        params += "&" + URLEncoder.encode("p2", "UTF-8") + "=" + URLEncoder.encode("2", "UTF-8");
        params += "&" + URLEncoder.encode("p3", "UTF-8") + "=" + URLEncoder.encode("string value", "UTF-8");
        response = httpCall("http://localhost:8080/sample_1/View/g1simplepost", "POST", params);
        assertEquals("response did not match expected", "<rows p1=\"456\" p2=\"2\" p3=\"string value\"><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></rows>", response);

        // ad-hoc procedure
        params = URLEncoder.encode("sql", "UTF-8") + "=" + URLEncoder.encode("SELECT XMLELEMENT(NAME \"rows\", XMLAGG(XMLELEMENT(NAME \"row\", XMLFOREST(e1, e2)))) AS xml_out FROM Txns.G1", "UTF-8");
        response = httpCall("http://localhost:8080/sample_1/View/query", "POST", params);
        assertEquals("response did not match expected", "<rows><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></rows>", response);
        admin.undeploy("sample-vdb.xml");
        Thread.sleep(2000);
    }

    private void deployVDB() throws AdminException, FileNotFoundException {
        //we'll remove the war from the previous test as we are now lazily removing it on the server side
        try {
            admin.undeploy("sample_1.war");
        } catch (AdminException e) {

        }
        admin.deploy("sample-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("sample-vdb.xml")));
        assertTrue(AdminUtil.waitForVDBLoad(admin, "sample", 1));
    }

    @Test
    public void testMultipartPostOperation() throws Exception {
        deployVDB();
        assertTrue(AdminUtil.waitForVDBLoad(admin, "sample", 1));

        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:sample@mm://localhost:31000;user=user;password=user", null);

        execute("SELECT * FROM Txns.G1"); //$NON-NLS-1$
        this.internalResultSet.next();

        assertTrue("sample_1.war not found", AdminUtil.waitForDeployment(admin, "sample_1.war", 5));

        String params = URLEncoder.encode("p1", "UTF-8") + "=" + URLEncoder.encode("456", "UTF-8");
        HttpEntity entity = MultipartEntityBuilder.create()
                .addTextBody("p1", "456")
                .build();


        // post based call with default
        String response = httpMultipartPost(entity, "http://localhost:8080/sample_1/View/g1post");
        assertEquals("response did not match expected", "<rows p1=\"456\" p2=\"1\"><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></rows>", response);

        // post based call
        entity = MultipartEntityBuilder.create()
                .addTextBody("p1", "456")
                .addTextBody("p2", "2")
                .addTextBody("p3", "string value")
                .addBinaryBody("p4", "<root><p4>bar</p4></root>".getBytes("UTF-8"), ContentType.create("application/xml", "UTF-8"), "foo.xml")
                .build();
        response = httpMultipartPost(entity, "http://localhost:8080/sample_1/View/g1post");
        assertEquals("response did not match expected", "<rows p1=\"456\" p2=\"2\" p3=\"string value\" p4=\"bar\"><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></rows>", response);

        // ad-hoc procedure
        params = URLEncoder.encode("sql", "UTF-8") + "=" + URLEncoder.encode("SELECT XMLELEMENT(NAME \"rows\", XMLAGG(XMLELEMENT(NAME \"row\", XMLFOREST(e1, e2)))) AS xml_out FROM Txns.G1", "UTF-8");
        response = httpCall("http://localhost:8080/sample_1/View/query", "POST", params);
        assertEquals("response did not match expected", "<rows><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></rows>", response);

        // blob multipost
        entity = MultipartEntityBuilder.create()
                .addBinaryBody("p1", "<root><p4>bar</p4></root>".getBytes("UTF-8"), ContentType.create("application/xml", "UTF-8"), "foo.xml")
                .build();
        response = httpMultipartPost(entity, "http://localhost:8080/sample_1/View/blobpost");
        assertEquals("response did not match expected", "<root><p4>bar</p4></root>", response);

        // clob multipost
        entity = MultipartEntityBuilder.create()
                .addBinaryBody("p1", "<root><p4>bar</p4></root>".getBytes("UTF-8"), ContentType.create("application/xml", "UTF-8"), "foo.xml")
                .build();
        response = httpMultipartPost(entity, "http://localhost:8080/sample_1/View/clobpost");
        assertEquals("response did not match expected", "<root><p4>bar</p4></root>", response);

        // varbinary multipost -- doesn't work as multipart is not expected
        entity = MultipartEntityBuilder.create()
                .addBinaryBody("p1", Base64.encodeBytes("<root><p4>bar</p4></root>".getBytes("UTF-8")).getBytes("UTF-8"), ContentType.create("application/xml", "UTF-8"), "foo.xml")
                .build();
        response = httpMultipartPost(entity, "http://localhost:8080/sample_1/View/binarypost");
        assertTrue("response did not match expected", response.contains("RESTEASY003065"));

        params = URLEncoder.encode("p1", "UTF-8") + "=" + URLEncoder.encode(Base64.encodeBytes("<root><p4>bar</p4></root>".getBytes("UTF-8")), "UTF-8");
        response = httpCall("http://localhost:8080/sample_1/View/binarypost", "POST", params);
        assertEquals("response did not match expected", "<root><p4>bar</p4></root>", response);

        admin.undeploy("sample-vdb.xml");
        Thread.sleep(2000);
    }

    @Test
    public void testSemanticVersion() throws Exception {
        String vdb = ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("sample-vdb.xml"));
        vdb = StringUtil.replace(vdb, "name=\"sample\" version=\"1\"", "name=\"sample\" version=\"2.1.1\"");
        admin.deploy("sample-vdb.xml", new ByteArrayInputStream(vdb.getBytes("UTF-8")));
        assertTrue(AdminUtil.waitForVDBLoad(admin, "sample", "2.1.1"));

        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:sample@mm://localhost:31000;user=user;password=user", null);

        execute("SELECT * FROM Txns.G1"); //$NON-NLS-1$
        this.internalResultSet.next();

        assertTrue("sample_2.1.1.war not found", AdminUtil.waitForDeployment(admin, "sample_2.1.1.war", 5));

        HttpEntity entity = MultipartEntityBuilder.create()
                .addTextBody("p1", "456")
                .build();

        String response = httpMultipartPost(entity, "http://localhost:8080/sample_2.1.1/View/g1post");
        assertEquals("response did not match expected", "<rows p1=\"456\" p2=\"1\"><row><e1>ABCDEFGHIJ</e1><e2>0</e2></row></rows>", response);

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

        int code = connection.getResponseCode();
        if (code >= 400) {
            throw new TeiidRuntimeException(String.valueOf(code));
        }

        return ObjectConverterUtil.convertToString(new InputStreamReader(connection.getInputStream(), Charset.forName("UTF-8")));
    }
}
