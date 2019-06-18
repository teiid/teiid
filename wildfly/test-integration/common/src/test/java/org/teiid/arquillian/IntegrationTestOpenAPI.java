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

import java.io.StringReader;
import java.nio.charset.Charset;
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
import org.teiid.core.util.ReaderInputStream;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.TeiidDriver;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestOpenAPI extends AbstractMMQueryTestCase {

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

        Properties p = new Properties();
        p.setProperty("class-name", "org.teiid.resource.adapter.ws.WSManagedConnectionFactory");
        p.setProperty("EndPoint", "http://localhost:8080/odata4/Loopy.1/MarketData");
        p.setProperty("SecurityType", "HTTPBasic");
        p.setProperty("AuthUserName", "user");
        p.setProperty("AuthPassword", "user");
        admin.createDataSource("openapi", "webservice", p);

        String openapi = "<vdb name=\"openapi\" version=\"1\">\n" +
                "    <!-- example VDB -->\n" +
                "    <model visible=\"true\" name=\"m\">\n" +
                "        <property name=\"importer.metadataUrl\" value=\"/openapi.json?version=3\"/>\n" +
                "        <source name=\"s\" translator-name=\"openapi\" connection-jndi-name=\"java:/openapi\"/> \n" +
                "    </model>\n" +
                "</vdb>";

        admin.deploy("openapi-vdb.xml", new ReaderInputStream(new StringReader(openapi), Charset.forName("UTF-8")));

        assertTrue(AdminUtil.waitForVDBLoad(admin, "openapi", 1));

        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:openapi@mm://localhost:31000;user=user;password=user", null);

        execute("call G2_GET()"); //$NON-NLS-1$
        assertRowCount(1);

        String openapi2 = "<vdb name=\"openapi2\" version=\"1\">\n" +
                "    <!-- example VDB -->\n" +
                "    <model visible=\"true\" name=\"m\">\n" +
                "        <property name=\"importer.metadataUrl\" value=\"/openapi.json?version=2\"/>\n" +
                "        <source name=\"s\" translator-name=\"openapi\" connection-jndi-name=\"java:/openapi\"/> \n" +
                "    </model>\n" +
                "</vdb>";

        admin.deploy("openapi2-vdb.xml", new ReaderInputStream(new StringReader(openapi2), Charset.forName("UTF-8")));

        assertTrue(AdminUtil.waitForVDBLoad(admin, "openapi2", 1));

        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:openapi2@mm://localhost:31000;user=user;password=user", null);

        execute("call G2_GET()"); //$NON-NLS-1$
        assertRowCount(1);
    }

}
