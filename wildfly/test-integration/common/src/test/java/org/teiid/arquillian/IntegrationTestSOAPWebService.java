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

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.Properties;

import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.jboss.AdminFactory;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.TeiidDriver;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestSOAPWebService {

    private Admin admin;

    @Before
    public void setup() throws Exception {
        admin = AdminFactory.getInstance().createAdmin("localhost", AdminUtil.MANAGEMENT_PORT, "admin",
                "admin".toCharArray());
    }

    @After
    public void teardown() throws AdminException {
        AdminUtil.cleanUp(admin);
        admin.close();
    }

    @Test
    public void testVDBDeployment() throws Exception {
        Collection<?> vdbs = admin.getVDBs();
        assertTrue(vdbs.isEmpty());

        assertTrue(admin.getDataSourceTemplateNames().contains("webservice"));
        String raSource = "web-ds";
        assertFalse(admin.getDataSourceNames().contains(raSource));

        admin.deploy("addressing-service.war", new FileInputStream(UnitTestUtil.getTestDataFile("addressing-service.war")));

        Properties p = new Properties();
        p.setProperty("class-name", "org.teiid.resource.adapter.ws.WSManagedConnectionFactory");
        p.setProperty("EndPoint", "http://localhost:8080/jboss-jaxws-addressing/AddressingService");

        admin.createDataSource(raSource, "webservice", p);

        assertTrue(admin.getDataSourceNames().contains(raSource));

        admin.deploy("soapsvc-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("soapsvc-vdb.xml")));
        vdbs = admin.getVDBs();
        assertFalse(vdbs.isEmpty());

        VDB vdb = admin.getVDB("WSMSG", "1");
        AdminUtil.waitForVDBLoad(admin, "WSMSG", 1);

        vdb = admin.getVDB("WSMSG", "1");
        assertTrue(vdb.isValid());
        assertTrue(vdb.getStatus().equals(Status.ACTIVE));

        Connection conn = TeiidDriver.getInstance().connect("jdbc:teiid:WSMSG@mm://localhost:31000;user=user;password=user;", null);
        Statement stmt = conn.createStatement();
        String sql = "SELECT *\n" +
                "FROM ADDRESSINGSERVICE.SAYHELLO\n" +
                "WHERE MESSAGEID = 'uuid:73e4d992-6bfe-4434-b8c3-37e00f36ad97' AND SAYHELLO = 'Teiid'\n" +
                "AND ADDRESSINGSERVICE.SAYHELLO.To = 'http://localhost:8080/jboss-jaxws-addressing/AddressingService'\n" +
                "AND ADDRESSINGSERVICE.SAYHELLO.ReplyTo = 'http://www.w3.org/2005/08/addressing/anonymous'\n" +
                "AND ADDRESSINGSERVICE.SAYHELLO.Action = 'http://www.jboss.org/jbossws/ws-extensions/wsaddressing/ServiceIface/sayHello'";
        ResultSet rs = stmt.executeQuery(sql);
        assertTrue(rs.next());
        assertEquals("Hello World!", rs.getString(1));
        conn.close();
    }
}
