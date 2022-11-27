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
import java.util.Properties;

import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.jboss.AdminFactory;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.TeiidDriver;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestVDBSeviceCleanup extends AbstractMMQueryTestCase {

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
    public void testServiceCleanup() throws Exception {
        admin.deploy("service-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("service-vdb.xml")));

        createDS("ServiceDS");

        assertTrue(AdminUtil.waitForVDBLoad(admin, "service", 1));

        Connection c = TeiidDriver.getInstance().connect("jdbc:teiid:service@mm://localhost:31000;user=user;password=user", null);
        Statement s = c.createStatement();
        s.execute("select ID,SESSION_START from example.SESSIONS ORDER BY ID");
        ResultSet rs = s.getResultSet();
        rs.next();
        String id = rs.getString(1);
        c.close();

        admin.undeploy("service-vdb.xml");

        admin.deleteDataSource("ServiceDS");

        admin.deploy("service-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("service-vdb.xml")));

        createDS("ServiceDS");

        assertTrue(AdminUtil.waitForVDBLoad(admin, "service", 1));

        c = TeiidDriver.getInstance().connect("jdbc:teiid:service@mm://localhost:31000;user=user;password=user", null);
        s = c.createStatement();
        s.execute("select ID,SESSION_START from example.SESSIONS ORDER BY ID");
        rs = s.getResultSet();
        rs.next();
        String id2 = rs.getString(1);

        //the ids should be different if the datasource got fully bounced
        assertNotEquals(id, id2);
        c.close();
    }

    private void createDS(String deployName) throws AdminException {
        Properties props = new Properties();
        props.setProperty("connection-url","jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        props.setProperty("user-name", "sa");
        props.setProperty("password", "sa");
        AdminUtil.createDataSource(admin, deployName, "h2", props);
    }

}
