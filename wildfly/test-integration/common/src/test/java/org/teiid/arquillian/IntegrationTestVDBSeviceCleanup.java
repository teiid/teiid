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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
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

        assertNotNull(TeiidDriver.getInstance().connect("jdbc:teiid:service@mm://localhost:31000;user=user;password=user", null));

        admin.undeploy("service-vdb.xml");

        admin.deleteDataSource("ServiceDS");

        /*
        admin.deploy("service-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("service-vdb.xml")));

        createDS("ServiceDS");

        assertTrue(AdminUtil.waitForVDBLoad(admin, "service", 1, 3));

        assertNotNull(TeiidDriver.getInstance().connect("jdbc:teiid:service@mm://localhost:31000;user=user;password=user", null));
        */
    }

    private void createDS(String deployName) throws AdminException {
        Properties props = new Properties();
        props.setProperty("connection-url","jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        props.setProperty("user-name", "sa");
        props.setProperty("password", "sa");
        AdminUtil.createDataSource(admin, deployName, "h2", props);
    }

}
