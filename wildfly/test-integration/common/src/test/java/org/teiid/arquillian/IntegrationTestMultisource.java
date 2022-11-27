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
public class IntegrationTestMultisource extends AbstractMMQueryTestCase {

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
    public void testSourceOperations() throws Exception {

        admin.deploy("multisource-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("arquillian/multisource-vdb.xml")));

        Properties props = new Properties();
        props.setProperty("ParentDirectory", UnitTestUtil.getTestDataFile("/arquillian/txt/").getAbsolutePath());
        props.setProperty("AllowParentPaths", "true");
        props.setProperty("class-name", "org.teiid.resource.adapter.file.FileManagedConnectionFactory");

        AdminUtil.createDataSource(admin, "test-file", "file", props);

        assertTrue(AdminUtil.waitForVDBLoad(admin, "multisource", 1));

        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:multisource@mm://localhost:31000;user=user;password=user", null);

        execute("exec getfiles('*.txt')", new Object[] {}); //$NON-NLS-1$
        assertRowCount(1);

        admin.addSource("multisource", 1, "MarketData", "text-connector1", "file1", "java:/test-file");

        execute("exec getfiles('*.txt')", new Object[] {}); //$NON-NLS-1$
        assertRowCount(2);

        admin.removeSource("multisource", 1, "MarketData", "text-connector");

        execute("exec getfiles('*.txt')", new Object[] {}); //$NON-NLS-1$
        assertRowCount(1);
    }

    @Test
    public void testSourceOperationsDDLVDB() throws Exception {

        admin.deploy("multisource-vdb.ddl",new FileInputStream(UnitTestUtil.getTestDataFile("arquillian/multisource-vdb.ddl")));

        Properties props = new Properties();
        props.setProperty("ParentDirectory", UnitTestUtil.getTestDataFile("/arquillian/txt/").getAbsolutePath());
        props.setProperty("AllowParentPaths", "true");
        props.setProperty("class-name", "org.teiid.resource.adapter.file.FileManagedConnectionFactory");

        AdminUtil.createDataSource(admin, "test-file", "file", props);

        assertTrue(AdminUtil.waitForVDBLoad(admin, "multisource", 1));

        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:multisource@mm://localhost:31000;user=user;password=user", null);

        execute("exec getfiles('*.txt')", new Object[] {}); //$NON-NLS-1$
        assertRowCount(1);

        admin.addSource("multisource", 1, "MarketData", "text-connector1", "file1", "java:/test-file");

        execute("exec getfiles('*.txt')", new Object[] {}); //$NON-NLS-1$
        assertRowCount(2);

        admin.removeSource("multisource", 1, "MarketData", "text-connector");

        execute("exec getfiles('*.txt')", new Object[] {}); //$NON-NLS-1$
        assertRowCount(1);
    }

}
