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
public class IntegrationTestVDBReuse extends AbstractMMQueryTestCase {

    private Admin admin;

    @Before
    public void setup() throws Exception {
        admin = AdminFactory.getInstance().createAdmin("localhost", 9990,	"admin", "admin".toCharArray());
        assertEquals(0, admin.getSessions().size());
    }

    @After
    public void teardown() throws AdminException {
        AdminUtil.cleanUp(admin);
        assertEquals(0, admin.getSessions().size());
        admin.close();
    }

    @Test
    public void testReuse() throws Exception {
        admin.deploy("dynamicview-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("dynamicview-vdb.xml")));

        Properties props = new Properties();
        props.setProperty("ParentDirectory", UnitTestUtil.getTestDataFile("/arquillian/txt/").getAbsolutePath());
        props.setProperty("AllowParentPaths", "true");
        props.setProperty("class-name", "org.teiid.resource.adapter.file.FileManagedConnectionFactory");

        AdminUtil.createDataSource(admin, "marketdata-file", "file", props);

        assertTrue(AdminUtil.waitForVDBLoad(admin, "dynamic", 1));

        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:dynamic@mm://localhost:31000;user=user;password=user", null);
        execute("SELECT * FROM Stock"); //$NON-NLS-1$

        execute("SELECT count(*) FROM Sys.Columns"); //$NON-NLS-1$
        this.internalResultSet.next();
        int cols = this.internalResultSet.getInt(1);

        admin.deploy("reuse-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("reuse-vdb.xml")));

        assertTrue(AdminUtil.waitForVDBLoad(admin, "reuse", 1));

        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:reuse@mm://localhost:31000;user=user;password=user", null);

        execute("SELECT count(*) FROM Sys.Columns"); //$NON-NLS-1$
        this.internalResultSet.next();
        assertTrue(this.internalResultSet.getInt(1) > cols);


        admin.deploy("reuse1-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("reuse1-vdb.xml")));
        assertTrue(AdminUtil.waitForVDBLoad(admin, "reuse1", 1));
        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:reuse1@mm://localhost:31000;user=user;password=user", null);

        execute("SELECT * FROM Stock2"); //$NON-NLS-1$

        //redeploy
        admin.deploy("dynamicview-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("dynamicview-vdb.xml")));
        assertTrue(AdminUtil.waitForVDBLoad(admin, "dynamic", 1));

        //ensure we are up
        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:dynamic@mm://localhost:31000;user=user;password=user", null);
        execute("SELECT * FROM Stock"); //$NON-NLS-1$

        //ensure the importing vdb came back up
        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:reuse@mm://localhost:31000;user=user;password=user", null);

        execute("SELECT count(*) FROM Sys.Columns"); //$NON-NLS-1$
    }

}
