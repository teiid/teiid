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
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.ArchiveAsset;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
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
public class IntegrationTestDynamicViewDefinition extends AbstractMMQueryTestCase {

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
    public void testViewDefinition() throws Exception {

        admin.deploy("dynamicview-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("dynamicview-vdb.xml")));

        Properties props = new Properties();
        props.setProperty("ParentDirectory", UnitTestUtil.getTestDataFile("/arquillian/txt/").getAbsolutePath());
        props.setProperty("AllowParentPaths", "true");
        props.setProperty("class-name", "org.teiid.resource.adapter.file.FileManagedConnectionFactory");

        AdminUtil.createDataSource(admin, "marketdata-file", "file", props);

        assertTrue(AdminUtil.waitForVDBLoad(admin, "dynamic", 1));

        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:dynamic@mm://localhost:31000;user=user;password=user", null);

        execute("SELECT * FROM Sys.Columns WHERE tablename='stock'"); //$NON-NLS-1$
        assertRowCount(2);
    }

    @Test public void testUdfClasspath() throws Exception {
        JavaArchive jar = ShrinkWrap.create(JavaArchive.class, "func.jar")
                  .addClasses(SampleFunctions.class);
        admin.deploy("func.jar", jar.as(ZipExporter.class).exportAsInputStream());

        admin.deploy("dynamicfunc-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("dynamicfunc-vdb.xml")));

        assertTrue(AdminUtil.waitForVDBLoad(admin, "dynamic-func", 1));

        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:dynamic-func@mm://localhost:31000;user=user;password=user", null);

        execute("SELECT func('a')"); //$NON-NLS-1$
        assertRowCount(1);
    }

    @Test public void testVdbZipWithDDLAndUDF() throws Exception {
        JavaArchive udfJar = ShrinkWrap.create(JavaArchive.class, "func.jar").addClasses(SampleFunctions.class);
        JavaArchive jar = ShrinkWrap.create(JavaArchive.class, "dynamic-ddl.vdb")
                  .addAsManifestResource(UnitTestUtil.getTestDataFile("vdb.xml"))
                  .addAsResource(UnitTestUtil.getTestDataFile("test.ddl"))
                  .addAsResource(new ArchiveAsset(udfJar, ZipExporter.class), "lib/udf.jar");
        admin.deploy("dynamic-ddl.vdb", jar.as(ZipExporter.class).exportAsInputStream());

        assertTrue(AdminUtil.waitForVDBLoad(admin, "dynamic-ddl", 1));

        this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:dynamic-ddl@mm://localhost:31000;user=user;password=user", null);

        execute("SELECT * from stock"); //$NON-NLS-1$
        assertRowCount(1);
        execute("SELECT func('a')"); //$NON-NLS-1$
        assertRowCount(1);
    }

}
