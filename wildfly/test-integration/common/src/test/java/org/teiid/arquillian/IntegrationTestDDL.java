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
import java.sql.SQLException;

import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.jboss.AdminFactory;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.TeiidDriver;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestDDL extends AbstractMMQueryTestCase {

    private Admin admin;

    @Before
    public void setup() throws Exception {
        admin = AdminFactory.getInstance().createAdmin("localhost",
                AdminUtil.MANAGEMENT_PORT, "admin", "admin".toCharArray());
    }

    @After
    public void teardown() throws AdminException {
        AdminUtil.cleanUp(admin);
        admin.close();
    }

    @Test
    public void testDDL() throws Exception {
        String ddl = "create database foo version '1';"
                + "use database foo version '1';"
                + "create server NONE type 'NONE' foreign data wrapper loopback;"
                + "create schema test server NONE;"
                + "set schema test;"
                + "CREATE FOREIGN TABLE G1 (e1 integer PRIMARY KEY, e2 varchar(25), e3 double)";

        this.admin.deploy("foo-vdb.ddl", new ByteArrayInputStream(ddl.getBytes()), false);
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:foo@mm://localhost:31000;user=user;password=user;autoFailover=true", null);

        execute("SELECT * FROM test.G1"); //$NON-NLS-1$
        assertRowCount(1);
        try {
            execute("SELECT * FROM test.G2"); //$NON-NLS-1$
            fail("should have failed as there is no G2 Table");
        } catch (Exception e) {
        }

        // add table
        ddl = ddl + "CREATE FOREIGN TABLE G2 (e1 integer PRIMARY KEY, e2 varchar(25), e3 double)";
        this.admin.deploy("foo-vdb.ddl", new ByteArrayInputStream(ddl.getBytes()), false);

        // THIS SHOULD BE REMOVED, AUTOFAILOVER NOT WORKING
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:foo@mm://localhost:31000;user=user;password=user;autoFailover=true", null);

        // and execute, using the same connection
        execute("SELECT * FROM test.G2"); //$NON-NLS-1$
        assertRowCount(1);
        printResults();

        String exportedDdl = admin.getSchema("foo", "1", null, null, null);
        Assert.assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("exported-vdb.ddl")),
                exportedDdl);

        closeConnection();
        admin.undeploy("foo-vdb.ddl");
    }

    @Test
    public void testOverrideTranslator() throws Exception {
        String ddl = "create database foo;"
        + "use database foo version '1';"
        + "create foreign data wrapper loopy type loopback OPTIONS(IncrementRows true, RowCount 500);"
        + "create server serverOne type 'NONE' foreign data wrapper loopy;"
        + "create schema test server serverOne;"
        + "set schema test;"
        + "CREATE FOREIGN TABLE G1 (e1 integer PRIMARY KEY, e2 varchar(25), e3 double);";

        this.admin.deploy("foo-vdb.ddl", new ByteArrayInputStream(ddl.getBytes()), false);

        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:foo@mm://localhost:31000;user=user;password=user", null);

        execute("SELECT * FROM test.G1"); //$NON-NLS-1$
        assertRowCount(500);
        closeConnection();
        admin.undeploy("foo-vdb.ddl");
    }

    @Test
    public void testVDBImport() throws Exception {
        String ddl = "create database foo;"
                + "use database foo version '1';"
                + "create foreign data wrapper loopy type loopback OPTIONS(IncrementRows true, RowCount 500);"
                + "create server serverOne type 'NONE' foreign data wrapper loopy;"
                + "create schema test server serverOne;"
                + "set schema test;"
                + "CREATE FOREIGN TABLE G1 (e1 integer PRIMARY KEY, e2 varchar(25), e3 double);";
        String bar =  "create database BAR;"
                + "IMPORT database foo VERSION '1';";

        this.admin.deploy("foo-vdb.ddl", new ByteArrayInputStream(ddl.getBytes()), false);
        this.admin.deploy("bar-vdb.ddl", new ByteArrayInputStream(bar.getBytes()), false);

        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:BAR@mm://localhost:31000;user=user;password=user", null);

        execute("SELECT * FROM test.G1"); //$NON-NLS-1$
        assertRowCount(500);
        closeConnection();
        admin.undeploy("bar-vdb.ddl");
        admin.undeploy("foo-vdb.ddl");
    }

    @Test
    public void testUdfClasspath() throws Exception {
        JavaArchive jar = ShrinkWrap.create(JavaArchive.class, "func.jar")
                  .addClasses(SampleFunctions.class);
        admin.deploy("func.jar", jar.as(ZipExporter.class).exportAsInputStream());

        String ddl = "create database \"dynamic-func\" OPTIONS(lib 'deployment.func.jar');"
                + "USE DATABASE \"dynamic-func\" version '1';"
                + "CREATE VIRTUAL schema test;"
                + "SET SCHEMA test;"
                + "CREATE function func (val string) returns integer "
                + "options (JAVA_CLASS 'org.teiid.arquillian.SampleFunctions',  JAVA_METHOD 'doSomething');";

        this.admin.deploy("dynamic-vdb.ddl", new ByteArrayInputStream(ddl.getBytes()), false);
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:dynamic-func@mm://localhost:31000;user=user;password=user", null);

        execute("SELECT func('a')"); //$NON-NLS-1$
        assertRowCount(1);
        admin.undeploy("dynamic-vdb.ddl");
        closeConnection();
    }

    @Test(expected=SQLException.class)
    public void testDDLOverJDBCNoAuth() throws Exception {
        this.internalConnection = TeiidDriver.getInstance()
                .connect("jdbc:teiid:foo2@mm://localhost:31000;user=dummy;password=user;autoFailover=true;vdbEdit=true", null);
    }
}
