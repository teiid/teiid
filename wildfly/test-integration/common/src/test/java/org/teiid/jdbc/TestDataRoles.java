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

package org.teiid.jdbc;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.teiid.deployers.VDBRepository;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.dqp.internal.process.DataRolePolicyDecider;
import org.teiid.dqp.internal.process.DefaultAuthorizationValidator;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.MaterializationManager;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestDataRoles {

    private static final class ExtendedEmbeddedServer extends EmbeddedServer {
        @Override
        public MaterializationManager getMaterializationManager() {
            return super.getMaterializationManager();
        }

        @Override
        public VDBRepository getVDBRepository() {
            return super.getVDBRepository();
        }
    }

    private ExtendedEmbeddedServer es;

    @After public void tearDown() {
        es.stop();
    }

    @Test public void testMaterializationWithSecurity() throws Exception {
        es = new ExtendedEmbeddedServer();
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);
        es.deployVDB(new ByteArrayInputStream(new String("<vdb name=\"role-1\" version=\"1\">"
                + "<model name=\"myschema\" type=\"virtual\">"
                + "<metadata type = \"DDL\"><![CDATA[CREATE VIEW vw as select 'a' as col;]]></metadata></model>"
                + "<data-role name=\"y\" any-authenticated=\"true\"/></vdb>").getBytes()));
        Connection c = es.getDriver().connect("jdbc:teiid:role-1", null);
        Statement s = c.createStatement();

        try {
            s.execute("select * from vw");
            Assert.fail();
        } catch (SQLException e) {
            //not authorized
        }
        es.getMaterializationManager().executeQuery(es.getVDBRepository().getLiveVDB("role-1"), "select * from vw");
    }

    private void setAuthorizationValidator(EmbeddedConfiguration ec, boolean metadataRequiresPermission) {
        DefaultAuthorizationValidator authorizationValidator = new DefaultAuthorizationValidator();
        authorizationValidator.setMetadataRequiresPermission(metadataRequiresPermission);
        authorizationValidator.setPolicyDecider(new DataRolePolicyDecider());
        ec.setAuthorizationValidator(authorizationValidator);
    }

    @Test public void testExecuteImmediate() throws Exception {
        es = new ExtendedEmbeddedServer();
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);
        es.deployVDB(new ByteArrayInputStream(new String("<vdb name=\"role-1\" version=\"1\">"
                + "<model name=\"myschema\" type=\"virtual\">"
                + "<metadata type = \"DDL\"><![CDATA[CREATE VIEW vw as select 'a' as col;]]></metadata></model>"
                + "<data-role name=\"y\" any-authenticated=\"true\"/></vdb>").getBytes()));
        Connection c = es.getDriver().connect("jdbc:teiid:role-1", null);
        Statement s = c.createStatement();
        s.execute("set autoCommitTxn off");

        try {
            s.execute("begin execute immediate 'select * from vw'; end");
            fail();
        } catch (TeiidSQLException e) {

        }

        //should be valid
        s.execute("begin execute immediate 'select 1'; end");

        //no temp permission
        try {
            s.execute("begin execute immediate 'select 1' as x integer into #temp; end");
            fail();
        } catch (TeiidSQLException e) {

        }

        //nested should not pass either
        try {
            s.execute("begin execute immediate 'begin execute immediate ''select * from vw''; end'; end");
            fail();
        } catch (TeiidSQLException e) {

        }


        //nested explain analyze should not pass either
        try {
            s.execute("explain analyze begin execute immediate 'begin execute immediate ''select * from vw''; end'; end");
            fail();
        } catch (TeiidSQLException e) {

        }

        //just an explain is fine as it doesn't show anything in the plan related to vw
        s.execute("explain begin execute immediate 'begin execute immediate ''select * from vw''; end'; end");
    }

    @Test public void testMetadataWithSecurity() throws Exception {
        es = new ExtendedEmbeddedServer();
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);
        deploySampleVDB();

        Connection c = es.getDriver().connect("jdbc:teiid:role-1", null);
        Statement s = c.createStatement();

        ResultSet rs = s.executeQuery("select * from sys.tables where name like 't_' order by name");
        //only t1/t3 should be visible - both have at least 1 permission
        assertTrue(rs.next());
        assertEquals("t1", rs.getString("name"));
        rs.next();
        assertEquals("t3", rs.getString("name"));
        assertFalse(rs.next());

        rs = s.executeQuery("select * from sys.schemas where name like 's_' order by name");
        //only s1 is visible, nothing in s2 is visible
        assertTrue(rs.next());
        assertEquals("s1", rs.getString("name"));
        assertFalse(rs.next());

        rs = s.executeQuery("select * from sys.columns where tablename like 't_'");
        //only col should be visible
        assertTrue(rs.next());
        assertEquals("col", rs.getString("name"));
        assertTrue(rs.next());
        assertFalse(rs.next());

        rs = s.executeQuery("select * from sys.procedures where name like 'proc_'");
        //only proc1 should be visible
        assertTrue(rs.next());
        assertEquals("proc1", rs.getString("name"));
        assertFalse(rs.next());

        rs = s.executeQuery("select * from sys.procedureparams where procedurename like 'proc_'");
        //only proc1 should be visible
        assertTrue(rs.next());
        assertEquals("param1", rs.getString("name"));
        assertTrue(rs.next());
        assertEquals("proc_col", rs.getString("name"));
        assertFalse(rs.next());

        rs = s.executeQuery("select * from sysadmin.usage where schemaname like 's_'");
        //nothing should be visible
        assertFalse(rs.next());

        rs = s.executeQuery("select * from sys.properties where name = 'x'");
        assertTrue(rs.next());
        assertEquals("y1", rs.getString("Value"));
        assertFalse(rs.next());

        rs = s.executeQuery("select * from sys.keycolumns where tablename = 't1'");
        //nothing should be visible
        assertFalse(rs.next());

        rs = s.executeQuery("select * from sys.keys where tablename = 't1'");
        //nothing should be visible
        assertFalse(rs.next());

        rs = s.executeQuery("select count(*) from pg_catalog.pg_constraint, pg_class where pg_class.oid = conrelid and relname like 't_'");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));

        rs = s.executeQuery("select count(*) from pg_catalog.pg_attribute, pg_class where pg_class.oid = pg_attribute.attrelid and relname like 't_'");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
    }

    @Test public void testMetadataWithoutPermission() throws Exception {
        es = new ExtendedEmbeddedServer();
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        setAuthorizationValidator(ec, false);
        es.start(ec);
        deploySampleVDB();

        Connection c = es.getDriver().connect("jdbc:teiid:role-1", null);
        Statement s = c.createStatement();

        ResultSet rs = s.executeQuery("select count(*) from sys.tables where name like 't_'");
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));

        rs = s.executeQuery("select count(*) from sys.schemas where name like 's_'");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));

        rs = s.executeQuery("select count(*) from sys.columns where tablename like 't_'");
        assertTrue(rs.next());
        assertEquals(6, rs.getInt(1));

        rs = s.executeQuery("select count(*) from sys.procedures where name like 'proc_'");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));

        rs = s.executeQuery("select count(*) from sysadmin.usage where schemaname like 's_'");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));

        rs = s.executeQuery("select count(*) from sys.properties where name = 'x'");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));

        rs = s.executeQuery("select count(*) from sys.keycolumns where tablename = 't1'");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));

        rs = s.executeQuery("select count(*) from sys.keys where tablename = 't1'");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));

        rs = s.executeQuery("select count(*) from pg_catalog.pg_constraint, pg_class where pg_class.oid = conrelid and relname like 't_'");
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));

        rs = s.executeQuery("select count(*) from pg_catalog.pg_attribute, pg_class where pg_class.oid = pg_attribute.attrelid and relname like 't_'");
        assertTrue(rs.next());
        assertEquals(6, rs.getInt(1));
    }

    private void deploySampleVDB() throws VirtualDatabaseException,
            ConnectorManagerException, TranslatorException, IOException {
        es.deployVDB(new ByteArrayInputStream(new String("<vdb name=\"role-1\" version=\"1\">"
                + "<model name=\"s1\" type=\"virtual\">"
                + "<metadata type = \"DDL\"><![CDATA["
                + "CREATE VIEW t3 (col string primary key) as select '3' as col;\n"
                + "CREATE VIEW t2 (col string primary key) options (x 'y') as select 'a' as col;\n"
                + "CREATE VIEW t1 (col string, col_hidden string, primary key (col, col_hidden), foreign key (col) references t2) options (x 'y1') as select col, 'b' as col_hidden from t2;\n"
                + "CREATE virtual procedure proc1 (param1 string) returns table (proc_col string) as begin end;\n"
                + "]]></metadata></model>"
                + "<model name=\"s2\" type=\"virtual\">"
                + "<metadata type = \"DDL\"><![CDATA["
                + "CREATE VIEW t3 as select 'a' as col, 'b' as col_hidden;\n"
                + "CREATE virtual procedure proc2 (param2 string) returns table (proc_col string) as begin end;\n"
                + "]]></metadata></model>"
                + "<data-role name=\"y\" any-authenticated=\"true\">"
                + "<permission><resource-name>s1</resource-name><allow-read>true</allow-read></permission>"
                + "<permission><resource-name>s1</resource-name><allow-create>true</allow-create></permission>"
                + "<permission><resource-name>s1.t1.col_hidden</resource-name><allow-read>false</allow-read></permission>"
                + "<permission><resource-name>s1.t2</resource-name><allow-read>false</allow-read></permission>"
                + "<permission><resource-name>sysadmin</resource-name><allow-read>true</allow-read></permission>"
                + "</data-role></vdb>").getBytes()));
    }

}
