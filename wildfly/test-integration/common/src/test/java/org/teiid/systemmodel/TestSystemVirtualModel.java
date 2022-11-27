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

package org.teiid.systemmodel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TestMMDatabaseMetaData;
import org.teiid.runtime.HardCodedExecutionFactory;


/**
 * Exercises each virtual table in the system model.
 */
@SuppressWarnings("nls")
public class TestSystemVirtualModel extends AbstractMMQueryTestCase {
    private static final String VDB = "PartsSupplier"; //$NON-NLS-1$

    private static FakeServer server;

    @BeforeClass public static void setup() throws Exception {
        server = new FakeServer(true);
        server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("x");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("DDL", "create view t as select 1");
        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("y");
        mmd1.setModelType(Type.VIRTUAL);
        mmd1.addSourceMetadata("DDL", "create view T as select 1");
        server.deployVDB("test", mmd, mmd1);

        ModelMetaData mmd2 = new ModelMetaData();
        mmd2.setName("x");
        mmd2.setModelType(Type.VIRTUAL);
        mmd2.addSourceMetadata("DDL", "create view t (g geometry options (\"teiid_spatial:srid\" 3819)) as select null;");
        server.deployVDB("test1", mmd2);
    }

    @AfterClass public static void teardown() throws Exception {
        server.stop();
    }

    public TestSystemVirtualModel() {
        // this is needed because the result files are generated
        // with another tool which uses tab as delimiter
        super.DELIMITER = "\t"; //$NON-NLS-1$
    }

    @Before public void setUp() throws Exception {
        this.internalConnection = server.createConnection("jdbc:teiid:" + VDB); //$NON-NLS-1$ //$NON-NLS-2$
       }

    protected void checkResult(String testName, String query) throws Exception {
        execute(query);
        TestMMDatabaseMetaData.compareResultSet("TestSystemVirtualModel/" + testName, this.internalResultSet);
    }

    @Test public void testModels() throws Exception {
        checkResult("testSchemas", "select* from SYS.Schemas order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testKeys() throws Exception {
        checkResult("testKeys", "select* from SYS.Keys order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testGroups() throws Exception {
        checkResult("testTables", "select* from SYS.Tables order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDataTypes() throws Exception {
        checkResult("testDataTypes", "select * from SYS.DataTypes order by name"); //$NON-NLS-1$ //$NON-NLS-2$
        execute("select * from SYS.DataTypes where name = 'string'"); //$NON-NLS-1$ //$NON-NLS-2$
        assertRowCount(1);
        execute("select * from SYS.DataTypes where isstandard"); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue(getRowCount() >= 20);
    }

    @Test public void testProcedureParams() throws Exception {
        checkResult("testProcedureParams", "select * from SYS.ProcedureParams order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testProcedures() throws Exception {
        checkResult("testProcedures", "select* from SYS.Procedures order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testProperties() throws Exception {
        checkResult("testProperties", "select* from SYS.Properties"); //$NON-NLS-1$
    }

    @Test public void testVirtualDatabase() throws Exception {

        String[] expected = { "Name[string]	Version[string]	Description[string] ", "PartsSupplier	1	null", //$NON-NLS-1$ //$NON-NLS-2$

        };
        executeAndAssertResults("select Name, Version, Description from SYS.VirtualDatabases", //$NON-NLS-1$
                expected);
    }

    @Test public void testKeyColumns() throws Exception {
        checkResult("testKeyColumns", "select* from SYS.KeyColumns order by Name, KeyName"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testVDBResources() throws IOException, SQLException {
        execute("select * from vdbresources order by resourcePath",new Object[] {}); //$NON-NLS-1$
        TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
    }

    @Test public void testColumns() throws Exception {
        checkResult("testColumns", "select* from SYS.Columns order by Name, uid"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testTableType() throws Exception {

        String[] expected = { "Type[string]	", "Table", }; //$NON-NLS-1$ //$NON-NLS-2$
        executeAndAssertResults(
                "select distinct Type from SYS.Tables order by Type", //$NON-NLS-1$
                expected);
    }

    @Test public void testTableIsSystem() throws Exception {
        checkResult("testTableIsSystem", "select Name from SYS.Tables where IsSystem = 'false' order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDefect12064() throws Exception {
        checkResult("testDefect12064", "select KeyName, RefKeyUID FROM SYS.KeyColumns WHERE RefKeyUID IS NULL order by KeyName"); //$NON-NLS-1$
    }

    @Test public void testReferenceKeyColumns() throws Exception {
        checkResult("testReferenceKeyColumns", "select* FROM SYS.ReferenceKeyColumns order by PKTABLE_NAME"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testLogMsg() throws Exception {
        execute("call logMsg(level=>'DEBUG', context=>'org.teiid.foo', msg=>'hello world')"); //$NON-NLS-1$
    }

    @Test(expected=SQLException.class) public void testNotNullParameter() throws Exception {
        execute("begin declare string x = null; call logMsg(level=>'DEBUG', context=>x, msg=>'a'); end"); //$NON-NLS-1$
    }

    @Test(expected=SQLException.class) public void testLogMsg1() throws Exception {
        execute("call logMsg(level=>'foo', context=>'org.teiid.foo', msg=>'hello world')"); //$NON-NLS-1$
    }

    @Test public void testCallableParametersByName() throws Exception {
        CallableStatement cs = this.internalConnection.prepareCall("{? = call logMsg(?, ?, ?)}");
        ParameterMetaData pmd = cs.getParameterMetaData();
        assertEquals(3, pmd.getParameterCount());
        cs.registerOutParameter("logged", Types.BOOLEAN);
        //different case
        cs.setString("LEVEL", "DEBUG");
        try {
            //invalid param
            cs.setString("n", "");
            fail();
        } catch (SQLException e) {
        }
        cs.setString("context", "org.teiid.foo");
        cs.setString("msg", "hello world");
        cs.execute();
        assertEquals(cs.getBoolean(1), cs.getBoolean("logged"));
    }

    @Test public void testArrayAggType() throws Exception {
        String sql = "SELECT array_agg(name) from sys.tables"; //$NON-NLS-1$
        checkResult("testArrayAggType", sql); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testExecuteUpdateWithStoredProcedure() throws Exception {
        PreparedStatement cs = this.internalConnection.prepareStatement("call logMsg(?, ?, ?)");
        //different case
        cs.setString(1, "DEBUG");
        cs.setString(2, "org.teiid.foo");
        cs.setString(3, "hello world");
        assertEquals(0, cs.executeUpdate());

        Statement s = this.internalConnection.createStatement();
        assertEquals(0, s.executeUpdate("call logMsg('DEBUG', 'org.teiid.foo', 'hello world')"));
    }

    @Test public void testExpectedTypes() throws Exception {
        ResultSet rs = this.internalConnection.createStatement().executeQuery("select name, schemaname from sys.tables where schemaname in ('SYS', 'SYSADMIN')");
        while (rs.next()) {
            String name = rs.getString(1);
            String schemaName = rs.getString(2);
            ResultSet rs1 = this.internalConnection.createStatement().executeQuery("select * from " + schemaName + "." + name + " limit 1");
            ResultSetMetaData metadata = rs1.getMetaData();
            if (rs1.next()) {
                for (int i = 1; i <= metadata.getColumnCount(); i++) {
                    Object o = rs1.getObject(i);
                    assertTrue("Type mismatch for " + name + " " + metadata.getColumnName(i), o == null || Class.forName(metadata.getColumnClassName(i)).isAssignableFrom(o.getClass()));
                }
            }
        }
    }

    @Test public void testPrefixSearches() throws Exception {
        this.execute("select name from schemas where ucase(name) >= 'BAZ_BAR' and ucase(name) <= 'A'");
        //should be 0 rows rather than an exception
        assertRowCount(0);

        this.execute("select name from schemas where upper(name) like 'ab[_'");
        //should be 0 rows rather than an exception
        assertRowCount(0);
    }

    @Test public void testColumnsIn() throws Exception {
        this.internalConnection.close();
        this.internalConnection = server.createConnection("jdbc:teiid:test");
        this.execute("select tablename, name from sys.columns where tablename in ('t', 'T')");
        //should be 2, not 4 rows
        assertRowCount(2);

        this.execute("select tablename, name from sys.columns where upper(tablename) in ('t', 's')");
        assertRowCount(0);
    }

    @Test public void testViews() throws Exception {
        checkResult("testViews", "select Name, Body from sysadmin.Views order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testStoredProcedures() throws Exception {
        checkResult("testStoredProcedures", "select Name, Body from StoredProcedures order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testUsage() throws Exception {
        checkResult("testUsage", "select * from usage"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testFunctions() throws Exception {
        checkResult("testFunctions", "select * from functions"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testFunctionParameters() throws Exception {
        checkResult("testFunctionParams", "select * from FunctionParams"); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("TestSystemVirtualModel/testFunctionParams.expected")).contains("Missing message"));
    }

    @Test public void testTriggers() throws Exception {
        this.internalConnection.close();

        ModelMetaData mmd3 = new ModelMetaData();
        mmd3.setName("x");
        mmd3.setModelType(Type.PHYSICAL);
        mmd3.addSourceMapping("x", "x", null);
        mmd3.addSourceMetadata("DDL", "create foreign table t (intkey integer); "
                + " create trigger tr on t after insert as for each row begin select intkey from t where intkey = new.intkey; end; "
                + " create trigger tr1 on t after update as for each row begin select intkey from t where intkey = new.intkey + old.intkey; end;");
        HardCodedExecutionFactory ef = new HardCodedExecutionFactory() {
            @Override
            public boolean supportsCompareCriteriaEquals() {
                return true;
            }
        };
        ef.addData("SELECT t.intkey FROM t WHERE t.intkey = 1", new ArrayList<List<?>>());
        server.addTranslator("x", ef);
        server.deployVDB("test3", mmd3);

        this.internalConnection = server.createConnection("jdbc:teiid:test3");
        checkResult("testTriggers", "select * from sysadmin.triggers"); //$NON-NLS-1$ //$NON-NLS-2$
        closeStatement();

        //insert event
        ResultsFuture<?> future = server.getEventDistributor().dataModification("test3", "1", "x", "t", null, new Object[] {1}, null);
        future.get(2, TimeUnit.SECONDS);

        assertEquals("SELECT t.intkey FROM t WHERE t.intkey = 1", ef.getCommands().get(0).toString());

        ef.addData("SELECT t.intkey FROM t WHERE t.intkey = 3", new ArrayList<List<?>>());
        //update event
        future = server.getEventDistributor().dataModification("test3", "1", "x", "t", new Object[] {2}, new Object[] {1}, null);
        future.get(2, TimeUnit.SECONDS);

        assertEquals("SELECT t.intkey FROM t WHERE t.intkey = 3", ef.getCommands().get(1).toString());

        //delete event
        future = server.getEventDistributor().dataModification("test3", "1", "x", "t", new Object[] {2}, null, null);
        future.get(2, TimeUnit.SECONDS);

        //no trigger
        assertEquals(2, ef.getCommands().size());
    }

    @Test public void testSpatial() throws Exception {
        checkResult("testSpatial", "select * from spatial_ref_sys"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testGeometryColumns() throws Exception {
        this.internalConnection.close();
        this.internalConnection = server.createConnection("jdbc:teiid:test1");
        checkResult("testGeometryColumns", "select * from GEOMETRY_COLUMNS"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testArrayIterate() throws Exception {
        String sql = "select array_get(cast(x.col as string[]), 2) from (exec arrayiterate((('a', 'b'),('c','d')))) x"; //$NON-NLS-1$
        this.execute(sql);
        assertResults(new String[] {"expr1[string]", "b", "d"});

        sql = "select array_get(cast(x.col as string[]), 2) from (exec arrayiterate(null)) x"; //$NON-NLS-1$
        this.execute(sql);
        assertRowCount(0);

    }

    @Test public void testCloseOnCompletion() throws Exception {
        String sql = "values (1)"; //$NON-NLS-1$
        this.execute(sql);
        this.internalStatement.closeOnCompletion();
        this.internalResultSet.close();
        assertTrue(this.internalStatement.isClosed());

        sql = "values (1)"; //$NON-NLS-1$
        this.execute(sql);
        this.internalStatement.closeOnCompletion();
        try {
            this.internalStatement.execute(sql);
            fail();
        } catch (SQLException e) {
            //implicitly closed
        }
    }

    @Test public void testImplicitResolvingWithParameter() throws Exception {
        this.execute("create local temporary table #x (e1 string)");
        this.execute("insert into #x (e1) values (?)", "a");
    }

    @Test public void testCommandDeterministic() throws Exception {
        String sql = "select now(), a.* from sys.columns a"; //$NON-NLS-1$
        this.execute(sql);
        Timestamp ts = null;
        while (this.internalResultSet.next()) {
            if (ts == null) {
                ts = this.internalResultSet.getTimestamp(1);
            } else {
                assertEquals(ts, this.internalResultSet.getTimestamp(1));
            }
        }
    }

    @Test public void testRequests() throws Exception {
        checkResult("testRequests", "select VDBName, ExecutionId, Command, TransactionId from sysadmin.requests"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testTransactions() throws Exception {
        this.internalConnection.setAutoCommit(false);
        checkResult("testTransactions", "select TransactionID, Scope from sysadmin.transactions"); //$NON-NLS-1$ //$NON-NLS-2$
        this.internalConnection.setAutoCommit(true);
    }

    @Test public void testSessions() throws Exception {
        checkResult("testSessions", "select VDBName, UserName, ApplicationName, IPAddress from sysadmin.sessions"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testCancelRequest() throws Exception {
        CallableStatement cs = this.internalConnection.prepareCall("{? = call cancelRequest('abc', 1)}");
        cs.execute();
        assertEquals(cs.getBoolean(1), cs.getBoolean("cancelled"));
    }

    @Test public void testTerminateTransaction() throws Exception {
        execute("call terminateTransaction('abc')"); //$NON-NLS-1$
    }

    @Test public void testTerminateSession() throws Exception {
        CallableStatement cs = this.internalConnection.prepareCall("{? = call terminateSession('abc')}");
        cs.execute();
        assertEquals(cs.getBoolean(1), cs.getBoolean("terminated"));
    }

    @Test public void testSchemaSources() throws Exception {
        CallableStatement cs = this.internalConnection.prepareCall("call sysadmin.schemaSources(?)");
        cs.setString(1, "abc");
        cs.execute();
        ResultSet rs = cs.getResultSet();
        assertFalse(rs.next());
        cs.setString(1, "PartsSupplier");
        cs.execute();
        rs = cs.getResultSet();
        assertTrue(rs.next());
        assertEquals("source",rs.getString(1));
    }
}
