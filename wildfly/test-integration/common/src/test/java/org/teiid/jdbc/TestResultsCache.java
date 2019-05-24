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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Call;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.CacheDirective.Scope;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;


@SuppressWarnings("nls")
public class TestResultsCache {

    private Connection conn;
    private static FakeServer server;

    @BeforeClass public static void oneTimeSetup() throws Exception {
        server = new FakeServer(true);
        server.deployVDB("test", UnitTestUtil.getTestDataPath() + "/TestCase3473/test.vdb");
    }

    @AfterClass public static void oneTimeTeardown() {
        server.stop();
    }

    @Before public void setUp() throws Exception {
        conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @After public void teardown() throws SQLException {
        conn.close();
    }

    @Test public void testCacheHint() throws Exception {
        Statement s = conn.createStatement();
        s.execute("set showplan on");
        ResultSet rs = s.executeQuery("/* cache */ select 1");
        assertTrue(rs.next());
        s.execute("set noexec on");
        rs = s.executeQuery("/* cache */ select 1");
        assertTrue(rs.next());
        rs = s.executeQuery("select 1");
        assertFalse(rs.next());
    }

    @Test public void testCacheHintWithMaxRows() throws Exception {
        Statement s = conn.createStatement();
        s.setMaxRows(1);
        ResultSet rs = s.executeQuery("/* cache */ select 1 union all select 2");
        assertTrue(rs.next());
        assertFalse(rs.next());
        s.setMaxRows(2);
        rs = s.executeQuery("/* cache */ select 1 union all select 2");
        assertTrue(rs.next());
        assertTrue(rs.next());
    }

    @Test public void testCacheHintTtl() throws Exception {
        Statement s = conn.createStatement();
        s.execute("set showplan on");
        ResultSet rs = s.executeQuery("/*+ cache(ttl:50) */ select 1");
        assertTrue(rs.next());
        s.execute("set noexec on");
        Thread.sleep(60);
        rs = s.executeQuery("/*+ cache(ttl:50) */ select 1");
        assertFalse(rs.next());
    }

    @Test public void testExecutionProperty() throws Exception {
        Statement s = conn.createStatement();
        s.execute("set showplan on");
        s.execute("set resultSetCacheMode true");
        ResultSet rs = s.executeQuery("select 1");
        assertTrue(rs.next());
        s.execute("set noexec on");
        rs = s.executeQuery("select 1");
        assertTrue(rs.next());
        s.execute("set resultSetCacheMode false");
        rs = s.executeQuery("select 1");
        assertFalse(rs.next());
    }

    @Test public void testCacheHintWithLargeSQLXML() throws Exception {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("/* cache */ WITH t(n) AS ( VALUES (1) UNION ALL SELECT n+1 FROM t WHERE n < 10000 ) SELECT xmlelement(root, xmlagg(xmlelement(val, n))) FROM t");
        assertTrue(rs.next());
        assertEquals(148907, rs.getString(1).length());
        assertFalse(rs.next());
        rs.close();
        rs = s.executeQuery("/* cache */ WITH t(n) AS ( VALUES (1) UNION ALL SELECT n+1 FROM t WHERE n < 10000 ) SELECT xmlelement(root, xmlagg(xmlelement(val, n))) FROM t");
        assertTrue(rs.next());
        assertEquals(148907, rs.getString(1).length());
        assertFalse(rs.next());
    }

    @Test public void testScope() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("x");
        mmd.addProperty("teiid_rel:determinism", "USER_DETERMINISTIC");
        mmd.addSourceMapping("x", "x", null);
        mmd.addSourceMetadata("ddl", "create foreign table t (c string); create foreign procedure p () returns table (c string);");
        final AtomicBoolean setScope = new AtomicBoolean();
        server.addTranslator("x", new ExecutionFactory() {
            @Override
            public boolean isSourceRequired() {
                return false;
            }

            @Override
            public ResultSetExecution createResultSetExecution(
                    QueryExpression command, final ExecutionContext executionContext,
                    RuntimeMetadata metadata, Object connection)
                    throws TranslatorException {
                return createProcedureExecution(null, executionContext, metadata, connection);
            }

            @Override
            public ProcedureExecution createProcedureExecution(Call command,
                    final ExecutionContext executionContext,
                    RuntimeMetadata metadata, Object connection)
                    throws TranslatorException {
                return new ProcedureExecution() {

                    boolean returned = false;

                    @Override
                    public void execute() throws TranslatorException {

                    }

                    @Override
                    public void close() {

                    }

                    @Override
                    public void cancel() throws TranslatorException {

                    }

                    @Override
                    public List<?> next() throws TranslatorException, DataNotAvailableException {
                        if (setScope.get()) {
                            executionContext.setScope(Scope.SESSION); //prevent caching altogether
                        }
                        if (returned) {
                            return null;
                        }
                        returned = true;
                        return Arrays.asList(executionContext.getSession().getSessionId());
                    }

                    @Override
                    public List<?> getOutputParameterValues()
                            throws TranslatorException {
                        return null;
                    }
                };
            }
        });
        server.deployVDB("x", mmd);
        Connection c = server.getDriver().connect("jdbc:teiid:x;user=alice", null);
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("/* cache */ select * from t");
        assertTrue(rs.next());
        String sessionid = rs.getString(1);

        //should be the same with same user/session
        rs = s.executeQuery("/* cache */ select * from t");
        assertTrue(rs.next());
        assertEquals(sessionid, rs.getString(1));
        c.close();

        c = server.getDriver().connect("jdbc:teiid:x;user=alice", null);
        s = c.createStatement();
        rs = s.executeQuery("/* cache */ select * from t");
        assertTrue(rs.next());
        assertEquals(sessionid, rs.getString(1));
        c.close();

        //for the final test
        setScope.set(true);

        //should be different with another user
        c = server.getDriver().connect("jdbc:teiid:x;user=bill", null);
        s = c.createStatement();
        rs = s.executeQuery("/* cache */ select * from t");
        assertTrue(rs.next());
        String sessionid1 = rs.getString(1);
        c.close();

        assertNotEquals(sessionid, sessionid1);

        c = server.getDriver().connect("jdbc:teiid:x;user=bill", null);
        s = c.createStatement();
        rs = s.executeQuery("/* cache */ select * from t");
        assertTrue(rs.next());

        //scope session should prevent reuse
        assertNotEquals(sessionid1, rs.getString(1));

        setScope.set(false);

        rs = s.executeQuery("/* cache */ call p()");
        assertTrue(rs.next());
        sessionid = rs.getString(1);
        c.close();

        c = server.getDriver().connect("jdbc:teiid:x;user=alice", null);
        s = c.createStatement();
        rs = s.executeQuery("/* cache */ call p()");
        assertTrue(rs.next());
        assertNotEquals(sessionid, rs.getString(1));
        c.close();
    }

}
