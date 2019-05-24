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

import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.client.util.ResultsFuture;
import org.teiid.jdbc.AsynchPositioningException;
import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.ContinuousStatementCallback;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.RequestOptions;
import org.teiid.jdbc.StatementCallback;
import org.teiid.jdbc.TeiidResultSet;
import org.teiid.jdbc.TeiidStatement;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.runtime.HardCodedExecutionFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestAsynch {

    private static FakeServer server;
    private ConnectionImpl internalConnection;
    private static HardCodedExecutionFactory ef;
    private static List<String> partIds = Collections.synchronizedList(new ArrayList<String>());

    @BeforeClass public static void oneTimeSetup() throws Exception {
        server = new FakeServer(true);
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("v");
        mmd.setModelType(Type.PHYSICAL);
        mmd.setSchemaSourceType("ddl");
        mmd.addSourceMapping("z", "z", null);
        mmd.setSchemaText("create view test (col integer) as select 1; create foreign table someTable (col integer);");
        ef = new HardCodedExecutionFactory() {
            @Override
            public ResultSetExecution createResultSetExecution(
                    QueryExpression command, ExecutionContext executionContext,
                    RuntimeMetadata metadata, Object connection)
                    throws TranslatorException {
                partIds.add(executionContext.getPartIdentifier());
                return super.createResultSetExecution(command, executionContext, metadata,
                        connection);
            }
        };
        server.addTranslator("z", ef);
        server.deployVDB("x", mmd);
    }

    @After public void teardown() {
        partIds.clear();
    }

    @AfterClass public static void oneTimeTeardown() throws Exception {
        server.stop();
    }

    @Before public void setUp() throws Exception {
        this.internalConnection = server.createConnection("jdbc:teiid:x"); //$NON-NLS-1$ //$NON-NLS-2$
       }

    @Test public void testAsynch() throws Exception {
        Statement stmt = this.internalConnection.createStatement();
        TeiidStatement ts = stmt.unwrap(TeiidStatement.class);
        final ResultsFuture<Integer> result = new ResultsFuture<Integer>();
        ts.submitExecute("select * from sys.tables a, sys.tables b, sys.tables c", new StatementCallback() {
            int rowCount;
            @Override
            public void onRow(Statement s, ResultSet rs) {
                rowCount++;
                try {
                    if (!rs.isLast()) {
                        assertTrue(rs.unwrap(TeiidResultSet.class).available() > 0);
                    }
                    if (rowCount == 10000) {
                        s.close();
                    }
                } catch (AsynchPositioningException e) {
                    try {
                        assertEquals(0, rs.unwrap(TeiidResultSet.class).available());
                    } catch (SQLException e1) {
                        result.getResultsReceiver().exceptionOccurred(e1);
                    }
                } catch (SQLException e) {
                    result.getResultsReceiver().exceptionOccurred(e);
                }
            }

            @Override
            public void onException(Statement s, Exception e) {
                result.getResultsReceiver().exceptionOccurred(e);
            }

            @Override
            public void onComplete(Statement s) {
                result.getResultsReceiver().receiveResults(rowCount);
            }
        }, new RequestOptions());
        assertEquals(10000, result.get().intValue());
    }

    @Test public void testAsynchContinuousEmpty() throws Exception {
        Statement stmt = this.internalConnection.createStatement();
        TeiidStatement ts = stmt.unwrap(TeiidStatement.class);
        final ResultsFuture<Integer> result = new ResultsFuture<Integer>();
        ts.submitExecute("select * from SYS.Schemas where 1 = 0", new ContinuousStatementCallback() {

            int execCount;
            @Override
            public void onRow(Statement s, ResultSet rs) throws SQLException {
                fail();
            }

            @Override
            public void onException(Statement s, Exception e) {
                result.getResultsReceiver().exceptionOccurred(e);
            }

            @Override
            public void onComplete(Statement s) {
                result.getResultsReceiver().receiveResults(execCount);
            }

            @Override
            public void beforeNextExecution(Statement s) throws SQLException {
                execCount++;
                assertEquals(-1, s.getResultSet().unwrap(TeiidResultSet.class).available());
                if (execCount == 1024) {
                    s.close();
                }
            }
        }, new RequestOptions().continuous(true));
        assertEquals(1024, result.get().intValue());
    }

    @Test public void testAsynchContinuousNonEmpty() throws Exception {
        Statement stmt = this.internalConnection.createStatement();
        TeiidStatement ts = stmt.unwrap(TeiidStatement.class);
        final ResultsFuture<Integer> result = new ResultsFuture<Integer>();
        ts.submitExecute("select 1", new ContinuousStatementCallback() {

            int execCount;
            @Override
            public void onRow(Statement s, ResultSet rs) throws SQLException {
                assertEquals(0, rs.unwrap(TeiidResultSet.class).available());
                s.close();
            }

            @Override
            public void onException(Statement s, Exception e) {
                result.getResultsReceiver().exceptionOccurred(e);
            }

            @Override
            public void onComplete(Statement s) {
                result.getResultsReceiver().receiveResults(execCount);
            }

            @Override
            public void beforeNextExecution(Statement s) throws SQLException {
                execCount++;
            }
        }, new RequestOptions().continuous(true));
        assertEquals(0, result.get().intValue());
    }

    @Test public void testAsynchContinuous() throws Exception {
        Statement stmt = this.internalConnection.createStatement();
        TeiidStatement ts = stmt.unwrap(TeiidStatement.class);
        final ResultsFuture<Integer> result = new ResultsFuture<Integer>();
        ts.submitExecute("select xmlelement(name x) from SYS.Schemas", new StatementCallback() {
            int rowCount;
            @Override
            public void onRow(Statement s, ResultSet rs) throws SQLException {
                rowCount++;
                if (rowCount == 1024) {
                    s.close();
                }
            }

            @Override
            public void onException(Statement s, Exception e) {
                result.getResultsReceiver().exceptionOccurred(e);
            }

            @Override
            public void onComplete(Statement s) {
                result.getResultsReceiver().receiveResults(rowCount);
            }
        }, new RequestOptions().continuous(true));
        assertEquals(1024, result.get().intValue());
    }

    @Test public void testAsynchContinuousMergeBlock() throws Exception {
        Statement stmt = this.internalConnection.createStatement();
        stmt.execute("create temporary table t (c string, primary key (c))");
        stmt.execute("set autoCommitTxn off");
        TeiidStatement ts = stmt.unwrap(TeiidStatement.class);
        final ResultsFuture<Integer> result = new ResultsFuture<Integer>();
        ts.submitExecute("begin merge into t select name from schemas limit 2; select rowcount; end", new StatementCallback() {
            int rowCount;
            @Override
            public void onRow(Statement s, ResultSet rs) throws SQLException {
                rowCount++;
                if (rowCount == 10) {
                    s.close();
                }
            }

            @Override
            public void onException(Statement s, Exception e) {
                result.getResultsReceiver().exceptionOccurred(e);
            }

            @Override
            public void onComplete(Statement s) {
                result.getResultsReceiver().receiveResults(rowCount);
            }
        }, new RequestOptions().continuous(true));
        assertEquals(10, result.get().intValue());
        stmt = this.internalConnection.createStatement();
        ResultSet rs = stmt.executeQuery("select count(*) from t");
        rs.next();
        assertEquals(2, rs.getInt(1));
    }

    @Test public void testAsynchContinuousWithAlter() throws Exception {
        Statement stmt = this.internalConnection.createStatement();
        TeiidStatement ts = stmt.unwrap(TeiidStatement.class);
        final ResultsFuture<Integer> result = new ResultsFuture<Integer>();
        ts.submitExecute("select * from test", new StatementCallback() {
            int rowCount;
            @Override
            public void onRow(Statement s, ResultSet rs) {
                try {
                    rowCount++;
                    if (rowCount < 3) {
                        assertEquals(1, rs.getInt(1));
                        if (rowCount == 2) {
                            Statement st = internalConnection.createStatement();
                            st.execute("alter view v.test as select 2");
                            st.close();
                            try {
                                Thread.sleep(100); //we only track down to millisecond resolution
                            } catch (InterruptedException e) {
                                result.getResultsReceiver().exceptionOccurred(e);
                            }
                        }
                    } else {
                        assertEquals(2, rs.getInt(1)); //new value
                        s.close();
                    }
                } catch (SQLException e) {
                    result.getResultsReceiver().exceptionOccurred(e);
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onException(Statement s, Exception e) {
                result.getResultsReceiver().exceptionOccurred(e);
            }

            @Override
            public void onComplete(Statement s) {
                result.getResultsReceiver().receiveResults(rowCount);
            }
        }, new RequestOptions().continuous(true));
        assertEquals(3, result.get().intValue());
    }

    @Test public void testAsynchPlaning() throws Exception {
        Statement stmt = this.internalConnection.createStatement();
        TeiidStatement ts = stmt.unwrap(TeiidStatement.class);
        ef.addData("SELECT someTable.col FROM someTable", Arrays.asList(Arrays.asList(1)));
        final ResultsFuture<Integer> result = new ResultsFuture<Integer>();
        ts.submitExecute("select * from someTable", new StatementCallback() {
            int rowCount;
            @Override
            public void onRow(Statement s, ResultSet rs) {
                try {
                    rowCount++;
                    if (rowCount == 3) {
                        s.close();
                    }
                } catch (SQLException e) {
                    result.getResultsReceiver().exceptionOccurred(e);
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onException(Statement s, Exception e) {
                result.getResultsReceiver().exceptionOccurred(e);
            }

            @Override
            public void onComplete(Statement s) {
                result.getResultsReceiver().receiveResults(rowCount);
            }
        }, new RequestOptions().continuous(true));
        assertEquals(3, result.get().intValue());
        assertEquals(3, partIds.size());
        assertEquals(partIds.get(0), partIds.get(1));
        assertEquals(partIds.get(1), partIds.get(2));
    }

    /**
     * Runs sufficient iterations to ensure the temptablestore state is correct
     * @throws Exception
     */
    @Test public void testAsynchAnon() throws Exception {
        String query = "begin\n" +
                "insert into #mom_collectors select 'a' as hostname, 123 as port, 1 as id from sometable;\n" +
                "insert into #apm_collectors select 'a' as hostname, 124 as port, 12 as id from sometable;\n"+
                "select 'add', hostname, port, id from #mom_collectors\n"+
                "where (hostname, port) not in (select (hostname, port) from #apm_collectors)\n"+
                "union select 'delete', hostname, port, id from #apm_collectors\n"+
                "where (hostname, port) not in (select (hostname, port) from #mom_collectors) with return; end";
        Statement stmt = this.internalConnection.createStatement();
        TeiidStatement ts = stmt.unwrap(TeiidStatement.class);
        ef.addData("SELECT someTable.col FROM someTable", Collections.nCopies(1, Arrays.asList(1)));
        ts.setExecutionProperty("autoCommitTxn", "off");
        final ResultsFuture<Integer> result = new ResultsFuture<Integer>();
        ts.submitExecute(query, new StatementCallback() {
            int rowCount;
            @Override
            public void onRow(Statement s, ResultSet rs) {
                try {
                    rowCount++;
                    if (rowCount == 100000) {
                        s.close();
                    }
                } catch (SQLException e) {
                    result.getResultsReceiver().exceptionOccurred(e);
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onException(Statement s, Exception e) {
                result.getResultsReceiver().exceptionOccurred(e);
            }

            @Override
            public void onComplete(Statement s) {
                result.getResultsReceiver().receiveResults(rowCount);
            }
        }, new RequestOptions().continuous(true));
        assertEquals(100000, result.get().intValue());
    }

    @Test public void testTransactionCycle() throws Exception {
        Statement stmt = this.internalConnection.createStatement();
        TeiidStatement ts = stmt.unwrap(TeiidStatement.class);
        final ResultsFuture<Void> result = new ResultsFuture<Void>();
        ts.submitExecute("start transaction", new StatementCallback() {
            @Override
            public void onRow(Statement s, ResultSet rs) {
            }

            @Override
            public void onException(Statement s, Exception e) {
                result.getResultsReceiver().exceptionOccurred(e);
            }

            @Override
            public void onComplete(Statement s) {
                result.getResultsReceiver().receiveResults(null);
            }
        }, new RequestOptions());
        result.get();
        final ResultsFuture<Void> rollBackResult = new ResultsFuture<Void>();
        ts.submitExecute("rollback", new StatementCallback() {
            @Override
            public void onRow(Statement s, ResultSet rs) {
            }

            @Override
            public void onException(Statement s, Exception e) {
                rollBackResult.getResultsReceiver().exceptionOccurred(e);
            }

            @Override
            public void onComplete(Statement s) {
                rollBackResult.getResultsReceiver().receiveResults(null);
            }
        }, new RequestOptions());
        rollBackResult.get();
    }

}
