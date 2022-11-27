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
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.teiid.client.DQP;
import org.teiid.client.RequestMessage;
import org.teiid.client.ResultsMessage;
import org.teiid.client.util.ResultsFuture;
import org.teiid.net.ServerConnection;

@SuppressWarnings("nls")
public class TestStatement {

    @Test public void testBatchExecution() throws Exception {
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        Mockito.stub(conn.getConnectionProps()).toReturn(new Properties());
        DQP dqp = Mockito.mock(DQP.class);
        ResultsFuture<ResultsMessage> results = new ResultsFuture<ResultsMessage>();
        Mockito.stub(dqp.executeRequest(Mockito.anyLong(), (RequestMessage)Mockito.anyObject())).toReturn(results);
        ResultsMessage rm = new ResultsMessage();
        rm.setResults(new List<?>[] {Arrays.asList(1), Arrays.asList(2)});
        rm.setUpdateResult(true);
        results.getResultsReceiver().receiveResults(rm);
        Mockito.stub(conn.getDQP()).toReturn(dqp);
        StatementImpl statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.clearBatch(); //previously caused npe
        statement.addBatch("delete from table"); //$NON-NLS-1$
        statement.addBatch("delete from table1"); //$NON-NLS-1$
        assertTrue(Arrays.equals(new int[] {1, 2}, statement.executeBatch()));
    }

    @Test public void testWarnings() throws Exception {
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        Mockito.stub(conn.getConnectionProps()).toReturn(new Properties());
        DQP dqp = Mockito.mock(DQP.class);
        ResultsFuture<ResultsMessage> results = new ResultsFuture<ResultsMessage>();
        Mockito.stub(dqp.executeRequest(Mockito.anyLong(), (RequestMessage)Mockito.anyObject())).toReturn(results);
        ResultsMessage rm = new ResultsMessage();
        rm.setResults(new List<?>[] {Arrays.asList(1)});
        rm.setWarnings(Arrays.asList(new Throwable()));
        rm.setColumnNames(new String[] {"expr1"});
        rm.setDataTypes(new String[] {"string"});
        results.getResultsReceiver().receiveResults(rm);
        Mockito.stub(conn.getDQP()).toReturn(dqp);
        StatementImpl statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY) {
            @Override
            protected java.util.TimeZone getServerTimeZone() throws java.sql.SQLException {
                return null;
            }
        };
        statement.execute("select 'a'");
        assertNotNull(statement.getResultSet());
        SQLWarning warning = statement.getWarnings();
        assertNotNull(warning);
        assertNull(warning.getNextWarning());
    }

    @Test public void testGetMoreResults() throws Exception {
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        Mockito.stub(conn.getConnectionProps()).toReturn(new Properties());
        DQP dqp = Mockito.mock(DQP.class);
        ResultsFuture<ResultsMessage> results = new ResultsFuture<ResultsMessage>();
        Mockito.stub(dqp.executeRequest(Mockito.anyLong(), (RequestMessage)Mockito.anyObject())).toReturn(results);
        ResultsMessage rm = new ResultsMessage();
        rm.setUpdateResult(true);
        rm.setColumnNames(new String[] {"expr1"});
        rm.setDataTypes(new String[] {"integer"});
        rm.setResults(new List<?>[] {Arrays.asList(1)});
        results.getResultsReceiver().receiveResults(rm);
        Mockito.stub(conn.getDQP()).toReturn(dqp);
        StatementImpl statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY) {
            @Override
            protected java.util.TimeZone getServerTimeZone() throws java.sql.SQLException {
                return null;
            }
        };
        statement.execute("update x set a = b");
        assertEquals(1, statement.getUpdateCount());
        statement.getMoreResults(Statement.CLOSE_ALL_RESULTS);
        assertEquals(-1, statement.getUpdateCount());

        statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY) {
            @Override
            protected java.util.TimeZone getServerTimeZone() throws java.sql.SQLException {
                return null;
            }
        };
        statement.execute("update x set a = b");
        assertEquals(1, statement.getUpdateCount());
        statement.getMoreResults();
        assertEquals(-1, statement.getUpdateCount());
    }

    @Test public void testSetStatement() throws Exception {
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        StatementImpl statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertFalse(statement.execute("set foo bar")); //$NON-NLS-1$
        Mockito.verify(conn).setExecutionProperty("foo", "bar");

        assertFalse(statement.execute("set foo 'b''ar' ; ")); //$NON-NLS-1$
        Mockito.verify(conn).setExecutionProperty("foo", "b'ar");

        assertFalse(statement.execute("set \"foo\" 'b''a1r' ; ")); //$NON-NLS-1$
        Mockito.verify(conn).setExecutionProperty("foo", "b'a1r");

        assertFalse(statement.execute("set \"foo\" = 'bar'; ")); //$NON-NLS-1$
        Mockito.verify(conn).setExecutionProperty("foo", "bar");
    }

    @Test public void testSetPayloadStatement() throws Exception {
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        Properties p = new Properties();
        Mockito.stub(conn.getExecutionProperties()).toReturn(p);
        StatementImpl statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertFalse(statement.execute("set payload foo bar")); //$NON-NLS-1$
    }

    @Test public void testSetAuthorizationStatement() throws Exception {
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        Properties p = new Properties();
        Mockito.stub(conn.getExecutionProperties()).toReturn(p);
        StatementImpl statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertFalse(statement.execute("set session authorization bar")); //$NON-NLS-1$
        Mockito.verify(conn).changeUser("bar", null);
    }

    @Test public void testPropertiesOverride() throws Exception {
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        Properties p = new Properties();
        p.setProperty(ExecutionProperties.ANSI_QUOTED_IDENTIFIERS, Boolean.TRUE.toString());
        Mockito.stub(conn.getExecutionProperties()).toReturn(p);
        StatementImpl statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertEquals(Boolean.TRUE.toString(), statement.getExecutionProperty(ExecutionProperties.ANSI_QUOTED_IDENTIFIERS));
        statement.setExecutionProperty(ExecutionProperties.ANSI_QUOTED_IDENTIFIERS, Boolean.FALSE.toString());
        assertEquals(Boolean.FALSE.toString(), statement.getExecutionProperty(ExecutionProperties.ANSI_QUOTED_IDENTIFIERS));
        assertEquals(Boolean.TRUE.toString(), p.getProperty(ExecutionProperties.ANSI_QUOTED_IDENTIFIERS));
    }

    @Test public void testTransactionStatements() throws Exception {
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        Properties p = new Properties();
        Mockito.stub(conn.getExecutionProperties()).toReturn(p);
        StatementImpl statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertFalse(statement.execute("start transaction")); //$NON-NLS-1$
        Mockito.verify(conn).setAutoCommit(false);
        assertFalse(statement.execute("commit")); //$NON-NLS-1$
        Mockito.verify(conn).setAutoCommit(true);
        assertFalse(statement.execute("start transaction")); //$NON-NLS-1$
        assertFalse(statement.execute("rollback")); //$NON-NLS-1$
        Mockito.verify(conn).rollback(false);
        assertFalse(statement.execute("abort transaction")); //$NON-NLS-1$
        Mockito.verify(conn, Mockito.times(2)).rollback(false);
        assertFalse(statement.execute("rollback work")); //$NON-NLS-1$
        Mockito.verify(conn, Mockito.times(3)).rollback(false);

        assertFalse(statement.execute("start transaction isolation level repeatable read")); //$NON-NLS-1$
        Mockito.verify(conn).setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    }

    @Test public void testDisableLocalTransations() throws Exception {
        ServerConnection mock = Mockito.mock(ServerConnection.class);
        DQP dqp = Mockito.mock(DQP.class);
        Mockito.stub(mock.getService(DQP.class)).toReturn(dqp);
        ConnectionImpl conn = new ConnectionImpl(mock, new Properties(), "x");
        StatementImpl statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertTrue(conn.getAutoCommit());
        statement.execute("set disablelocaltxn true");
        assertFalse(statement.execute("start transaction")); //$NON-NLS-1$
        conn.beginLocalTxnIfNeeded();
        assertFalse(conn.isInLocalTxn());

        statement.execute("set disablelocaltxn false");
        assertFalse(statement.execute("start transaction")); //$NON-NLS-1$
        conn.beginLocalTxnIfNeeded();
        assertTrue(conn.isInLocalTxn());
    }

    @SuppressWarnings("unchecked")
    @Test public void testTransactionStatementsAsynch() throws Exception {
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        Mockito.stub(conn.submitSetAutoCommitTrue(Mockito.anyBoolean())).toReturn((ResultsFuture)ResultsFuture.NULL_FUTURE);
        Properties p = new Properties();
        Mockito.stub(conn.getExecutionProperties()).toReturn(p);
        StatementImpl statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.submitExecute("start transaction", null); //$NON-NLS-1$
        Mockito.verify(conn).setAutoCommit(false);
        statement.submitExecute("commit", null); //$NON-NLS-1$
        Mockito.verify(conn).submitSetAutoCommitTrue(true);
        statement.submitExecute("start transaction", null); //$NON-NLS-1$
        statement.submitExecute("rollback", null); //$NON-NLS-1$
        Mockito.verify(conn).submitSetAutoCommitTrue(false);
    }

    @Test public void testAsynchTimeout() throws Exception {
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        Mockito.stub(conn.getConnectionProps()).toReturn(new Properties());
        final StatementImpl statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setQueryTimeoutMS(1);
        DQP dqp = Mockito.mock(DQP.class);
        Mockito.stub(statement.getDQP()).toReturn(dqp);
        final AtomicInteger counter = new AtomicInteger();
        Mockito.stub(dqp.cancelRequest(0)).toAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                synchronized (statement) {
                    counter.incrementAndGet();
                    statement.notifyAll();
                }
                return true;
            }
        });
        ResultsFuture<ResultsMessage> future = new ResultsFuture<ResultsMessage>();
        Mockito.stub(dqp.executeRequest(Mockito.anyLong(), (RequestMessage) Mockito.anyObject())).toReturn(future);
        statement.submitExecute("select 'hello world'", null);
        synchronized (statement) {
            while (counter.get() != 1) {
                statement.wait();
            }
        }
        statement.setQueryTimeoutMS(1);
        statement.submitExecute("select 'hello world'", null);
        synchronized (statement) {
            while (counter.get() != 2) {
                statement.wait();
            }
        }
    }

    @Test public void testTimeoutProperty() throws Exception {
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        Properties p = new Properties();
        p.setProperty(ExecutionProperties.QUERYTIMEOUT, "2");
        Mockito.stub(conn.getExecutionProperties()).toReturn(p);
        StatementImpl statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertEquals(2, statement.getQueryTimeout());
    }

    @Test public void testUseJDBC4ColumnNameAndLabelSemantics() throws Exception {
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        Properties p = new Properties();
        p.setProperty(ExecutionProperties.JDBC4COLUMNNAMEANDLABELSEMANTICS, "false");

        Mockito.stub(conn.getExecutionProperties()).toReturn(p);
        StatementImpl statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertEquals(Boolean.FALSE.toString(), statement.getExecutionProperty(ExecutionProperties.JDBC4COLUMNNAMEANDLABELSEMANTICS));

    }

    @Test public void testSet() {
        Matcher m = StatementImpl.SET_STATEMENT.matcher("set foo to 1");
        assertTrue(m.matches());
    }

    @Test public void testQuotedSet() {
        Matcher m = StatementImpl.SET_STATEMENT.matcher("set \"foo\"\"\" to 1");
        assertTrue(m.matches());
        assertEquals("\"foo\"\"\"", m.group(2));
        m = StatementImpl.SHOW_STATEMENT.matcher("show \"foo\"");
        assertTrue(m.matches());
    }

    @Test public void testSetTxnIsolationLevel() throws SQLException {
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        StatementImpl statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        assertFalse(statement.execute("set session characteristics as transaction isolation level read committed")); //$NON-NLS-1$
        Mockito.verify(conn).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        assertFalse(statement.execute("set session characteristics as transaction isolation level read uncommitted")); //$NON-NLS-1$
        Mockito.verify(conn).setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        assertFalse(statement.execute("set session characteristics as transaction isolation level serializable")); //$NON-NLS-1$
        Mockito.verify(conn).setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        assertFalse(statement.execute("set session characteristics as transaction isolation level repeatable read")); //$NON-NLS-1$
        Mockito.verify(conn).setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    }

    @Test public void testShowTxnIsolationLevel() throws SQLException {
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        StatementImpl statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY) {
            @Override
            protected TimeZone getServerTimeZone() throws SQLException {
                return TimeZone.getDefault();
            }
        };
        Mockito.stub(conn.getTransactionIsolation()).toReturn(Connection.TRANSACTION_READ_COMMITTED);
        assertTrue(statement.execute("show transaction isolation level")); //$NON-NLS-1$
        ResultSet rs = statement.getResultSet();
        rs.next();
        assertEquals("READ COMMITTED", rs.getString(1));
        assertFalse(rs.next());
    }

}
