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

import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.teiid.client.DQP;
import org.teiid.client.RequestMessage;
import org.teiid.client.RequestMessage.ResultsMode;
import org.teiid.client.ResultsMessage;
import org.teiid.client.security.LogonResult;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.TeiidException;
import org.teiid.net.ServerConnection;


/**
 * Test case to validate general operations on an <code>MMPreparedStatement
 * </code>
 */
public class TestPreparedStatement {

    /**
     * Verify that the <code>executeBatch()</code> method of <code>
     * MMPreparedStatement</code> is resulting in the correct command,
     * parameter values for each command of the batch, and the request type
     * are being set in the request message that would normally be sent to the
     * server.
     *
     * @throws Exception
     */
    @Test public void testBatchedUpdateExecution() throws Exception {
        // Build up a fake connection instance for use with the prepared statement
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        Mockito.stub(conn.getConnectionProps()).toReturn(new Properties());
        DQP dqp = Mockito.mock(DQP.class);
        ServerConnection serverConn = Mockito.mock(ServerConnection.class);
        LogonResult logonResult = Mockito.mock(LogonResult.class);

        // stub methods
        Mockito.stub(conn.getServerConnection()).toReturn(serverConn);
        Mockito.stub(serverConn.getLogonResult()).toReturn(logonResult);
        Mockito.stub(logonResult.getTimeZone()).toReturn(TimeZone.getDefault());

        // a dummy result message that is specific to this test case
        final ResultsFuture<ResultsMessage> results = new ResultsFuture<ResultsMessage>();
        final int[] count = new int[1];
        final ResultsMessage rm = new ResultsMessage();
        Mockito.stub(dqp.executeRequest(Matchers.anyLong(), (RequestMessage)Matchers.anyObject())).toAnswer(new Answer<ResultsFuture<ResultsMessage>>() {
            @Override
            public ResultsFuture<ResultsMessage> answer(InvocationOnMock invocation)
                    throws Throwable {
                RequestMessage requestMessage = (RequestMessage)invocation.getArguments()[1];
                count[0] += requestMessage.getParameterValues().size();
                if (count[0] == 100000) {
                    rm.setException(new TeiidException());
                    rm.setResults(new List<?>[] {Arrays.asList(Statement.EXECUTE_FAILED)});
                } else {
                    List<?>[] vals = new List<?>[requestMessage.getParameterValues().size()];
                    Arrays.fill(vals, Arrays.asList(0));
                    rm.setResults(Arrays.asList(vals));
                }
                return results;
            }});
        rm.setUpdateResult(true);
        results.getResultsReceiver().receiveResults(rm);
        Mockito.stub(conn.getDQP()).toReturn(dqp);

        // some update SQL
        String sqlCommand = "delete from table where col=?"; //$NON-NLS-1$
        TestableMMPreparedStatement statement = (TestableMMPreparedStatement) getMMPreparedStatement(conn, sqlCommand);

        ArrayList<ArrayList<Object>> expectedParameterValues = new ArrayList<ArrayList<Object>>(3);
        // Add some batches and their parameter values
        expectedParameterValues.add( new ArrayList<Object>( Arrays.asList( new Object[] { new Integer(1) } ) ) );
        statement.setInt(1, new Integer(1));
        statement.addBatch();
        expectedParameterValues.add( new ArrayList<Object>( Arrays.asList( new Object[] { new Integer(2) } ) ) );
        statement.setInt(1, new Integer(2));
        statement.addBatch();
        expectedParameterValues.add( new ArrayList<Object>( Arrays.asList( new Object[] { new Integer(3) } ) ) );
        statement.setInt(1, new Integer(3));
        statement.addBatch();

        // execute the batch and verify that it matches our dummy results
        // message set earlier
        assertTrue(Arrays.equals(new int[] {0, 0, 0}, statement.executeBatch()));

        // Now verify the statement's RequestMessage is what we expect
        assertEquals("Command does not match", sqlCommand, statement.requestMessage.getCommandString()); //$NON-NLS-1$
        assertEquals("Parameter values do not match", expectedParameterValues, statement.requestMessage.getParameterValues()); //$NON-NLS-1$
        assertTrue("RequestMessage.isBatchedUpdate should be true", statement.requestMessage.isBatchedUpdate()); //$NON-NLS-1$
        assertFalse("RequestMessage.isCallableStatement should be false", statement.requestMessage.isCallableStatement()); //$NON-NLS-1$
        assertTrue("RequestMessage.isPreparedStatement should be true", statement.requestMessage.isPreparedStatement()); //$NON-NLS-1$

        count[0] = 0;
        //large batch handling - should split into 5
        for (int i = 0; i < 100000; i++) {
            statement.setInt(1, new Integer(1));
            statement.addBatch();
        }
        try {
            statement.executeBatch();
            fail();
        } catch (BatchUpdateException e) {
            assertEquals(100000, count[0]);
            assertEquals(95309, e.getUpdateCounts().length);
            assertEquals(Statement.EXECUTE_FAILED, e.getUpdateCounts()[95308]);
        }
    }

    /**
     * Verify that the <code>clearBatch()</code> method of
     * <code>MMPreparedStatement</code> is clearing the list of batched
     * commands.
     * <p>
     * This is done by first adding command parameter values to the batch and
     * then invoking the <code>clearBatch()</code> method.
     *
     * @throws Exception
     */
    @Test public void testClearBatch() throws Exception {
        PreparedStatementImpl statement = getMMPreparedStatement("delete from table where col=?"); //$NON-NLS-1$
        // Add some stuff
        statement.setInt(1, new Integer(1));
        statement.addBatch();
        statement.setInt(1, new Integer(2));
        statement.addBatch();
        // Make sure something is really there
        assertTrue("MMPreparedStatement.ParameterValuesList should not be empty", statement.getParameterValuesList().size() > 0); //$NON-NLS-1$
        // Now clear it
        statement.clearBatch();
        assertTrue("MMPreparedStatement.ParameterValuesList should be empty", statement.getParameterValuesList().size() == 0); //$NON-NLS-1$
    }

    /**
     * Adds additional batches of command parameter values to a prepared
     * statement after a previous list has been cleared.
     * <p>
     * This is done by first adding command parameter values to the batch and
     * then invoking the <code>clearBatch()</code> method.  Then a different
     * set of command parameter values are added to the existing batch command.
     * <p>
     * The expected result is the command parameter list for the batches will
     * only reflect what was added after <code>clearBatch()</code> was invoked.
     *
     * @throws Exception
     */
    @Test public void testClearBatchAddBatch() throws Exception {
        PreparedStatementImpl statement = getMMPreparedStatement("delete from table where col=?"); //$NON-NLS-1$

        statement.setInt(1, new Integer(1));
        statement.addBatch();
        statement.setInt(1, new Integer(2));
        statement.addBatch();
        // Make sure something is really there
        assertTrue("MMPreparedStatement.ParameterValuesList should not be empty", statement.getParameterValuesList().size() > 0); //$NON-NLS-1$
        // Now clear it
        statement.clearBatch();
        // Make sure it is empty now
        assertTrue("MMPreparedStatement.ParameterValuesList should be empty", statement.getParameterValuesList().size() == 0); //$NON-NLS-1$

        ArrayList<ArrayList<Object>> expectedParameterValues = new ArrayList<ArrayList<Object>>(1);

        // Now add something for validation
        expectedParameterValues.add( new ArrayList<Object>( Arrays.asList( new Object[] { new Integer(5) } ) ) );
        statement.setInt(1, new Integer(5));
        statement.addBatch();
        assertEquals("MMPreparedStatement.ParameterValuesList does not match", expectedParameterValues, statement.getParameterValuesList()); //$NON-NLS-1$
    }

    /**
     * Test the <code>addBatch()</code> method of <code>MMPreparedStatement</code>
     * to verify that the command parameter values of the batch are added to the
     * command parameter values list.
     *
     * @throws Exception
     */
    @Test public void testAddBatch() throws Exception {
        PreparedStatementImpl statement = getMMPreparedStatement("delete from table where col=?"); //$NON-NLS-1$

        ArrayList<ArrayList<Object>> expectedParameterValues = new ArrayList<ArrayList<Object>>(1);

        // First we add a single batch
        expectedParameterValues.add( new ArrayList<Object>( Arrays.asList( new Object[] { new Integer(1) } ) ) );
        statement.setInt(1, new Integer(1));
        statement.addBatch();
        assertEquals("MMPreparedStatement.ParameterValuesList does not match", expectedParameterValues, statement.getParameterValuesList()); //$NON-NLS-1$

        // Now add some more batches just for sanity sake
        expectedParameterValues.add( new ArrayList<Object>( Arrays.asList( new Object[] { new Integer(3) } ) ) );
        expectedParameterValues.add( new ArrayList<Object>( Arrays.asList( new Object[] { new Integer(5) } ) ) );
        statement.setInt(1, new Integer(3));
        statement.addBatch();
        statement.setInt(1, new Integer(5));
        statement.addBatch();
        assertEquals("MMPreparedStatement.ParameterValuesList does not match", expectedParameterValues, statement.getParameterValuesList()); //$NON-NLS-1$

        assertEquals(Arrays.asList(5), statement.getParameterValues());
    }

    @Test public void testSetBlob() throws Exception {
        PreparedStatementImpl stmt = getMMPreparedStatement("delete from table where col=?"); //$NON-NLS-1$
        stmt.setBlob(1, (Blob)null);
    }

    @Test public void testShowParameterMetadata() throws Exception {
        PreparedStatementImpl stmt = getMMPreparedStatement("show plan"); //$NON-NLS-1$
        assertEquals(0, stmt.getParameterMetaData().getParameterCount());
    }

    /**
     * Test the <code>addBatch()</code> method of <code>MMPreparedStatement</code>
     * using a batch with an empty parameter value list.  The test will verify
     * no failures occur when there are no command parameter values defined
     * when the <code>addBatch()</code> method is invoked.
     * <p>
     * It is valid to add an empty parameter value list to a batch list.
     * <p>
     * For example:
     * <p>
     * <code>PreparedStatement stmt = conn.prepareStatement(sql);<br \>
     *  stmt.addBatch();<br \>
     *  stmt.addBatch();<br \>
     *  stmt.executeBatch();</code>
     *
     * @throws Exception
     */
    @Test public void testAddBatchNoParameterValues() throws Exception {
        PreparedStatementImpl statement = getMMPreparedStatement("delete from table where col=?"); //$NON-NLS-1$

        // This will hold our expected values list
        ArrayList<ArrayList<Object>> expectedParameterValues = new ArrayList<ArrayList<Object>>(1);

        // First batch has an empty parameter value list
        expectedParameterValues.add( new ArrayList<Object>(Collections.emptyList()) );

        // No values have been set  so we are adding a batch with an empty
        // parameter value list
        statement.addBatch();

        // Second batch contains a parameter value list
        expectedParameterValues.add( new ArrayList<Object>( Arrays.asList( new Object[] { new Integer(1) } ) ) );

        // We now are adding a batch that does have parameter values
        statement.setInt(1, new Integer(1));
        statement.addBatch();

        // Check to see if our statement contains our expected parameter value list
        assertEquals("MMPreparedStatement.ParameterValuesList does not match", expectedParameterValues, statement.getParameterValuesList()); //$NON-NLS-1$
    }

    /**
     * A helper method to get an <code>MMPreparedStatement</code> that can be
     * used for simple test cases.
     * <p>
     * The returned value is an instance of <code>TestableMMPreparedStatement</code>
     * <p>
     * This method invokes <code>getMMPreparedStatement(final MMConnection conn,
     * final String sql)</code> with a fake connection object constructed by
     * <code>Mockito</code>.
     *
     * @param sql the query for the prepared statement
     * @return an instance of TestableMMPreparedStatement
     * @throws SQLException
     */
    protected PreparedStatementImpl getMMPreparedStatement(final String sql) throws SQLException {
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        ServerConnection serverConn = Mockito.mock(ServerConnection.class);
        LogonResult logonResult = Mockito.mock(LogonResult.class);

        Mockito.stub(conn.getServerConnection()).toReturn(serverConn);
        Mockito.stub(serverConn.getLogonResult()).toReturn(logonResult);
        Mockito.stub(logonResult.getTimeZone()).toReturn(TimeZone.getDefault());

        return getMMPreparedStatement(conn, sql);
    }

    /**
     * A helper method to get an <code>MMPreparedStatement</code> that can be
     * used for simple test cases.
     * <p>
     * The returned value is an instance of <code>TestableMMPreparedStatement</code>
     * <p>
     * <code>conn</code> should be a valid instance of <code>MMConnection</code>
     * or this method will fail.
     *
     * @param conn an instance of <code>MMConnection</code>
     * @param sql the query for the prepared statement
     * @return an instance of TestableMMPreparedStatement
     * @throws SQLException
     */
    protected PreparedStatementImpl getMMPreparedStatement(final ConnectionImpl conn, final String sql) throws SQLException {
        TestableMMPreparedStatement statement = new TestableMMPreparedStatement(conn, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        // Make sure everything is empty on start
        assertTrue("MMPreparedStatement.ParameterValuesList should be empty", statement.getParameterValuesList().size() == 0); //$NON-NLS-1$
        assertTrue("MMPreparedStatement.ParameterValues should be empty", statement.getParameterValues().size() == 0); //$NON-NLS-1$

        return statement;
    }

    /**
     * Represents an extension to <code>MMPreparedStatement</code> that
     * gives access to the <code>RequestMessage</code> that is passed
     * around inside <code>MMPreparedStatement</code>.
     * <p>
     * This extension simply adds a field named <code>requestMessage</code>
     * which is <code>public</code>.  This field gets set when the <code>protected</code>
     * method <code>createRequestMessage()</code> is called.
     * <p>
     * This extension also overrides <code>RequestMessage createRequestMessage(String[] commands,
     *            boolean isBatchedCommand, Boolean requiresResultSet)</code> so that
     * reference to the created <code>RequestMessage</code> can be retained in
     * the field <code>requestMessage</code>.
     */
    class TestableMMPreparedStatement extends PreparedStatementImpl {
        /**
         * Contains a reference to the <code>RequestMessage</code> created by
         * a call to <code>createRequestMessage(String[] commands,
         * boolean isBatchedCommand, Boolean requiresResultSet)</code>.  This
         * will allow easy access to the prepared statement's request message
         * generated by a call to one of the statement's execute methods.
         */
        public RequestMessage requestMessage;
        @Override
        protected RequestMessage createRequestMessage(String[] commands,
                boolean isBatchedCommand, ResultsMode resultsMode) {
            this.requestMessage = super
                    .createRequestMessage(commands, isBatchedCommand, resultsMode);
            return this.requestMessage;
        }

        public TestableMMPreparedStatement(ConnectionImpl connection,
                String sql, int resultSetType, int resultSetConcurrency)
                throws SQLException {
            super(connection, sql, resultSetType, resultSetConcurrency);
        }

    }

}
