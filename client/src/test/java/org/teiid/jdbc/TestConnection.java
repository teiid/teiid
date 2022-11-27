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
import static org.mockito.Mockito.*;

import java.sql.Array;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.teiid.client.DQP;
import org.teiid.client.security.LogonResult;
import org.teiid.client.security.SessionToken;
import org.teiid.client.util.ResultsFuture;
import org.teiid.client.xa.XATransactionException;
import org.teiid.client.xa.XidImpl;
import org.teiid.net.ServerConnection;

@SuppressWarnings("nls")
public class TestConnection {

    protected static final String STD_DATABASE_NAME         = "QT_Ora9DS"; //$NON-NLS-1$
    protected static final int STD_DATABASE_VERSION      = 1;

    static String serverUrl = "jdbc:teiid:QT_Ora9DS@mm://localhost:7001;version=1;user=metamatrixadmin;password=mm"; //$NON-NLS-1$

    static class  InnerDriver extends TeiidDriver {
        String iurl = null;
        public InnerDriver(String url) {
            iurl = url;
        }

        public void parseUrl(Properties props) throws SQLException {
                 super.parseURL(iurl, props);
        }
    }

    public static ConnectionImpl getMMConnection() {
        return getMMConnection(serverUrl);
    }

    public static ConnectionImpl getMMConnection(String url) {
        ServerConnection mock = mock(ServerConnection.class);
        DQP dqp = mock(DQP.class);
        try {
            stub(dqp.start((XidImpl)Mockito.anyObject(), Mockito.anyInt(), Mockito.anyInt())).toAnswer(new Answer() {
                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                    return ResultsFuture.NULL_FUTURE;
                }
            });
            stub(dqp.rollback((XidImpl)Mockito.anyObject())).toAnswer(new Answer() {
                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                    return ResultsFuture.NULL_FUTURE;
                }
            });
            stub(dqp.rollback()).toAnswer(new Answer() {
                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                    return ResultsFuture.NULL_FUTURE;
                }
            });
        } catch (XATransactionException e) {
            throw new RuntimeException(e);
        }

        Properties props = new Properties();

        try {
            new InnerDriver(url).parseUrl(props);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        stub(mock.getService(DQP.class)).toReturn(dqp);

        stub(mock.getLogonResult()).toReturn(new LogonResult(new SessionToken(1, "admin"), STD_DATABASE_NAME, "fake")); //$NON-NLS-1$
        return new ConnectionImpl(mock, props, url);
    }

    @Test public void testGetMetaData() throws Exception {
        assertNotNull(getMMConnection().getMetaData());
    }

    @Test public void testNullSorts() throws Exception {
        DatabaseMetaData metadata = getMMConnection("jdbc:teiid:QT_Ora9DS@mm://localhost:7001;version=1;NullsAreSorted=AtEnd").getMetaData();
        assertTrue(metadata.nullsAreSortedAtEnd());
        assertFalse(metadata.nullsAreSortedLow());
        metadata = getMMConnection("jdbc:teiid:QT_Ora9DS@mm://localhost:7001;version=1").getMetaData();
        assertFalse(metadata.nullsAreSortedAtEnd());
    }

    @Test public void testGetSchema() throws Exception {
        assertEquals("Actual schema is not equql to the expected one. ", STD_DATABASE_NAME, getMMConnection().getVDBName()); //$NON-NLS-1$
    }

    @Test public void testNativeSql() throws Exception {
        String sql = "SELECT * FROM BQT1.SmallA"; //$NON-NLS-1$
        assertEquals("Actual schema is not equql to the expected one. ", sql, getMMConnection().nativeSQL(sql)); //$NON-NLS-1$
    }

    /** test getUserName() through DriverManager */
    @Test public void testGetUserName2() throws Exception {
        assertEquals("Actual userName is not equal to the expected one. ", "admin", getMMConnection().getUserName()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /** test isReadOnly default value on Connection */
    @Test public void testIsReadOnly() throws Exception {
        assertEquals(false, getMMConnection().isReadOnly());
    }

    /** test setReadOnly on Connection */
    @Test public void testSetReadOnly1() throws Exception {
        ConnectionImpl conn = getMMConnection();
        conn.setReadOnly(true);
        assertEquals(true, conn.isReadOnly());
    }

    /** test setReadOnly on Connection during a transaction */
    @Test public void testSetReadOnly2() throws Exception {
        ConnectionImpl conn = getMMConnection();
        conn.setAutoCommit(false);
        conn.setReadOnly(true);
        conn.setInLocalTxn(true);
        try {
            conn.setReadOnly(false);
            fail("Error Expected"); //$NON-NLS-1$
        } catch (SQLException e) {
            // error expected
        }
    }

    /**
     * Test the default of the JDBC4 spec semantics is true
     */
    @Test public void testDefaultSpec() throws Exception {
        assertEquals("true",
                (getMMConnection().getExecutionProperties().getProperty(ExecutionProperties.JDBC4COLUMNNAMEANDLABELSEMANTICS) == null ? "true" : "false"));
    }

    /**
     * Test turning off the JDBC 4 semantics
     */
    @Test public void testTurnOnSpec() throws Exception {
        assertEquals("true", getMMConnection(serverUrl + ";useJDBC4ColumnNameAndLabelSemantics=true").getExecutionProperties().getProperty(ExecutionProperties.JDBC4COLUMNNAMEANDLABELSEMANTICS));
    }

    /**
     * Test turning off the JDBC 4 semantics
     */
    @Test public void testTurnOffSpec() throws Exception {
        assertEquals("false", getMMConnection(serverUrl + ";useJDBC4ColumnNameAndLabelSemantics=false").getExecutionProperties().getProperty(ExecutionProperties.JDBC4COLUMNNAMEANDLABELSEMANTICS));
    }

    @Test public void testCreateArray() throws SQLException {
        Array array = getMMConnection().createArrayOf("integer[]", new Integer[] {3, 4});
        assertEquals(3, java.lang.reflect.Array.get(array.getArray(), 0));
    }

    @Test public void testXACommit() throws Exception {
        ConnectionImpl conn = getMMConnection();
        conn.setAutoCommit(false);
        conn.setTransactionXid(Mockito.mock(XidImpl.class));
        try {
            conn.setAutoCommit(true);
            fail("Error Expected"); //$NON-NLS-1$
        } catch (SQLException e) {
            // error expected
        }
    }

    @Test public void testMaxOpenStatements() throws SQLException {
        ConnectionImpl conn = getMMConnection();
        for(int i = 0; i < 1000; i++){
            conn.createStatement();
        }
        try{
            conn.createStatement();
            fail("MaxOpenStatements not limited to required number.");
        } catch (TeiidSQLException ex){
            assertThat(ex.getMessage(), CoreMatchers.containsString(JDBCPlugin.Event.TEIID20036.name()));
        }
    }
}
