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
import java.sql.SQLException;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.xa.XidImpl;
import org.teiid.net.ServerConnection;

public class TestXAConnection {

    @Test public void testConnectionClose() throws Exception {

        final ConnectionImpl mmConn = TestConnection.getMMConnection();

        XAConnectionImpl xaConn = new XAConnectionImpl(mmConn);

        Connection conn = xaConn.getConnection();
        StatementImpl stmt = (StatementImpl)conn.createStatement();
        conn.setAutoCommit(false);
        conn.close();

        ServerConnection sc = xaConn.getConnectionImpl().getServerConnection();

        assertTrue(stmt.isClosed());
        assertTrue(conn.getAutoCommit());

        conn = xaConn.getConnection();
        stmt = (StatementImpl)conn.createStatement();
        XAResource resource = xaConn.getXAResource();
        resource.start(new XidImpl(1, new byte[0], new byte[0]), XAResource.TMNOFLAGS);
        conn.close();

        assertTrue(stmt.isClosed());
        assertTrue(conn.getAutoCommit());
    }

    @Test public void testNotification() throws Exception {
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        Mockito.doThrow(new SQLException(new InvalidSessionException())).when(conn).commit();
        XAConnectionImpl xaConn = new XAConnectionImpl(conn);
        ConnectionEventListener cel = Mockito.mock(ConnectionEventListener.class);
        xaConn.addConnectionEventListener(cel);
        Connection c = xaConn.getConnection();
        try {
            c.commit();
        } catch (SQLException e) {

        }
        Mockito.verify(cel).connectionErrorOccurred((ConnectionEvent) Mockito.anyObject());
    }

    @Test(expected=XAException.class) public void testStartFailure() throws Exception {
        ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
        XidImpl xid = new XidImpl();
        Mockito.doThrow(new SQLException(new InvalidSessionException())).when(conn).startTransaction(xid, XAResource.TMNOFLAGS, 100);
        XAConnectionImpl xaConn = new XAConnectionImpl(conn);
        xaConn.setTransactionTimeout(100);
        xaConn.start(xid, XAResource.TMNOFLAGS);
    }

}
