/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.transaction.xa.XAResource;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.xa.XidImpl;

public class TestXAConnection {
	
	@Test public void testConnectionClose() throws Exception {

		final ConnectionImpl mmConn = TestConnection.getMMConnection();

		XAConnectionImpl xaConn = new XAConnectionImpl(new XAConnectionImpl.ConnectionSource() {
			@Override
			public ConnectionImpl createConnection() throws SQLException {
				return mmConn;
			}
		});
		
		Connection conn = xaConn.getConnection();
		StatementImpl stmt = (StatementImpl)conn.createStatement();
		conn.setAutoCommit(false);
		conn.close();

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
		XAConnectionImpl xaConn = new XAConnectionImpl(new XAConnectionImpl.ConnectionSource() {
			@Override
			public ConnectionImpl createConnection() throws SQLException {
				ConnectionImpl c = Mockito.mock(ConnectionImpl.class);
				Mockito.doThrow(new SQLException(new InvalidSessionException())).when(c).commit();
				return c;
			}
		});
		ConnectionEventListener cel = Mockito.mock(ConnectionEventListener.class);
		xaConn.addConnectionEventListener(cel);
		Connection c = xaConn.getConnection();
		try {
			c.commit();
		} catch (SQLException e) {
			
		}
		Mockito.verify(cel).connectionErrorOccurred((ConnectionEvent) Mockito.anyObject());
	}

}
