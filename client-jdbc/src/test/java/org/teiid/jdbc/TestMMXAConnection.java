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

import java.sql.Connection;
import java.sql.SQLException;

import javax.transaction.xa.XAResource;

import org.teiid.jdbc.ConnectionImpl;
import org.teiid.jdbc.StatementImpl;
import org.teiid.jdbc.XAConnectionImpl;

import junit.framework.TestCase;

import com.metamatrix.common.xa.XidImpl;


public class TestMMXAConnection extends TestCase {
	
	public void testConnectionClose() throws Exception {

		final ConnectionImpl mmConn = TestMMConnection.getMMConnection();

		XAConnectionImpl xaConn = new XAConnectionImpl(new XAConnectionImpl.ConnectionSource() {
			//## JDBC4.0-begin ##
			@Override
			//## JDBC4.0-end ##
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

}
