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

package com.metamatrix.common.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import junit.framework.TestCase;

import org.mockito.Mockito;

import com.metamatrix.api.exception.MetaMatrixException;

public class TestSimplePooledConnectionSource extends TestCase {
	//## JDBC4.0-begin ##
	public void testMax() throws Exception {
		Properties p = new Properties();
		p.setProperty(SimplePooledConnectionSource.MAXIMUM_RESOURCE_POOL_SIZE, String.valueOf(2));
		p.setProperty(SimplePooledConnectionSource.WAIT_TIME_FOR_RESOURCE, String.valueOf(1));
		p.setProperty(SimplePooledConnectionSource.RESOURCE_TEST_INTERVAL, String.valueOf(0));
		SimplePooledConnectionSource pool = new SimplePooledConnectionSource(p) {
			@Override
			protected Connection createConnection() throws MetaMatrixException {
				Connection c = Mockito.mock(Connection.class);
				try {
					Mockito.stub(c.unwrap(Connection.class)).toReturn(c);
					Mockito.stub(c.isValid(1)).toReturn(true);
				} catch (SQLException e) {
					
				}
				return c;
			}
		};
		Connection c = pool.getConnection();
		pool.getConnection();
		try {
			pool.getConnection();
			fail();
		} catch (SQLException e) {
			assertEquals("Timeout waiting for connection", e.getMessage()); //$NON-NLS-1$
		}
		c.close();
		assertSame(c, pool.getConnection());
	}
	//## JDBC4.0-end ##
}
