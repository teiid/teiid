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
package org.teiid.runtime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.resource.ResourceException;
import javax.sql.DataSource;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.junit.Ignore;
import org.junit.Test;

public class TestEmbeddedHelper {
	
	@Test
	public void commitTransactionManager() throws Exception {
		
		TransactionManager tm = EmbeddedHelper.getTransactionManager();
		
		tm.begin();
		
		tm.commit();
	}
	
	@Test
	public void rollbackTransaction() throws Exception {

		TransactionManager tm = EmbeddedHelper.getTransactionManager();
		
		tm.begin();
		
		tm.rollback();
	}
	
	@Test(expected = RollbackException.class)
	public void setRollbackOnly() throws Exception {
		
		TransactionManager tm = EmbeddedHelper.getTransactionManager();
		
		tm.begin();
		
		tm.setRollbackOnly();
		
		tm.commit();
	}
	
	@Test
	public void transactionStatus() throws Exception {
		
		TransactionManager tm = EmbeddedHelper.getTransactionManager();
		
		tm.begin();
		
		assertEquals(Status.STATUS_ACTIVE, tm.getTransaction().getStatus());
		
		tm.setRollbackOnly();
		
		assertEquals(Status.STATUS_MARKED_ROLLBACK, tm.getTransaction().getStatus());
		
		tm.rollback();	
	}
	
	@Test(expected=RollbackException.class)
	public void transactionTimeout() throws Exception{
		
		TransactionManager tm = EmbeddedHelper.getTransactionManager();
		
		tm.setTransactionTimeout(3);
		
		tm.begin();
		
		Thread.sleep(1000 * 5);
		
		tm.commit();
	}
	
	@Test
	public void testDataSource() throws ResourceException, SQLException {
		
		String driverClass = "org.h2.Driver";
		String connURL = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
		String user = "sa";
		String password = "sa";

		
		DataSource ds = EmbeddedHelper.newDataSource(driverClass, connURL, user, password);
		
		assertNotNull(ds);
		
		Connection conn = ds.getConnection();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT CURRENT_DATE()");
		assertTrue(rs.next());
		
		rs.close();
		stmt.close();
		conn.close();

	}
	
	@Test
	@Ignore("testPoolMaxSize will block 30 seconds")
	public void testPoolMaxSize() throws ResourceException, SQLException{
		
		String driverClass = "org.h2.Driver";
		String connURL = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
		String user = "sa";
		String password = "sa";

		// ironjacamar default max size is 20
		List<Connection> list = new ArrayList<Connection>();
		DataSource ds = EmbeddedHelper.newDataSource(driverClass, connURL, user, password);
		try {
			for(int i = 0 ; i < 21 ; i ++) {
				list.add(ds.getConnection());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		assertEquals(20, list.size());
		
		for(Connection conn : list) {
			conn.close();
		}
		
		
	}

}
