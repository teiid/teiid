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

package org.teiid.systemmodel;

import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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

@SuppressWarnings("nls")
public class TestAsynch {
	
	private static FakeServer server;
	private ConnectionImpl internalConnection;
    
    @BeforeClass public static void oneTimeSetup() throws Exception {
    	server = new FakeServer(true);
    	ModelMetaData mmd = new ModelMetaData();
    	mmd.setName("v");
    	mmd.setModelType(Type.VIRTUAL);
    	mmd.setSchemaSourceType("ddl");
    	mmd.setSchemaText("create view test (col integer) as select 1;");
    	server.deployVDB("x", mmd);
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

}
