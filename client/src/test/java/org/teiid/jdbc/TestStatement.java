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

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.client.DQP;
import org.teiid.client.RequestMessage;
import org.teiid.client.ResultsMessage;
import org.teiid.client.util.ResultsFuture;

@SuppressWarnings("nls")
public class TestStatement {

	@Test public void testBatchExecution() throws Exception {
		ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
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
	
	@Test public void testSetStatement() throws Exception {
		ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
		Properties p = new Properties();
		Mockito.stub(conn.getExecutionProperties()).toReturn(p);
		StatementImpl statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		assertFalse(statement.execute("set foo bar")); //$NON-NLS-1$
		assertEquals("bar", p.get("foo")); //$NON-NLS-1$ //$NON-NLS-2$
		
		assertFalse(statement.execute("set foo 'b''ar'")); //$NON-NLS-1$
		assertEquals("b'ar", p.get("foo")); //$NON-NLS-1$ //$NON-NLS-2$
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
	}

	@SuppressWarnings("unchecked")
	@Test public void testTransactionStatementsAsynch() throws Exception {
		ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
		Mockito.stub(conn.submitSetAutoCommitTrue(Mockito.anyBoolean())).toReturn((ResultsFuture)ResultsFuture.NULL_FUTURE);
		Properties p = new Properties();
		Mockito.stub(conn.getExecutionProperties()).toReturn(p);
		StatementImpl statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		statement.submitExecute("start transaction"); //$NON-NLS-1$
		Mockito.verify(conn).setAutoCommit(false);
		statement.submitExecute("commit"); //$NON-NLS-1$
		Mockito.verify(conn).submitSetAutoCommitTrue(true);
		statement.submitExecute("start transaction"); //$NON-NLS-1$
		statement.submitExecute("rollback"); //$NON-NLS-1$
		Mockito.verify(conn).submitSetAutoCommitTrue(false);
	}
	
	@Test public void testAsynchTimeout() throws Exception {
		ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
		StatementImpl statement = new StatementImpl(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		statement.setQueryTimeoutMS(1);
		DQP dqp = Mockito.mock(DQP.class);
		Mockito.stub(statement.getDQP()).toReturn(dqp);
		ResultsFuture<ResultsMessage> future = new ResultsFuture<ResultsMessage>();
		Mockito.stub(dqp.executeRequest(Mockito.anyLong(), (RequestMessage) Mockito.anyObject())).toReturn(future);
		statement.submitExecute("select 'hello world'");
		Thread.sleep(300);
		Mockito.verify(dqp).cancelRequest(0);
		statement.setQueryTimeoutMS(1);
		statement.submitExecute("select 'hello world'");
		Thread.sleep(300);
		Mockito.verify(dqp, Mockito.times(2)).cancelRequest(0);
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
	
}
