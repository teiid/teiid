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

package org.teiid.translator.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.dqp.internal.datamgr.FakeExecutionContextImpl;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;

@SuppressWarnings("nls")
public class TestJDBCDirectQueryExecution {
	
	@Test public void testSelectExecution() throws Exception {
		Command command = TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "call native('select * from Source')"); //$NON-NLS-1$
		Connection connection = Mockito.mock(Connection.class);
		Statement stmt = Mockito.mock(Statement.class);
		ResultSet rs = Mockito.mock(ResultSet.class);
		ResultSetMetaData rsm = Mockito.mock(ResultSetMetaData.class);
		
		Mockito.stub(stmt.getUpdateCount()).toReturn(-1);
		Mockito.stub(stmt.getResultSet()).toReturn(rs);
		Mockito.stub(rs.getMetaData()).toReturn(rsm);
		Mockito.stub(rsm.getColumnCount()).toReturn(2);
		Mockito.stub(connection.createStatement()).toReturn(stmt); //$NON-NLS-1$
		Mockito.stub(stmt.execute("select * from Source")).toReturn(true);
		Mockito.stub(rs.next()).toReturn(true);
		Mockito.stub(rs.getObject(1)).toReturn(5);
		Mockito.stub(rs.getObject(2)).toReturn("five");
		Mockito.stub(connection.getMetaData()).toReturn(Mockito.mock(DatabaseMetaData.class));
		
		JDBCExecutionFactory ef = new JDBCExecutionFactory();
		ef.setSupportsDirectQueryProcedure(true);
		ResultSetExecution execution = (ResultSetExecution)ef.createExecution(command,  Mockito.mock(ExecutionContext.class), Mockito.mock(RuntimeMetadata.class), connection);
		execution.execute();
		assertArrayEquals(new Object[] {5, "five"}, (Object[])execution.next().get(0));
	}

	@Test public void testPrepareExecution() throws Exception {
		Command command = TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "call native('select * from Source where e1 = ?', 2)"); //$NON-NLS-1$
		Connection connection = Mockito.mock(Connection.class);
		PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
		ResultSet rs = Mockito.mock(ResultSet.class);
		ResultSetMetaData rsm = Mockito.mock(ResultSetMetaData.class);
		
		Mockito.stub(stmt.getUpdateCount()).toReturn(-1);
		Mockito.stub(stmt.getResultSet()).toReturn(rs);
		Mockito.stub(stmt.execute()).toReturn(true);
		Mockito.stub(rs.getMetaData()).toReturn(rsm);
		Mockito.stub(rsm.getColumnCount()).toReturn(2);
		Mockito.stub(connection.prepareStatement("select * from Source where e1 = ?")).toReturn(stmt); //$NON-NLS-1$
		Mockito.stub(rs.next()).toReturn(true);
		Mockito.stub(rs.getObject(1)).toReturn(5);
		Mockito.stub(rs.getObject(2)).toReturn("five");
		Mockito.stub(connection.getMetaData()).toReturn(Mockito.mock(DatabaseMetaData.class));
		
		JDBCExecutionFactory ef = new JDBCExecutionFactory();
		ef.setSupportsDirectQueryProcedure(true);
		ResultSetExecution execution = (ResultSetExecution)ef.createExecution(command,  new FakeExecutionContextImpl(), Mockito.mock(RuntimeMetadata.class), connection);
		execution.execute();
		assertArrayEquals(new Object[] {5, "five"}, (Object[])execution.next().get(0));
	}
	
	@Test public void testPrepareUpdateCount() throws Exception {
		Command command = TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "call native('update source set e1=? where e2 = ?', 2, 'foo')"); //$NON-NLS-1$
		Connection connection = Mockito.mock(Connection.class);
		PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
		ResultSet rs = Mockito.mock(ResultSet.class);
		ResultSetMetaData rsm = Mockito.mock(ResultSetMetaData.class);
		
		Mockito.stub(stmt.getUpdateCount()).toReturn(-1);
		Mockito.stub(stmt.getUpdateCount()).toReturn(5);
		Mockito.stub(stmt.execute()).toReturn(false);
		Mockito.stub(rs.getMetaData()).toReturn(rsm);
		Mockito.stub(rsm.getColumnCount()).toReturn(2);
		Mockito.stub(connection.prepareStatement("update source set e1=? where e2 = ?")).toReturn(stmt); //$NON-NLS-1$
		Mockito.stub(connection.getMetaData()).toReturn(Mockito.mock(DatabaseMetaData.class));
		
		JDBCExecutionFactory ef = new JDBCExecutionFactory();
		ef.setSupportsDirectQueryProcedure(true);
		ResultSetExecution execution = (ResultSetExecution)ef.createExecution(command,  new FakeExecutionContextImpl(), Mockito.mock(RuntimeMetadata.class), connection);
		execution.execute();
		assertArrayEquals(new Object[] {5}, (Object[])execution.next().get(0));
	}	
}
