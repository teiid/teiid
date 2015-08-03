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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;
import java.util.Arrays;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.language.Command;
import org.teiid.translator.ExecutionContext;

public class TestJDBCProcedureExecution {
	
	@Test public void testProcedureExecution() throws Exception {
		Command command = TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "exec pm2.spTest8a()"); //$NON-NLS-1$
		Connection connection = Mockito.mock(Connection.class);
		CallableStatement cs = Mockito.mock(CallableStatement.class);
		Mockito.stub(cs.getUpdateCount()).toReturn(-1);
		Mockito.stub(cs.getInt(1)).toReturn(5);
		Mockito.stub(connection.prepareCall("{call spTest8a(?)}")).toReturn(cs); //$NON-NLS-1$
		JDBCExecutionFactory ef = new JDBCExecutionFactory();
		
		JDBCProcedureExecution procedureExecution = new JDBCProcedureExecution(command, connection, Mockito.mock(ExecutionContext.class),  ef);
		procedureExecution.execute();
		assertEquals(Arrays.asList(5), procedureExecution.getOutputParameterValues());
		Mockito.verify(cs, Mockito.times(1)).registerOutParameter(1, Types.INTEGER);
	}
	@Test public void testProcedureExecution1() throws Exception {
		Command command = TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "exec pm2.spTest8(1)"); //$NON-NLS-1$
		Connection connection = Mockito.mock(Connection.class);
		CallableStatement cs = Mockito.mock(CallableStatement.class);
		Mockito.stub(cs.getUpdateCount()).toReturn(-1);
		Mockito.stub(cs.getInt(2)).toReturn(5);
		Mockito.stub(connection.prepareCall("{call spTest8(?,?)}")).toReturn(cs); //$NON-NLS-1$
		JDBCExecutionFactory config = new JDBCExecutionFactory();

		JDBCProcedureExecution procedureExecution = new JDBCProcedureExecution(command, connection, Mockito.mock(ExecutionContext.class), config);
		procedureExecution.execute();
		assertEquals(Arrays.asList(5), procedureExecution.getOutputParameterValues());
		Mockito.verify(cs, Mockito.times(1)).registerOutParameter(2, Types.INTEGER);
	}

}
