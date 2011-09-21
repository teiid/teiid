/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.client.RequestMessage;
import org.teiid.client.ResultsMessage;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.client.security.LogonResult;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.net.ServerConnection;


public class TestCallableStatement extends TestCase {
	
	public void testWasNull() throws Exception {
		CallableStatementImpl mmcs = getCallableStatement();
		
		Map<Integer, Integer> params = new HashMap<Integer, Integer>();
		mmcs.outParamIndexMap = params;
		params.put(Integer.valueOf(1), Integer.valueOf(1));
		params.put(Integer.valueOf(2), Integer.valueOf(2));
		ResultSetImpl rs = Mockito.mock(ResultSetImpl.class);
		mmcs.resultSet = rs;
		Mockito.stub(rs.getOutputParamValue(1)).toReturn(null);
		Mockito.stub(rs.getOutputParamValue(2)).toReturn(Boolean.TRUE);
		mmcs.getBoolean(1);
		assertTrue(mmcs.wasNull());
		assertTrue(mmcs.getBoolean(2));
		assertFalse(mmcs.wasNull());
	}
	
	public void testGetOutputParameter() throws Exception {
		CallableStatementImpl mmcs = getCallableStatement();
		
		RequestMessage request = new RequestMessage();
		request.setExecutionId(1);
		ResultsMessage resultsMsg = new ResultsMessage();
		List[] results = new List[] {Arrays.asList(null, null, null), Arrays.asList(null, 1, 2)};
		resultsMsg.setResults(results);
		resultsMsg.setColumnNames(new String[] { "IntNum", "Out1", "Out2" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		resultsMsg.setDataTypes(new String[] { JDBCSQLTypeInfo.INTEGER, JDBCSQLTypeInfo.INTEGER, JDBCSQLTypeInfo.INTEGER }); 
		resultsMsg.setFinalRow(results.length);
		resultsMsg.setLastRow(results.length);
		resultsMsg.setFirstRow(1);
		resultsMsg.setParameters(Arrays.asList(new ParameterInfo(ParameterInfo.RESULT_SET, 1), new ParameterInfo(ParameterInfo.OUT, 1), new ParameterInfo(ParameterInfo.OUT, 1)));
		mmcs.createResultSet(resultsMsg);
		assertEquals(1, mmcs.getInt(1));
		assertEquals(2, mmcs.getInt(2));
	}
	
	public void testUnknownIndex() throws Exception {
		CallableStatementImpl mmcs = getCallableStatement();
		
		mmcs.outParamIndexMap = new HashMap<Integer, Integer>();
		
		try {
			mmcs.getBoolean(0);
			fail("expected exception"); //$NON-NLS-1$
		} catch (IllegalArgumentException e) {
			assertEquals("Parameter is not found at index 0.", e.getMessage());
		}
	}

	private CallableStatementImpl getCallableStatement() throws SQLException {
		ConnectionImpl conn = Mockito.mock(ConnectionImpl.class);
		ServerConnection sc = Mockito.mock(ServerConnection.class);
		
		Mockito.stub(sc.getLogonResult()).toReturn(new LogonResult());
		Mockito.stub(conn.getServerConnection()).toReturn(sc);
		
		CallableStatementImpl mmcs = new CallableStatementImpl(conn, "{?=call x(?)}", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		return mmcs;
	}

}
