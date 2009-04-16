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

package com.metamatrix.jdbc;

import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;

import com.metamatrix.dqp.client.ClientSideDQP;
import com.metamatrix.dqp.client.ResultsFuture;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.message.ResultsMessage;

public class TestMMStatement {

	@Test(expected=MMSQLException.class) public void testUpdateException() throws Exception {
		MMStatement statement = new MMStatement(Mockito.mock(MMConnection.class), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		statement.executeQuery("delete from table"); //$NON-NLS-1$
	}
	
	@Test public void testBatchExecution() throws Exception {
		MMConnection conn = Mockito.mock(MMConnection.class);
		ClientSideDQP dqp = Mockito.mock(ClientSideDQP.class);
		ResultsFuture<ResultsMessage> results = new ResultsFuture<ResultsMessage>(); 
		Mockito.stub(dqp.executeRequest(Mockito.anyLong(), (RequestMessage)Mockito.anyObject())).toReturn(results);
		ResultsMessage rm = new ResultsMessage();
		rm.setResults(new List<?>[] {Arrays.asList(1), Arrays.asList(2)});
		rm.setUpdateResult(true);
		results.getResultsReceiver().receiveResults(rm);
		Mockito.stub(conn.getDQP()).toReturn(dqp);
		MMStatement statement = new MMStatement(conn, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		statement.addBatch("delete from table"); //$NON-NLS-1$
		statement.addBatch("delete from table1"); //$NON-NLS-1$
		assertTrue(Arrays.equals(new int[] {1, 2}, statement.executeBatch()));
	}
	
}
