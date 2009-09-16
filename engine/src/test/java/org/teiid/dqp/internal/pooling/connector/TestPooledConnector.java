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

package org.teiid.dqp.internal.pooling.connector;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Properties;

import javax.transaction.Transaction;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.xa.api.TransactionContext;
import org.teiid.connector.xa.api.XAConnector;
import org.teiid.dqp.internal.datamgr.impl.ConnectorEnvironmentImpl;

import com.metamatrix.admin.objects.MMConnectionPool;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.dqp.service.TransactionService;

public class TestPooledConnector {
	
	@Test public void testGetXAConnection() throws Exception {
		XAConnector connector = Mockito.mock(XAConnector.class);
		PooledConnector pc = new PooledConnector("foo", connector, Mockito.mock(TransactionService.class)); //$NON-NLS-1$
		pc.start(new ConnectorEnvironmentImpl(new Properties(), Mockito.mock(ConnectorLogger.class), new ApplicationEnvironment()));
		TransactionContext tc = Mockito.mock(TransactionContext.class);
		Mockito.stub(tc.getTransaction()).toReturn(Mockito.mock(Transaction.class));
		Mockito.stub(tc.getTxnID()).toReturn("1"); //$NON-NLS-1$
		Connection conn = pc.getConnection(Mockito.mock(ExecutionContext.class), tc);
		conn.close();
		Connection conn1 = pc.getConnection(Mockito.mock(ExecutionContext.class), tc);
		assertSame(conn, conn1);
		
		List<MMConnectionPool> stats = pc.getConnectionPoolStats();
		assertEquals(2, stats.size());
		assertEquals(1, stats.get(1).getConnectionsCreated());
		conn1.close(); //this is still in a txn so it should still be in use
		
		stats = pc.getConnectionPoolStats();
		assertEquals(1, stats.get(1).getConnectionsInuse());
		
		pc.stop();
	}

}
