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

package org.teiid.dqp.internal.datamgr.impl;

import static junit.framework.Assert.*;

import java.util.Arrays;
import java.util.List;

import javax.transaction.xa.Xid;

import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.client.RequestMessage;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.language.Call;
import org.teiid.dqp.internal.datamgr.language.LanguageBridgeFactory;
import org.teiid.dqp.internal.process.AbstractWorkItem;
import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.service.TransactionContext;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.unittest.FakeMetadataFactory;

public class TestConnectorWorkItem {

	private static final QueryMetadataInterface EXAMPLE_BQT = FakeMetadataFactory.exampleBQTCached();

	private static Command helpGetCommand(String sql,
			QueryMetadataInterface metadata) throws Exception {
		Command command = QueryParser.getQueryParser().parseCommand(sql);
		QueryResolver.resolveCommand(command, metadata);
		return command;
	}

	static AtomicRequestMessage createNewAtomicRequestMessage(int requestid, int nodeid) throws Exception {
		RequestMessage rm = new RequestMessage();
		
		DQPWorkContext workContext = FakeMetadataFactory.buildWorkContext(EXAMPLE_BQT, FakeMetadataFactory.exampleBQTVDB());
		workContext.getSession().setSessionId(String.valueOf(1));
		workContext.getSession().setUserName("foo"); //$NON-NLS-1$
		
		AtomicRequestMessage request = new AtomicRequestMessage(rm, workContext, nodeid);
		request.setCommand(helpGetCommand("SELECT BQT1.SmallA.INTKEY FROM BQT1.SmallA", EXAMPLE_BQT)); //$NON-NLS-1$
		request.setRequestID(new RequestID(requestid));
		request.setConnectorName("testing"); //$NON-NLS-1$
		request.setFetchSize(5);
		return request;
	}

	@Test public void testProcedureBatching() throws Exception {
		ProcedureExecution exec = new FakeProcedureExecution(2, 1);

		// this has two result set columns and 1 out parameter
		int total_columns = 3;
		StoredProcedure command = (StoredProcedure)helpGetCommand("{call pm2.spTest8(?)}", EXAMPLE_BQT); //$NON-NLS-1$      
		command.getInputParameters().get(0).setExpression(new Constant(1));
		Call proc = (Call)new LanguageBridgeFactory(EXAMPLE_BQT).translate(command);

		ProcedureBatchHandler pbh = new ProcedureBatchHandler(proc, exec);

		assertEquals(total_columns, pbh.padRow(Arrays.asList(null, null)).size());

		List params = pbh.getParameterRow();
		
		assertEquals(total_columns, params.size());
		// check the parameter value
		assertEquals(Integer.valueOf(0), params.get(2));

		try {
			pbh.padRow(Arrays.asList(1));
			fail("Expected exception from resultset mismatch"); //$NON-NLS-1$
		} catch (ConnectorException err) {
			assertEquals(
					"Could not process stored procedure results for EXEC spTest8(1).  Expected 2 result set columns, but was 1.  Please update your models to allow for stored procedure results batching.", err.getMessage()); //$NON-NLS-1$
		}
	}

    @Test public void testUpdateExecution() throws Throwable {
		AtomicResultsMessage results = helpExecuteUpdate();
		assertEquals(Integer.valueOf(1), results.getResults()[0].get(0));
	}

	private AtomicResultsMessage helpExecuteUpdate() throws Exception,
			Throwable {
		Command command = helpGetCommand("update bqt1.smalla set stringkey = 1 where stringkey = 2", EXAMPLE_BQT); //$NON-NLS-1$
		AtomicRequestMessage arm = createNewAtomicRequestMessage(1, 1);
		arm.setCommand(command);
		ConnectorWorkItem synchConnectorWorkItem = new ConnectorWorkItem(arm, Mockito.mock(AbstractWorkItem.class), 
				TestConnectorManager.getConnectorManager(Mockito.mock(ConnectorEnvironment.class)));
		return synchConnectorWorkItem.execute();
	}
	
	@Test public void testExecutionWarning() throws Throwable {
		AtomicResultsMessage results = helpExecuteUpdate();
		assertEquals(1, results.getWarnings().size());
	}

	@Ignore    
	@Test public void testIsImmutablePropertySucceeds() throws Exception {
    	/*
    	 * Setup:
    	 *  1. requestMsg.isTransactional() must be TRUE 
    	 *  2. manager.isXa() must be FALSE  ()
    	 *  3. command must NOT be a SELECT
    	 *  4. Then, set isImmutable to TRUE, we should SUCCEED
    	 */
    	ConnectorEnvironment env = Mockito.mock(ConnectorEnvironment.class);
    	Mockito.stub(env.isImmutable()).toReturn(true);
		ConnectorManager cm = TestConnectorManager.getConnectorManager(env);

		// command must not be a SELECT
		Command command = helpGetCommand("update bqt1.smalla set stringkey = 1 where stringkey = 2", EXAMPLE_BQT); //$NON-NLS-1$
		AtomicRequestMessage requestMsg = createNewAtomicRequestMessage(1, 1);
		requestMsg.setCommand(command);
		
		// To make the AtomicRequestMessage transactional, construct your own
		requestMsg.setTransactionContext( new TransactionContext(){
			@Override
			public Xid getXid() {
				return Mockito.mock(Xid.class);
			}} );
		
		new ConnectorWorkItem(requestMsg, Mockito.mock(AbstractWorkItem.class), cm);
    }
    
	@Ignore
	@Test(expected=ConnectorException.class) public void testIsImmutablePropertyFails() throws Exception {
    	/*
    	 * Setup:
    	 *  1. requestMsg.isTransactional() must be TRUE 
    	 *  2. manager.isXa() must be FALSE  ()
    	 *  3. command must NOT be a SELECT
    	 *  4. Then, set isImmutable to FALSE, and we should FAIL
    	 */
    	ConnectorEnvironment env = Mockito.mock(ConnectorEnvironment.class);
    	Mockito.stub(env.isImmutable()).toReturn(false);
		ConnectorManager cm = TestConnectorManager.getConnectorManager(env);
        
		// command must not be a SELECT
		Command command = helpGetCommand("update bqt1.smalla set stringkey = 1 where stringkey = 2", EXAMPLE_BQT); //$NON-NLS-1$
		AtomicRequestMessage requestMsg = createNewAtomicRequestMessage(1, 1);
		requestMsg.setCommand(command);
		
		// To make the AtomicRequestMessage transactional, construct your own
		requestMsg.setTransactionContext( new TransactionContext(){
			@Override
			public Xid getXid() {
				return Mockito.mock(Xid.class);
			}} );
		
		new ConnectorWorkItem(requestMsg, Mockito.mock(AbstractWorkItem.class), cm);
    }

}
