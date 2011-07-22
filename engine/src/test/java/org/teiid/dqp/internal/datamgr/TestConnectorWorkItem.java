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

package org.teiid.dqp.internal.datamgr;

import static junit.framework.Assert.*;

import java.util.Arrays;
import java.util.List;

import javax.transaction.xa.Xid;

import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.client.RequestMessage;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorWorkItem;
import org.teiid.dqp.internal.datamgr.LanguageBridgeFactory;
import org.teiid.dqp.internal.datamgr.ProcedureBatchHandler;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.dqp.message.AtomicResultsMessage;
import org.teiid.dqp.message.RequestID;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.language.Call;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;


public class TestConnectorWorkItem {

	private static final QueryMetadataInterface EXAMPLE_BQT = RealMetadataFactory.exampleBQTCached();

	private static Command helpGetCommand(String sql,
			QueryMetadataInterface metadata) throws Exception {
		Command command = QueryParser.getQueryParser().parseCommand(sql);
		QueryResolver.resolveCommand(command, metadata);
		return command;
	}

	static AtomicRequestMessage createNewAtomicRequestMessage(int requestid, int nodeid) throws Exception {
		RequestMessage rm = new RequestMessage();
		
		DQPWorkContext workContext = RealMetadataFactory.buildWorkContext(EXAMPLE_BQT, RealMetadataFactory.exampleBQTVDB());
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
		Call proc = new LanguageBridgeFactory(EXAMPLE_BQT).translate(command);

		ProcedureBatchHandler pbh = new ProcedureBatchHandler(proc, exec);

		assertEquals(total_columns, pbh.padRow(Arrays.asList(null, null)).size());

		List params = pbh.getParameterRow();
		
		assertEquals(total_columns, params.size());
		// check the parameter value
		assertEquals(Integer.valueOf(0), params.get(2));

		try {
			pbh.padRow(Arrays.asList(1));
			fail("Expected exception from resultset mismatch"); //$NON-NLS-1$
		} catch (TranslatorException err) {
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
		ConnectorWorkItem synchConnectorWorkItem = new ConnectorWorkItem(arm, TestConnectorManager.getConnectorManager());
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
		ConnectorManager cm = TestConnectorManager.getConnectorManager();
		((FakeConnector)cm.getExecutionFactory()).setImmutable(true);
		

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
		
		new ConnectorWorkItem(requestMsg, cm);
    }
    
	@Ignore
	@Test(expected=TranslatorException.class) public void testIsImmutablePropertyFails() throws Exception {
    	/*
    	 * Setup:
    	 *  1. requestMsg.isTransactional() must be TRUE 
    	 *  2. manager.isXa() must be FALSE  ()
    	 *  3. command must NOT be a SELECT
    	 *  4. Then, set isImmutable to FALSE, and we should FAIL
    	 */
		ConnectorManager cm = TestConnectorManager.getConnectorManager();
		((FakeConnector)cm.getExecutionFactory()).setImmutable(false);
		
        
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
		
		new ConnectorWorkItem(requestMsg, cm);
    }

}
