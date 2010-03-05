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

package org.teiid.dqp.internal.process;

import java.util.List;

import javax.resource.spi.work.WorkManager;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.connector.api.ConnectorException;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManager;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.impl.FakeTransactionService;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.queue.FakeWorkManager;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.service.FakeBufferService;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.processor.dynamic.SimpleQueryProcessorFactory;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.util.CommandContext;


public class TestDataTierManager extends TestCase {
    
    private DQPCore rm;
    private DataTierManagerImpl dtm;
    private CommandContext context;
    private AtomicRequestMessage request;
    private Command command;
    private DataTierTupleSource info;
    private int executeRequestFailOnCall = 10000;
    private FakeConnectorManager connectorManager;
    private RequestWorkItem workItem;
    
    public TestDataTierManager(String name) {
        super(name);
    }
    
    private static Command helpGetCommand(String sql, QueryMetadataInterface metadata) throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand(sql);
        QueryResolver.resolveCommand(command, metadata);
        return command;
    }
    
    private void helpSetup(int nodeId) throws Exception {
        helpSetup("SELECT * FROM BQT1.SmallA", nodeId); //$NON-NLS-1$
    }
    
    private void helpSetup(String sql, int nodeId) throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleBQTCached();
        DQPWorkContext workContext = FakeMetadataFactory.buildWorkContext(metadata, FakeMetadataFactory.exampleBQTVDB());
        
        connectorManager = new FakeConnectorManager("FakeConnectorID", executeRequestFailOnCall);
        rm = new DQPCore();
        rm.setTransactionService(new FakeTransactionService());
        
        FakeBufferService bs = new FakeBufferService();

        ConnectorManagerRepository repo = Mockito.mock(ConnectorManagerRepository.class);
        Mockito.stub(repo.getConnectorManager(Mockito.anyString())).toReturn(connectorManager);
        
        
        dtm = new DataTierManagerImpl(rm,
                                  repo,
                                  bs,
                                  new FakeWorkManager(),
                                  20,
                                  1000,
                                  1000);
        command = helpGetCommand(sql, metadata);
        
        RequestMessage original = new RequestMessage();
        original.setExecutionId(1);
        
        RequestID requestID = workContext.getRequestID(original.getExecutionId());
        
        context = new CommandContext();
        context.setProcessorID(requestID);
        context.setVdbName("test"); //$NON-NLS-1$
        context.setVdbVersion(1); //$NON-NLS-1$
        context.setQueryProcessorFactory(new SimpleQueryProcessorFactory(bs.getBufferManager(), dtm, new DefaultCapabilitiesFinder(), null, metadata));
        workItem = TestDQPCoreRequestHandling.addRequest(rm, original, requestID, null, workContext);
        
        request = new AtomicRequestMessage(original, workContext, nodeId);
        request.setCommand(command);
        request.setConnectorName("FakeConnectorID"); //$NON-NLS-1$

        info = new DataTierTupleSource(command.getProjectedSymbols(), request, dtm, request.getConnectorName(), workItem);
        workItem.addConnectorRequest(request.getAtomicRequestID(), info);
    }
    
    private AtomicResultsMessage helpSetup(boolean isFirst, boolean isLast) throws Exception {
        return helpSetup("SELECT * FROM BQT1.SmallA", isFirst, isLast); //$NON-NLS-1$
    }

    private AtomicResultsMessage helpSetup(String sql, boolean isFirst, boolean isLast) throws Exception {
    	return helpSetup(sql, isFirst, isLast, 3);
    }
    
    private AtomicResultsMessage helpSetup(String sql, boolean isFirst, boolean isLast, int nodeId) throws Exception {
        helpSetup(sql, nodeId);        
        
        request.setCommand(command);

        AtomicResultsMessage results = new AtomicResultsMessage(request, new List[0], new String[0]);
        
        if (isLast) {
            results.setFinalRow(10);
        }
        return results;
    }
    
    /**
     * Defect 15646 - Ensure that when the final batch is received, the ConnectorRequestInfo object is removed from the RequestManager
     * @throws Exception
     * @since 4.2
     */
    public void testDeliverMessageNEWResponseFinal_Defect15646() throws Exception {
        AtomicResultsMessage results = helpSetup(true, true);
        info.receiveResults(results);
        
        assertNotNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
        
        DataTierTupleSource connRequest = workItem.getConnectorRequest(request.getAtomicRequestID());
        
        connRequest.closeSource();
        
        assertNotNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
        
        AtomicResultsMessage closeResult = new AtomicResultsMessage(request);
        closeResult.setRequestClosed(true);
        info.receiveResults(closeResult);
        
        assertNull(workItem.getConnectorRequest(request.getAtomicRequestID()));        
    }

    public void testDeliverMessageNEWResponseNonFinal() throws Exception {
    	AtomicResultsMessage results = helpSetup(true, false);
        info.receiveResults(results);
        assertNotNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
    }

    public void testDeliverMessageMOREResponseFinal_Defect15646() throws Exception {
    	AtomicResultsMessage results = helpSetup(false, true);
        info.receiveResults(results);
        
        assertNotNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
        
        DataTierTupleSource connRequest = workItem.getConnectorRequest(request.getAtomicRequestID());
        
        connRequest.closeSource();
        
        assertNotNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
        
        AtomicResultsMessage closeResult = new AtomicResultsMessage(request);
        closeResult.setRequestClosed(true);
        info.receiveResults(closeResult);
        
        assertNull(workItem.getConnectorRequest(request.getAtomicRequestID()));                
    }
    
    public void testExplicitClose() throws Exception {
        String sql = "select ClobValue from LOB.LobTbl"; //$NON-NLS-1$
        
        AtomicResultsMessage results = helpSetup(sql, true, true);
        results.setSupportsImplicitClose(false);
        
        info.receiveResults(results);
        assertNotNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
        
        DataTierTupleSource connRequest = workItem.getConnectorRequest(request.getAtomicRequestID());
        connRequest.closeSource();
        assertFalse(this.connectorManager.closed);
        
        assertNotNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
        
        // now explicit close and see it gone        
        connRequest.fullyCloseSource();
        
        assertNotNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
        
        AtomicResultsMessage closeResult = new AtomicResultsMessage(request);
        closeResult.setRequestClosed(true);
        info.receiveResults(closeResult);
        
        assertNull(workItem.getConnectorRequest(request.getAtomicRequestID()));        
    }    
    
    
    public void testImplictClose() throws Exception {
        String sql = "select ClobValue from LOB.LobTbl"; //$NON-NLS-1$
        
        AtomicResultsMessage results = helpSetup(sql, true, true);
        results.setSupportsImplicitClose(true);
        
        info.receiveResults(results);
        
        assertNotNull(workItem.getConnectorRequest(request.getAtomicRequestID()));

        DataTierTupleSource connRequest = workItem.getConnectorRequest(request.getAtomicRequestID());
        connRequest.closeSource();
        assertTrue(this.connectorManager.closed);
        
        assertNotNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
        
        AtomicResultsMessage closeResult = new AtomicResultsMessage(request);
        closeResult.setRequestClosed(true);
        info.receiveResults(closeResult);

        assertNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
    } 
    
    public void testImplictCloseWithNotAllowedState() throws Exception {
        String sql = "select ClobValue from LOB.LobTbl"; //$NON-NLS-1$
        
        AtomicResultsMessage results = helpSetup(sql, true, true);
        results.setSupportsImplicitClose(false);
        
        info.receiveResults(results);
        assertFalse(this.connectorManager.closed);
        assertNotNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
        
        DataTierTupleSource connRequest = workItem.getConnectorRequest(request.getAtomicRequestID());
        
        // now implicitly close the request then check to make sure it is not gone 
        connRequest.closeSource();
        assertFalse(this.connectorManager.closed);        
        assertNotNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
    }     
    
    
    public void testDeliverMessageMOREResponseNonFinal() throws Exception {
    	AtomicResultsMessage results = helpSetup(false, false);
        info.receiveResults(results);
        
        assertNotNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
    }
    
    public void testCodeTableResponseException() throws Exception {
    	helpSetup(3);
    	this.connectorManager.throwExceptionOnExecute = true;
        
        try {
            dtm.lookupCodeValue(context, "BQT1.SmallA", "IntKey", "StringKey", "49");  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            fail("processor should have failed"); //$NON-NLS-1$
        } catch (MetaMatrixException e) {
            assertEquals("Connector Exception", e.getMessage()); //$NON-NLS-1$
        }
    }
    
    public void testCodeTableResponse_MoreRequestFails() throws Exception {
        executeRequestFailOnCall = 1;
        
        AtomicResultsMessage results = helpSetup("SELECT * FROM BQT1.SmallA", true, false, -1); //$NON-NLS-1$
        
        this.connectorManager.results = results;
        
        try {
            dtm.lookupCodeValue(context, "BQT1.SmallA", "IntKey", "StringKey", "49");  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            fail("processor should have failed"); //$NON-NLS-1$
        } catch (MetaMatrixException e) {
            assertEquals("Force fail on executeRequest for call # 1", e.getMessage()); //$NON-NLS-1$
        }
    }
    
    private static class FakeConnectorManager extends ConnectorManager {
        private int failOnCall = 10000;
        private int calls = 0;
        private boolean closed = false;
        boolean throwExceptionOnExecute;
        AtomicResultsMessage results;
        
        private FakeConnectorManager(String name, int failOnCallNumber) {
        	super(name);
            this.failOnCall = failOnCallNumber;
        }
        @Override
		public void executeRequest(WorkManager workManager, ResultsReceiver<AtomicResultsMessage> resultListener, AtomicRequestMessage request)
				throws ConnectorException {
            if (closed) {
                throw new ConnectorException("Already closed"); //$NON-NLS-1$
            }
            if (throwExceptionOnExecute) {
            	resultListener.exceptionOccurred(new RuntimeException("Connector Exception")); //$NON-NLS-1$
            } else {
            	resultListener.receiveResults(results);
            }
        }

		@Override
		public void closeRequest(AtomicRequestID request) {
			closed = true;
		}
		
		@Override
		public SourceCapabilities getCapabilities() throws ConnectorException{
			return null;
		}
		@Override
		public void requstMore(AtomicRequestID requestId) throws ConnectorException {
            calls++;
            if (calls == failOnCall) {
                throw new ConnectorException("Force fail on executeRequest for call # " + calls); //$NON-NLS-1$
            }            
		}
    }
}
