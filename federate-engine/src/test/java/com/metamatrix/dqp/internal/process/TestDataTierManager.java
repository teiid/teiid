/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
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

package com.metamatrix.dqp.internal.process;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.data.monitor.AliveStatus;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.dqp.service.DataService;
import com.metamatrix.dqp.service.FakeBufferService;
import com.metamatrix.dqp.service.FakeVDBService;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.QueryProcessor;
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
    private QueryProcessor processor;
    private int executeRequestFailOnCall = 10000;
    private FakeDataService dataService;
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
        
        dataService = new FakeDataService(executeRequestFailOnCall);
//      dataService.addResults("SELECT StringKey, IntKey FROM BQT1.SmallA", helpCreateFakeCodeTableResults(1, 50)); //$NON-NLS-1$
        rm = new DQPCore();
        
        FakeVDBService vdbService = new FakeVDBService();
        vdbService.addBinding("MyVDB", "1", "BQT1", "mmuuid:binding", "bindingName"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
        
        FakeBufferService bs = new FakeBufferService();
        
        dtm = new DataTierManagerImpl(rm,
                                  dataService,
                                  new MockSingleMetadataService(metadata),
                                  vdbService,
                                  bs,
                                  1000,
                                  1000);
        command = helpGetCommand(sql, metadata);
        
        RequestMessage original = new RequestMessage();
        original.setCommand(command);
        original.setExecutionId(1);
        
        DQPWorkContext workContext = new DQPWorkContext();
        workContext.setVdbName("MyVDB"); //$NON-NLS-1$
        workContext.setVdbVersion("1"); //$NON-NLS-1$
        workContext.setSessionId(new MetaMatrixSessionID(1));
        RequestID requestID = workContext.getRequestID(original.getExecutionId());
        
        context = new CommandContext();
        context.setProcessorID(requestID);
        context.setTupleSourceID(new TupleSourceID("fakeid")); //$NON-NLS-1$
        context.setVdbName("test"); //$NON-NLS-1$
        context.setVdbVersion("1"); //$NON-NLS-1$
        context.setQueryProcessorFactory(new SimpleQueryProcessorFactory(bs.getBufferManager(), dtm, new DefaultCapabilitiesFinder(), null, metadata));
        processor = new QueryProcessor(new FakeProcessorPlan(), context, bs.getBufferManager(), dtm);
        workItem = TestDQPCoreRequestHandling.addRequest(rm, original, requestID, null, processor, null, null, null, workContext);
        
        request = new AtomicRequestMessage(original, workContext, nodeId);
        request.setCommand(command);
        request.setConnectorID(new ConnectorID("FakeConnectorID")); //$NON-NLS-1$

        info = new DataTierTupleSource(command.getProjectedSymbols(), request, dtm, request.getConnectorID(), workItem);
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

        AtomicResultsMessage results = new AtomicResultsMessage(request, new List[0], new String[0], new String[0]);
        
        if (isFirst) {
            results.setFirstRow(1);
        } else {
            results.setFirstRow(5);
        }
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
        
        assertNotNull(workItem.getConnectorRequest(results.getAtomicRequestId()));
        
        DataTierTupleSource connRequest = workItem.getConnectorRequest(results.getAtomicRequestId());
        
        connRequest.closeSource();
        
        assertNotNull(workItem.getConnectorRequest(results.getAtomicRequestId()));
        
        AtomicResultsMessage closeResult = new AtomicResultsMessage(request);
        closeResult.setRequestClosed(true);
        info.receiveResults(closeResult);
        
        assertNull(workItem.getConnectorRequest(closeResult.getAtomicRequestId()));        
    }

    public void testDeliverMessageNEWResponseNonFinal() throws Exception {
    	AtomicResultsMessage results = helpSetup(true, false);
        info.receiveResults(results);
        assertNotNull(workItem.getConnectorRequest(results.getAtomicRequestId()));
    }

    public void testDeliverMessageMOREResponseFinal_Defect15646() throws Exception {
    	AtomicResultsMessage results = helpSetup(false, true);
        info.receiveResults(results);
        
        assertNotNull(workItem.getConnectorRequest(results.getAtomicRequestId()));
        
        DataTierTupleSource connRequest = workItem.getConnectorRequest(results.getAtomicRequestId());
        
        connRequest.closeSource();
        
        assertNotNull(workItem.getConnectorRequest(results.getAtomicRequestId()));
        
        AtomicResultsMessage closeResult = new AtomicResultsMessage(request);
        closeResult.setRequestClosed(true);
        info.receiveResults(closeResult);
        
        assertNull(workItem.getConnectorRequest(closeResult.getAtomicRequestId()));                
    }
    
    public void testExplicitClose() throws Exception {
        String sql = "select ClobValue from LOB.LobTbl"; //$NON-NLS-1$
        
        AtomicResultsMessage results = helpSetup(sql, true, true);
        results.setSupportsImplicitClose(false);
        
        info.receiveResults(results);
        assertNotNull(workItem.getConnectorRequest(results.getAtomicRequestId()));
        
        DataTierTupleSource connRequest = workItem.getConnectorRequest(results.getAtomicRequestId());
        connRequest.closeSource();
        assertFalse(this.dataService.closed);
        
        assertNotNull(workItem.getConnectorRequest(results.getAtomicRequestId()));
        
        // now explicit close and see it gone        
        connRequest.fullyCloseSource();
        
        assertNotNull(workItem.getConnectorRequest(results.getAtomicRequestId()));
        
        AtomicResultsMessage closeResult = new AtomicResultsMessage(request);
        closeResult.setRequestClosed(true);
        info.receiveResults(closeResult);
        
        assertNull(workItem.getConnectorRequest(results.getAtomicRequestId()));        
    }    
    
    
    public void testImplictClose() throws Exception {
        String sql = "select ClobValue from LOB.LobTbl"; //$NON-NLS-1$
        
        AtomicResultsMessage results = helpSetup(sql, true, true);
        results.setSupportsImplicitClose(true);
        
        info.receiveResults(results);
        
        assertNotNull(workItem.getConnectorRequest(results.getAtomicRequestId()));

        DataTierTupleSource connRequest = workItem.getConnectorRequest(results.getAtomicRequestId());
        connRequest.closeSource();
        assertTrue(this.dataService.closed);
        
        assertNotNull(workItem.getConnectorRequest(results.getAtomicRequestId()));
        
        AtomicResultsMessage closeResult = new AtomicResultsMessage(request);
        closeResult.setRequestClosed(true);
        info.receiveResults(closeResult);

        assertNull(workItem.getConnectorRequest(closeResult.getAtomicRequestId()));
    } 
    
    public void testImplictCloseWithNotAllowedState() throws Exception {
        String sql = "select ClobValue from LOB.LobTbl"; //$NON-NLS-1$
        
        AtomicResultsMessage results = helpSetup(sql, true, true);
        results.setSupportsImplicitClose(false);
        
        info.receiveResults(results);
        assertFalse(this.dataService.closed);
        assertNotNull(workItem.getConnectorRequest(results.getAtomicRequestId()));
        
        DataTierTupleSource connRequest = workItem.getConnectorRequest(results.getAtomicRequestId());
        
        // now implicitly close the request then check to make sure it is not gone 
        connRequest.closeSource();
        assertFalse(this.dataService.closed);        
        assertNotNull(workItem.getConnectorRequest(results.getAtomicRequestId()));
    }     
    
    
    public void testDeliverMessageMOREResponseNonFinal() throws Exception {
    	AtomicResultsMessage results = helpSetup(false, false);
        info.receiveResults(results);
        
        assertNotNull(workItem.getConnectorRequest(results.getAtomicRequestId()));
    }
    
    public void testCodeTableResponseException() throws Exception {
    	helpSetup(3);
    	this.dataService.throwExceptionOnExecute = true;
        
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
        
        this.dataService.results = results;
        
        try {
            dtm.lookupCodeValue(context, "BQT1.SmallA", "IntKey", "StringKey", "49");  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            fail("processor should have failed"); //$NON-NLS-1$
        } catch (MetaMatrixException e) {
            assertEquals("Force fail on executeRequest for call # 1", e.getMessage()); //$NON-NLS-1$
        }
    }
    
    private static class FakeProcessorPlan implements ProcessorPlan {
        public Object clone() {return this;}
        public void close() throws MetaMatrixComponentException {}
        public List getAndClearWarnings() {return null;}
        public CommandContext getContext() {return null;}
        public List getOutputElements() {return null;}
        public void initialize(CommandContext context,ProcessorDataManager dataMgr,BufferManager bufferMgr) {}
        public TupleBatch nextBatch() throws BlockedException,MetaMatrixComponentException {
        	ArrayList one = new ArrayList(); one.add("1");
        	ArrayList two = new ArrayList(); two.add("2");
        	List[] results = {one, two};
        	return new TupleBatch(1, results);
        }
        public void open() throws MetaMatrixComponentException {}
        public void reset() {}
        public Map getDescriptionProperties() {return null;}
        public Collection getChildPlans() { return Collections.EMPTY_LIST; }
    }

    private static class FakeDataService implements DataService {
        private int failOnCall = 10000;
        private int calls = 0;
        private boolean closed = false;
        boolean throwExceptionOnExecute;
        AtomicResultsMessage results;
        
        private FakeDataService(int failOnCallNumber) {
            this.failOnCall = failOnCallNumber;
        }
		public void executeRequest(AtomicRequestMessage request,
				ConnectorID connector,
				ResultsReceiver<AtomicResultsMessage> resultListener)
				throws MetaMatrixComponentException {
            if (closed) {
                throw new MetaMatrixComponentException("Already closed"); //$NON-NLS-1$
            }
            if (throwExceptionOnExecute) {
            	resultListener.exceptionOccurred(new RuntimeException("Connector Exception")); //$NON-NLS-1$
            } else {
            	resultListener.receiveResults(results);
            }
        }
        public ConnectorID selectConnector(String connectorBindingID) {
            if (connectorBindingID.equals("mmuuid:binding")) { //$NON-NLS-1$
                return new ConnectorID("FakeConnectorID"); //$NON-NLS-1$
            }
            return null;
        }
        public void initialize(Properties props) throws ApplicationInitializationException {}
        public void start(ApplicationEnvironment environment) throws ApplicationLifecycleException {}
        public void stop() throws ApplicationLifecycleException {}
        public void startConnectorBinding(String connectorBindingName) throws ApplicationLifecycleException,ComponentNotFoundException {}
        public void stopConnectorBinding(String connectorBindingName) throws ApplicationLifecycleException,ComponentNotFoundException {}
        public List getConnectorBindings() throws ComponentNotFoundException {return null;}
        public AliveStatus getConnectorBindingState(String connectorBindingName) throws MetaMatrixComponentException {return null;}
        public String getConnectorBindingName(ConnectorID connector) {return null;}       
        public ConnectorBinding getConnectorBinding(String connectorBindingName) throws MetaMatrixComponentException {return null;}
        public Collection getConnectorBindingStatistics(String connectorBindingName) throws MetaMatrixComponentException {return null;}
        public void clearConnectorBindingCache(String connectorBindingName) throws MetaMatrixComponentException {}
        
		public void cancelRequest(AtomicRequestID request,
				ConnectorID connectorId) throws MetaMatrixComponentException {
		}
		public void closeRequest(AtomicRequestID request,
				ConnectorID connectorId) throws MetaMatrixComponentException {
			closed = true;
		}
		public SourceCapabilities getCapabilities(RequestMessage request,
				DQPWorkContext dqpWorkContext, ConnectorID connector)
				throws MetaMatrixComponentException {
			return null;
		}
		public void requestBatch(AtomicRequestID request,
				ConnectorID connectorId) throws MetaMatrixComponentException {
            calls++;
            if (calls == failOnCall) {
                throw new MetaMatrixComponentException("Force fail on executeRequest for call # " + calls); //$NON-NLS-1$
            }            
		}
    }
}
