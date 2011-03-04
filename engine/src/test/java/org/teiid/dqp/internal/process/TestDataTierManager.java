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

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.client.RequestMessage;
import org.teiid.common.buffer.BlockedException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.FakeTransactionService;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.dqp.message.RequestID;
import org.teiid.dqp.service.AutoGenDataService;
import org.teiid.dqp.service.FakeBufferService;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.FakeMetadataFactory;
import org.teiid.query.util.CommandContext;

public class TestDataTierManager {
    
    private DQPCore rm;
    private DataTierManagerImpl dtm;
    private CommandContext context;
    private AtomicRequestMessage request;
    private Command command;
    private DataTierTupleSource info;
    private AutoGenDataService connectorManager = new AutoGenDataService();
    private RequestWorkItem workItem;
    private int limit = -1;
    
    @Before public void setUp() {
    	limit = -1;
    	connectorManager = new AutoGenDataService();
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
        
        rm = new DQPCore();
        rm.setTransactionService(new FakeTransactionService());
        rm.setBufferService(new FakeBufferService());
        rm.setCacheFactory(new DefaultCacheFactory());
        rm.start(new DQPConfiguration());
        FakeBufferService bs = new FakeBufferService();

        ConnectorManagerRepository repo = Mockito.mock(ConnectorManagerRepository.class);
        Mockito.stub(repo.getConnectorManager(Mockito.anyString())).toReturn(connectorManager);
        
        
        dtm = new DataTierManagerImpl(rm,
                                  bs);
        command = helpGetCommand(sql, metadata);
        
        RequestMessage original = new RequestMessage();
        original.setExecutionId(1);
        original.setPartialResults(true);
        RequestID requestID = workContext.getRequestID(original.getExecutionId());
        
        context = new CommandContext();
        context.setProcessorID(requestID);
        context.setVdbName("test"); //$NON-NLS-1$
        context.setVdbVersion(1);
        context.setQueryProcessorFactory(new QueryProcessorFactoryImpl(bs.getBufferManager(), dtm, new DefaultCapabilitiesFinder(), null, metadata));
        workItem = TestDQPCoreRequestHandling.addRequest(rm, original, requestID, null, workContext);
        
        request = new AtomicRequestMessage(original, workContext, nodeId);
        request.setCommand(command);
        request.setConnectorName("FakeConnectorID"); //$NON-NLS-1$
        info = new DataTierTupleSource(request, workItem, connectorManager.registerRequest(request), dtm, limit);
    }
    
    @Test public void testDataTierTupleSource() throws Exception {
    	helpSetup(1);
    	for (int i = 0; i < 10;) {
	    	try {
	    		info.nextTuple();
	    		i++;
	    	} catch (BlockedException e) {
	    		Thread.sleep(50);
	    	}
    	}
        assertNotNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
        assertNull(info.nextTuple());
        info.closeSource();
        assertNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
    }
    
    @Test public void testDataTierTupleSourceLimit() throws Exception {
    	limit = 1;
    	helpSetup(1);
    	for (int i = 0; i < 1;) {
	    	try {
	    		info.nextTuple();
	    		i++;
	    	} catch (BlockedException e) {
	    		Thread.sleep(50);
	    	}
    	}
        assertNotNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
        assertNull(info.nextTuple());
        info.closeSource();
        assertNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
    }
    
    @Test public void testPartialResults() throws Exception {
    	helpSetup(1);
    	connectorManager.throwExceptionOnExecute = true;
    	for (int i = 0; i < 10; i++) {
	    	try {
	    		assertNull(info.nextTuple());
	    		return;
	    	} catch (BlockedException e) {
	    		Thread.sleep(50);
	    	}
    	}
    	fail();
    }
    
    @Test public void testNoRowsException() throws Exception {
    	this.connectorManager.setRows(0);
    	helpSetup(3);
    	while (true) {
	    	try {
	        	assertNull(info.nextTuple());
	    		break;
	    	} catch (BlockedException e) {
	    		Thread.sleep(50);
	    	}
    	}
    }
    
}
