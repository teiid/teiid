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

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.client.RequestMessage;
import org.teiid.core.TeiidException;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.impl.FakeTransactionService;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.dqp.message.RequestID;
import org.teiid.dqp.service.AutoGenDataService;
import org.teiid.dqp.service.FakeBufferService;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.dynamic.SimpleQueryProcessorFactory;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.FakeMetadataFactory;
import org.teiid.query.util.CommandContext;



public class TestDataTierManager extends TestCase {
    
    private DQPCore rm;
    private DataTierManagerImpl dtm;
    private CommandContext context;
    private AtomicRequestMessage request;
    private Command command;
    private DataTierTupleSource info;
    private AutoGenDataService connectorManager;
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
        
        connectorManager = new AutoGenDataService();
        rm = new DQPCore();
        rm.setTransactionService(new FakeTransactionService());
        
        FakeBufferService bs = new FakeBufferService();

        ConnectorManagerRepository repo = Mockito.mock(ConnectorManagerRepository.class);
        Mockito.stub(repo.getConnectorManager(Mockito.anyString())).toReturn(connectorManager);
        
        
        dtm = new DataTierManagerImpl(rm,
                                  repo,
                                  bs,
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
        context.setVdbVersion(1);
        context.setQueryProcessorFactory(new SimpleQueryProcessorFactory(bs.getBufferManager(), dtm, new DefaultCapabilitiesFinder(), null, metadata));
        workItem = TestDQPCoreRequestHandling.addRequest(rm, original, requestID, null, workContext);
        
        request = new AtomicRequestMessage(original, workContext, nodeId);
        request.setCommand(command);
        request.setConnectorName("FakeConnectorID"); //$NON-NLS-1$

        info = new DataTierTupleSource(command.getProjectedSymbols(), request, dtm, request.getConnectorName(), workItem);
    }
    
    public void testDataTierTupleSource() throws Exception {
    	helpSetup(1);
    	info.nextTuple();
        assertNotNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
        info.closeSource();
        assertNull(workItem.getConnectorRequest(request.getAtomicRequestID()));
    }
    
    public void testCodeTableResponseException() throws Exception {
    	helpSetup(3);
    	this.connectorManager.throwExceptionOnExecute = true;
        
        try {
            dtm.lookupCodeValue(context, "BQT1.SmallA", "IntKey", "StringKey", "49");  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            fail("processor should have failed"); //$NON-NLS-1$
        } catch (TeiidException e) {
            assertEquals("Connector Exception", e.getMessage()); //$NON-NLS-1$
        }
    }
    
    public void testNoRowsException() throws Exception {
    	helpSetup(3);
    	this.connectorManager.setRows(0);
    	assertNull(info.nextTuple());
    }
    
    public void testCodeTableResponseDataNotAvailable() throws Exception {
    	helpSetup(3);
    	this.connectorManager.dataNotAvailable = 5;
        
        assertNull(dtm.lookupCodeValue(context, "BQT1.SmallA", "IntKey", "StringKey", "49"));  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }
    
}
